import os
import sys
import time
import logging
import json
import threading
import numpy as np

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

src_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.normpath(os.path.join(src_dir, "../.."))
local_hadoop_home = os.path.join(project_root, "hadoop")
local_winutils = os.path.join(local_hadoop_home, "bin", "winutils.exe")
if os.path.exists(local_winutils):
    os.environ.setdefault("HADOOP_HOME", local_hadoop_home)
    os.environ.setdefault("hadoop.home.dir", local_hadoop_home)
    hadoop_bin = os.path.join(local_hadoop_home, "bin")
    if hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

build_dirs = [
    os.path.normpath(os.path.join(src_dir, '../../build')),
    os.path.normpath(os.path.join(src_dir, '../../build-py311-v2')),
    os.path.normpath(os.path.join(src_dir, '../../build-msvc-nmake')),
    os.path.normpath(os.path.join(src_dir, '../../build-msvc/Debug')),
    os.path.normpath(os.path.join(src_dir, '../../build-msvc/Release')),
]

if src_dir not in sys.path:
    sys.path.insert(0, src_dir)
for build_dir in build_dirs:
    if build_dir not in sys.path:
        sys.path.insert(0, build_dir)

os.environ['PYTHONPATH'] = os.pathsep.join([src_dir] + build_dirs + [os.environ.get('PYTHONPATH', '')])
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

try:
    import lob_core
except ImportError as e:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("StreamingEngine")
    logger.critical("Không tìm thấy thư viện C++. Hãy chạy CMake trước. Chi tiết: %s", e)
    sys.exit(1)

from ml_filter import load_ml_model, ml_predict_suspicion
from spark_engine import analyze_scenarios_distributed, shapley_spark
from shapley_analyzer import detect_wash_trading_communities

try:
    from redis_manager import RedisManager
    redis_available = True
    rm = RedisManager()
except Exception as e:
    redis_available = False
    rm = None

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("StreamingEngine")

STREAM_DIR = "/tmp/binance_stream"
os.makedirs(STREAM_DIR, exist_ok=True)

VOLUME_SCALE = 100

global_lob = lob_core.LOB()
global_lob.set_market_order_volume(100)
_batch_lock = threading.Lock()

ml_model = None

_id_map = {}
_id_counter = 1
_id_lock = threading.Lock()

def get_price_level_id(side_prefix, price):
    global _id_counter
    key = (side_prefix, f"{price:.8f}".rstrip('0').rstrip('.'))
    with _id_lock:
        if key not in _id_map:
            _id_map[key] = _id_counter
            _id_counter += 1
        return _id_map[key], price

def _find_suspect_order(lob_state):
    """Dynamically detect the most suspicious order: largest bid volume near best bid."""
    all_orders = lob_state.get_all_orders()
    best_bid = lob_state.get_best_bid()
    if best_bid <= 0:
        return 0
    bid_orders = [o for o in all_orders if o.side == lob_core.Side.BID and o.price >= best_bid * 0.99]
    if not bid_orders:
        return 0
    suspect = max(bid_orders, key=lambda o: o.volume)
    return suspect.id


def process_micro_batch(df, epoch_id):
    """
    Process each Spark micro-batch: update C++ LOB, run ML filter,
    if suspicious, run C++ Alpha-Beta parallel analysis.
    """
    with _batch_lock:
        rows = df.collect()
        if not rows:
            return

        logger.info(f"--- Bắt đầu xử lý Batch ID: {epoch_id} ({len(rows)} events) ---")

        orders_before = set(o.id for o in global_lob.get_all_orders())

        for row in rows:
            timestamp = row["E"] or 0

            if row["b"] is not None:
                for bid in row["b"]:
                    price = float(bid[0])
                    raw_vol_str = bid[1]
                    try:
                        vol = int(round(float(raw_vol_str) * VOLUME_SCALE))
                    except (ValueError, TypeError):
                        logger.warning(f"Không thể parse volume '{raw_vol_str}', bỏ qua")
                        continue

                    order_id, _ = get_price_level_id(1, price)
                    global_lob.cancel_order(order_id)
                    if vol > 0:
                        global_lob.add_order(order_id, lob_core.Side.BID, price, vol, timestamp)

            if row["a"] is not None:
                for ask in row["a"]:
                    price = float(ask[0])
                    raw_vol_str = ask[1]
                    try:
                        vol = int(round(float(raw_vol_str) * VOLUME_SCALE))
                    except (ValueError, TypeError):
                        logger.warning(f"Không thể parse volume '{raw_vol_str}', bỏ qua")
                        continue

                    order_id, _ = get_price_level_id(2, price)
                    global_lob.cancel_order(order_id)
                    if vol > 0:
                        global_lob.add_order(order_id, lob_core.Side.ASK, price, vol, timestamp)

        orders_after = set(o.id for o in global_lob.get_all_orders())
        genuine_cancels = len(orders_before - orders_after)
        genuine_new = len(orders_after - orders_before)
        persistent = len(orders_before & orders_after)
        global_lob.set_total_orders_added(persistent + genuine_new)
        global_lob.set_total_orders_cancelled(genuine_cancels)

        best_bid = global_lob.get_best_bid()
        best_ask = global_lob.get_best_ask()
        spread = global_lob.get_spread()
        spread_str = f"{spread:.2f}" if not np.isinf(spread) else "INF"
        if spread < 0:
            spread_str = f"CROSSED({spread:.2f})"
        logger.info(f"LOB Snapshot -> Best Bid: {best_bid:.2f} | Best Ask: {best_ask:.2f} | Spread: {spread_str}")

        if global_lob.is_empty() or best_bid == 0 or best_ask == 0:
            logger.info("Sổ lệnh chưa đủ dữ liệu. Bỏ qua.")
            return

        try:
            active_model = ml_model or load_ml_model(spark=df.sparkSession)
            prob_spoofing = ml_predict_suspicion(active_model, global_lob, df.sparkSession)
        except Exception as e:
            logger.error(f"ML inference lỗi: {e}. Bỏ qua batch.")
            return

        logger.info(f"Xác suất thao túng (ML): {prob_spoofing * 100:.2f}%")

        ML_THRESHOLD = 0.6
        if prob_spoofing > ML_THRESHOLD:
            logger.warning("CẢNH BÁO: ML Filter phát hiện bất thường. Chuyển cho Layer 2 (C++)...")

            suspect_id = _find_suspect_order(global_lob)
            if suspect_id != 0:
                global_lob.set_suspect_order_id(suspect_id)

            try:
                num_scenarios = 200
                spark_session = df.sparkSession
                results_df, summary = analyze_scenarios_distributed(spark_session, global_lob, num_scenarios)
                avg_risk = summary['avg_risk']
                max_risk = summary['max_risk']
                elapsed = summary['elapsed_seconds']

                logger.warning(
                    f"Spark Distributed Alpha-Beta xong trong {elapsed:.3f}s. "
                    f"Avg Risk: {avg_risk:.2f}, Max Risk: {max_risk:.2f}"
                )

                if avg_risk > 30.0:
                    logger.critical("RED FLAG: Phát hiện Spoofing nguy hiểm dựa trên Game Theory!")

                    if redis_available and rm:
                        try:
                            rm.publish_alert({
                                "type": "SPOOFING",
                                "severity": "RED_FLAG",
                                "avg_risk": round(avg_risk, 2),
                                "max_risk": round(max_risk, 2),
                                "prob_spoofing": round(prob_spoofing, 4),
                                "best_bid": best_bid,
                                "best_ask": best_ask,
                                "spread": spread,
                                "epoch_id": epoch_id,
                                "timestamp": time.time()
                            })
                            rm.store_detection_result({
                                "type": "SPOOFING",
                                "avg_risk": round(avg_risk, 2),
                                "max_risk": round(max_risk, 2),
                                "epoch_id": epoch_id
                            })
                            rm.cache_lob_snapshot(global_lob)
                        except Exception as e:
                            logger.warning(f"Redis alert storage failed: {e}")

                    logger.warning("Kích hoạt Layer 4 (Wash Trading & Shapley Ringleader detection)...")
                    try:
                        mock_transactions = [
                            ("ACC_1", "ACC_2", 100),
                            ("ACC_2", "ACC_RINGLEADER", 150),
                            ("ACC_RINGLEADER", "ACC_1", 200),
                            ("ACC_3", "ACC_RINGLEADER", 300),
                            ("ACC_RINGLEADER", "ACC_3", 250),
                            ("USER_A", "USER_B", 50),
                            ("USER_B", "USER_C", 30)
                        ]
                        communities = detect_wash_trading_communities(mock_transactions)
                        if communities:
                            suspect_community = communities[0]
                            logger.warning(f"Cụm giao dịch chéo nghi vấn: {suspect_community}")
                            
                            if redis_available and rm:
                                rm.store_detection_result({
                                    "type": "WASH_TRADING",
                                    "community": suspect_community,
                                    "epoch_id": epoch_id,
                                    "timestamp": time.time()
                                })
                                
                            shapley_df = shapley_spark(spark_session, set(suspect_community), mock_transactions, num_permutations=1000)
                            logger.warning("Kết quả Shapley Value (Spark DataFrame):")
                            shapley_df.show(truncate=False)
                            
                            shapley_rows = shapley_df.collect()
                            if shapley_rows:
                                ringleader = max(shapley_rows, key=lambda r: r["shapley_value"])["account"]
                                logger.critical(f"RINGLEADER được xác định: {ringleader}")
                                if redis_available and rm:
                                    rm.store_detection_result({
                                        "type": "RINGLEADER",
                                        "ringleader": ringleader,
                                        "avg_risk": avg_risk,
                                        "epoch_id": epoch_id,
                                        "timestamp": time.time()
                                    })
                        else:
                            logger.info("Không phát hiện cụm giao dịch chéo.")
                    except Exception as e:
                        logger.error(f"Lỗi phân tích Layer 4 (Wash Trading / Shapley): {e}")

            except Exception as e:
                logger.error(f"Spark Distributed Alpha-Beta analysis failed: {e}")
        else:
            logger.info("Thị trường bình thường.")

        logger.info("--- Batch xử lý hoàn tất ---")


def main():
    global ml_model

    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType
    from pyspark.sql.functions import col, from_json

    redis_host = os.environ.get('REDIS_HOST', 'localhost')
    redis_port = os.environ.get('REDIS_PORT', '6379')
    redis_stream = os.environ.get('REDIS_STREAM', 'binance:depth')

    if redis_available and rm:
        try:
            client = rm._client
            try:
                client.xgroup_create(redis_stream, "spark-source", id="0", mkstream=True)
                logger.info(f"Đã tự động tạo consumer group 'spark-source' cho stream '{redis_stream}' trên Redis.")
            except Exception as e:
                if "BUSYGROUP" in str(e):
                    logger.info(f"Consumer group 'spark-source' đã sẵn sàng cho stream '{redis_stream}'.")
                else:
                    logger.warning(f"Cảnh báo cấu hình XGROUP: {e}")
        except Exception as e:
            logger.warning(f"Không thể kết nối tới Redis để tạo consumer group: {e}")

    logger.info("Khởi động Spark Structured Streaming Engine (Redis Streams)...")
    spark = SparkSession.builder \
        .appName("BinanceLOBStreaming") \
        .master("local[*]") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.caseSensitive", "true") \
        .config("spark.jars.packages", "com.redislabs:spark-redis_2.12:3.1.0") \
        .config("spark.redis.host", redis_host) \
        .config("spark.redis.port", redis_port) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    ml_model = load_ml_model(spark=spark, prefer_spark=True)
    logger.info("ML filter model loaded: %s", type(ml_model).__name__)

    schema = StructType([
        StructField("e", StringType(), True),
        StructField("E", LongType(), True),
        StructField("b", ArrayType(ArrayType(StringType())), True),
        StructField("a", ArrayType(ArrayType(StringType())), True)
    ])

    redis_stream_schema = StructType([
        StructField("data", StringType(), True)
    ])

    redis_stream_df = spark.readStream \
        .format("redis") \
        .option("stream.keys", redis_stream) \
        .option("stream.read.batch.size", "100") \
        .schema(redis_stream_schema) \
        .load()

    streaming_df = redis_stream_df.select(
        from_json(col("data"), schema).alias("parsed_data")
    ).select("parsed_data.*")

    query = streaming_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_micro_batch) \
        .trigger(processingTime="5 seconds") \
        .option("checkpointLocation", "/tmp/binance_redis_checkpoint") \
        .start()

    logger.info(f"Listening on Redis Stream '{redis_stream}'. Run binance_ws.py separately.")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Đang tắt hệ thống...")
        query.stop()


if __name__ == "__main__":
    main()
