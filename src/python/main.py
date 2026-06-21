import sys
import os
import time
import logging

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

src_dir = os.path.dirname(os.path.abspath(__file__))
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
    logger = logging.getLogger("MarketManipulationDetector")
    logger.critical("Không tìm thấy thư viện C++. Hãy chạy CMake trước. Chi tiết: %s", e)
    sys.exit(1)

from pyspark.sql import SparkSession
from spark_engine import analyze_scenarios_distributed, shapley_spark
from shapley_analyzer import detect_wash_trading_communities
from ml_filter import extract_features, train_mock_ml_model, ml_predict_suspicion
from output_recorder import OutputRecorder

try:
    from redis_manager import RedisManager
    redis_available = True
except ImportError:
    redis_available = False


def create_spoofed_lob_state():
    """
    Tạo một trạng thái Sổ lệnh bị Spoofing cực mạnh:
    - Bids (Mua): Có lệnh cực lớn 50,000 Volume (Spoofing) sát giá Best Ask
    - Asks (Bán): Khối lượng thưa thớt
    """
    state = lob_core.LOB()
    state.add_order(1, lob_core.Side.BID, 100.0, 50, 1000)
    state.add_order(2, lob_core.Side.BID, 99.75, 100, 1001)
    state.add_order(99999, lob_core.Side.BID, 100.0, 50000, 1004)
    state.add_order(3, lob_core.Side.ASK, 100.25, 10, 1002)
    state.add_order(4, lob_core.Side.ASK, 100.50, 20, 1003)

    state.set_suspect_order_id(99999)
    state.set_market_order_volume(1000)
    state.set_total_orders_added(100)
    state.set_total_orders_cancelled(90)

    return state


def lob_snapshot(lob_state):
    orders = []
    for order in lob_state.get_all_orders():
        side = getattr(order.side, "name", str(order.side))
        orders.append({
            "id": order.id,
            "price": order.price,
            "volume": order.volume,
            "side": side,
            "timestamp": order.timestamp
        })

    return {
        "best_bid": lob_state.get_best_bid(),
        "best_ask": lob_state.get_best_ask(),
        "spread": lob_state.get_spread(),
        "cancellation_rate": lob_state.get_cancellation_rate(),
        "heuristic_score": lob_state.evaluate_state(),
        "suspect_order_id": lob_state.get_suspect_order_id(),
        "market_order_volume": lob_state.get_market_order_volume(),
        "orders": orders
    }


def main():
    print("=" * 80)
    print("  HỆ THỐNG PHÁT HIỆN THAO TÚNG THỊ TRƯỜNG — SPARK + C++ + MACHINE LEARNING")
    print("=" * 80)

    recorder = OutputRecorder()
    print(f"  Output artifacts: {recorder.run_dir}")

    rm = None
    if redis_available:
        try:
            rm = RedisManager()
        except Exception as e:
            print(f"Không thể khởi tạo RedisManager: {e}")
    if redis_available and rm:
        rm.report_health("main_pipeline", "starting", {"mode": "static"})
        rm.report_health("spark", "starting")
        rm.report_health("ml_filter", "starting")

    print("\n[0] Khởi tạo Apache Spark Session...")
    spark = SparkSession.builder \
        .appName("MarketManipulationDetector") \
        .master("local[*]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    num_cores = spark.sparkContext.defaultParallelism
    print(f"  Spark đã sẵn sàng. Chế độ: local[*] ({num_cores} cores)")

    if redis_available and rm:
        rm.report_health("spark", "running", {"cores": num_cores})

    try:
        print("\n[1] Tạo trạng thái sổ lệnh (C++ LOB với Red-Black Tree)")
        initial_state = create_spoofed_lob_state()
        print(f"  Best Bid: {initial_state.get_best_bid()} | Best Ask: {initial_state.get_best_ask()}")
        print(f"  Spread: {initial_state.get_spread()}")
        print(f"  Heuristic Score: {initial_state.evaluate_state():.2f}")
        recorder.save_json("01_initial_lob_snapshot.json", lob_snapshot(initial_state))

        if redis_available:
            rm.cache_lob_snapshot(initial_state)

        print("\n[Layer 1] Machine Learning Fast Filter (Random Forest)...")
        ml_model = train_mock_ml_model()

        if redis_available:
            rm.report_health("ml_filter", "ready", {"model": "RandomForest"})

        t0 = time.time()
        ml_features = extract_features(initial_state)
        prob_spoofing = ml_predict_suspicion(ml_model, initial_state)
        t1 = time.time()

        print(f"  Thời gian ML Inference: {(t1 - t0)*1000:.3f} ms")
        print(f"  Xác suất thao túng: {prob_spoofing*100:.1f}%")

        recorder.save_json("02_ml_filter_result.json", {
            "features": {
                "spread": ml_features[0],
                "bid_vol": ml_features[1],
                "ask_vol": ml_features[2],
                "imbalance": ml_features[3],
                "cancel_rate": ml_features[4]
            },
            "prob_spoofing": prob_spoofing,
            "inference_ms": round((t1 - t0) * 1000, 3)
        })

        ML_THRESHOLD = 0.6
        if prob_spoofing < ML_THRESHOLD:
            print("\n  [OK] ML Filter: Thị trường bình thường. Dừng phân tích.")
            recorder.save_json("10_final_result.json", {
                "status": "NORMAL",
                "reason": "ML probability below threshold",
                "prob_spoofing": prob_spoofing,
                "ml_threshold": ML_THRESHOLD
            })
            if redis_available:
                rm.store_detection_result({
                    "type": "NORMAL",
                    "prob_spoofing": prob_spoofing,
                    "timestamp": time.time()
                })
                rm.report_health("main_pipeline", "completed", {"result": "normal"})
            return

        print(f"\n  [!] Rủi ro cao (>{ML_THRESHOLD*100:.0f}%). Kích hoạt Spark phân tích chuyên sâu...")

        num_scenarios = 200
        print(f"\n[Layer 2 & 3] Spark RDD: Phân tán {num_scenarios} kịch bản Alpha-Beta trên {num_cores} cores...")

        results_df, summary = analyze_scenarios_distributed(spark, initial_state, num_scenarios)
        scenario_rows = [row.asDict() for row in results_df.collect()]
        recorder.save_csv("03_scenario_results.csv", scenario_rows)
        recorder.save_json("04_spark_summary.json", summary)

        print(f"  Thời gian Spark xử lý: {summary['elapsed_seconds']:.4f} giây")
        print(f"\n  ┌─────────── Spark SQL: Thống kê rủi ro ───────────┐")
        print(f"  │  Tổng kịch bản phân tích : {summary['total_scenarios']:>10}           │")
        print(f"  │  Rủi ro Trung bình (AVG) : {summary['avg_risk']:>10.2f}           │")
        print(f"  │  Rủi ro Cực đại   (MAX)  : {summary['max_risk']:>10.2f}           │")
        print(f"  │  Rủi ro Cực tiểu  (MIN)  : {summary['min_risk']:>10.2f}           │")
        print(f"  │  Độ lệch chuẩn  (STDDEV) : {summary['stddev_risk']:>10.2f}           │")
        print(f"  │  Phân vị 95%     (P95)   : {summary['p95_risk']:>10.2f}           │")
        print(f"  └─────────────────────────────────────────────────┘")

        print("\n  Top 5 kịch bản rủi ro cao nhất (Spark SQL):")
        top5_df = spark.sql("""
            SELECT scenario_id,
                   ROUND(risk_score, 2) AS risk_score,
                   market_volume,
                   noise_orders
            FROM scenario_results
            ORDER BY risk_score DESC
            LIMIT 5
        """)
        top5_df.show(truncate=False)
        top5_rows = [row.asDict() for row in top5_df.collect()]
        recorder.save_json("05_top5_scenarios.json", top5_rows)
        recorder.save_csv("05_top5_scenarios.csv", top5_rows)

        if redis_available:
            rm.store_detection_result({
                "type": "SPOOFING_ANALYSIS",
                "avg_risk": summary['avg_risk'],
                "max_risk": summary['max_risk'],
                "prob_spoofing": prob_spoofing,
                "timestamp": time.time()
            })

        RED_FLAG_THRESHOLD = 30.0
        avg_risk = summary['avg_risk']

        if avg_risk > RED_FLAG_THRESHOLD:
            print("[!] ═══ CẢNH BÁO MÀU ĐỎ (RED FLAG) — PHÁT HIỆN SPOOFING NGHIÊM TRỌNG ═══")

            if redis_available:
                rm.publish_alert({
                    "type": "SPOOFING",
                    "severity": "RED_FLAG",
                    "avg_risk": avg_risk,
                    "max_risk": summary['max_risk'],
                    "prob_spoofing": prob_spoofing,
                    "timestamp": time.time()
                })

            print("\n[Layer 4] Spark RDD: Phân tán Graph Analytics + Shapley Value...")

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

            if not communities:
                print("  Không phát hiện cụm giao dịch chéo nào.")
                recorder.save_json("10_final_result.json", {
                    "status": "SPOOFING",
                    "severity": "RED_FLAG",
                    "avg_risk": avg_risk,
                    "max_risk": summary["max_risk"],
                    "prob_spoofing": prob_spoofing,
                    "wash_trading": False,
                    "ringleader": None
                })
                return

            suspect_community = communities[0]
            print(f"  Cụm nghi vấn chính: {suspect_community}")
            recorder.save_json("06_wash_trading_community.json", {
                "communities": communities,
                "suspect_community": suspect_community,
                "transactions": mock_transactions
            })

            if redis_available:
                rm.store_detection_result({
                    "type": "WASH_TRADING",
                    "community": suspect_community,
                    "timestamp": time.time()
                })

            print(f"\n  Spark RDD: Phân tán 3000 hoán vị Monte-Carlo Shapley trên {num_cores} cores...")
            shapley_df = shapley_spark(spark, set(suspect_community), mock_transactions, num_permutations=3000)

            print("\n  Kết quả Shapley Value (Spark DataFrame):")
            shapley_df.show(truncate=False)
            shapley_rows = [row.asDict() for row in shapley_df.collect()]
            recorder.save_json("07_shapley_values.json", shapley_rows)
            recorder.save_csv("07_shapley_values.csv", shapley_rows)

            ringleader = shapley_rows[0]["account"] if shapley_rows else "N/A"
            print(f"  => KẾT LUẬN: Tài khoản '{ringleader}' là Trùm cuối (Ringleader).")
            recorder.save_json("10_final_result.json", {
                "status": "SPOOFING_AND_WASH_TRADING",
                "severity": "CRITICAL",
                "ringleader": ringleader,
                "community": suspect_community,
                "avg_risk": avg_risk,
                "max_risk": summary["max_risk"],
                "prob_spoofing": prob_spoofing
            })

            if redis_available:
                rm.publish_alert({
                    "type": "RINGLEADER",
                    "severity": "CRITICAL",
                    "ringleader": ringleader,
                    "community": suspect_community,
                    "avg_risk": avg_risk,
                    "timestamp": time.time()
                })
                rm.store_detection_result({
                    "type": "RINGLEADER",
                    "ringleader": ringleader,
                    "avg_risk": avg_risk,
                    "timestamp": time.time()
                })

        else:
            print("\n  Thị trường hoạt động bình thường, không có dấu hiệu thao túng.")
            recorder.save_json("10_final_result.json", {
                "status": "NO_RED_FLAG",
                "reason": "Average risk below threshold",
                "avg_risk": avg_risk,
                "red_flag_threshold": RED_FLAG_THRESHOLD,
                "prob_spoofing": prob_spoofing
            })

    finally:
        print("\n[END] Đang tắt Spark Session...")
        if redis_available and rm:
            try:
                rm.report_health("main_pipeline", "stopped")
                rm.report_health("spark", "stopped")
            except Exception as e:
                print(f"Lỗi khi gửi báo cáo health cho Redis: {e}")
        if 'spark' in locals() and spark:
            spark.stop()
        print("  Spark đã tắt an toàn.")


if __name__ == "__main__":
    main()
