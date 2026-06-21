import numpy as np
from sklearn.ensemble import RandomForestClassifier
import os
import random
import logging
import lob_core
import joblib

logger = logging.getLogger(__name__)

VOLUME_SCALE = 100
SRC_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.normpath(os.path.join(SRC_DIR, "../.."))
FEATURE_COLUMNS = ["spread", "bid_vol", "ask_vol", "imbalance", "cancel_rate"]
SPARK_MODEL_DIR = os.path.join(PROJECT_ROOT, "models", "spark_ml_rf")
SKLEARN_MODEL_PATH = os.path.join(PROJECT_ROOT, "models", "spoofing_rf_model.pkl")


def configure_local_hadoop():
    local_hadoop_home = os.path.join(PROJECT_ROOT, "hadoop")
    local_winutils = os.path.join(local_hadoop_home, "bin", "winutils.exe")
    if not os.path.exists(local_winutils):
        return

    os.environ.setdefault("HADOOP_HOME", local_hadoop_home)
    os.environ.setdefault("hadoop.home.dir", local_hadoop_home)
    hadoop_bin = os.path.join(local_hadoop_home, "bin")
    if hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")


configure_local_hadoop()


def extract_features(lob_state):
    """
    Trích xuất các đặc trưng tĩnh (static features) từ cấu trúc LOB
    để nạp vào mô hình Machine Learning.
    Đầu ra: [Spread, Bid Volume, Ask Volume, Imbalance, Cancellation Rate]
    """
    spread = lob_state.get_spread()
    if np.isinf(spread):
        spread = 10000.0

    all_orders = lob_state.get_all_orders()
    
    bid_levels = {}
    ask_levels = {}
    for o in all_orders:
        if o.side == lob_core.Side.BID:
            bid_levels[o.price] = bid_levels.get(o.price, 0) + o.volume
        else:
            ask_levels[o.price] = ask_levels.get(o.price, 0) + o.volume
            
    bid_vol = max(bid_levels.values()) if bid_levels else 0
    ask_vol = max(ask_levels.values()) if ask_levels else 0

    if bid_vol == 0:
        best_bid = lob_state.get_best_bid()
        bid_vol = lob_state.get_bid_volume(best_bid) if best_bid > 0 else 0
    if ask_vol == 0:
        best_ask = lob_state.get_best_ask()
        ask_vol = lob_state.get_ask_volume(best_ask) if best_ask > 0 else 0

    total_vol = bid_vol + ask_vol
    imbalance = (bid_vol - ask_vol) / total_vol if total_vol > 0 else 0.0

    cancel_rate = lob_state.get_cancellation_rate()

    return [spread, bid_vol, ask_vol, abs(imbalance), cancel_rate]


def _generate_mock_data():
    rng = random.Random(42)
    X_train = []
    y_train = []

    for _ in range(2000):
        spread = rng.uniform(0.01, 2.0)
        bid_vol = rng.uniform(1, 500)
        ask_vol = rng.uniform(1, 500)
        imbalance = rng.uniform(0.0, 0.3)
        cancel_rate = rng.uniform(0.0, 0.1)
        X_train.append([spread, bid_vol, ask_vol, imbalance, cancel_rate])
        y_train.append(0)

    for _ in range(500):
        spread = rng.uniform(0.01, 15.0)
        bid_vol = rng.uniform(5000, 50000)
        ask_vol = rng.uniform(1, 100)
        imbalance = rng.uniform(0.7, 1.0)
        cancel_rate = rng.uniform(0.0, 0.8)
        X_train.append([spread, bid_vol, ask_vol, imbalance, cancel_rate])
        y_train.append(1)
        
    return X_train, y_train


def train_mock_ml_model():
    """
    Huấn luyện mô hình ML (Random Forest) với dữ liệu từ file CSV nếu có,
    hoặc tự phát sinh dữ liệu giả lập để huấn luyện.
    """
    import os
    import csv
    
    candidate_csv_paths = [
        os.path.join(PROJECT_ROOT, "data", "ml", "synthetic_lob_data.csv"),
        os.path.join(PROJECT_ROOT, "data", "synthetic_lob_data.csv"),
    ]
    csv_path = next((p for p in candidate_csv_paths if os.path.exists(p)), candidate_csv_paths[0])
    
    X_train = []
    y_train = []
    
    if os.path.exists(csv_path):
        logger.info(f"[ML Filter] Đang nạp dữ liệu huấn luyện từ {csv_path}...")
        try:
            with open(csv_path, mode='r', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader)
                for row in reader:
                    if len(row) >= 6:
                        X_train.append([float(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4])])
                        y_train.append(int(row[5]))
            logger.info(f"[ML Filter] Nạp thành công {len(X_train)} mẫu dữ liệu từ file.")
        except Exception as e:
            logger.warning(f"[ML Filter] Lỗi nạp CSV: {e}. Chuyển sang sinh dữ liệu giả lập.")
            X_train, y_train = _generate_mock_data()
    else:
        logger.info("[ML Filter] Không thấy file CSV, tiến hành phát sinh dữ liệu trong RAM...")
        X_train, y_train = _generate_mock_data()
        
    logger.info("[ML Filter] Đang huấn luyện mô hình Light-weight ML Classifier...")
    model = RandomForestClassifier(n_estimators=50, max_depth=5, random_state=42)
    model.fit(X_train, y_train)
    logger.info("[ML Filter] Huấn luyện hoàn tất. Sẵn sàng lọc real-time.")
    return model


def _is_spark_ml_model(model):
    try:
        from pyspark.ml.classification import RandomForestClassificationModel
        return isinstance(model, RandomForestClassificationModel)
    except Exception:
        return False


def _get_spark_session(spark=None):
    if spark is not None:
        return spark
    try:
        from pyspark.sql import SparkSession
        return SparkSession.getActiveSession()
    except Exception:
        return None


def ml_predict_suspicion(model, lob_state, spark=None):
    features = extract_features(lob_state)
    logger.info(
        f"[Feature Extraction] spread={features[0]:.4f}, "
        f"bid_vol={features[1]:.2f}, ask_vol={features[2]:.2f}, "
        f"imbalance={features[3]:.4f}, cancel_rate={features[4]:.4f}"
    )

    if _is_spark_ml_model(model):
        spark = _get_spark_session(spark)
        if spark is None:
            logger.warning("[ML Filter] Spark ML model detected but no active SparkSession. Falling back to sklearn mock model.")
            fallback_model = train_mock_ml_model()
            return float(fallback_model.predict_proba([features])[0][1])

        from pyspark.ml.linalg import Vectors
        row_df = spark.createDataFrame([(Vectors.dense(features),)], ["features"])
        prediction = model.transform(row_df).select("probability").collect()[0]
        return float(prediction["probability"][1])

    prob_spoofing = model.predict_proba([features])[0][1]
    return float(prob_spoofing)


def load_spark_ml_model(spark=None, model_path=None):
    if model_path is None:
        model_path = SPARK_MODEL_DIR

    if not os.path.exists(model_path):
        return None

    configure_local_hadoop()
    spark = _get_spark_session(spark)
    if spark is None:
        logger.warning("[ML Filter] Found Spark ML model at %s but SparkSession is not active.", model_path)
        return None

    try:
        from pyspark.ml.classification import RandomForestClassificationModel
        model = RandomForestClassificationModel.load(model_path)
        logger.info("[ML Filter] Loaded Spark MLlib RandomForest model from %s", model_path)
        return model
    except Exception as e:
        logger.warning("[ML Filter] Cannot load Spark MLlib model from %s: %s", model_path, e)
        return None


def load_ml_model(model_path=None, spark=None, prefer_spark=True):
    if prefer_spark:
        spark_model = load_spark_ml_model(spark=spark)
        if spark_model is not None:
            return spark_model

    if model_path is None:
        model_path = SKLEARN_MODEL_PATH

    if not os.path.exists(model_path):
        logger.warning(
            "[ML Filter] Khong tim thay trained model tai %s. "
            "Fallback sang model lightweight tu CSV/mock data.",
            model_path
        )
        return train_mock_ml_model()

    bundle = joblib.load(model_path)

    if isinstance(bundle, dict) and "model" in bundle:
        model = bundle["model"]
        logger.info(f"[ML Filter] Loaded trained model from {model_path}")

        if "metrics" in bundle:
            metrics = bundle["metrics"].get("metrics", {})
            logger.info(f"[ML Filter] Model metrics: {metrics}")

        return model

    logger.info(f"[ML Filter] Loaded trained model from {model_path}")
    return bundle
