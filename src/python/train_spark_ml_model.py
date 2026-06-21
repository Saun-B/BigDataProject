import argparse
import json
import os
import shutil
import stat
import sys
import time
from datetime import datetime

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when


FEATURE_COLUMNS = ["spread", "bid_vol", "ask_vol", "imbalance", "cancel_rate"]
SRC_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.normpath(os.path.join(SRC_DIR, "../.."))


def resolve_path(path):
    if os.path.isabs(path):
        return path
    return os.path.normpath(os.path.join(PROJECT_ROOT, path))


def display_path(path):
    try:
        return os.path.relpath(path, PROJECT_ROOT).replace(os.sep, "/")
    except ValueError:
        return path


def remove_existing_model_dir(path):
    abs_path = os.path.abspath(path)
    project_root = os.path.abspath(PROJECT_ROOT)

    if not abs_path.startswith(project_root + os.sep):
        raise ValueError(f"Refusing to delete model path outside project: {path}")

    if os.path.isdir(abs_path):
        def handle_remove_readonly(func, item_path, _):
            os.chmod(item_path, stat.S_IWRITE)
            func(item_path)

        shutil.rmtree(abs_path, onerror=handle_remove_readonly)


def configure_windows_hadoop():
    local_hadoop_home = os.path.join(PROJECT_ROOT, "hadoop")
    local_winutils = os.path.join(local_hadoop_home, "bin", "winutils.exe")
    if not os.path.exists(local_winutils):
        return

    os.environ.setdefault("HADOOP_HOME", local_hadoop_home)
    os.environ.setdefault("hadoop.home.dir", local_hadoop_home)
    hadoop_bin = os.path.join(local_hadoop_home, "bin")
    if hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")


def create_spark():
    configure_windows_hadoop()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    return (
        SparkSession.builder
        .appName("BigDataSangSparkMLTraining")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def load_feature_csv(spark, path):
    path = resolve_path(path)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Khong tim thay CSV: {path}")

    df = spark.read.csv(path, header=True, inferSchema=True)
    missing = [c for c in FEATURE_COLUMNS + ["label"] if c not in df.columns]
    if missing:
        raise ValueError(f"CSV thieu cot: {missing}")

    selected = [col(c).cast("double").alias(c) for c in FEATURE_COLUMNS]
    selected.append(col("label").cast("int").alias("label"))
    return df.select(*selected), path


def add_class_weights(df):
    counts = {int(row["label"]): int(row["count"]) for row in df.groupBy("label").count().collect()}
    total = sum(counts.values())
    neg = counts.get(0, 0)
    pos = counts.get(1, 0)

    if neg == 0 or pos == 0:
        return df.withColumn("class_weight", lit(1.0)), counts

    neg_weight = total / (2.0 * neg)
    pos_weight = total / (2.0 * pos)
    weighted = df.withColumn(
        "class_weight",
        when(col("label") == 1, lit(pos_weight)).otherwise(lit(neg_weight))
    )
    return weighted, counts


def prepare_features(df):
    assembler = VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="features")
    return assembler.transform(df).select("features", "label")


def safe_binary_metric(evaluator, predictions):
    try:
        return float(evaluator.evaluate(predictions))
    except Exception:
        return None


def collect_confusion_matrix(predictions):
    rows = predictions.groupBy("label", "prediction").count().collect()
    matrix = {"tn": 0, "fp": 0, "fn": 0, "tp": 0}
    for row in rows:
        label = int(row["label"])
        pred = int(row["prediction"])
        count = int(row["count"])
        if label == 0 and pred == 0:
            matrix["tn"] = count
        elif label == 0 and pred == 1:
            matrix["fp"] = count
        elif label == 1 and pred == 0:
            matrix["fn"] = count
        elif label == 1 and pred == 1:
            matrix["tp"] = count
    return matrix


def main():
    parser = argparse.ArgumentParser(description="Train Spark MLlib RandomForest for BigDataSang")
    parser.add_argument("--train-csv", default="data/ml/fi2010_train_features.csv")
    parser.add_argument("--test-csv", default="data/ml/fi2010_test_features.csv")
    parser.add_argument("--model-output", default="models/spark_ml_rf")
    parser.add_argument("--metrics-output", default="outputs/spark_ml_fi2010_metrics.json")
    parser.add_argument("--num-trees", type=int, default=80)
    parser.add_argument("--max-depth", type=int, default=8)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    train_csv = resolve_path(args.train_csv)
    test_csv = resolve_path(args.test_csv)
    model_output = resolve_path(args.model_output)
    metrics_output = resolve_path(args.metrics_output)

    print("=" * 70)
    print("Train Spark MLlib RandomForest")
    print("=" * 70)

    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        train_raw, train_csv = load_feature_csv(spark, train_csv)
        test_raw, test_csv = load_feature_csv(spark, test_csv)

        train_prepared = prepare_features(train_raw)
        test_prepared = prepare_features(test_raw)
        train_prepared, train_counts = add_class_weights(train_prepared)

        print("Train CSV:", train_csv)
        print("Test CSV :", test_csv)
        print("Train label counts:", train_counts)
        print("Features:", FEATURE_COLUMNS)

        rf = RandomForestClassifier(
            featuresCol="features",
            labelCol="label",
            weightCol="class_weight",
            numTrees=args.num_trees,
            maxDepth=args.max_depth,
            seed=args.seed
        )

        print("\n[Train] Dang train Spark MLlib RandomForest...")
        started = time.time()
        model = rf.fit(train_prepared)
        train_seconds = time.time() - started
        print(f"[Train] Done in {train_seconds:.2f}s")

        predictions = model.transform(test_prepared).cache()

        multiclass = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
        accuracy = multiclass.evaluate(predictions, {multiclass.metricName: "accuracy"})
        weighted_precision = multiclass.evaluate(predictions, {multiclass.metricName: "weightedPrecision"})
        weighted_recall = multiclass.evaluate(predictions, {multiclass.metricName: "weightedRecall"})
        f1 = multiclass.evaluate(predictions, {multiclass.metricName: "f1"})
        auc = safe_binary_metric(
            BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"),
            predictions
        )
        confusion = collect_confusion_matrix(predictions)

        os.makedirs(os.path.dirname(model_output), exist_ok=True)
        os.makedirs(os.path.dirname(metrics_output), exist_ok=True)
        remove_existing_model_dir(model_output)
        model.write().overwrite().save(model_output)

        metrics = {
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "train_csv": display_path(train_csv),
            "test_csv": display_path(test_csv),
            "model_output": display_path(model_output),
            "feature_columns": FEATURE_COLUMNS,
            "model": {
                "type": "SparkMLlib RandomForestClassifier",
                "numTrees": args.num_trees,
                "maxDepth": args.max_depth,
                "seed": args.seed,
                "weightCol": "class_weight"
            },
            "train": {
                "label_counts": train_counts,
                "seconds": round(train_seconds, 3)
            },
            "metrics": {
                "accuracy": float(accuracy),
                "weighted_precision": float(weighted_precision),
                "weighted_recall": float(weighted_recall),
                "f1": float(f1),
                "roc_auc": auc,
                "confusion_matrix": confusion
            }
        }

        with open(metrics_output, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2)

        print("\nEvaluation")
        print("Accuracy          :", round(accuracy, 4))
        print("WeightedPrecision :", round(weighted_precision, 4))
        print("WeightedRecall    :", round(weighted_recall, 4))
        print("F1                :", round(f1, 4))
        print("ROC-AUC           :", None if auc is None else round(auc, 4))
        print("Confusion         :", confusion)
        print("\nSaved Spark model :", model_output)
        print("Saved metrics     :", metrics_output)
        print("=" * 70)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
