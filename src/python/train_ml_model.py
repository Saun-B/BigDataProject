import argparse
import csv
import json
import os
import sys
from datetime import datetime

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import joblib
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    confusion_matrix,
    classification_report
)


FEATURE_COLUMNS = [
    "spread",
    "bid_vol",
    "ask_vol",
    "imbalance",
    "cancel_rate"
]


src_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.normpath(os.path.join(src_dir, "../.."))


def resolve_path(path):
    if os.path.isabs(path):
        return path

    return os.path.normpath(os.path.join(PROJECT_ROOT, path))


def display_path(path):
    try:
        return os.path.relpath(path, PROJECT_ROOT).replace(os.sep, "/")
    except ValueError:
        return path


def load_feature_csv(path):
    path = resolve_path(path)

    if not os.path.exists(path):
        raise FileNotFoundError(f"Không tìm thấy CSV: {path}")

    X = []
    y = []

    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        missing = [c for c in FEATURE_COLUMNS + ["label"] if c not in reader.fieldnames]
        if missing:
            raise ValueError(f"CSV thiếu cột: {missing}")

        for row in reader:
            try:
                X.append([float(row[col]) for col in FEATURE_COLUMNS])
                y.append(int(row["label"]))
            except Exception:
                continue

    X = np.array(X, dtype=float)
    y = np.array(y, dtype=int)

    return X, y, path


def label_summary(y):
    total = len(y)
    pos = int(np.sum(y == 1))
    neg = int(np.sum(y == 0))

    return {
        "total": total,
        "normal_0": neg,
        "spoofing_1": pos,
        "spoofing_ratio_percent": round(pos / total * 100, 4) if total else 0.0
    }


def safe_roc_auc(y_true, y_prob):
    try:
        if len(set(y_true.tolist())) < 2:
            return None
        return float(roc_auc_score(y_true, y_prob))
    except Exception:
        return None


def main():
    parser = argparse.ArgumentParser(description="Train FI-2010 spoofing ML model")
    parser.add_argument(
        "--train-csv",
        default="data/ml/fi2010_train_features.csv",
        help="Training feature CSV"
    )
    parser.add_argument(
        "--test-csv",
        default="data/ml/fi2010_test_features.csv",
        help="Testing feature CSV"
    )
    parser.add_argument(
        "--model-output",
        default="models/spoofing_rf_model.pkl",
        help="Output model path"
    )
    parser.add_argument(
        "--metrics-output",
        default="outputs/ml_fi2010_metrics.json",
        help="Output metrics JSON path"
    )
    parser.add_argument(
        "--n-estimators",
        type=int,
        default=200,
        help="Number of trees in RandomForest"
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=10,
        help="Max depth of RandomForest trees"
    )

    args = parser.parse_args()

    train_csv = resolve_path(args.train_csv)
    test_csv = resolve_path(args.test_csv)
    model_output = resolve_path(args.model_output)
    metrics_output = resolve_path(args.metrics_output)

    print("=" * 70)
    print("Train FI-2010 Spoofing ML Model")
    print("=" * 70)

    X_train, y_train, train_csv = load_feature_csv(train_csv)
    X_test, y_test, test_csv = load_feature_csv(test_csv)

    print("Train CSV:", train_csv)
    print("Test CSV:", test_csv)
    print("Train summary:", label_summary(y_train))
    print("Test summary:", label_summary(y_test))

    if len(set(y_train.tolist())) < 2:
        print("[ERROR] Train data chỉ có 1 class. Cần có cả label 0 và label 1.")
        sys.exit(1)

    if len(set(y_test.tolist())) < 2:
        print("[WARNING] Test data chỉ có 1 class. Metrics sẽ không đầy đủ.")

    model = RandomForestClassifier(
        n_estimators=args.n_estimators,
        max_depth=args.max_depth,
        random_state=42,
        class_weight="balanced_subsample",
        n_jobs=-1
    )

    print("\n[Train] Đang huấn luyện RandomForest...")
    model.fit(X_train, y_train)
    print("[Train] Huấn luyện xong.")

    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]

    acc = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, zero_division=0)
    recall = recall_score(y_test, y_pred, zero_division=0)
    f1 = f1_score(y_test, y_pred, zero_division=0)
    auc = safe_roc_auc(y_test, y_prob)

    cm = confusion_matrix(y_test, y_pred).tolist()

    report = classification_report(
        y_test,
        y_pred,
        zero_division=0,
        target_names=["normal", "spoofing"]
    )

    feature_importances = {
        col: float(score)
        for col, score in zip(FEATURE_COLUMNS, model.feature_importances_)
    }

    metrics = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "train_csv": display_path(train_csv),
        "test_csv": display_path(test_csv),
        "model_output": display_path(model_output),
        "feature_columns": FEATURE_COLUMNS,
        "train_summary": label_summary(y_train),
        "test_summary": label_summary(y_test),
        "model": {
            "type": "RandomForestClassifier",
            "n_estimators": args.n_estimators,
            "max_depth": args.max_depth,
            "class_weight": "balanced_subsample",
            "random_state": 42
        },
        "metrics": {
            "accuracy": float(acc),
            "precision": float(precision),
            "recall": float(recall),
            "f1": float(f1),
            "roc_auc": auc,
            "confusion_matrix": cm
        },
        "feature_importances": feature_importances
    }

    os.makedirs(os.path.dirname(model_output), exist_ok=True)
    os.makedirs(os.path.dirname(metrics_output), exist_ok=True)

    bundle = {
        "model": model,
        "feature_columns": FEATURE_COLUMNS,
        "trained_at": metrics["created_at"],
        "metrics": metrics
    }

    joblib.dump(bundle, model_output)

    with open(metrics_output, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    print("\n" + "=" * 70)
    print("Evaluation on test data")
    print("=" * 70)
    print("Accuracy :", round(acc, 4))
    print("Precision:", round(precision, 4))
    print("Recall   :", round(recall, 4))
    print("F1-score :", round(f1, 4))
    print("ROC-AUC  :", None if auc is None else round(auc, 4))
    print("\nConfusion matrix:")
    print(cm)
    print("\nClassification report:")
    print(report)

    print("Feature importances:")
    for k, v in feature_importances.items():
        print(f"  {k}: {v:.4f}")

    print("\nSaved model:", model_output)
    print("Saved metrics:", metrics_output)
    print("=" * 70)


if __name__ == "__main__":
    main()
