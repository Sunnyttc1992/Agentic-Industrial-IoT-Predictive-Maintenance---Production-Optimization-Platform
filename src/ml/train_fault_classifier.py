from __future__ import annotations

import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    average_precision_score,
    classification_report,
    confusion_matrix,
    fbeta_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

FEATURE_FILE = (
    PROJECT_ROOT
    / "data"
    / "features"
    / "telemetry_features.parquet"
)

OUTPUT_FOLDER = (
    PROJECT_ROOT
    / "data"
    / "model_output"
)

OUTPUT_FILE = (
    OUTPUT_FOLDER
    / "fault_predictions.parquet"
)

METRICS_FILE = (
    OUTPUT_FOLDER
    / "fault_classifier_metrics.json"
)

MODEL_FOLDER = (
    PROJECT_ROOT
    / "models"
)

MODEL_FILE = (
    MODEL_FOLDER
    / "fault_classifier.joblib"
)

NORMAL_LABEL = "NORMAL"

# Preserve time order:
# 70% train, 10% validation, 20% final test.
TRAIN_RATIO = 0.70
VALIDATION_RATIO = 0.10
RANDOM_STATE = 42

# ============================================================
# FEATURE COLUMNS
# ============================================================

sensor_columns = [
    "temperature_c",
    "vibration_mm_s",
    "current_amp",
    "pressure_bar",
    "rpm",
]

feature_columns = []

for column in sensor_columns:
    feature_columns.extend(
        [
            column,
            f"{column}_change",
            f"{column}_rolling_mean_5",
            f"{column}_rolling_std_5",
            f"{column}_deviation",
        ]
    )

# ============================================================
# HELPER FUNCTIONS
# ============================================================


def select_alert_threshold(y_true: pd.Series, probabilities: np.ndarray) -> float:
    """
    Select an alert threshold on validation data.

    F2 gives recall twice the importance of precision because,
    in predictive maintenance, missing a true fault is costly.
    """
    candidates = np.arange(0.10, 0.91, 0.05)

    best_threshold = 0.50
    best_f2_score = -1.0

    for threshold in candidates:
        predictions = (probabilities >= threshold).astype(int)

        score = fbeta_score(
            y_true,
            predictions,
            beta=2,
            zero_division=0,
        )

        if score > best_f2_score:
            best_f2_score = score
            best_threshold = float(threshold)

    return best_threshold


def predict_fault_outputs(
    model: RandomForestClassifier,
    X: pd.DataFrame,
    threshold: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Return:
    - probability that a row is any fault
    - binary fault alert
    - most likely fault type when an alert is raised
    """
    probabilities = model.predict_proba(X)
    classes = model.classes_

    normal_index = np.where(classes == NORMAL_LABEL)[0]

    if len(normal_index) != 1:
        raise ValueError(
            f"Expected one '{NORMAL_LABEL}' class. Found: {list(classes)}"
        )

    fault_probability = 1 - probabilities[:, normal_index[0]]
    predicted_fault = (fault_probability >= threshold).astype(int)

    fault_class_indices = np.array(
        [
            index
            for index, label in enumerate(classes)
            if label != NORMAL_LABEL
        ]
    )

    if len(fault_class_indices) == 0:
        raise ValueError("The training data contains no fault-type classes.")

    strongest_fault_index = np.argmax(
        probabilities[:, fault_class_indices],
        axis=1,
    )

    most_likely_fault_type = classes[
        fault_class_indices[strongest_fault_index]
    ]

    predicted_fault_type = np.where(
        predicted_fault == 1,
        most_likely_fault_type,
        NORMAL_LABEL,
    )

    return (
        fault_probability,
        predicted_fault,
        predicted_fault_type,
    )


# ============================================================
# LOAD AND VALIDATE DATA
# ============================================================

print("Loading feature dataset...")

df = pd.read_parquet(FEATURE_FILE)

df = df.sort_values(
    by=["timestamp", "machine_id"]
).reset_index(drop=True)

required_columns = feature_columns + [
    "machine_id",
    "timestamp",
    "is_fault",
    "fault_type",
]

missing_columns = [
    column
    for column in required_columns
    if column not in df.columns
]

if missing_columns:
    raise ValueError(
        f"Missing required columns: {missing_columns}"
    )

df = df[
    df["is_fault"].notna()
    & df["fault_type"].notna()
].copy()

df["is_fault"] = df["is_fault"].astype(int)
df["fault_type"] = df["fault_type"].astype(str)

missing_feature_values = df[feature_columns].isna().sum().sum()

if missing_feature_values > 0:
    raise ValueError(
        "Missing values found in ML features: "
        f"{missing_feature_values}"
    )

print(f"Dataset shape: {df.shape}")
print(f"ML feature count: {len(feature_columns)}")

# ============================================================
# CHRONOLOGICAL TRAIN / VALIDATION / TEST SPLIT
# ============================================================

train_end = int(len(df) * TRAIN_RATIO)
validation_end = int(
    len(df) * (TRAIN_RATIO + VALIDATION_RATIO)
)

train_df = df.iloc[:train_end].copy()
validation_df = df.iloc[train_end:validation_end].copy()
test_df = df.iloc[validation_end:].copy()

print("\n==============================")
print("CHRONOLOGICAL DATA SPLIT")
print("==============================")
print(f"Training rows:   {len(train_df)}")
print(f"Validation rows: {len(validation_df)}")
print(f"Testing rows:    {len(test_df)}")

# ============================================================
# TRAIN MULTI-CLASS FAULT MODEL
# ============================================================

X_train = train_df[feature_columns]
y_train = train_df["fault_type"]

print("\nTraining supervised fault classifier...")

model = RandomForestClassifier(
    n_estimators=400,
    max_depth=12,
    min_samples_leaf=2,
    class_weight="balanced_subsample",
    random_state=RANDOM_STATE,
    n_jobs=-1,
)

model.fit(X_train, y_train)

# ============================================================
# SELECT ALERT THRESHOLD ON VALIDATION DATA
# ============================================================

validation_probability, _, _ = predict_fault_outputs(
    model=model,
    X=validation_df[feature_columns],
    threshold=0.50,
)

alert_threshold = select_alert_threshold(
    y_true=validation_df["is_fault"],
    probabilities=validation_probability,
)

print(
    f"\nSelected fault-alert threshold: "
    f"{alert_threshold:.2f}"
)

# ============================================================
# FINAL TEST EVALUATION
# ============================================================

(
    test_fault_probability,
    test_predicted_fault,
    test_predicted_fault_type,
) = predict_fault_outputs(
    model=model,
    X=test_df[feature_columns],
    threshold=alert_threshold,
)

y_true = test_df["is_fault"]

tn, fp, fn, tp = confusion_matrix(
    y_true,
    test_predicted_fault,
    labels=[0, 1],
).ravel()

fault_recall = recall_score(
    y_true,
    test_predicted_fault,
    zero_division=0,
)

fault_precision = precision_score(
    y_true,
    test_predicted_fault,
    zero_division=0,
)

roc_auc = roc_auc_score(
    y_true,
    test_fault_probability,
)

average_precision = average_precision_score(
    y_true,
    test_fault_probability,
)

print("\n==============================")
print("SUPERVISED FAULT PREDICTION REPORT")
print("==============================")
print(f"Fault recall:        {fault_recall:.2%}")
print(f"Fault precision:     {fault_precision:.2%}")
print(f"ROC-AUC:             {roc_auc:.3f}")
print(f"Average precision:   {average_precision:.3f}")
print("\nConfusion Matrix [[TN, FP], [FN, TP]]:")
print([[tn, fp], [fn, tp]])

print("\nFault-type classification report:")
print(
    classification_report(
        test_df["fault_type"],
        test_predicted_fault_type,
        zero_division=0,
    )
)

# ============================================================
# SAVE MODEL, METRICS, AND PREDICTIONS
# ============================================================

OUTPUT_FOLDER.mkdir(
    parents=True,
    exist_ok=True,
)

MODEL_FOLDER.mkdir(
    parents=True,
    exist_ok=True,
)

prediction_columns = [
    "machine_id",
    "timestamp",
    "fault_type",
    "is_fault",
]

test_results = test_df[prediction_columns].copy()

test_results["fault_probability"] = test_fault_probability
test_results["predicted_fault"] = test_predicted_fault
test_results["predicted_fault_type"] = test_predicted_fault_type
test_results["alert_threshold"] = alert_threshold

test_results.to_parquet(
    OUTPUT_FILE,
    index=False,
)

joblib.dump(
    {
        "model": model,
        "feature_columns": feature_columns,
        "normal_label": NORMAL_LABEL,
        "alert_threshold": alert_threshold,
    },
    MODEL_FILE,
)

metrics = {
    "fault_recall": float(fault_recall),
    "fault_precision": float(fault_precision),
    "roc_auc": float(roc_auc),
    "average_precision": float(average_precision),
    "alert_threshold": float(alert_threshold),
    "true_negatives": int(tn),
    "false_positives": int(fp),
    "false_negatives": int(fn),
    "true_positives": int(tp),
}

with open(METRICS_FILE, "w", encoding="utf-8") as file:
    json.dump(
        metrics,
        file,
        indent=2,
    )

print("\nSaved artifacts:")
print(f"Predictions: {OUTPUT_FILE}")
print(f"Model:       {MODEL_FILE}")
print(f"Metrics:     {METRICS_FILE}")
