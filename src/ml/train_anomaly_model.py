import joblib
import pandas as pd

from pathlib import Path

from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    confusion_matrix,
    classification_report
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
    / "anomaly_predictions.parquet"
)

MODEL_FOLDER = (
    PROJECT_ROOT
    / "models"
)

MODEL_FILE = (
    MODEL_FOLDER
    / "isolation_forest.joblib"
)

SCALER_FILE = (
    MODEL_FOLDER
    / "scaler.joblib"
)

TRAIN_RATIO = 0.80


# ============================================================
# FEATURE COLUMNS
# ============================================================

sensor_columns = [
    "temperature_c",
    "vibration_mm_s",
    "current_amp",
    "pressure_bar",
    "rpm"
]

feature_columns = []

for column in sensor_columns:

    feature_columns.extend(
        [
            column,
            f"{column}_change",
            f"{column}_rolling_mean_5",
            f"{column}_rolling_std_5",
            f"{column}_deviation"
        ]
    )


# ============================================================
# LOAD DATA
# ============================================================

print("Loading feature dataset...")

df = pd.read_parquet(
    FEATURE_FILE
)

df = df.sort_values(
    by=[
        "timestamp",
        "machine_id"
    ]
).reset_index(
    drop=True
)

print(
    f"Dataset shape: "
    f"{df.shape}"
)

print(
    f"ML feature count: "
    f"{len(feature_columns)}"
)


# ============================================================
# CHECK GROUND TRUTH COLUMNS
# ============================================================

required_ground_truth_columns = [
    "is_fault",
    "fault_type"
]

missing_ground_truth = [
    column
    for column in required_ground_truth_columns
    if column not in df.columns
]

if missing_ground_truth:

    raise ValueError(
        f"Missing ground-truth columns: "
        f"{missing_ground_truth}"
    )


# ============================================================
# CHRONOLOGICAL TRAIN / TEST SPLIT
# ============================================================

split_index = int(
    len(df) * TRAIN_RATIO
)

train_df = (
    df.iloc[:split_index]
    .copy()
)

test_df = (
    df.iloc[split_index:]
    .copy()
)


print("\n==============================")
print("INITIAL TRAIN / TEST SPLIT")
print("==============================")

print(
    f"Training rows: "
    f"{len(train_df)}"
)

print(
    f"Testing rows: "
    f"{len(test_df)}"
)


# ============================================================
# TRAIN ONLY ON KNOWN NORMAL DATA
# ============================================================

train_normal_df = (
    train_df[
        train_df["is_fault"] == 0
    ]
    .copy()
)


# ============================================================
# EVALUATE ONLY ON LABELED TEST DATA
# ============================================================

test_df = (
    test_df[
        test_df["is_fault"].notna()
    ]
    .copy()
)


print(
    f"\nKnown normal training rows: "
    f"{len(train_normal_df)}"
)

print(
    f"Labeled testing rows: "
    f"{len(test_df)}"
)


# ============================================================
# SAFETY CHECK
# ============================================================

if len(train_normal_df) == 0:

    raise ValueError(
        "No known NORMAL rows are available "
        "in the training split."
    )

if len(test_df) == 0:

    raise ValueError(
        "No labeled rows are available "
        "in the test split."
    )


# ============================================================
# SELECT FEATURES
# ============================================================

X_train = (
    train_normal_df[
        feature_columns
    ]
    .copy()
)

X_test = (
    test_df[
        feature_columns
    ]
    .copy()
)


# ============================================================
# CHECK MISSING ML VALUES
# ============================================================

train_missing = (
    X_train
    .isna()
    .sum()
    .sum()
)

test_missing = (
    X_test
    .isna()
    .sum()
    .sum()
)


print(
    f"\nMissing training feature values: "
    f"{train_missing}"
)

print(
    f"Missing testing feature values: "
    f"{test_missing}"
)


if train_missing > 0 or test_missing > 0:

    raise ValueError(
        "Missing values found in ML features."
    )


# ============================================================
# SCALE FEATURES
# ============================================================

print("\nScaling features...")

scaler = StandardScaler()


# IMPORTANT:
# Fit scaler ONLY using training data

X_train_scaled = (
    scaler.fit_transform(
        X_train
    )
)


# Test data only uses learned scaler

X_test_scaled = (
    scaler.transform(
        X_test
    )
)


# ============================================================
# TRAIN ISOLATION FOREST
# ============================================================

print(
    "Training Isolation Forest..."
)

model = IsolationForest(
    n_estimators=200,
    contamination=0.10,
    random_state=42,
    n_jobs=-1
)

model.fit(
    X_train_scaled
)


# ============================================================
# SCORE TEST DATA
# ============================================================

print(
    "Scoring test telemetry..."
)

test_predictions = (
    model.predict(
        X_test_scaled
    )
)

test_scores = (
    model.decision_function(
        X_test_scaled
    )
)


# ============================================================
# ADD MODEL OUTPUT
# ============================================================

test_df[
    "anomaly_prediction"
] = test_predictions


test_df[
    "anomaly_score"
] = test_scores


# Isolation Forest:
#
#  1 = NORMAL
# -1 = ANOMALY

test_df[
    "anomaly_status"
] = (
    test_df[
        "anomaly_prediction"
    ]
    .map(
        {
            1: "NORMAL",
            -1: "ANOMALY"
        }
    )
)


# ============================================================
# CONVERT ANOMALY OUTPUT TO FAULT PREDICTION
# ============================================================

test_df[
    "predicted_fault"
] = (
    test_df[
        "anomaly_prediction"
    ]
    == -1
).astype(int)


# ============================================================
# BASIC ANOMALY REPORT
# ============================================================

normal_count = (
    test_df[
        "anomaly_status"
    ]
    .eq("NORMAL")
    .sum()
)

anomaly_count = (
    test_df[
        "anomaly_status"
    ]
    .eq("ANOMALY")
    .sum()
)

anomaly_rate = (
    anomaly_count
    / len(test_df)
)


print("\n==============================")
print("ANOMALY DETECTION REPORT")
print("==============================")


print(
    f"Test observations: "
    f"{len(test_df)}"
)

print(
    f"Normal observations: "
    f"{normal_count}"
)

print(
    f"Anomalies detected: "
    f"{anomaly_count}"
)

print(
    f"Observed anomaly rate: "
    f"{anomaly_rate:.2%}"
)


# ============================================================
# GROUND TRUTH SUMMARY
# ============================================================

print("\n==============================")
print("GROUND TRUTH SUMMARY")
print("==============================")


print(
    test_df[
        "is_fault"
    ]
    .value_counts()
    .sort_index()
)


print(
    "\nFault types:"
)

print(
    test_df[
        "fault_type"
    ]
    .value_counts()
)


# ============================================================
# FAULT DETECTION EVALUATION
# ============================================================

y_true = (
    test_df[
        "is_fault"
    ]
    .astype(int)
)

y_pred = (
    test_df[
        "predicted_fault"
    ]
)


cm = confusion_matrix(
    y_true,
    y_pred,
    labels=[0, 1]
)


print("\n==============================")
print("FAULT DETECTION EVALUATION")
print("==============================")


print("\nConfusion Matrix:")
print(cm)


# Confusion matrix layout:
#
# [[TN, FP],
#  [FN, TP]]

tn = cm[0][0]
fp = cm[0][1]
fn = cm[1][0]
tp = cm[1][1]


print("\nConfusion Matrix Breakdown:")

print(
    f"True Negatives:  {tn}"
)

print(
    f"False Positives: {fp}"
)

print(
    f"False Negatives: {fn}"
)

print(
    f"True Positives:  {tp}"
)


print(
    "\nClassification Report:"
)

print(
    classification_report(
        y_true,
        y_pred,
        labels=[
            0,
            1
        ],
        target_names=[
            "NORMAL",
            "FAULT"
        ],
        zero_division=0
    )
)


# ============================================================
# ADDITIONAL BUSINESS METRICS
# ============================================================

if (tp + fn) > 0:

    fault_recall = (
        tp
        / (tp + fn)
    )

else:

    fault_recall = 0


if (tp + fp) > 0:

    fault_precision = (
        tp
        / (tp + fp)
    )

else:

    fault_precision = 0


if (fp + tn) > 0:

    false_alarm_rate = (
        fp
        / (fp + tn)
    )

else:

    false_alarm_rate = 0


print("\n==============================")
print("PREDICTIVE MAINTENANCE METRICS")
print("==============================")


print(
    f"Fault recall: "
    f"{fault_recall:.2%}"
)

print(
    f"Fault precision: "
    f"{fault_precision:.2%}"
)

print(
    f"False alarm rate: "
    f"{false_alarm_rate:.2%}"
)


# ============================================================
# ANOMALIES BY MACHINE
# ============================================================

print("\nAnomalies by machine:")


anomalies_by_machine = (
    test_df[
        test_df[
            "anomaly_status"
        ]
        == "ANOMALY"
    ]
    .groupby(
        "machine_id"
    )
    .size()
    .sort_values(
        ascending=False
    )
)


print(
    anomalies_by_machine
)


# ============================================================
# DETECTION BY FAULT TYPE
# ============================================================

print("\n==============================")
print("DETECTION BY FAULT TYPE")
print("==============================")


fault_rows = (
    test_df[
        test_df[
            "is_fault"
        ]
        == 1
    ]
    .copy()
)


fault_type_report = (
    fault_rows
    .groupby(
        "fault_type"
    )
    .agg(
        actual_faults=(
            "is_fault",
            "size"
        ),
        detected_faults=(
            "predicted_fault",
            "sum"
        )
    )
)


fault_type_report[
    "detection_rate"
] = (
    fault_type_report[
        "detected_faults"
    ]
    /
    fault_type_report[
        "actual_faults"
    ]
)


print(
    fault_type_report
)


# ============================================================
# STRONGEST ANOMALIES
# ============================================================

display_columns = [
    "machine_id",
    "timestamp",
    "temperature_c",
    "vibration_mm_s",
    "current_amp",
    "pressure_bar",
    "rpm",
    "fault_type",
    "is_fault",
    "predicted_fault",
    "anomaly_score",
    "anomaly_status"
]


strongest_anomalies = (
    test_df[
        test_df[
            "anomaly_status"
        ]
        == "ANOMALY"
    ]
    .sort_values(
        "anomaly_score"
    )
    [display_columns]
    .head(10)
)


print(
    "\nTop 10 strongest anomalies:"
)

print(
    strongest_anomalies
)


# ============================================================
# FALSE NEGATIVES
# ============================================================

false_negatives = (
    test_df[
        (
            test_df[
                "is_fault"
            ]
            == 1
        )
        &
        (
            test_df[
                "predicted_fault"
            ]
            == 0
        )
    ]
    .copy()
)


print(
    f"\nMissed faults: "
    f"{len(false_negatives)}"
)


if len(false_negatives) > 0:

    print(
        "\nExample missed faults:"
    )

    print(
        false_negatives[
            display_columns
        ]
        .head(10)
    )


# ============================================================
# FALSE POSITIVES
# ============================================================

false_positives = (
    test_df[
        (
            test_df[
                "is_fault"
            ]
            == 0
        )
        &
        (
            test_df[
                "predicted_fault"
            ]
            == 1
        )
    ]
    .copy()
)


print(
    f"\nFalse alarms: "
    f"{len(false_positives)}"
)


# ============================================================
# CREATE OUTPUT DIRECTORIES
# ============================================================

OUTPUT_FOLDER.mkdir(
    parents=True,
    exist_ok=True
)

MODEL_FOLDER.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# SAVE OUTPUT
# ============================================================

test_df.to_parquet(
    OUTPUT_FILE,
    index=False
)


joblib.dump(
    model,
    MODEL_FILE
)


joblib.dump(
    scaler,
    SCALER_FILE
)


print(
    "\nPrediction dataset saved to:"
)

print(
    OUTPUT_FILE
)


print(
    "\nModel saved to:"
)

print(
    MODEL_FILE
)


print(
    "\nScaler saved to:"
)

print(
    SCALER_FILE
)
