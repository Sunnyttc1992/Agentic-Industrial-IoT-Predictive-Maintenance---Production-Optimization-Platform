import pandas as pd

from pathlib import Path


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_FILE = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "telemetry_clean.parquet"
)

FEATURE_FOLDER = (
    PROJECT_ROOT
    / "data"
    / "features"
)

FEATURE_FILE = (
    FEATURE_FOLDER
    / "telemetry_features.parquet"
)


# Rolling window:
# 5 observations from the same machine
ROLLING_WINDOW = 5


# ============================================================
# LOAD SILVER DATA
# ============================================================

print("Loading Silver telemetry...")

df = pd.read_parquet(
    SILVER_FILE
)

print(
    f"Silver dataset shape: "
    f"{df.shape}"
)


# ============================================================
# SORT BY MACHINE AND TIME
# ============================================================

df = df.sort_values(
    by=[
        "machine_id",
        "timestamp"
    ]
).reset_index(drop=True)


# ============================================================
# SENSOR COLUMNS
# ============================================================

sensor_columns = [
    "temperature_c",
    "vibration_mm_s",
    "current_amp",
    "pressure_bar",
    "rpm"
]


# ============================================================
# 1. CHANGE FEATURES
# ============================================================

print("\nCreating change features...")


for column in sensor_columns:

    df[f"{column}_change"] = (
        df
        .groupby("machine_id")[column]
        .diff()
        .fillna(0)
    )


# ============================================================
# 2. ROLLING MEAN FEATURES
# ============================================================

print("Creating rolling mean features...")


for column in sensor_columns:

    df[f"{column}_rolling_mean_5"] = (
        df
        .groupby("machine_id")[column]
        .transform(
            lambda x:
            x.rolling(
                window=ROLLING_WINDOW,
                min_periods=1
            ).mean()
        )
    )


# ============================================================
# 3. ROLLING STANDARD DEVIATION
# ============================================================

print("Creating rolling variability features...")


for column in sensor_columns:

    df[f"{column}_rolling_std_5"] = (
        df
        .groupby("machine_id")[column]
        .transform(
            lambda x:
            x.rolling(
                window=ROLLING_WINDOW,
                min_periods=1
            ).std(ddof=0)
        )
    )


# ============================================================
# 4. DEVIATION FROM RECENT BEHAVIOR
# ============================================================

print("Creating deviation features...")


for column in sensor_columns:

    df[f"{column}_deviation"] = (

        df[column]

        -

        df[
            f"{column}_rolling_mean_5"
        ]

    )


# ============================================================
# CHECK FEATURE DATA
# ============================================================

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


print(
    f"\nML features created: "
    f"{len(feature_columns)}"
)


# ============================================================
# CHECK MISSING VALUES
# ============================================================

missing_values = (
    df[feature_columns]
    .isna()
    .sum()
    .sum()
)


print(
    f"Missing ML feature values: "
    f"{missing_values}"
)


# ============================================================
# FEATURE SUMMARY
# ============================================================

print("\nFeature preview:")

preview_columns = [
    "machine_id",
    "timestamp",
    "temperature_c",
    "temperature_c_change",
    "temperature_c_rolling_mean_5",
    "temperature_c_rolling_std_5",
    "temperature_c_deviation",
    "vibration_mm_s",
    "vibration_mm_s_change",
    "vibration_mm_s_rolling_mean_5",
    "vibration_mm_s_rolling_std_5"
]


print(
    df[preview_columns]
    .head(15)
)


# ============================================================
# SAVE FEATURE DATASET
# ============================================================

FEATURE_FOLDER.mkdir(
    parents=True,
    exist_ok=True
)


df.to_parquet(
    FEATURE_FILE,
    index=False
)


print(
    "\nFeature dataset saved to:"
)

print(
    FEATURE_FILE
)


print(
    f"\nFinal feature dataset shape: "
    f"{df.shape}"
)

print(df["is_fault"].value_counts())
print(df["fault_type"].value_counts())