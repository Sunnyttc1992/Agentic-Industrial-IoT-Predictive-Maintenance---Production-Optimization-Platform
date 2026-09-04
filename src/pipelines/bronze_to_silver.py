import boto3
import gzip
import json
import pandas as pd

from pathlib import Path


# ============================================================
# CONFIGURATION
# ============================================================

BUCKET_NAME = "sunny-iot-data-lake-2026"

BRONZE_PREFIX = "bronze/telemetry/"

AWS_PROFILE = "sunny-iot"


# Find project root automatically
PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_FOLDER = PROJECT_ROOT / "data" / "silver"

SILVER_FILE = SILVER_FOLDER / "telemetry_clean.parquet"


# ============================================================
# FUNCTION: Parse concatenated JSON
# ============================================================

def parse_concatenated_json(text_data):

    decoder = json.JSONDecoder()

    records = []

    position = 0

    while position < len(text_data):

        # Skip whitespace
        while (
            position < len(text_data)
            and text_data[position].isspace()
        ):
            position += 1

        if position >= len(text_data):
            break

        try:

            record, next_position = decoder.raw_decode(
                text_data,
                position
            )

            records.append(record)

            position = next_position

        except json.JSONDecodeError as error:

            print(
                f"JSON parsing error at "
                f"position {position}: {error}"
            )

            break

    return records


# ============================================================
# CONNECT TO AWS
# ============================================================

session = boto3.Session(
    profile_name=AWS_PROFILE
)

s3 = session.client("s3")


# ============================================================
# FIND ALL BRONZE FILES
# ============================================================

print("Finding Bronze telemetry files...")

paginator = s3.get_paginator(
    "list_objects_v2"
)

pages = paginator.paginate(
    Bucket=BUCKET_NAME,
    Prefix=BRONZE_PREFIX
)


bronze_files = []


for page in pages:

    for file in page.get("Contents", []):

        if file["Key"].endswith(".gz"):

            bronze_files.append(file["Key"])


print(
    f"Bronze files found: "
    f"{len(bronze_files)}"
)


# ============================================================
# READ ALL BRONZE FILES
# ============================================================

all_records = []


for file_number, object_key in enumerate(
    bronze_files,
    start=1
):

    print(
        f"Reading file "
        f"{file_number}/{len(bronze_files)}"
    )

    s3_object = s3.get_object(
        Bucket=BUCKET_NAME,
        Key=object_key
    )

    compressed_data = (
        s3_object["Body"].read()
    )

    text_data = gzip.decompress(
        compressed_data
    ).decode("utf-8")

    records = parse_concatenated_json(
        text_data
    )

    # Add lineage information
    for record in records:

        record["source_file"] = object_key

    all_records.extend(records)


print(
    f"\nTotal records loaded: "
    f"{len(all_records)}"
)


# ============================================================
# CREATE DATAFRAME
# ============================================================

df = pd.DataFrame(
    all_records
)


print(
    f"Raw DataFrame shape: "
    f"{df.shape}"
)


# ============================================================
# DATA QUALITY - REQUIRED COLUMNS
# ============================================================

required_columns = [
    "machine_id",
    "timestamp",
    "temperature_c",
    "vibration_mm_s",
    "current_amp",
    "pressure_bar",
    "rpm",
    "status",
    "machine_id_from_topic",
    "ingestion_timestamp",
    "fault_type",
    "is_fault"
]


missing_columns = [

    column
    for column in required_columns
    if column not in df.columns

]


if missing_columns:

    raise ValueError(
        f"Missing required columns: "
        f"{missing_columns}"
    )


# ============================================================
# CONVERT TIMESTAMPS
# ============================================================

df["timestamp"] = pd.to_datetime(
    df["timestamp"],
    utc=True,
    errors="coerce"
)


df["ingestion_timestamp"] = pd.to_datetime(
    df["ingestion_timestamp"],
    unit="ms",
    utc=True,
    errors="coerce"
)


# ============================================================
# DATA TYPE CLEANING
# ============================================================

numeric_columns = [

    "temperature_c",
    "vibration_mm_s",
    "current_amp",
    "pressure_bar",
    "rpm"

]


for column in numeric_columns:

    df[column] = pd.to_numeric(
        df[column],
        errors="coerce"
    )


# ============================================================
# MACHINE ID VALIDATION
# ============================================================

df["machine_id_match"] = (

    df["machine_id"]
    ==
    df["machine_id_from_topic"]

)


# ============================================================
# PIPELINE LATENCY
# ============================================================

df["ingestion_latency_ms"] = (

    (
        df["ingestion_timestamp"]
        -
        df["timestamp"]
    )
    .dt.total_seconds()
    * 1000

)


# ============================================================
# CHECK MISSING VALUES
# ============================================================

df["has_missing_values"] = (

    df[required_columns]
    .isna()
    .any(axis=1)

)


# ============================================================
# SENSOR SANITY CHECK
# ============================================================

#
# IMPORTANT:
# These are broad physical sanity checks,
# NOT normal operating thresholds.
#
# We do NOT want to remove useful anomalies.
#

df["sensor_values_valid"] = (

    df["temperature_c"].between(
        -50,
        200
    )

    &

    df["vibration_mm_s"].between(
        0,
        100
    )

    &

    df["current_amp"].between(
        0,
        500
    )

    &

    df["pressure_bar"].between(
        0,
        100
    )

    &

    df["rpm"].between(
        0,
        10000
    )

)


# ============================================================
# OVERALL DATA QUALITY FLAG
# ============================================================

df["data_quality_pass"] = (

    df["machine_id_match"]

    &

    ~df["has_missing_values"]

    &

    df["sensor_values_valid"]

)


# ============================================================
# CHECK DUPLICATES
# ============================================================

duplicate_count = df.duplicated(
    subset=[
        "machine_id",
        "timestamp"
    ]
).sum()


print(
    f"Duplicate records found: "
    f"{duplicate_count}"
)


# Keep first copy
df = df.drop_duplicates(

    subset=[
        "machine_id",
        "timestamp"
    ],

    keep="first"

)


# ============================================================
# SORT DATA
# ============================================================

df = df.sort_values(

    by=[
        "machine_id",
        "timestamp"
    ]

)


df = df.reset_index(
    drop=True
)


# ============================================================
# DATA QUALITY REPORT
# ============================================================

print("\n==============================")
print("DATA QUALITY REPORT")
print("==============================")


print(
    f"Rows after deduplication: "
    f"{len(df)}"
)


print(
    f"Machines: "
    f"{df['machine_id'].nunique()}"
)


print(
    f"Missing-value rows: "
    f"{df['has_missing_values'].sum()}"
)


print(
    f"Machine ID mismatches: "
    f"{(~df['machine_id_match']).sum()}"
)


print(
    f"Invalid sensor rows: "
    f"{(~df['sensor_values_valid']).sum()}"
)


print(
    f"Quality-pass rows: "
    f"{df['data_quality_pass'].sum()}"
)


print(
    "\nRecords per machine:"
)

print(
    df["machine_id"]
    .value_counts()
    .sort_index()
)


print(
    "\nSensor summary:"
)

print(
    df[numeric_columns]
    .describe()
)


# ============================================================
# CREATE SILVER DIRECTORY
# ============================================================

SILVER_FOLDER.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# SAVE SILVER DATA
# ============================================================

df.to_parquet(

    SILVER_FILE,

    index=False

)


print(
    f"\nSilver dataset saved to:"
)

print(
    SILVER_FILE
)


print(
    f"\nFinal Silver shape: "
    f"{df.shape}"
)


