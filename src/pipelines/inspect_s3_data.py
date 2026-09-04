import boto3
import gzip
import json
import pandas as pd


# ---------------------------------------------------
# Configuration
# ---------------------------------------------------

BUCKET_NAME = "sunny-iot-data-lake-2026"
PREFIX = "bronze/telemetry/"


# ---------------------------------------------------
# Connect to AWS
# ---------------------------------------------------

session = boto3.Session(profile_name="sunny-iot")

s3 = session.client("s3")


# ---------------------------------------------------
# Find telemetry files
# ---------------------------------------------------

response = s3.list_objects_v2(
    Bucket=BUCKET_NAME,
    Prefix=PREFIX
)

files = response.get("Contents", [])


if not files:
    print("No telemetry files found.")
    exit()


print(f"Telemetry files found: {len(files)}")


# ---------------------------------------------------
# Keep only .gz files
# ---------------------------------------------------

gzip_files = [
    file
    for file in files
    if file["Key"].endswith(".gz")
]


# ---------------------------------------------------
# Find latest telemetry file
# ---------------------------------------------------

latest_file = max(
    gzip_files,
    key=lambda x: x["LastModified"]
)

object_key = latest_file["Key"]

print("\nLatest file:")
print(object_key)


# ---------------------------------------------------
# Download file from S3
# ---------------------------------------------------

s3_object = s3.get_object(
    Bucket=BUCKET_NAME,
    Key=object_key
)

compressed_data = s3_object["Body"].read()


# ---------------------------------------------------
# Decompress GZIP
# ---------------------------------------------------

text_data = gzip.decompress(
    compressed_data
).decode("utf-8")


print("\nRaw telemetry preview:\n")
print(text_data[:1000])


# ---------------------------------------------------
# Parse concatenated JSON records
# ---------------------------------------------------

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
            f"JSON parsing stopped at "
            f"position {position}: {error}"
        )

        break


# ---------------------------------------------------
# Convert records into Pandas
# ---------------------------------------------------

df = pd.DataFrame(records)


# ---------------------------------------------------
# Inspect results
# ---------------------------------------------------

print("\nRecords parsed:")
print(len(records))


print("\nDataFrame shape:")
print(df.shape)


print("\nColumns:")
print(df.columns.tolist())


print("\nFirst 10 records:")
print(df.head(10))


print("\nData types:")
print(df.dtypes)