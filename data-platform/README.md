# Data platform

The lakehouse uses a Bronze/Silver/Gold layout: immutable raw events, validated telemetry, and analytics-ready maintenance/production aggregates. Airflow orchestrates jobs; dbt owns SQL transformations and tests.

## Silver to Gold batch pipeline

Run the local Gold transformation after the Silver Parquet dataset is built:

```powershell
python -m src.pipelines.gold_pipeline
```

The job accepts only records marked `data_quality_pass`, removes duplicate
machine/timestamp observations, and writes:

- `data/gold/maintenance_daily.parquet` — daily machine health, sensor, and
  fault-event KPIs.
- `data/gold/production_hourly.parquet` — hourly availability, runtime,
  downtime, RPM, and estimated-revolution KPIs.
- `data/gold/gold_pipeline_manifest.json` — lineage, row counts, time range,
  settings, and output locations for the run.

Use `--input`, `--output-dir`, `--max-observation-gap-seconds`, and repeated
`--active-status` options to override the defaults. Runtime and downtime are
bounded by the observation-gap setting so missing telemetry is not counted as
machine time. OEE and production-unit counts are not derived because those
measures are not present in the Silver telemetry contract.
