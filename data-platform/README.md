# Data platform

The lakehouse uses a Bronze/Silver/Gold layout: immutable raw events, validated telemetry, and analytics-ready maintenance/production aggregates. Airflow orchestrates jobs; dbt owns SQL transformations and tests.
