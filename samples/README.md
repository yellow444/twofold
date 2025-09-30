# Stage 0 Sample Dataset

This directory contains a compact CSV snapshot that mirrors the canonical Stage 0 ingestion schema for Rosaviatsiya drone flight reports. The file `rosaviation_sample.csv` provides representative rows that satisfy the baseline aggregation rules captured in `docs/requirements.md` and `plan.md`.

## File overview

| File | Description |
| --- | --- |
| `rosaviation_sample.csv` | Synthetic dataset covering multiple Russian regions and months for 2024, structured according to the Stage 0 metrics contract. |

## Schema and units

| Column | Type | Units / Format | Notes |
| --- | --- | --- | --- |
| `flight_id` | string | UUID-like string | Unique identifier used for deduplication (`flight_id`, `start_time`, `region_code`). |
| `start_time` | datetime | ISO 8601 UTC | Normalized timestamp of take-off in UTC to support time-based aggregation. |
| `end_time` | datetime | ISO 8601 UTC | Landing timestamp in UTC; combined with `start_time` to derive durations. |
| `duration_minutes` | integer | minutes | Pre-computed duration to simplify QA; Stage 0 logic can recompute to validate. |
| `region_code` | string | Rosstat/ISO-style code | Key for joining to region directory tables (e.g., `RU-MOW`). |
| `region_name` | string | UTF-8 text | Human-readable region label for quick inspection. |
| `latitude` | decimal | degrees | Take-off latitude; supports geo-quality checks and Stage 0 geofencing rules. |
| `longitude` | decimal | degrees | Take-off longitude; supports geo-quality checks and Stage 0 geofencing rules. |
| `vehicle_category` | string | enum | Differentiates quadrotor/fixed-wing/hybrid for optional breakdowns. |
| `operator_type` | string | enum | Enables Stage 0 grouping (commercial/government/research/etc.). |
| `flight_purpose` | string | enum | Provides downstream insight context without affecting Stage 0 totals. |
| `payload_type` | string | enum | Recorded payload classification; used for optional filters.

## How this sample supports Stage 0 metrics

The dataset spans multiple regions and months, enabling validation of the baseline aggregation rules:

1. **Flight count and deduplication:** Distinct `flight_id` values allow verification of unique flight counting across regions.
2. **Duration metrics:** `start_time`, `end_time`, and `duration_minutes` cover computation of total/average durations while enforcing the >0 minute rule.
3. **Monthly trends:** Records across January–March 2024 ensure month-over-month calculations are testable.
4. **Regional rankings:** Multiple `region_code` values support Top-N region summaries.
5. **Optional segmentations:** `vehicle_category` and `operator_type` provide the categorical dimensions referenced in Stage 0 planning documents.

Ingest agents can load this file directly to exercise validation, quality, and aggregation workflows before scaling to full production batches.
