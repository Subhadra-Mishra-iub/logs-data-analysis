# Broadcast Log Analytics & Monitoring System

An end-to-end Python pipeline for converting daily TV broadcast automation log PDFs into structured analytics outputs, anomaly reports, and visual summaries.

## Objective

Broadcast operations rely on precise timing, repeatable schedules, and consistent content playout.  
This project exists to:

- convert raw daily PDF logs into analysis-ready tabular data,
- monitor airtime utilization across content categories,
- identify repetition and scheduling patterns,
- detect operational anomalies early.

The result is a foundation for broadcast reliability monitoring and operational optimization.

## Dataset

- Source: daily TV program logs in `Logs/`
- Format: PDF files (example: `030226.pdf`, `030326.pdf`, ...)
- Scope used in this study: one-week sample (7 daily logs)
- Scale extension: same pipeline works for multi-week or multi-month archives by dropping additional PDFs into `Logs/`

Each row in the PDFs includes fields like:

- `SCHED TIME`
- `ACTUAL TIME`
- `LENGTH`
- `VIDEO SOURCE`
- `TYPE`
- `TITLE / SUBTITLE / NOTES`

## Methodology

### 1) PDF Extraction

- Primary extractor: `pdfplumber`
- Fallback extractor: `pymupdf` when primary extraction fails
- Handles multi-page documents and continuation lines

### 2) Data Cleaning

- Removes repeated headers/metadata (`PAGE`, `PRINTED`, `OPERATOR`, etc.)
- Detects new rows using schedule time pattern (`HH:MM:SS:FF`)
- Merges wrapped lines into the previous event row
- Preserves raw row text for traceability

### 3) Feature Engineering

Derived fields include:

- `log_date` (from filename)
- `duration_seconds`
- `hour_of_day`
- `content_category` (rule-based)
- parser diagnostics (`parse_status`, `parse_error`)

### 4) Rule-Based Classification

Type-to-category mapping:

- `PG` -> `Program`
- `PR` -> `Promo`
- `FI` -> `Filler`
- `ID` -> `ID`
- `GX` -> `Graphics`
- `AD` -> `Audio`
- `TX` -> `Text`
- `PS` -> `PublicService`
- else -> `Other`

### 5) Analysis

The analysis stage generates:

1. Airtime distribution by category
2. Frequency by type code
3. Top repeated assets
4. Hourly event and airtime distribution
5. Anomaly detection:
   - very short duration (< 5 sec)
   - very long duration (> 2 hours)
   - repeated identical asset within short interval (<= 15 minutes)
   - missing/malformed fields

## Project Structure

```text
logs-data-analysis/
├── Logs/
├── parse_broadcast_logs.py
├── analyze_broadcast_logs.py
├── requirements.txt
├── README.md
└── data/
    └── output/
        ├── parsed_logs.csv
        ├── summary_by_category.csv
        ├── top_repeated_assets.csv
        ├── hourly_distribution.csv
        ├── anomalies.csv
        └── plots/
            ├── content_distribution_pie.png
            ├── type_frequency_bar.png
            ├── top_repeated_assets_bar.png
            ├── hourly_distribution.png
            └── anomalies_count_bar.png
```

## Results from One-Week Sample

Based on generated outputs from `7` daily logs (`2316` parsed events):

- **Airtime distribution** shows `Filler` as the largest category, followed by `Program` and `Other`.
- **Type/event frequency** highlights recurring automation/support assets (for example station bug and lower-third graphics).
- **Repetition trends** show specific promos/graphics/text elements repeating frequently across days.
- **Hourly patterns** indicate strong activity concentration around daytime and early evening hours.
- **Anomaly report** captured repeated identical assets within short windows (`75` rapid-repeat events in this sample), useful for operational review.

## Findings & Insights

- Broadcast time is not only used for long-form programs; significant airtime is consumed by fillers, IDs, graphics, and continuity assets.
- Repeated short assets (promos/bugs/lower-thirds) form predictable operational blocks and can be monitored as health indicators.
- High repeat density may be intentional scheduling behavior, but also serves as a signal for potential over-rotation.
- A structured log-pipeline enables day-over-day consistency checks and supports evidence-based scheduling optimization.

## Operational Impact

This system can support broadcast engineers and operations teams by:

- creating a searchable historical record of playout behavior,
- surfacing anomalies before they become viewer-facing issues,
- quantifying airtime utilization by operational category,
- supporting tuning decisions for promo/filler strategy.

## Future Work

- Multi-week and multi-month trend dashboards
- Near real-time ingestion from live automation logs
- Alerting pipeline for anomaly thresholds
- ML-assisted anomaly scoring and pattern detection
- Channel-level or region-level comparison reports

## How to Run

## 1) Install dependencies

```bash
python -m pip install -r requirements.txt
```

## 2) Parse PDFs into structured CSV

```bash
python parse_broadcast_logs.py
```

Output:

- `data/output/parsed_logs.csv`

## 3) Run analysis + plots

```bash
python analyze_broadcast_logs.py
```

Outputs:

- `data/output/summary_by_category.csv`
- `data/output/top_repeated_assets.csv`
- `data/output/hourly_distribution.csv`
- `data/output/anomalies.csv`
- plots in `data/output/plots/`

## Notes on Robustness

- Parsing failures are retained with `parse_status="failed"` and `parse_error` details (no silent data drops).
- Continuation lines are merged to preserve full title/notes context.
- Raw row text is preserved (`full_row_text`) for debugging and auditability.

