#!/usr/bin/env python3
"""
Analyze parsed broadcast logs and export insight tables + plots.
"""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def timecode_to_seconds(timecode: str) -> float:
    timecode = (timecode or "").strip()
    if not timecode:
        return float("nan")
    parts = timecode.split(":")
    try:
        values = [int(p) for p in parts]
    except ValueError:
        return float("nan")

    if len(values) == 4:
        hh, mm, ss, ff = values
        return hh * 3600 + mm * 60 + ss + ff / 30.0
    if len(values) == 3:
        a, b, c = values
        return a * 3600 + b * 60 + c
    if len(values) == 2:
        mm, ss = values
        return mm * 60 + ss
    if len(values) == 1:
        return float(values[0])
    return float("nan")


def clean_title_for_grouping(text: str) -> str:
    if pd.isna(text):
        text = ""
    text = str(text).strip()
    if not text:
        return "UNKNOWN"
    return " ".join(text.split())


def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    records: list[dict] = []

    # 1) Very short duration
    short_mask = df["duration_seconds"] < 5
    for _, row in df[short_mask].iterrows():
        records.append(
            {
                "anomaly_type": "very_short_duration",
                "log_date": row.get("log_date", ""),
                "sched_time": row.get("sched_time", ""),
                "video_source": row.get("video_source", ""),
                "type_code": row.get("type_code", ""),
                "title_raw": row.get("title_raw", ""),
                "duration_seconds": row.get("duration_seconds", 0.0),
                "details": "Duration is < 5 seconds",
            }
        )

    # 2) Very long duration
    long_mask = df["duration_seconds"] > (2 * 3600)
    for _, row in df[long_mask].iterrows():
        records.append(
            {
                "anomaly_type": "very_long_duration",
                "log_date": row.get("log_date", ""),
                "sched_time": row.get("sched_time", ""),
                "video_source": row.get("video_source", ""),
                "type_code": row.get("type_code", ""),
                "title_raw": row.get("title_raw", ""),
                "duration_seconds": row.get("duration_seconds", 0.0),
                "details": "Duration is > 2 hours",
            }
        )

    # 3) Missing/malformed fields
    malformed_mask = (
        (df["sched_time"].fillna("").str.strip() == "")
        | (df["duration_raw"].fillna("").str.strip() == "")
        | (df["type_code"].fillna("").str.strip() == "")
        | (df["parse_status"].fillna("ok") != "ok")
    )
    for _, row in df[malformed_mask].iterrows():
        records.append(
            {
                "anomaly_type": "missing_or_malformed_fields",
                "log_date": row.get("log_date", ""),
                "sched_time": row.get("sched_time", ""),
                "video_source": row.get("video_source", ""),
                "type_code": row.get("type_code", ""),
                "title_raw": row.get("title_raw", ""),
                "duration_seconds": row.get("duration_seconds", 0.0),
                "details": f"parse_status={row.get('parse_status', 'unknown')}; parse_error={row.get('parse_error', '')}",
            }
        )

    # 4) Repeated identical asset within short interval (<= 15 min)
    work = df.copy()
    work["sched_seconds"] = work["sched_time"].map(timecode_to_seconds)
    work["asset_key"] = (
        work["video_source"].fillna("").astype(str).str.strip()
        + "||"
        + work["title_raw"].fillna("").astype(str).str.strip()
    )
    work = work.sort_values(["log_date", "asset_key", "sched_seconds"])

    for _, group in work.groupby(["log_date", "asset_key"], dropna=False):
        if len(group) < 2:
            continue
        previous_time = None
        for _, row in group.iterrows():
            current_time = row.get("sched_seconds")
            if pd.notna(current_time) and previous_time is not None:
                gap = current_time - previous_time
                if 0 <= gap <= 900:
                    records.append(
                        {
                            "anomaly_type": "rapid_repeat_asset",
                            "log_date": row.get("log_date", ""),
                            "sched_time": row.get("sched_time", ""),
                            "video_source": row.get("video_source", ""),
                            "type_code": row.get("type_code", ""),
                            "title_raw": row.get("title_raw", ""),
                            "duration_seconds": row.get("duration_seconds", 0.0),
                            "details": f"Repeated within {gap:.1f} seconds",
                        }
                    )
            if pd.notna(current_time):
                previous_time = current_time

    if not records:
        return pd.DataFrame(
            columns=[
                "anomaly_type",
                "log_date",
                "sched_time",
                "video_source",
                "type_code",
                "title_raw",
                "duration_seconds",
                "details",
            ]
        )
    return pd.DataFrame(records)


def save_plots(
    category_summary: pd.DataFrame,
    type_frequency: pd.DataFrame,
    top_assets: pd.DataFrame,
    hourly_distribution: pd.DataFrame,
    anomalies: pd.DataFrame,
    plots_dir: Path,
) -> None:
    plots_dir.mkdir(parents=True, exist_ok=True)

    # 1) Pie chart: content distribution
    if not category_summary.empty:
        plt.figure(figsize=(8, 8))
        plt.pie(
            category_summary["duration_seconds"],
            labels=category_summary["content_category"],
            autopct="%1.1f%%",
        )
        plt.title("Airtime Distribution by Content Category")
        plt.tight_layout()
        plt.savefig(plots_dir / "content_distribution_pie.png", dpi=150)
        plt.close()

    # 2) Bar chart: type frequency
    if not type_frequency.empty:
        plt.figure(figsize=(10, 6))
        plt.bar(type_frequency["type_code"], type_frequency["row_count"])
        plt.title("Frequency by Type Code")
        plt.xlabel("Type Code")
        plt.ylabel("Count")
        plt.tight_layout()
        plt.savefig(plots_dir / "type_frequency_bar.png", dpi=150)
        plt.close()

    # 3) Bar chart: top repeated assets
    if not top_assets.empty:
        chart_data = top_assets.head(15).iloc[::-1]
        plt.figure(figsize=(12, 7))
        plt.barh(chart_data["asset_title"], chart_data["count"])
        plt.title("Top Repeated Assets")
        plt.xlabel("Occurrences")
        plt.ylabel("Asset")
        plt.tight_layout()
        plt.savefig(plots_dir / "top_repeated_assets_bar.png", dpi=150)
        plt.close()

    # 4) Hourly distribution (events and airtime)
    if not hourly_distribution.empty:
        fig, axes = plt.subplots(2, 1, figsize=(12, 9), sharex=True)
        axes[0].bar(hourly_distribution["hour_of_day"], hourly_distribution["events_count"])
        axes[0].set_title("Events per Hour")
        axes[0].set_ylabel("Events")

        axes[1].bar(hourly_distribution["hour_of_day"], hourly_distribution["airtime_seconds"])
        axes[1].set_title("Airtime per Hour")
        axes[1].set_xlabel("Hour of Day")
        axes[1].set_ylabel("Airtime (seconds)")

        plt.tight_layout()
        plt.savefig(plots_dir / "hourly_distribution.png", dpi=150)
        plt.close()

    # 5) Optional: anomaly counts
    if not anomalies.empty:
        anomaly_counts = (
            anomalies.groupby("anomaly_type", as_index=False).size().rename(columns={"size": "count"})
        )
        plt.figure(figsize=(10, 6))
        plt.bar(anomaly_counts["anomaly_type"], anomaly_counts["count"])
        plt.title("Anomaly Counts")
        plt.xlabel("Anomaly Type")
        plt.ylabel("Count")
        plt.xticks(rotation=20, ha="right")
        plt.tight_layout()
        plt.savefig(plots_dir / "anomalies_count_bar.png", dpi=150)
        plt.close()


def run_analysis(parsed_csv: Path, output_dir: Path) -> None:
    if not parsed_csv.exists():
        raise FileNotFoundError(f"Parsed CSV not found: {parsed_csv}")

    output_dir.mkdir(parents=True, exist_ok=True)
    plots_dir = output_dir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(parsed_csv)
    if df.empty:
        logger.warning("Parsed CSV is empty: %s", parsed_csv)

    for col in ["duration_seconds", "hour_of_day", "type_code", "content_category", "title_raw"]:
        if col not in df.columns:
            if col in {"duration_seconds", "hour_of_day"}:
                df[col] = 0
            else:
                df[col] = ""

    df["duration_seconds"] = pd.to_numeric(df["duration_seconds"], errors="coerce").fillna(0.0)
    df["hour_of_day"] = pd.to_numeric(df["hour_of_day"], errors="coerce").fillna(-1).astype(int)
    df["type_code"] = df["type_code"].fillna("UNKNOWN").astype(str).str.strip().replace("", "UNKNOWN")
    df["content_category"] = df["content_category"].fillna("Other").astype(str).str.strip().replace("", "Other")
    df["asset_title"] = df["title_raw"].map(clean_title_for_grouping)

    # 1) Airtime Distribution
    category_summary = (
        df.groupby("content_category", dropna=False, as_index=False)
        .agg(duration_seconds=("duration_seconds", "sum"), row_count=("content_category", "size"))
        .sort_values("duration_seconds", ascending=False)
    )
    category_summary.to_csv(output_dir / "summary_by_category.csv", index=False)

    # 2) Frequency Distribution by type_code
    type_frequency = (
        df.groupby("type_code", dropna=False, as_index=False)
        .size()
        .rename(columns={"size": "row_count"})
        .sort_values("row_count", ascending=False)
    )

    # 3) Top repeated assets
    top_assets = (
        df.groupby(["asset_title", "content_category"], dropna=False, as_index=False)
        .agg(count=("asset_title", "size"), total_duration_seconds=("duration_seconds", "sum"))
        .sort_values("count", ascending=False)
    )
    top_assets.to_csv(output_dir / "top_repeated_assets.csv", index=False)

    # 4) Hourly pattern
    hourly_distribution = (
        df[df["hour_of_day"].between(0, 23)]
        .groupby("hour_of_day", as_index=False)
        .agg(events_count=("hour_of_day", "size"), airtime_seconds=("duration_seconds", "sum"))
        .sort_values("hour_of_day")
    )
    # include missing hours
    hours = pd.DataFrame({"hour_of_day": list(range(24))})
    hourly_distribution = hours.merge(hourly_distribution, on="hour_of_day", how="left").fillna(0)
    hourly_distribution.to_csv(output_dir / "hourly_distribution.csv", index=False)

    # 5) Anomaly detection
    anomalies = detect_anomalies(df)
    anomalies.to_csv(output_dir / "anomalies.csv", index=False)

    save_plots(
        category_summary=category_summary,
        type_frequency=type_frequency,
        top_assets=top_assets,
        hourly_distribution=hourly_distribution,
        anomalies=anomalies,
        plots_dir=plots_dir,
    )

    logger.info("Saved summary_by_category.csv")
    logger.info("Saved top_repeated_assets.csv")
    logger.info("Saved hourly_distribution.csv")
    logger.info("Saved anomalies.csv")
    logger.info("Saved plots in %s", plots_dir)


def main() -> None:
    project_root = Path(__file__).resolve().parent
    parsed_csv = project_root / "data" / "output" / "parsed_logs.csv"
    output_dir = project_root / "data" / "output"
    run_analysis(parsed_csv, output_dir)


if __name__ == "__main__":
    main()
