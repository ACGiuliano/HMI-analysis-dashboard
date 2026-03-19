import pandas as pd
import glob
import os
import re

# =========================
# CONFIG
# =========================
input_folder = "Data"        # folder with your CSV files
output_folder = "output"     # folder for results
severity_filter = "High Severity"

os.makedirs(output_folder, exist_ok=True)


# =========================
# HELPERS
# =========================
def classify_timestamp_format(ts):
    """
    Classify a timestamp string like 19-03-2026_07:26:05 or 03-19-2026_07:26:05

    Returns:
    - 'dd-mm' if definitely DD-MM-YYYY
    - 'mm-dd' if definitely MM-DD-YYYY
    - 'ambiguous' if both first and second parts are <= 12
    - 'other' if malformed
    """
    ts = str(ts).strip()
    m = re.match(r"^(\d{2})-(\d{2})-(\d{4})_(\d{2}):(\d{2}):(\d{2})$", ts)
    if not m:
        return "other"

    first = int(m.group(1))
    second = int(m.group(2))

    if first > 12 and second <= 12:
        return "dd-mm"
    elif second > 12 and first <= 12:
        return "mm-dd"
    elif first <= 12 and second <= 12:
        return "ambiguous"
    else:
        return "other"


def infer_file_date_format(timestamp_series):
    """
    Infer the most likely file-wide date format from non-ambiguous rows.
    Defaults to mm-dd if nothing definite is found.
    """
    classifications = timestamp_series.astype(str).map(classify_timestamp_format)
    counts = classifications.value_counts()

    dd_count = counts.get("dd-mm", 0)
    mm_count = counts.get("mm-dd", 0)

    if dd_count > mm_count:
        return "%d-%m-%Y_%H:%M:%S", "dd-mm"
    else:
        return "%m-%d-%Y_%H:%M:%S", "mm-dd"


def parse_file_timestamps(timestamp_series):
    """
    Infer one format for the whole file, then parse using that format.
    Returns:
    - parsed timestamps
    - format label
    """
    fmt, fmt_label = infer_file_date_format(timestamp_series)
    parsed = pd.to_datetime(timestamp_series.astype(str).str.strip(), format=fmt, errors="coerce")
    return parsed, fmt_label


# =========================
# MAIN
# =========================
files = glob.glob(os.path.join(input_folder, "*.csv"))
summary_results = []

if not files:
    print(f"No CSV files found in '{input_folder}'")

for file_path in files:
    try:
        print(f"\nProcessing: {file_path}")

        df = pd.read_csv(file_path)

        required_cols = {"Timestamp", "Severity"}
        missing_cols = required_cols - set(df.columns)
        if missing_cols:
            print(f"  Skipped: missing columns {missing_cols}")
            continue

        # Infer and parse timestamps per file
        parsed_ts, inferred_format_label = parse_file_timestamps(df["Timestamp"])
        df["Timestamp"] = parsed_ts

        bad_timestamp_count = df["Timestamp"].isna().sum()
        df = df.dropna(subset=["Timestamp"])

        # Filter severity
        df = df[df["Severity"].astype(str).str.strip().eq(severity_filter)]

        if df.empty:
            print(f"  Skipped: no rows with Severity = '{severity_filter}'")
            continue

        # Sort and index
        df = df.sort_values("Timestamp")
        df = df.set_index("Timestamp")

        # Weekly counts
        weekly_counts = df.resample("W").size().to_frame(name="count")

        if weekly_counts.empty:
            print("  Skipped: no weekly data after resampling")
            continue

        # Add readable week boundaries
        weekly_counts["week_end"] = weekly_counts.index
        weekly_counts["week_start"] = weekly_counts["week_end"] - pd.Timedelta(days=6)

        # Week-to-week percent change
        weekly_counts["weekly_pct_change"] = weekly_counts["count"].pct_change() * 100
        weekly_counts["weekly_pct_change"] = weekly_counts["weekly_pct_change"].replace(
            [float("inf"), -float("inf")], pd.NA
        )

        # Start-to-finish percent change
        start_value = weekly_counts["count"].iloc[0]
        end_value = weekly_counts["count"].iloc[-1]

        if start_value == 0:
            overall_pct_change = pd.NA
        else:
            overall_pct_change = ((end_value - start_value) / start_value) * 100

        # Trend direction
        if pd.isna(overall_pct_change):
            trend_direction = "Undefined"
        elif overall_pct_change < 0:
            trend_direction = "Downward"
        elif overall_pct_change > 0:
            trend_direction = "Upward"
        else:
            trend_direction = "Flat"

        # Output path
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        weekly_output_path = os.path.join(output_folder, f"{base_name}_weekly_analysis.csv")

        weekly_output = weekly_counts.reset_index(drop=True)[
            ["week_start", "week_end", "count", "weekly_pct_change"]
        ]
        weekly_output.to_csv(weekly_output_path, index=False)

        summary_results.append({
            "file_name": base_name,
            "inferred_date_format": inferred_format_label,
            "start_week": weekly_output["week_start"].iloc[0],
            "end_week": weekly_output["week_end"].iloc[-1],
            "start_week_count": start_value,
            "end_week_count": end_value,
            "overall_pct_change": overall_pct_change,
            "trend_direction": trend_direction,
            "num_high_severity_events": len(df),
            "num_weeks": len(weekly_output),
            "bad_timestamps_dropped": int(bad_timestamp_count),
            "weekly_output_file": weekly_output_path
        })

        print(f"  Done: {base_name}")
        print(f"  Inferred format:   {inferred_format_label}")
        print(f"  Bad timestamps:    {bad_timestamp_count}")
        print(f"  Start week count:  {start_value}")
        print(f"  End week count:    {end_value}")
        print(f"  Overall % change:  {round(overall_pct_change, 2) if pd.notna(overall_pct_change) else 'N/A'}")

    except Exception as e:
        print(f"  Error processing {file_path}: {e}")

# Save summary
if summary_results:
    summary_df = pd.DataFrame(summary_results)
    summary_df = summary_df.sort_values(by="overall_pct_change", na_position="last")

    summary_path = os.path.join(output_folder, "summary_results.csv")
    summary_df.to_csv(summary_path, index=False)

    print("\nAll processing complete.")
    print(f"Summary saved to: {summary_path}")
    print("\nSummary preview:")
    print(summary_df)
else:
    print("\nNo valid files were processed.")