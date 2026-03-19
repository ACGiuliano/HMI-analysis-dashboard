import pandas as pd
import glob
import os
import re

# =========================
# CONFIG
# =========================
input_folder = "Data"
output_folder = "output"
severity_filter = "High Severity"

os.makedirs(output_folder, exist_ok=True)


# =========================
# HELPERS
# =========================
def classify_timestamp_format(ts):
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
    classifications = timestamp_series.astype(str).map(classify_timestamp_format)
    counts = classifications.value_counts()

    dd_count = counts.get("dd-mm", 0)
    mm_count = counts.get("mm-dd", 0)

    if dd_count > mm_count:
        return "%d-%m-%Y_%H:%M:%S", "dd-mm"
    else:
        return "%m-%d-%Y_%H:%M:%S", "mm-dd"


def parse_file_timestamps(timestamp_series):
    fmt, fmt_label = infer_file_date_format(timestamp_series)
    parsed = pd.to_datetime(timestamp_series.astype(str).str.strip(), format=fmt, errors="coerce")
    return parsed, fmt_label


# =========================
# PASS 1: PER-FILE ANALYSIS
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

        # Parse timestamps per file
        parsed_ts, inferred_format_label = parse_file_timestamps(df["Timestamp"])
        df["Timestamp"] = parsed_ts

        bad_timestamp_count = df["Timestamp"].isna().sum()
        df = df.dropna(subset=["Timestamp"])

        # Filter to High Severity
        df = df[df["Severity"].astype(str).str.strip().eq(severity_filter)]

        if df.empty:
            print(f"  Skipped: no rows with Severity = '{severity_filter}'")
            continue

        # Sort + index
        df = df.sort_values("Timestamp")
        df = df.set_index("Timestamp")

        # Weekly counts
        weekly_counts = df.resample("W").size().to_frame(name="count")

        if weekly_counts.empty:
            print("  Skipped: no weekly data after resampling")
            continue

        # Week boundaries
        weekly_counts["week_end"] = weekly_counts.index
        weekly_counts["week_start"] = weekly_counts["week_end"] - pd.Timedelta(days=6)

        # Week-to-week % change
        weekly_counts["weekly_pct_change"] = weekly_counts["count"].pct_change() * 100
        weekly_counts["weekly_pct_change"] = weekly_counts["weekly_pct_change"].replace(
            [float("inf"), -float("inf")], pd.NA
        )

        # Start-to-finish % change
        start_value = weekly_counts["count"].iloc[0]
        end_value = weekly_counts["count"].iloc[-1]

        if start_value == 0:
            overall_pct_change = pd.NA
        else:
            overall_pct_change = ((end_value - start_value) / start_value) * 100

        if pd.isna(overall_pct_change):
            trend_direction = "Undefined"
        elif overall_pct_change < 0:
            trend_direction = "Downward"
        elif overall_pct_change > 0:
            trend_direction = "Upward"
        else:
            trend_direction = "Flat"

        base_name = os.path.splitext(os.path.basename(file_path))[0]
        weekly_output_path = os.path.join(output_folder, f"{base_name}_weekly_analysis.csv")

        weekly_output = weekly_counts.reset_index(drop=True)[
            ["week_start", "week_end", "count", "weekly_pct_change"]
        ].copy()

        # Include total % change in every row
        weekly_output["total_pct_change"] = overall_pct_change

        weekly_output.to_csv(weekly_output_path, index=False)

        summary_results.append({
            "file_name": base_name,
            "file_path": file_path,
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
        print(f"  Overall % change: {round(overall_pct_change, 2) if pd.notna(overall_pct_change) else 'N/A'}")

    except Exception as e:
        print(f"  Error processing {file_path}: {e}")

# Save initial summary
if not summary_results:
    print("\nNo valid files were processed.")
    raise SystemExit

summary_df = pd.DataFrame(summary_results)

# =========================
# PASS 2: AUTOMATIC OUTLIER DETECTION
# =========================
valid_changes = summary_df["overall_pct_change"].dropna()

if len(valid_changes) >= 4:
    q1 = valid_changes.quantile(0.25)
    q3 = valid_changes.quantile(0.75)
    iqr = q3 - q1

    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    summary_df["is_outlier"] = summary_df["overall_pct_change"].apply(
        lambda x: pd.notna(x) and (x < lower_bound or x > upper_bound)
    )
else:
    # Not enough files for reliable IQR detection
    q1 = q3 = iqr = lower_bound = upper_bound = pd.NA
    summary_df["is_outlier"] = False

summary_df["outlier_reason"] = summary_df.apply(
    lambda row: (
        f"overall_pct_change outside IQR bounds [{round(lower_bound, 2)}, {round(upper_bound, 2)}]"
        if row["is_outlier"] else ""
    ),
    axis=1
)

summary_df = summary_df.sort_values(by="overall_pct_change", na_position="last")

summary_path = os.path.join(output_folder, "summary_results.csv")
summary_df.to_csv(summary_path, index=False)

print("\n=== OUTLIER DETECTION ===")
if len(valid_changes) >= 4:
    print(f"Q1: {round(q1, 2)}")
    print(f"Q3: {round(q3, 2)}")
    print(f"IQR: {round(iqr, 2)}")
    print(f"Lower bound: {round(lower_bound, 2)}")
    print(f"Upper bound: {round(upper_bound, 2)}")
    print("\nDetected outliers:")
    print(summary_df.loc[summary_df["is_outlier"], ["file_name", "overall_pct_change"]])
else:
    print("Not enough valid files to run reliable IQR outlier detection.")

print(f"\nSummary saved to: {summary_path}")

# =========================
# PASS 3: COMBINED ANALYSIS EXCLUDING OUTLIERS
# =========================
included_files = summary_df.loc[~summary_df["is_outlier"], "file_path"].tolist()

all_data = []

for file_path in included_files:
    try:
        df = pd.read_csv(file_path)

        if "Timestamp" not in df.columns or "Severity" not in df.columns:
            continue

        parsed_ts, _ = parse_file_timestamps(df["Timestamp"])
        df["Timestamp"] = parsed_ts
        df = df.dropna(subset=["Timestamp"])

        df = df[df["Severity"].astype(str).str.strip().eq(severity_filter)]

        if df.empty:
            continue

        df["source_file"] = os.path.splitext(os.path.basename(file_path))[0]
        all_data.append(df)

    except Exception as e:
        print(f"Error reloading {file_path}: {e}")

if all_data:
    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df = combined_df.sort_values("Timestamp")
    combined_df = combined_df.set_index("Timestamp")

    combined_weekly = combined_df.resample("W").size().to_frame(name="count")
    combined_weekly["week_end"] = combined_weekly.index
    combined_weekly["week_start"] = combined_weekly["week_end"] - pd.Timedelta(days=6)
    combined_weekly["weekly_pct_change"] = combined_weekly["count"].pct_change() * 100
    combined_weekly["weekly_pct_change"] = combined_weekly["weekly_pct_change"].replace(
        [float("inf"), -float("inf")], pd.NA
    )

    combined_start = combined_weekly["count"].iloc[0]
    combined_end = combined_weekly["count"].iloc[-1]

    if combined_start == 0:
        combined_total_pct_change = pd.NA
    else:
        combined_total_pct_change = ((combined_end - combined_start) / combined_start) * 100

    combined_output = combined_weekly.reset_index(drop=True)[
        ["week_start", "week_end", "count", "weekly_pct_change"]
    ].copy()
    combined_output["total_pct_change"] = combined_total_pct_change

    combined_path = os.path.join(output_folder, "combined_weekly_analysis_no_outliers.csv")
    combined_output.to_csv(combined_path, index=False)

    print("\n=== COMBINED ANALYSIS (NO OUTLIERS) ===")
    print(f"Included files: {len(included_files)}")
    print(f"Start count: {combined_start}")
    print(f"End count: {combined_end}")
    print(f"Overall % change: {round(combined_total_pct_change, 2) if pd.notna(combined_total_pct_change) else 'N/A'}")
    print(f"Combined output saved to: {combined_path}")
else:
    print("\nNo non-outlier files available for combined analysis.")