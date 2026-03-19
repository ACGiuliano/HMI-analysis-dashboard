import pandas as pd
import glob
import os
import re

input_folder = "data"

# =========================
# SAME HELPERS (REUSE)
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
        return "%d-%m-%Y_%H:%M:%S"
    else:
        return "%m-%d-%Y_%H:%M:%S"


# =========================
# COMBINE ALL FILES
# =========================
files = glob.glob(os.path.join(input_folder, "*.csv"))

all_data = []

for file_path in files:
    try:
        df = pd.read_csv(file_path)

        if "Timestamp" not in df.columns or "Severity" not in df.columns:
            continue

        fmt = infer_file_date_format(df["Timestamp"])
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], format=fmt, errors="coerce")

        df = df.dropna(subset=["Timestamp"])

        # Filter High Severity
        df = df[df["Severity"].astype(str).str.strip().eq("High Severity")]

        if df.empty:
            continue

        all_data.append(df)

    except Exception as e:
        print(f"Error with {file_path}: {e}")

# =========================
# MERGE EVERYTHING
# =========================
combined_df = pd.concat(all_data, ignore_index=True)

# Sort + set index
combined_df = combined_df.sort_values("Timestamp")
combined_df = combined_df.set_index("Timestamp")

# =========================
# WEEKLY COUNTS
# =========================
weekly = combined_df.resample("W").size().to_frame(name="count")

# Add week boundaries
weekly["week_end"] = weekly.index
weekly["week_start"] = weekly["week_end"] - pd.Timedelta(days=6)

# =========================
# % CHANGE CALCULATIONS
# =========================
weekly["weekly_pct_change"] = weekly["count"].pct_change() * 100
weekly["weekly_pct_change"] = weekly["weekly_pct_change"].replace(
    [float("inf"), -float("inf")], pd.NA
)

start = weekly["count"].iloc[0]
end = weekly["count"].iloc[-1]

if start == 0:
    overall_pct_change = pd.NA
else:
    overall_pct_change = ((end - start) / start) * 100

# =========================
# OUTPUT
# =========================
print("\n=== COMBINED ANALYSIS ===")
print("Start count:", start)
print("End count:", end)
print("Overall % change:", round(overall_pct_change, 2), "%")

weekly_output = weekly.reset_index(drop=True)[
    ["week_start", "week_end", "count", "weekly_pct_change"]
]

weekly_output.to_csv("combined_weekly_analysis.csv", index=False)

print("\nSaved: combined_weekly_analysis.csv")