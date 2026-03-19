# HMI Dashboard

This project analyzes Human Machine Interaction (HMI) events across multiple locations and tracks trends over time.

## Features
- Weekly % change analysis
- Total % change (start to finish)
- Automatic outlier detection
- Interactive dashboard (Streamlit)

## How to Run

1. Install dependencies:# HMI analysis dashboard
    pip install -r requirements.txt


2. Run analysis scripts:
    python WithoutOutlier.py


3. Launch dashboard:
    streamlit run app.py

## Output
- summary_results.csv
- combined_weekly_analysis_no_outliers.csv
- per-mill weekly analysis files

## Notes
- Outliers are automatically detected using IQR
- Dates are normalized across mixed formats