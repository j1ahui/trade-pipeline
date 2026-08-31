"""
Report generation for load test

Turns a list of RateStepResult into a printed summary, a CSV and a chart showing where throughput
falling behind the target rate
"""

import logging 
import matplotlib
matplotlib.use("Agg")                   # a backend determines how matplotlib produces/displays graph. agg generates graph as an image file rather than trying to open a graphical window
import matplotlib.pyplot as plt
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

REPORTS_DIR = Path(__file__).parent.parent / "reports"


def results_to_dataframe(results: list) -> pd.DataFrame:
    return pd.DataFrame([r.__dict__ for r in results])                  # creates a list of dicts, then turn those dicts into a DataFrame


def print_summary(results: list):
    df = results_to_dataframe(results)

    print("\n" + "=" * 70)
    print("LOAD TEST SUMMARY")
    print("=" * 70)

    for _, row in df.iterrows():
        status = "FELL BEHIND" if row["fell_behind"] else "kept up"
        print(f"target= {row["target_rate"]:>7.0f}/s actual={row["actual_rate"]:>7.1f}/s" f"max_pending= {row["max_pending"]:>6.0f} -> {status}")           # > = right align
    print("=" * 70)

    broke = df[df["fell_behind"]]                                           # accessing col then filtering based on condition inside []
    if not broke.empty:
        first_break = broke.iloc[0]
        print(f"\nBottleneck: consumers stopped keeping up somewhere around {first_break["target_rate"]:.0f} msgs/sec. \n")
    else:
        print(f"\nPipeline kept up at every tested rate, up to {df["target_rate"].max():.0f} msgs/sec. \n")


def save_report(results: list, filename: str = "load_test_report.csv") -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORTS_DIR / filename
    results_to_dataframe(results).to_csv(path, index=False)                 # to_csv() is pandas df method. it takes df and writes it to a csv file. saves df to location stored in path
    logger.info("Saved load test report to %s", path)
    return path


def save_chart(results: list, filename: str = "load_test_chart.png"):
    """
    Renders a PNG chart 
    """
    df = results_to_dataframe(results)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)                  # parents=True creates any missing parent directories needed to reach reports/
    path = REPORTS_DIR / filename

    fig, ax1 = plt.subplots(figsize=(8, 5))
    ax1.plot(df["target_rate"], df["actual_rate"], marker="o", label="Achieved rate (msg/s)")                   # plot(x_values, y_values)
    ax1.plot(df["target_rate"], df["target_rate"], linestyle="--", color="gray", label="Target rate (ideal)")
    ax1.set_xlabel("Target publish rate (msg/s)")
    ax1.set_ylabel("Rate (msg/s)")
    ax1.legend(loc="upper left")

    ax2 = ax1.twinx()                                               # creates second y-axis while sharing the same x-axis as ax1
    bar_width = max(df["target_rate"].max() * 0.03, 1)              # width will not be smaller than 1
    ax2.bar(df["target_rate"], df["max_pending"], alpha=0.25, width=bar_width, color="pink", label="Max backlog")
    ax2.set_ylabel("Max pending messages (backlog)")

    plt.title("Load Test: Throughput vs Backlog")
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)

    logger.info("Saved load test chart to %s", path)
    return path