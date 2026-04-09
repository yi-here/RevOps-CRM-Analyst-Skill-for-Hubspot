"""Chart generation for visual reports (optional — requires matplotlib)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def plot_pipeline_by_stage(stage_data: pd.DataFrame, output_path: str = "pipeline_stages.png") -> str:
    """Generate a horizontal bar chart of pipeline by stage."""
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return "matplotlib not installed — skipping chart generation"

    if stage_data.empty:
        return "No data to plot"

    fig, ax = plt.subplots(figsize=(10, 6))
    label_col = "stage_label" if "stage_label" in stage_data.columns else "dealstage"
    ax.barh(stage_data[label_col], stage_data["total_value"], color="#ff7a59")
    ax.set_xlabel("Pipeline Value ($)")
    ax.set_title("Pipeline by Stage")
    ax.invert_yaxis()

    for i, v in enumerate(stage_data["total_value"]):
        ax.text(v, i, f" ${v:,.0f}", va="center")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    return str(Path(output_path).resolve())


def plot_revenue_trend(monthly_data: pd.DataFrame, output_path: str = "revenue_trend.png") -> str:
    """Generate a line chart of monthly revenue."""
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return "matplotlib not installed — skipping chart generation"

    if monthly_data.empty:
        return "No data to plot"

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(monthly_data["month"], monthly_data["revenue"], marker="o", color="#ff7a59", linewidth=2)
    ax.fill_between(monthly_data["month"], monthly_data["revenue"], alpha=0.1, color="#ff7a59")
    ax.set_xlabel("Month")
    ax.set_ylabel("Revenue ($)")
    ax.set_title("Monthly Revenue Trend")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    return str(Path(output_path).resolve())


def plot_funnel(stage_counts: dict, output_path: str = "funnel.png") -> str:
    """Generate a funnel visualization."""
    try:
        import matplotlib.pyplot as plt
        import matplotlib.patches as patches
    except ImportError:
        return "matplotlib not installed — skipping chart generation"

    if not stage_counts:
        return "No data to plot"

    stages = list(stage_counts.keys())
    counts = list(stage_counts.values())
    max_count = max(counts) if counts else 1

    fig, ax = plt.subplots(figsize=(10, 8))

    for i, (stage, count) in enumerate(zip(stages, counts)):
        width = (count / max_count) * 0.8
        left = (1 - width) / 2
        rect = patches.FancyBboxPatch(
            (left, 1 - (i + 1) * (1 / len(stages))),
            width,
            0.8 / len(stages),
            boxstyle="round,pad=0.01",
            facecolor="#ff7a59",
            alpha=0.3 + 0.7 * (count / max_count),
            edgecolor="#333",
        )
        ax.add_patch(rect)
        ax.text(0.5, 1 - (i + 0.5) * (1 / len(stages)),
                f"{stage.replace('_', ' ').title()}\n{count:,}",
                ha="center", va="center", fontsize=12, fontweight="bold")

    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    ax.set_title("Conversion Funnel", fontsize=16, fontweight="bold")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    return str(Path(output_path).resolve())
