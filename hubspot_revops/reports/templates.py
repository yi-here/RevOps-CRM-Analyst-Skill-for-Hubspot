"""Markdown report templates for RevOps metrics."""

from __future__ import annotations

import pandas as pd

from hubspot_revops.extractors.base import TimeRange


def _fmt_currency(value: float) -> str:
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"${value / 1_000:.1f}K"
    return f"${value:,.0f}"


def _period_str(tr: TimeRange) -> str:
    return f"{tr.start.strftime('%Y-%m-%d')} to {tr.end.strftime('%Y-%m-%d')}"


def format_executive_summary(data: dict, tr: TimeRange) -> str:
    p = data["pipeline"]
    wr = data["win_rate"]
    ads = data["avg_deal_size"]
    vel = data["velocity"]
    rev = data["revenue"]
    wt = data["weighted"]

    return f"""# Executive Summary
**Period:** {_period_str(tr)}

## Pipeline
| Metric | Value |
|---|---|
| Open Pipeline | {_fmt_currency(p['total_value'])} |
| Open Deals | {p['total_deals']} |
| Weighted Pipeline | {_fmt_currency(wt['weighted_value'])} |
| Avg Deal Size | {_fmt_currency(p.get('avg_deal_size', 0))} |

## Performance
| Metric | Value |
|---|---|
| Closed Revenue | {_fmt_currency(rev['total_revenue'])} |
| Deals Won | {wr['won']} |
| Win Rate | {wr['win_rate']}% |
| Avg Deal Size (Won) | {_fmt_currency(ads['avg_deal_size'])} |
| Avg Sales Cycle | {vel['avg_cycle_days']:.0f} days |
| Pipeline Velocity | {_fmt_currency(vel['velocity_per_month'])}/month |
"""


def format_pipeline_report(data: dict, tr: TimeRange) -> str:
    total = data["total"]
    wr = data["win_rate"]
    vel = data["velocity"]
    cycle = data["cycle"]

    lines = [
        f"# Pipeline Report",
        f"**Period:** {_period_str(tr)}\n",
        f"**Total Open Pipeline:** {_fmt_currency(total['total_value'])} ({total['total_deals']} deals)\n",
    ]

    by_stage = data["by_stage"]
    if not isinstance(by_stage, pd.DataFrame) or by_stage.empty:
        lines.append("_No stage breakdown available._\n")
    else:
        lines.append("## By Stage\n")
        lines.append("| Stage | Deals | Value | Avg Size |")
        lines.append("|---|---|---|---|")
        for _, row in by_stage.iterrows():
            lines.append(
                f"| {row.get('stage_label', row.get('dealstage', 'N/A'))} "
                f"| {int(row['deal_count'])} "
                f"| {_fmt_currency(row['total_value'])} "
                f"| {_fmt_currency(row['avg_value'])} |"
            )

    lines.extend([
        f"\n## Key Metrics\n",
        f"- **Win Rate:** {wr['win_rate']}% ({wr['won']}W / {wr['lost']}L)",
        f"- **Avg Sales Cycle:** {cycle['avg_days']:.0f} days (median: {cycle['median_days']:.0f})",
        f"- **Pipeline Velocity:** {_fmt_currency(vel['velocity_per_month'])}/month",
    ])

    return "\n".join(lines)


def format_revenue_report(data: dict, tr: TimeRange) -> str:
    rev = data["closed"]
    mrr = data["mrr_arr"]

    lines = [
        f"# Revenue Report",
        f"**Period:** {_period_str(tr)}\n",
        f"## Closed Revenue\n",
        f"- **Total:** {_fmt_currency(rev['total_revenue'])}",
        f"- **Deals:** {rev['deal_count']}",
        f"- **Avg Deal Size:** {_fmt_currency(rev.get('avg_deal_size', 0))}",
        f"- **Largest Deal:** {_fmt_currency(rev.get('max_deal', 0))}",
    ]

    if mrr["mrr"] > 0 or mrr["arr"] > 0:
        lines.extend([
            f"\n## Recurring Revenue\n",
            f"- **MRR:** {_fmt_currency(mrr['mrr'])}",
            f"- **ARR:** {_fmt_currency(mrr['arr'])}",
        ])

    by_owner = data["by_owner"]
    if isinstance(by_owner, pd.DataFrame) and not by_owner.empty:
        lines.append("\n## Revenue by Rep\n")
        lines.append("| Rep | Revenue | Deals | Avg Size |")
        lines.append("|---|---|---|---|")
        for _, row in by_owner.iterrows():
            lines.append(
                f"| {row['owner_name']} "
                f"| {_fmt_currency(row['total_revenue'])} "
                f"| {int(row['deal_count'])} "
                f"| {_fmt_currency(row['avg_deal_size'])} |"
            )

    return "\n".join(lines)


def format_funnel_report(data: dict, tr: TimeRange) -> str:
    funnel = data["funnel"]

    lines = [
        f"# Funnel Report",
        f"**Period:** {_period_str(tr)}\n",
        f"**Total Contacts:** {funnel['total_contacts']}\n",
    ]

    if funnel.get("stages"):
        lines.append("## Lifecycle Stages\n")
        lines.append("| Stage | Count |")
        lines.append("|---|---|")
        for stage, count in funnel["stages"].items():
            lines.append(f"| {stage.replace('_', ' ').title()} | {count} |")

    if funnel.get("conversions"):
        lines.append("\n## Conversion Rates\n")
        lines.append("| Step | From | To | Rate |")
        lines.append("|---|---|---|---|")
        for step, info in funnel["conversions"].items():
            label = step.replace("_to_", " → ").replace("_", " ").title()
            lines.append(
                f"| {label} | {info['from_count']} | {info['to_count']} | {info['conversion_rate']}% |"
            )

    sources = data["sources"]
    if isinstance(sources, pd.DataFrame) and not sources.empty:
        lines.append("\n## Lead Sources\n")
        lines.append("| Source | Contacts |")
        lines.append("|---|---|")
        for _, row in sources.iterrows():
            lines.append(f"| {row['hs_analytics_source']} | {int(row['contact_count'])} |")

    return "\n".join(lines)


def format_rep_scorecard(scorecard: pd.DataFrame, tr: TimeRange) -> str:
    lines = [
        f"# Rep Scorecard",
        f"**Period:** {_period_str(tr)}\n",
    ]

    if scorecard.empty:
        lines.append("_No data available._")
        return "\n".join(lines)

    lines.append("| Rep | Pipeline | Open Deals | Won Revenue | Won | Win Rate | Avg Size |")
    lines.append("|---|---|---|---|---|---|---|")
    for _, row in scorecard.iterrows():
        lines.append(
            f"| {row['rep_name']} "
            f"| {_fmt_currency(row['open_pipeline'])} "
            f"| {int(row['open_deals'])} "
            f"| {_fmt_currency(row['closed_won_revenue'])} "
            f"| {int(row['deals_won'])} "
            f"| {row['win_rate']}% "
            f"| {_fmt_currency(row['avg_deal_size'])} |"
        )

    return "\n".join(lines)
