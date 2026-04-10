"""Markdown report templates for RevOps metrics."""

from __future__ import annotations

import pandas as pd

from hubspot_revops.extractors.base import TimeRange

CURRENCY_SYMBOLS = {
    "USD": "$",
    "JPY": "¥",
    "EUR": "€",
    "GBP": "£",
}


def _fmt_currency(value: float) -> str:
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"${value / 1_000:.1f}K"
    return f"${value:,.0f}"


def _fmt_currency_with_code(value: float, code: str) -> str:
    """Format a value using the correct symbol for its currency code.

    JPY is shown without fractional digits since the minor unit is rarely
    used. Unknown codes fall back to ``{code} {amount}``.
    """
    code = (code or "USD").upper()
    symbol = CURRENCY_SYMBOLS.get(code)
    if symbol is None:
        if abs(value) >= 1_000_000:
            return f"{code} {value / 1_000_000:.2f}M"
        if abs(value) >= 1_000:
            return f"{code} {value / 1_000:.1f}K"
        return f"{code} {value:,.0f}"
    if abs(value) >= 1_000_000:
        return f"{symbol}{value / 1_000_000:.2f}M"
    if abs(value) >= 1_000:
        return f"{symbol}{value / 1_000:.1f}K"
    return f"{symbol}{value:,.0f}"


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

    lines.append(
        "| Rep | Currency | Pipeline | Open | Won Rev | Lost Rev | Won | Lost | Win % | Loss % | Avg Size |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|")
    for _, row in scorecard.iterrows():
        code = row.get("currency", "USD")
        lines.append(
            f"| {row['rep_name']} "
            f"| {code} "
            f"| {_fmt_currency_with_code(row['open_pipeline'], code)} "
            f"| {int(row['open_deals'])} "
            f"| {_fmt_currency_with_code(row['closed_won_revenue'], code)} "
            f"| {_fmt_currency_with_code(row.get('lost_revenue', 0), code)} "
            f"| {int(row['deals_won'])} "
            f"| {int(row.get('deals_lost', 0))} "
            f"| {row['win_rate']}% "
            f"| {row.get('loss_rate', 0)}% "
            f"| {_fmt_currency_with_code(row['avg_deal_size'], code)} |"
        )

    return "\n".join(lines)


def format_closed_lost_report(data: dict, tr: TimeRange) -> str:
    lines = [
        "# Closed-Lost Report",
        f"**Period:** {_period_str(tr)}\n",
    ]

    total_deals = data.get("total_lost_deals", 0)
    if total_deals == 0:
        lines.append("_No closed-lost deals in this period._")
        return "\n".join(lines)

    coverage = data.get("lost_reason_coverage", 1.0)
    if data.get("coverage_warning"):
        pct = int(round(coverage * 100))
        lines.append(
            f"> ⚠️  **Data quality warning:** only {pct}% of lost deals have a "
            f"`closed_lost_reason` populated. Reason breakdown below is unreliable. "
            f"Consider making `closed_lost_reason` a required field in HubSpot."
        )
        lines.append("")

    lines.extend([
        f"- **Total lost deals:** {total_deals}",
        f"- **Total lost value:** {_fmt_currency(data.get('total_lost_value', 0))}",
        f"- **Ghost deals (zero engagement):** {data.get('ghost_deal_count', 0)}",
        f"- **Lost-reason coverage:** {int(round(coverage * 100))}%",
        "",
    ])

    rep_scorecard = data.get("rep_scorecard")
    if isinstance(rep_scorecard, pd.DataFrame) and not rep_scorecard.empty:
        lines.append("## Lost deals by rep\n")
        lines.append("| Rep | Deals Lost | Lost Value | Avg Lost Deal |")
        lines.append("|---|---|---|---|")
        for _, row in rep_scorecard.iterrows():
            lines.append(
                f"| {row['rep_name']} "
                f"| {int(row['deals_lost'])} "
                f"| {_fmt_currency(row['lost_value'])} "
                f"| {_fmt_currency(row['avg_lost_deal'])} |"
            )

    reason_breakdown = data.get("reason_breakdown")
    if isinstance(reason_breakdown, pd.DataFrame) and not reason_breakdown.empty:
        lines.append("\n## Reasons\n")
        lines.append("| Reason | Deals | Value |")
        lines.append("|---|---|---|")
        for _, row in reason_breakdown.iterrows():
            lines.append(
                f"| {row['closed_lost_reason']} "
                f"| {int(row['deals_lost'])} "
                f"| {_fmt_currency(row['lost_value'])} |"
            )

    return "\n".join(lines)


def format_forecast_report(data: dict) -> str:
    tr = data.get("period")
    period_label = f"{tr.start.strftime('%Y-%m')}" if tr else "Current month"

    lines = [
        "# Forecast Report",
        f"**Month:** {period_label}\n",
    ]

    per_rep = data.get("per_rep")
    totals = data.get("totals_by_bucket")
    currencies = data.get("currencies") or []

    if not isinstance(per_rep, pd.DataFrame) or per_rep.empty:
        lines.append("_No open deals with a close date in the current month._")
        return "\n".join(lines)

    for code in currencies:
        lines.append(f"## {code}\n")
        rep_rows = per_rep[per_rep["currency"] == code]
        if rep_rows.empty:
            lines.append("_(no deals)_\n")
            continue

        pivot = rep_rows.pivot_table(
            index="rep_name",
            columns="bucket",
            values="value",
            fill_value=0,
            aggfunc="sum",
        )
        # Ensure deterministic bucket order.
        for bucket in ("Commit", "Highly Likely", "Best Case"):
            if bucket not in pivot.columns:
                pivot[bucket] = 0
        pivot = pivot[["Commit", "Highly Likely", "Best Case"]]

        lines.append("| Rep | Commit | Highly Likely | Best Case | Total |")
        lines.append("|---|---|---|---|---|")
        for rep, row in pivot.iterrows():
            total = row.sum()
            lines.append(
                f"| {rep} "
                f"| {_fmt_currency_with_code(row['Commit'], code)} "
                f"| {_fmt_currency_with_code(row['Highly Likely'], code)} "
                f"| {_fmt_currency_with_code(row['Best Case'], code)} "
                f"| {_fmt_currency_with_code(total, code)} |"
            )

        if isinstance(totals, pd.DataFrame) and not totals.empty:
            currency_totals = totals[totals["currency"] == code]
            if not currency_totals.empty:
                lines.append("\n**Totals:**")
                for _, row in currency_totals.iterrows():
                    lines.append(
                        f"- {row['bucket']}: {_fmt_currency_with_code(row['value'], code)} "
                        f"({int(row['deals'])} deals)"
                    )
        lines.append("")

    return "\n".join(lines)


def format_meeting_history_report(data: dict, tr: TimeRange) -> str:
    lines = [
        "# Meeting History Report",
        f"**Period:** {_period_str(tr)}\n",
    ]

    analyzed = data.get("closed_deals_analyzed", 0)
    if analyzed == 0:
        lines.append("_No closed deals in this period to analyze._")
        return "\n".join(lines)

    lines.extend([
        f"- **Closed deals analyzed:** {analyzed}",
        f"- **Total meetings:** {data.get('total_meetings', 0)}",
    ])

    ttc = data.get("time_to_close", {})
    lines.extend([
        f"- **Median days first meeting → close (won):** {ttc.get('median_days_won', 0):.1f}",
        f"- **Median days first meeting → close (lost):** {ttc.get('median_days_lost', 0):.1f}",
        "",
    ])

    per_rep = data.get("per_rep")
    if isinstance(per_rep, pd.DataFrame) and not per_rep.empty:
        lines.append("## Avg meetings per deal by rep\n")
        lines.append("| Rep | Avg (Won) | Won Deals | Avg (Lost) | Lost Deals |")
        lines.append("|---|---|---|---|---|")
        for _, row in per_rep.iterrows():
            lines.append(
                f"| {row['rep_name']} "
                f"| {row.get('avg_meetings_won', 0):.1f} "
                f"| {int(row.get('won_deals', 0))} "
                f"| {row.get('avg_meetings_lost', 0):.1f} "
                f"| {int(row.get('lost_deals', 0))} |"
            )

    effort_sinks = data.get("effort_sinks")
    if isinstance(effort_sinks, pd.DataFrame) and not effort_sinks.empty:
        lines.append("\n## Effort sinks — most meetings on lost deals\n")
        header_cols = [c for c in ["dealname", "rep_name", "amount", "meeting_count", "closedate"] if c in effort_sinks.columns]
        if header_cols:
            lines.append("| " + " | ".join(h.replace("_", " ").title() for h in header_cols) + " |")
            lines.append("|" + "|".join(["---"] * len(header_cols)) + "|")
            for _, row in effort_sinks.iterrows():
                cells = []
                for col in header_cols:
                    val = row.get(col, "")
                    if col == "amount":
                        try:
                            cells.append(_fmt_currency(float(val)))
                        except (TypeError, ValueError):
                            cells.append(str(val))
                    elif col == "meeting_count":
                        cells.append(str(int(val)))
                    else:
                        cells.append(str(val))
                lines.append("| " + " | ".join(cells) + " |")

    return "\n".join(lines)
