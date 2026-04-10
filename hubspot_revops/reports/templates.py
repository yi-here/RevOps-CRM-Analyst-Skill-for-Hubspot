"""Markdown report templates for RevOps metrics."""

from __future__ import annotations

from datetime import datetime

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

    # Render closed revenue per-currency so the exec summary never
    # quietly sums ¥ + $ into a misleading "$2.02M". If there is only
    # one currency the output reads identically to the old single-line
    # format.
    by_currency = rev.get("by_currency") or {}
    if by_currency:
        revenue_lines = "<br>".join(
            f"{code}: {_fmt_currency_with_code(stats['total_revenue'], code)} "
            f"({int(stats['deal_count'])} deals)"
            for code, stats in sorted(by_currency.items())
        )
    else:
        revenue_lines = _fmt_currency(rev.get("total_revenue", 0))

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
| Closed Revenue | {revenue_lines} |
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

    # The open pipeline is a live snapshot — it does NOT respect the
    # ``--period`` flag. Previously the header said ``Period: Q1-2026``
    # next to a current snapshot, which led users to interpret the open
    # pipeline numbers as "pipeline state at Q1 2026 close". That data
    # does not exist in HubSpot without historical snapshots. Make the
    # distinction explicit: snapshot header for the open section, period
    # header only for the time-bound metrics below.
    snapshot_ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [
        f"# Pipeline Report",
        f"**Open pipeline snapshot:** as of {snapshot_ts} "
        f"(`--period` does not affect open-deal totals — HubSpot does not "
        f"expose historical pipeline state)\n",
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
        f"\n## Period Metrics ({_period_str(tr)})\n",
        "_These metrics use closed deals in the requested period._\n",
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
    ]

    by_currency = rev.get("by_currency") or {}
    if not by_currency:
        lines.append("_No closed-won revenue in this period._")
    else:
        lines.append("| Currency | Revenue | Deals | Avg Size | Largest |")
        lines.append("|---|---|---|---|---|")
        for code in sorted(by_currency.keys()):
            stats = by_currency[code]
            lines.append(
                f"| {code} "
                f"| {_fmt_currency_with_code(stats['total_revenue'], code)} "
                f"| {int(stats['deal_count'])} "
                f"| {_fmt_currency_with_code(stats['avg_deal_size'], code)} "
                f"| {_fmt_currency_with_code(stats['max_deal'], code)} |"
            )
        if len(by_currency) > 1:
            lines.append(
                "\n> Multi-currency totals are reported separately — JPY and "
                "USD amounts are never summed."
            )

    if mrr["mrr"] > 0 or mrr["arr"] > 0:
        lines.extend([
            f"\n## Recurring Revenue\n",
            f"- **MRR:** {_fmt_currency(mrr['mrr'])}",
            f"- **ARR:** {_fmt_currency(mrr['arr'])}",
        ])

    by_owner = data["by_owner"]
    if isinstance(by_owner, pd.DataFrame) and not by_owner.empty:
        lines.append("\n## Revenue by Rep\n")
        lines.append("| Rep | Currency | Revenue | Deals | Avg Size |")
        lines.append("|---|---|---|---|---|")
        for _, row in by_owner.iterrows():
            code = row.get("currency", "USD")
            lines.append(
                f"| {row['owner_name']} "
                f"| {code} "
                f"| {_fmt_currency_with_code(row['total_revenue'], code)} "
                f"| {int(row['deal_count'])} "
                f"| {_fmt_currency_with_code(row['avg_deal_size'], code)} |"
            )

    return "\n".join(lines)


def format_funnel_report(data: dict, tr: TimeRange) -> str:
    funnel = data["funnel"]

    lines = [
        f"# Funnel Report",
        f"**Period:** {_period_str(tr)}\n",
    ]

    if funnel.get("error"):
        lines.append(
            f"> ⚠️  **Contacts search failed** after retries: {funnel['error']}\n"
            "> The funnel report could not be generated. This is usually a\n"
            "> transient HubSpot 5xx — rerun in a minute. If it persists,\n"
            "> verify the access token has the `crm.objects.contacts.read`\n"
            "> scope.\n"
        )
        return "\n".join(lines)

    lines.append(f"**Total Contacts:** {funnel['total_contacts']}\n")

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

    # Header summary — show lost counts aggregated across all
    # currencies, and a per-currency value summary (never a mixed
    # total). ``ghost_deal_count`` and coverage are currency-agnostic.
    by_currency = data.get("by_currency") or {}
    lines.append(f"- **Total lost deals:** {total_deals}")
    if by_currency:
        value_summary = ", ".join(
            f"{_fmt_currency_with_code(stats['total_lost_value'], code)} "
            f"({int(stats['total_lost_deals'])} deals)"
            for code, stats in sorted(by_currency.items())
        )
        lines.append(f"- **Total lost value:** {value_summary}")
    lines.extend([
        f"- **Ghost deals (zero engagement):** {data.get('ghost_deal_count', 0)}",
        f"- **Lost-reason coverage:** {int(round(coverage * 100))}%",
        "",
    ])

    if len(by_currency) > 1:
        lines.append(
            "> Multi-currency losses are reported separately — JPY and "
            "USD lost values are never summed into a single total.\n"
        )

    # Render per-currency rep scorecard + reason breakdown. For
    # single-currency portals this renders exactly one section, so the
    # output shape is unchanged from the previous version.
    for code in sorted(by_currency.keys()):
        stats = by_currency[code]
        currency_header = f"## {code}" if len(by_currency) > 1 else "##"
        rep_scorecard = stats.get("rep_scorecard")
        if isinstance(rep_scorecard, pd.DataFrame) and not rep_scorecard.empty:
            title = (
                f"{currency_header} Lost deals by rep"
                if len(by_currency) > 1
                else "## Lost deals by rep"
            )
            lines.append(f"{title}\n")
            lines.append("| Rep | Deals Lost | Lost Value | Avg Lost Deal |")
            lines.append("|---|---|---|---|")
            for _, row in rep_scorecard.iterrows():
                lines.append(
                    f"| {row['rep_name']} "
                    f"| {int(row['deals_lost'])} "
                    f"| {_fmt_currency_with_code(row['lost_value'], code)} "
                    f"| {_fmt_currency_with_code(row['avg_lost_deal'], code)} |"
                )

        reason_breakdown = stats.get("reason_breakdown")
        if isinstance(reason_breakdown, pd.DataFrame) and not reason_breakdown.empty:
            title = (
                f"\n{currency_header} Reasons"
                if len(by_currency) > 1
                else "\n## Reasons"
            )
            lines.append(f"{title}\n")
            lines.append("| Reason | Deals | Value |")
            lines.append("|---|---|---|")
            for _, row in reason_breakdown.iterrows():
                lines.append(
                    f"| {row['closed_lost_reason']} "
                    f"| {int(row['deals_lost'])} "
                    f"| {_fmt_currency_with_code(row['lost_value'], code)} |"
                )
        lines.append("")

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
