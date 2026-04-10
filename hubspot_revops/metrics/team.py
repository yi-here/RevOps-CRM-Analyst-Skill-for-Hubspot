"""Team performance metrics — per-rep pipeline, win rate, activity."""

from __future__ import annotations

import pandas as pd

from hubspot_revops.extractors.base import TimeRange
from hubspot_revops.extractors.deals import DealExtractor
from hubspot_revops.metrics._utils import (
    attach_currency,
    to_bool_series,
    to_numeric_series,
)
from hubspot_revops.schema.models import Owner


def _filter_pipeline(df: pd.DataFrame, pipeline_filter: str | None) -> pd.DataFrame:
    if pipeline_filter and not df.empty and "pipeline" in df.columns:
        return df[df["pipeline"] == pipeline_filter]
    return df


def _prep(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize amount + currency + won flag columns for safe groupby."""
    if df.empty:
        return df
    df = attach_currency(df)
    df["amount"] = to_numeric_series(df, "amount")
    df["is_won"] = to_bool_series(df, "hs_is_closed_won")
    if "hubspot_owner_id" not in df.columns:
        df["hubspot_owner_id"] = ""
    return df


def rep_scorecard(
    deal_extractor: DealExtractor,
    time_range: TimeRange,
    owners: dict[str, Owner],
    pipeline_filter: str | None = None,
) -> pd.DataFrame:
    """Generate a per-rep, per-currency scorecard with key metrics.

    Multi-currency deals produce multiple rows per rep (one per currency).
    This avoids the silent bug where JPY amounts were summed into USD
    totals.
    """
    closed = _filter_pipeline(
        deal_extractor.get_closed_deals(time_range), pipeline_filter
    )
    open_deals = _filter_pipeline(deal_extractor.get_open_deals(), pipeline_filter)

    closed = _prep(closed)
    open_deals = _prep(open_deals)

    if closed.empty and open_deals.empty:
        return pd.DataFrame()

    # All (owner, currency) combinations appearing in either set.
    combos: set[tuple[str, str]] = set()
    if not closed.empty:
        combos.update(
            map(tuple, closed[["hubspot_owner_id", "currency"]].to_records(index=False).tolist())
        )
    if not open_deals.empty:
        combos.update(
            map(tuple, open_deals[["hubspot_owner_id", "currency"]].to_records(index=False).tolist())
        )

    rows = []
    for owner_id, currency in combos:
        rep_closed = closed[
            (closed["hubspot_owner_id"] == owner_id) & (closed["currency"] == currency)
        ] if not closed.empty else pd.DataFrame()
        rep_open = open_deals[
            (open_deals["hubspot_owner_id"] == owner_id) & (open_deals["currency"] == currency)
        ] if not open_deals.empty else pd.DataFrame()

        rep_won = rep_closed[rep_closed["is_won"]] if not rep_closed.empty else pd.DataFrame()
        rep_lost = rep_closed[~rep_closed["is_won"]] if not rep_closed.empty else pd.DataFrame()

        deals_closed = len(rep_closed)
        deals_won = len(rep_won)
        deals_lost = len(rep_lost)

        won_amount_series = rep_won["amount"] if not rep_won.empty else pd.Series([], dtype=float)
        lost_amount_series = rep_lost["amount"] if not rep_lost.empty else pd.Series([], dtype=float)
        open_amount_series = rep_open["amount"] if not rep_open.empty else pd.Series([], dtype=float)

        wr = round((deals_won / deals_closed * 100), 1) if deals_closed > 0 else 0.0
        lr = round((deals_lost / deals_closed * 100), 1) if deals_closed > 0 else 0.0
        avg_size = float(won_amount_series.mean()) if deals_won > 0 else 0.0

        owner = owners.get(owner_id)
        rep_name = owner.full_name if owner else (owner_id or "Unassigned")

        rows.append({
            "owner_id": owner_id,
            "rep_name": rep_name,
            "currency": currency,
            "open_pipeline": float(open_amount_series.sum()),
            "open_deals": len(rep_open),
            "closed_won_revenue": float(won_amount_series.sum()),
            "lost_revenue": float(lost_amount_series.sum()),
            "deals_won": deals_won,
            "deals_lost": deals_lost,
            "deals_closed": deals_closed,
            "win_rate": wr,
            "loss_rate": lr,
            "avg_deal_size": avg_size,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    # Sort by rep, then revenue descending — keeps a rep's currency rows
    # adjacent in the output table.
    return df.sort_values(["rep_name", "closed_won_revenue"], ascending=[True, False]).reset_index(drop=True)
