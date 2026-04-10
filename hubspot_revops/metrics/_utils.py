"""Shared helpers for metric computation.

These helpers normalize DataFrame column access so that downstream
``.sum()``, ``.mean()``, and boolean-filter operations always receive a
``pd.Series`` — never a scalar. The original ``df.get(col, 0)`` pattern
crashes when a column is entirely missing from the response, because
``pd.to_numeric(0)`` returns a numpy scalar on which ``.fillna()`` does
not exist.
"""

from __future__ import annotations

import pandas as pd

DEFAULT_CURRENCY = "USD"


def to_numeric_series(df: pd.DataFrame, col: str, default: float = 0) -> pd.Series:
    """Return ``df[col]`` coerced to numeric, or an empty/zero Series.

    Guarantees a ``pd.Series`` result even if ``df`` is empty or the
    column is missing — which is the root cause of the scalar
    ``AttributeError`` bug in the team scorecard and related metrics.
    """
    if df is None or df.empty or col not in df.columns:
        return pd.Series([], dtype=float)
    return pd.to_numeric(df[col], errors="coerce").fillna(default)


def to_bool_series(df: pd.DataFrame, col: str) -> pd.Series:
    """Return ``df[col]`` coerced to a boolean Series.

    HubSpot returns boolean-like properties as the strings ``"true"`` /
    ``"false"``. This helper lowercases and compares, returning an
    empty boolean Series when the column is missing (so the caller can
    safely use it as a filter mask without crashing).
    """
    if df is None or df.empty or col not in df.columns:
        return pd.Series([], dtype=bool)
    return df[col].astype(str).str.lower().eq("true")


def attach_currency(df: pd.DataFrame) -> pd.DataFrame:
    """Add a normalized ``currency`` column, defaulting to USD.

    Every currency-aware metric (team scorecard, revenue, closed-lost,
    pipeline) needs the same normalization: take ``deal_currency_code``,
    fill NaN and empty strings with the default, and expose it as a
    plain ``currency`` column ready for groupby. Previously each module
    implemented this inline; now every caller goes through the same
    helper so a null/empty currency is treated identically across the
    suite.

    Returns the DataFrame unchanged if it is empty. Does NOT mutate the
    input — always returns a copy when the column needs to be added.
    """
    if df is None or df.empty:
        return df
    df = df.copy()
    if "deal_currency_code" in df.columns:
        df["currency"] = (
            df["deal_currency_code"]
            .fillna(DEFAULT_CURRENCY)
            .replace("", DEFAULT_CURRENCY)
        )
    else:
        df["currency"] = DEFAULT_CURRENCY
    return df


def parse_hs_datetime(series: pd.Series) -> pd.Series:
    """Parse a HubSpot datetime column that may be epoch-ms or ISO strings.

    HubSpot returns different datetime properties in different shapes:

    - ``closedate`` / ``createdate``: ISO 8601 strings or epoch-ms
      depending on the SDK version and the endpoint.
    - ``hs_timestamp`` / ``hs_meeting_start_time``: epoch milliseconds
      as a string (e.g. ``"1712345678000"``).

    The previous code called ``pd.to_datetime(series, utc=True)`` with
    no ``unit`` hint — which silently parsed ``"1712345678000"`` as
    year 1712345678 and returned NaT for every row, causing the
    meetings report to show "Median days first meeting → close: 0.0"
    for both won and lost outcomes.

    This helper tries epoch-ms first and falls back to ISO parsing so
    either format lands on a valid UTC ``Timestamp``. Rows that can't
    be parsed by either path become ``NaT`` so the caller can ``dropna``
    before computing medians.
    """
    if series is None or len(series) == 0:
        return pd.Series([], dtype="datetime64[ns, UTC]")
    # Parse both ways independently, then combine. ``to_numeric``
    # coerces epoch-ms strings like "1712345678000" to int; anything
    # non-numeric (including ISO strings and empty values) becomes NaN
    # and contributes NaT to ``ms_result``. Conversely ``pd.to_datetime``
    # without a unit parses ISO strings — but pandas 2.x requires
    # ``format="mixed"`` for a Series containing both date-only and
    # full-timestamp forms (otherwise it tries to lock onto the first
    # value's format and drops the rest as NaT). ``combine_first``
    # then picks the non-null value per-row, preferring the epoch-ms
    # result when both succeed.
    numeric = pd.to_numeric(series, errors="coerce")
    ms_result = pd.to_datetime(numeric, unit="ms", errors="coerce", utc=True)
    try:
        iso_result = pd.to_datetime(
            series, errors="coerce", utc=True, format="mixed"
        )
    except (TypeError, ValueError):
        # Older pandas without ``format="mixed"`` support — fall back
        # to the plain call which handles homogeneous ISO inputs.
        iso_result = pd.to_datetime(series, errors="coerce", utc=True)
    return ms_result.combine_first(iso_result)


def pick_primary_currency(by_currency: dict, count_key: str = "deal_count") -> str:
    """Select the "primary" currency from a per-currency stats dict.

    The primary is whichever currency has the most deals (alphabetical
    tiebreak picks the LEXICOGRAPHICALLY LATER code — matches the
    existing revenue and closed-lost modules' behaviour, which pick
    "USD" over "JPY" on a tie because "USD" > "JPY"). Callers use this
    to populate back-compat top-level fields — ``total_value``,
    ``avg_deal_size`` — that represent a single currency rather than
    a meaningless sum across currencies. Used identically by revenue,
    closed-lost, and pipeline metrics.

    ``count_key`` lets callers point at e.g. ``total_lost_deals``
    instead of ``deal_count`` when their per-currency dict uses
    different field names.
    """
    if not by_currency:
        return DEFAULT_CURRENCY
    return max(
        by_currency.items(),
        key=lambda kv: (kv[1].get(count_key, 0), kv[0]),
    )[0]
