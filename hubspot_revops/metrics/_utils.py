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
