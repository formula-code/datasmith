from __future__ import annotations

import typing

import asv  # type: ignore[import-untyped]
import numpy as np
import pandas as pd
import ruptures as rpt  # type: ignore[import-untyped]


def get_breakpoints(df: pd.DataFrame) -> list[dict] | None:
    """Return a list of significant downward shifts for **one** benchmark."""
    y = df["time"].to_numpy(dtype=float)
    n = len(y)

    algo = rpt.Pelt(model="rbf").fit(y)
    penalty = 3 * np.log(n)
    bkps = algo.predict(pen=penalty)
    bkps = [b for b in bkps if b < n]
    results = []
    for end in bkps:
        m1 = y[end - 1]
        m2 = y[end]
        delta_pct = (m2 - m1) / (m1 + 1e-10) * 100.0  # negative percentage
        if delta_pct < 0:
            results.append({
                "hash": df["hash"].iloc[end - 1],
                "gt_hash": df["hash"].iloc[end],
                "delta_pct": delta_pct,
                "benchmark": df["benchmark"].iloc[end - 1],
                "start_time": df["time"].iloc[end - 1],
                "end_time": df["time"].iloc[end],
            })
    return results or None


def get_breakpoints_asv(df: pd.DataFrame) -> list[dict] | None:
    """Return a list of significant downward shifts for **one** benchmark."""
    y = df["time"].to_numpy(dtype=float)
    if "time_str" in df.columns:
        y_sigma = df["time_std"].to_numpy(dtype=float)
    else:
        print("Warning: No time_std column found, using None for sigma.")
        print("Robutstness of the detection may be reduced.")
        y_sigma = None

    _, _, regression_pos = asv.step_detect.detect_regressions(
        asv.step_detect.detect_steps(
            y=-1 * y,
            w=y_sigma,
        )
    )
    if not regression_pos:
        return None
    results = []
    for idx_before, idx_after, val_before, val_after in regression_pos:
        val_after = -1 * val_after
        val_before = -1 * val_before
        delta_pct = (val_after - val_before) / val_before * 100.0
        if delta_pct < 0:
            results.append({
                "hash": df["hash"].iloc[idx_after],
                "gt_hash": df["hash"].iloc[idx_before],
                "delta_pct": delta_pct,
                "benchmark": df["benchmark"].iloc[idx_after],
                "start_time": df["time"].iloc[idx_before],
                "end_time": df["time"].iloc[idx_after],
            })
    return results or None


def get_detection_method(method: str) -> typing.Callable:
    """Return the detection method based on the given string."""
    if method == "asv":
        return get_breakpoints_asv
    elif method == "rbf":
        return get_breakpoints
    else:
        raise ValueError(f"Unknown method: {method}. Use 'asv' or 'rbf'.")  # noqa: TRY003


def detect_all_breakpoints(summary_df: pd.DataFrame, method: str = "rbf") -> pd.DataFrame:
    """Detect break-points for every benchmark in *summary_df*."""

    detection_method = get_detection_method(method)

    needed = {"benchmark", "time", "hash"}
    if missing := needed - set(summary_df.columns):
        raise ValueError(str(missing))

    breakpoints: pd.DataFrame = (
        summary_df.groupby("benchmark", sort=False)
        .apply(detection_method)
        .dropna()
        .explode()
        .apply(pd.Series)
        .reset_index(drop=True)
    )
    return breakpoints
