"""First-class data-quality metrics for the stream.

Filtering bad readings is necessary but invisible — you cannot be accountable for a
pipeline you cannot see. This module turns each micro-batch's quality into numbers:
how many readings arrived, how many passed the contract, the accept rate, and the
rejection count broken down by reason. Those numbers are written to Redis TimeSeries
alongside the readings, so the Grafana dashboard shows data quality *live* — not as a
forensic exercise after a stakeholder complains.

Pure (DataFrame → metrics); the Redis write is a thin, separately-testable step. Maps to
Readiness Framework dimension 3 (Observability & drift).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from pyspark.sql import DataFrame


@dataclass(frozen=True)
class DQMetrics:
    """Quality summary for one micro-batch."""

    total: int
    valid: int
    rejected_total: int
    by_reason: dict[str, int] = field(default_factory=dict)

    @property
    def accept_rate(self) -> float:
        """Fraction of readings that passed the contract (1.0 for an empty batch)."""
        return 1.0 if self.total == 0 else self.valid / self.total

    @property
    def reject_rate(self) -> float:
        return 0.0 if self.total == 0 else self.rejected_total / self.total


def quality_metrics(parsed_df: DataFrame, rejected_fn: Callable[[DataFrame], DataFrame]) -> DQMetrics:
    """Compute the quality summary of a parsed batch.

    ``clean_data`` and ``rejected_data`` partition the input, so valid = total − rejected;
    only the rejected branch (which carries the reasons) needs to be evaluated for the
    breakdown.
    """
    parsed_df = parsed_df.cache()
    try:
        total = parsed_df.count()
        by_reason: dict[str, int] = {
            row["rejection_reason"]: row["count"]
            for row in rejected_fn(parsed_df).groupBy("rejection_reason").count().collect()
        }
        rejected_total = sum(by_reason.values())
        return DQMetrics(total=total, valid=total - rejected_total, rejected_total=rejected_total, by_reason=by_reason)
    finally:
        parsed_df.unpersist()


def dq_timeseries(metrics: DQMetrics) -> dict[str, float]:
    """Flatten the metrics into Redis-TimeSeries key → value pairs for the dashboard."""
    series: dict[str, float] = {
        "dq:total": float(metrics.total),
        "dq:valid": float(metrics.valid),
        "dq:rejected_total": float(metrics.rejected_total),
        "dq:accept_rate": round(metrics.accept_rate, 4),
    }
    for reason, count in metrics.by_reason.items():
        series[f"dq:rejected:{reason}"] = float(count)
    return series


def write_dq_metrics(r, metrics: DQMetrics, timestamp_ms: int) -> None:
    """Write the data-quality metrics to Redis TimeSeries (one sample per batch).

    ``r`` is any object exposing ``execute_command`` (a connection or pipeline). Keys are
    labelled ``kind=data_quality`` with ``series=summary`` (totals / accept rate) or
    ``series=rejection`` (per-reason counts) so the dashboard can split them with
    ``TS.MRANGE ... FILTER``.
    """
    for key, value in dq_timeseries(metrics).items():
        series = "rejection" if key.startswith("dq:rejected:") else "summary"
        reason = key.split("dq:rejected:", 1)[1] if series == "rejection" else key.split("dq:", 1)[1]
        r.execute_command(
            "TS.ADD", key, timestamp_ms, value,
            "ON_DUPLICATE", "LAST",
            "LABELS", "kind", "data_quality", "series", series, "metric", reason,
        )
