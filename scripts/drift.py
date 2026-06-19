"""Statistical drift detection on the sensor streams.

The range filter rejects *individually invalid* readings. Drift is the subtler failure it
cannot catch: every reading is still inside the valid band, but the whole distribution has
shifted — a miscalibrated sensor reading 5 °C high, a humidity probe slowly degrading. Left
unmonitored, the pipeline keeps "succeeding" while the data silently goes wrong.

For each micro-batch this compares a metric's batch mean to its commissioning baseline
(``metrics_spec`` normal mean/std) with a one-sample z-test of the mean:

    z = (batch_mean − baseline_mean) / (baseline_std / sqrt(n))

``|z|`` beyond the control limit ``k`` (default 3σ) raises a drift signal. The z-score is
written to Redis TimeSeries so the dashboard charts drift per metric and alerts on it.

Pure and stateless (no cross-batch state to corrupt); maps to Readiness Framework
dimension 3 (Observability & drift): *data drift is monitored, with thresholds; alerting
fires before a user reports it.*
"""

from __future__ import annotations

from dataclasses import dataclass
from math import sqrt

from metrics_spec import METRICS, MetricSpec

DEFAULT_K = 3.0  # control limit in standard errors (3σ)

STATUS_STABLE = "stable"
STATUS_DRIFT = "drift"


@dataclass(frozen=True)
class DriftResult:
    """Drift assessment for one metric in one batch."""

    metric: str
    n: int
    batch_mean: float
    z_score: float
    status: str

    @property
    def alert(self) -> bool:
        return self.status == STATUS_DRIFT


def drift_score(metric: MetricSpec, n: int, batch_mean: float, *, k: float = DEFAULT_K) -> DriftResult:
    """Standardised mean shift of a batch vs the metric's normal baseline.

    An empty batch (or a degenerate zero-std baseline) yields z = 0 / stable — there is
    nothing to compare.
    """
    if n <= 0 or metric.normal_std <= 0:
        return DriftResult(metric.name, n, batch_mean, 0.0, STATUS_STABLE)
    standard_error = metric.normal_std / sqrt(n)
    z = (batch_mean - metric.normal_mean) / standard_error
    status = STATUS_DRIFT if abs(z) > k else STATUS_STABLE
    return DriftResult(metric.name, n, batch_mean, round(z, 3), status)


def drift_timeseries(results: list[DriftResult]) -> dict[str, float]:
    """Flatten drift results into Redis-TimeSeries key → value pairs."""
    series: dict[str, float] = {}
    for r in results:
        series[f"drift:{r.metric}:z"] = r.z_score
        series[f"drift:{r.metric}:mean"] = round(r.batch_mean, 3)
    return series


def write_drift_metrics(r, results: list[DriftResult], timestamp_ms: int) -> None:
    """Write per-metric drift z-scores + batch means to Redis TimeSeries.

    Keys carry ``kind=drift`` with ``series=z|mean`` and the ``metric`` label so the
    dashboard can chart all z-scores together (``FILTER kind=drift series=z``).
    """
    for key, value in drift_timeseries(results).items():
        # key is "drift:<metric>:<series>"
        _, metric, series = key.split(":", 2)
        r.execute_command(
            "TS.ADD", key, timestamp_ms, value,
            "ON_DUPLICATE", "LAST",
            "LABELS", "kind", "drift", "series", series, "metric", metric,
        )


def batch_drift(summaries: dict[str, tuple[int, float]], *, k: float = DEFAULT_K) -> list[DriftResult]:
    """Compute drift for every known metric from per-metric ``(n, mean)`` summaries.

    Metrics absent from ``summaries`` are skipped (no data this batch).
    """
    results = []
    for m in METRICS:
        if m.name in summaries:
            n, mean = summaries[m.name]
            results.append(drift_score(m, n, mean, k=k))
    return results
