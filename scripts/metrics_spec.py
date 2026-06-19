"""The sensor data contract — single source of truth for validation and drift.

Each metric declares two things:

* its **valid range** — the acceptance contract enforced on every ingestion boundary
  (a reading outside it is rejected to the dead-letter topic with a reason), and
* its **normal operating distribution** — the commissioning baseline (mean/std) the live
  stream is compared against to detect *drift* (a statistically significant shift that the
  hard range filter would never catch, because the readings are still individually valid).

Keeping both in one place means the cleaning logic (``clean_data`` / ``rejected_data``), the
data-quality metrics, and the drift detector can never disagree about what "valid" or
"normal" means. This is the framework's dimension 1 in practice: *schema contracts on every
ingestion boundary, enforced — not assumed.*
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MetricSpec:
    """Validation range + normal-operation baseline for one sensor metric."""

    name: str
    valid_min: float
    valid_max: float
    # Expected distribution under normal operation (the drift reference).
    normal_mean: float
    normal_std: float

    @property
    def rejection_reason(self) -> str:
        return f"{self.name}_out_of_range"


# The three environmental metrics. Valid ranges are the acceptance contract; the
# normal mean/std are the commissioning baseline (the simulator's normal-operation
# uniform ranges: temperature 15–35, humidity 30–80, pressure 990–1025).
TEMPERATURE = MetricSpec("temperature", 10.0, 45.0, normal_mean=25.0, normal_std=5.77)
HUMIDITY = MetricSpec("humidity", 0.0, 100.0, normal_mean=55.0, normal_std=14.43)
PRESSURE = MetricSpec("pressure", 950.0, 1050.0, normal_mean=1007.5, normal_std=10.10)

METRICS: tuple[MetricSpec, ...] = (TEMPERATURE, HUMIDITY, PRESSURE)
METRIC_NAMES: tuple[str, ...] = tuple(m.name for m in METRICS)


def spec(name: str) -> MetricSpec:
    """Look up a metric spec by name."""
    for m in METRICS:
        if m.name == name:
            return m
    raise KeyError(f"unknown metric: {name!r}")
