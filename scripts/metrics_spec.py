"""The sensor data contract — single source of truth for validation and drift.

Each metric declares two things:

* its **valid range** — the acceptance contract enforced on every ingestion boundary
  (a reading outside it is rejected to the dead-letter topic with a reason), and
* its **normal operating range** — the commissioning window the sensor reads within under
  healthy operation. The drift baseline (mean / std) is *derived* from this range rather than
  hand-typed, so it can never silently disagree with the data the simulator actually emits: the
  simulator draws normal readings uniformly over the same range (see ``sensor_simulator`` —
  ``TEMPERATURE_RANGE`` etc.), and for a uniform U(a, b) the mean is (a+b)/2 and the standard
  deviation is (b−a)/√12.

Keeping both in one place means the cleaning logic (``clean_data`` / ``rejected_data``), the
data-quality metrics, and the drift detector can never disagree about what "valid" or
"normal" means. This is the framework's dimension 1 in practice: *schema contracts on every
ingestion boundary, enforced — not assumed.*
"""

from __future__ import annotations

from dataclasses import dataclass
from math import sqrt


@dataclass(frozen=True)
class MetricSpec:
    """Validation range + normal-operation range for one sensor metric.

    The drift baseline (``normal_mean`` / ``normal_std``) is computed from the
    ``[normal_min, normal_max]`` commissioning range under the assumption of uniform
    normal-operation readings — the distribution the simulator produces.
    """

    name: str
    valid_min: float
    valid_max: float
    # Commissioning normal-operation range; the drift reference is derived from it.
    normal_min: float
    normal_max: float

    @property
    def normal_mean(self) -> float:
        """Baseline mean of a uniform U(normal_min, normal_max)."""
        return (self.normal_min + self.normal_max) / 2.0

    @property
    def normal_std(self) -> float:
        """Baseline standard deviation of a uniform U(normal_min, normal_max): range/√12."""
        return (self.normal_max - self.normal_min) / sqrt(12.0)

    @property
    def rejection_reason(self) -> str:
        return f"{self.name}_out_of_range"


# The three environmental metrics. Valid ranges are the acceptance contract; the
# normal min/max are the simulator's normal-operation uniform ranges, from which the
# drift baseline (mean/std) is derived.
TEMPERATURE = MetricSpec("temperature", 10.0, 45.0, normal_min=15.0, normal_max=35.0)
HUMIDITY = MetricSpec("humidity", 0.0, 100.0, normal_min=30.0, normal_max=80.0)
PRESSURE = MetricSpec("pressure", 950.0, 1050.0, normal_min=990.0, normal_max=1025.0)

METRICS: tuple[MetricSpec, ...] = (TEMPERATURE, HUMIDITY, PRESSURE)
METRIC_NAMES: tuple[str, ...] = tuple(m.name for m in METRICS)


def spec(name: str) -> MetricSpec:
    """Look up a metric spec by name."""
    for m in METRICS:
        if m.name == name:
            return m
    raise KeyError(f"unknown metric: {name!r}")
