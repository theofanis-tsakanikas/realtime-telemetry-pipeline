"""Declarative data-quality contract for *cleaned* sensor readings.

``clean_data`` / ``rejected_data`` enforce the acceptance rules imperatively inside the Spark
job. This module expresses the *same* rules **declaratively** as a Pandera schema — a
versioned, self-documenting artifact you can point a stakeholder at, assert in CI, and run as
an ad-hoc report against a data snapshot.

It is built dynamically from :mod:`metrics_spec` (the single source of truth), so the
declarative contract and the imperative Spark filter can never drift apart: change a range in
``metrics_spec`` and both move together.

Why the pandas backend (not ``pandera.pyspark``)
------------------------------------------------
``pandera.pyspark`` imports ``pyspark.pandas``, which still imports ``distutils`` — removed
from the stdlib in Python 3.12 — and additionally requires PyArrow. The pandas backend has
neither problem, keeps the Spark image lean, and validates the data shapes that actually leave
the pipeline (the Streamlit/Redis read path is pandas, and Spark batches collect to pandas).
The contract is deliberately kept **out of the streaming hot path**: ingestion must never be
able to fail because a validation library raised. It is a verification artifact, not a gate.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandera.pandas as pa
from metrics_spec import METRICS
from pandera.pandas import Check, Column, DataFrameSchema


def build_contract(*, strict: bool = False) -> DataFrameSchema:
    """Build the cleaned-reading contract from the metrics spec.

    Each metric becomes a non-null float column constrained to its valid range; ``sensor_id``
    must be a non-null string and ``timestamp`` (when present) non-null. ``strict=False`` lets
    callers validate frames that carry extra columns (e.g. a ``metric`` label).
    """
    columns: dict[str, Column] = {
        "sensor_id": Column(str, nullable=False),
        # dtype-agnostic: just require it to be present and non-null when supplied.
        "timestamp": Column(nullable=False, required=False),
    }
    for m in METRICS:
        columns[m.name] = Column(
            float,
            Check.in_range(m.valid_min, m.valid_max),
            nullable=False,
            description=f"{m.name} in [{m.valid_min}, {m.valid_max}]",
        )
    return DataFrameSchema(columns, strict=strict, coerce=False, name="cleaned_sensor_reading")


#: The canonical contract instance. Import this for validation.
CONTRACT: DataFrameSchema = build_contract()


@dataclass(frozen=True)
class ContractReport:
    """Outcome of validating a frame against :data:`CONTRACT` (never raises)."""

    passed: bool
    total: int
    failures: int
    by_column: dict[str, int] = field(default_factory=dict)

    def summary(self) -> str:
        if self.passed:
            return f"PASS — {self.total} rows, 0 contract violations"
        worst = ", ".join(f"{c}={n}" for c, n in sorted(self.by_column.items()))
        return f"FAIL — {self.failures} violations across {self.total} rows ({worst})"


def validate(pdf, *, lazy: bool = True):
    """Validate a pandas frame against the contract.

    Raises :class:`pandera.errors.SchemaErrors` (lazy) listing every violation. Use this where
    a hard failure is wanted (tests, batch gates); use :func:`validation_report` where you only
    want to observe quality without raising.
    """
    return CONTRACT.validate(pdf, lazy=lazy)


def validation_report(pdf) -> ContractReport:
    """Validate without raising and return a per-column violation breakdown."""
    try:
        CONTRACT.validate(pdf, lazy=True)
        return ContractReport(passed=True, total=len(pdf), failures=0)
    except pa.errors.SchemaErrors as exc:
        cases = exc.failure_cases
        by_column: dict[str, int] = {}
        for col, group in cases.groupby("column", dropna=False):
            by_column[str(col)] = int(len(group))
        return ContractReport(
            passed=False, total=len(pdf), failures=int(len(cases)), by_column=by_column
        )


def _demo() -> None:
    """Print the contract and a self-check against a tiny mixed sample (runnable artifact)."""
    import pandas as pd

    print(CONTRACT)
    sample = pd.DataFrame(
        {
            "sensor_id": ["sensor_1", "sensor_2", None],
            "temperature": [25.0, 99.0, 22.0],  # 99.0 is out of range
            "humidity": [50.0, 55.0, 60.0],
            "pressure": [1005.0, 1010.0, 1008.0],
        }
    )
    print(validation_report(sample).summary())


if __name__ == "__main__":
    _demo()
