# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `CLAUDE.md` engineering reference covering repo structure, service ports, prerequisites, end-to-end data flow, and known failure modes.
- `Makefile` task runner wrapping `run.sh` and adding `test`, `lint`, and `clean` targets; auto-sets `JAVA_HOME` for local PySpark runs.
- GitHub Actions CI workflow (`.github/workflows/ci.yml`) running Ruff linting and pytest on every push and pull request, with Python 3.12, Java 17, and pip caching.
- pytest suite (`tests/`) covering `clean_data()`, `rejected_data()`, data-quality metrics, drift detection, the Redis sink command-building (mocked client), the sensor simulator, and the Streamlit data layer — with a session-scoped `SparkSession` fixture isolated to a temp warehouse/Derby home so no Spark scratch leaks into the repo.
- `tests/test_app_sensor_data.py` including a **contract-drift guard** that fails if the Streamlit app's metric ranges diverge from `scripts/metrics_spec.py` (the single source of truth).
- `pyproject.toml` centralising project metadata and the `ruff`, `pytest`, and `coverage` configuration.
- `requirements-dev.txt` for development-only dependencies (pytest, pytest-cov, ruff, pandas).
- `.pre-commit-config.yaml` with an optional local `ruff` lint + file-hygiene hooks.
- `make coverage` target producing a terminal + HTML coverage report.
- Grafana provisioning as code: auto-provisioned Redis datasource, dashboard provider, and a placeholder dashboard under `infra/grafana/`.
- `LICENSE` (MIT).
- `CHANGELOG.md` (this file).
- GitHub issue templates, pull request template, and Dependabot configuration.
- README badges (CI status, license, tech stack), table of contents, a "What This Demonstrates" section, and a "Tests & Code Quality" section.
- Module-level and function-level docstrings plus type hints across the Python source.

### Changed
- `infra/docker-compose.yml`: Grafana now installs the `redis-datasource` plugin, sets the admin password, and mounts the provisioning and dashboard directories; the Spark processor now publishes port `4040` for the Spark UI.
- CI now lints `app/` in addition to `scripts/` and `tests/`, and runs the test suite with a coverage report.
- Tooling configuration centralised in `pyproject.toml`; `ruff` lint set widened (import sorting, bugbear, pyupgrade, comprehensions) and applied across `scripts/`, `tests/`, and `app/`.
- Pinned the Streamlit app's dependencies (`app/requirements.txt`) for reproducible deploys.

### Removed
- `pytest.ini` — its configuration moved into `pyproject.toml`.

## [0.1.0] - 2026-05-30

### Added
- Initial IoT streaming pipeline: Python sensor simulator, Apache Kafka broker, Spark Structured Streaming transformation job, Redis TimeSeries sink, and Grafana visualization.
- Docker Compose stack orchestrating all services with custom images for the simulator and Spark processor.
- `run.sh` Docker Compose wrapper and `setup.sh` local environment bootstrap script.
- Project README with architecture overview, quick start guide, and verification screenshots.

[Unreleased]: https://github.com/theofanis-tsakanikas/kafka-spark-redis-streaming-etl/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/theofanis-tsakanikas/kafka-spark-redis-streaming-etl/releases/tag/v0.1.0
