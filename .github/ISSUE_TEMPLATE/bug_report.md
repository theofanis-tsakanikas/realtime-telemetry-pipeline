---
name: Bug report
about: Report a problem with the streaming pipeline
title: "[Bug] "
labels: bug
assignees: ''
---

## Describe the Bug

A clear and concise description of what the bug is.

## Affected Component

Which part of the pipeline is affected?

- [ ] Sensor simulator (`scripts/sensor_simulator.py`)
- [ ] Kafka broker / topic
- [ ] Spark transformation job (`scripts/spark_transform.py`)
- [ ] Redis TimeSeries sink
- [ ] Grafana dashboard / provisioning
- [ ] Docker Compose / infrastructure
- [ ] Other (describe below)

## Steps to Reproduce

1. ...
2. ...
3. ...

## Expected Behaviour

What you expected to happen.

## Actual Behaviour

What actually happened. Include error messages.

## Logs

Paste relevant container logs (e.g. `docker logs spark-processor`, `docker logs simulator`).

```
<paste logs here>
```

## Environment

- OS:
- Docker version (`docker --version`):
- Docker Compose version (`docker compose version`):
- RAM allocated to Docker:
- Branch / commit:

## Additional Context

Add any other context, screenshots, or notes about the problem here.
