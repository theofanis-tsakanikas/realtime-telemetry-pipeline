# Promo Video — Master Plan (Real-Time IoT Streaming)

## Goal & audience
A LinkedIn / portfolio piece. A recruiter or engineer must grasp in **~10 seconds**:
> *Live IoT telemetry, cleaned in-flight, streaming Kafka → Spark → Redis → a real-time wall — sub-second, fault-tolerant.*

This is the most **visceral** of the projects: the data *moves*. The whole pitch is motion — let
the live numbers and sparklines carry it. Keep it short; a streaming demo is best as a tight loop.

## One deliverable (short hero)
| | Hero promo |
|---|---|
| Length | **~30–45s** (loops well) |
| Audio | Captions + light music, **no voiceover** |
| Use | LinkedIn autoplay loop / top of the repo |
| Plan | this file + `01_caption_script_hero.md` + `02_shot_list.md` + `03_demo_recipe.md` |

> A voiceover deep-dive is overkill here — the value is the live stream and the architecture, both
> of which read in seconds. If you want depth, link the README's architecture section instead.

## The hero asset
The **Real-Time Sensor Wall** (`app/streamlit_app.py`) — auto-refreshing sensor cards with moving
sparklines, a combined multi-sensor chart, and a Kafka→Spark→Redis pipeline-health strip. Record
it in **demo mode** (continuous in-process stream, no Docker — see `03_demo_recipe.md`), or live
against the real Dockerised stack if you want a "real Redis" beat.

## 4 principles
1. **Motion is the message.** The first 5 seconds must show numbers *moving*. No static title hold.
2. **Muted-friendly.** One caption per beat; the wall does the talking.
3. **Architecture in one breath.** The pipeline-health strip (Simulator → Kafka → Spark → Redis) *is* the architecture explainer — no separate diagram needed.
4. **Honest.** Show the data-quality contract (~20% dirty data rejected). Label demo vs live.

## Structure — Hero (~40s, 5 scenes)
| # | Time | On screen | Caption |
|---|------|-----------|---------|
| 0 | 0–4s | Title card → cut straight to the live sensor wall (already moving) | *5 sensors. Thousands of readings a minute. Live.* |
| 1 | 4–16s | The sensor wall: 5 cards, sparklines moving; a value crosses out of band → red dot | *Streaming IoT telemetry — temperature, humidity, pressure.* |
| 2 | 16–26s | Combined multi-sensor chart for one metric; the "normal band" shading | *Cleaned in flight by Spark Structured Streaming.* |
| 3 | 26–36s | Pipeline-health strip lighting up: Simulator → Kafka → Spark (~80% kept) → Redis TS | *Kafka → Spark → Redis. Sub-second, fault-tolerant, stateful.* |
| 4 | 36–42s | End card: project name + value line + your name / GitHub | *Real-time data engineering — Kafka · Spark · Redis · Grafana* |

## Non-negotiables (the video must contain)
- The **moving** sensor wall (the hook — motion)
- The **multi-sensor chart** with the normal-band overlay (shows quality / range filtering)
- The **pipeline-health strip** (the architecture, in one frame)
- A nod to **data quality** (~20% rejected) — the engineering rigour
- A **demo vs live** label (honesty)

## Pre-production checklist
- [ ] `cd app && python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`
- [ ] `streamlit run streamlit_app.py` → confirm dark theme, no Deploy button, **"◆ DEMO STREAM"** badge.
- [ ] Sidebar: Refresh `1–2s`, window `8 min`, focus metric `temperature` (most legible motion).
- [ ] Let it run ~1 min before recording so the sparklines have a full window of history.
- [ ] (Optional live beat) `./run.sh up` and wait for Spark to start writing Redis; then "Redis (live)".
- [ ] Screen Studio: 16:9, retina, clean menu bar.

## Honest do / don't
- **DO** record the wall **live** while it refreshes — the motion is the whole point; don't fake it with a zoom.
- **DO** keep the **"◆ DEMO STREAM"** (or "● LIVE · REDIS") badge visible.
- **DON'T** show a Kafka/Spark cold-start, container logs, or a connection error — warm it up first.
- **DON'T** over-claim scale — it's a single-node demo of a pattern that *scales*; say "the pattern", not "production throughput".

## The one-line test
If a stranger watches the **first 5 seconds on mute** and says *"that's live sensor data streaming in"* — the opening works.
