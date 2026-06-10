# Hero Promo — Caption Script (exact words + timings)

**Target:** ~40s · captions + music, no voiceover · captions in English · loops well.
**Hero asset:** the **Real-Time Sensor Wall** (`app/streamlit_app.py`), demo mode (or Redis live).

Caption style: large, lower-third, white with a cyan accent on the key word, dark backing.
2.5–3.5s on screen. One line per beat. Let the motion breathe — fewer captions than usual.

---

### SCENE 0 — Hook (0:00–0:04)
- **Screen:** 1s title card → cut straight to the **already-moving** sensor wall (don't start static).
- **Caption (0:01):** `5 sensors. Thousands of readings a minute. Live.`
- **Music:** energetic-but-clean tech bed from frame one.

### SCENE 1 — The wall (0:04–0:16)
- **Screen:** The sensor wall — 5 cards, current temp/humidity/pressure, sparklines visibly moving.
  Time it so a value crosses **out of band** → the status **dot turns red**.
- **Caption (0:06):** `Streaming IoT telemetry — temperature, humidity, pressure.`
- **Caption (0:12):** `Every sensor, every second.`

### SCENE 2 — Clean in flight (0:16–0:26)
- **Screen:** The **combined multi-sensor chart** for one metric; the green **"normal band"** shading is visible; lines weave through it.
- **Caption (0:18):** `Cleaned in flight by Spark Structured Streaming.`
- **Caption (0:22):** `Range-filtered, deduped, schema-enforced.`

### SCENE 3 — The pipeline (0:26–0:36)
- **Screen:** The **pipeline-health strip**: Simulator → Kafka (msg/s) → Spark (~80% kept) → Redis TS keys. Let the msg/s number tick.
- **Caption (0:28):** `Kafka → Spark → Redis. Sub-second and fault-tolerant.`
- **Caption (0:33):** `~20% dirty data dropped before it ever lands.`

### SCENE 4 — Close (0:36–0:42)
- **Screen:** End card (dark). Project name + value line + your name / GitHub.
- **Caption (static):**
  > **Real-Time IoT Pipeline — Kafka · Spark · Redis · Grafana**
  > Streaming ETL · stateful processing · containerised
  > *<your name> — github.com/<you>*

---

## Caption master list (copy-paste ready)
```
1.  5 sensors. Thousands of readings a minute. Live.
2.  Streaming IoT telemetry — temperature, humidity, pressure.
3.  Every sensor, every second.
4.  Cleaned in flight by Spark Structured Streaming.
5.  Range-filtered, deduped, schema-enforced.
6.  Kafka → Spark → Redis. Sub-second and fault-tolerant.
7.  ~20% dirty data dropped before it ever lands.
8.  [End card] Real-time data engineering — Kafka · Spark · Redis · Grafana
```

## Notes
- This is a **loop** — design the last frame to cut cleanly back to the first (LinkedIn autoplay repeats it).
- If you record the **real Grafana** dashboard (`docker-compose` → :3000), you can swap scene 2's
  combined chart for a 3s Grafana cut — but the Streamlit wall is more on-brand with the portfolio.
- Keep it under 45s. Streaming demos lose people if they linger.
