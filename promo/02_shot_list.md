# Hero Promo — Shot List (record this, in this order)

A streaming demo is about **motion**, so most clips are just "let it run and capture the movement".
Record in demo mode (repeatable); switch to live only for an optional "real Redis" beat.

## Stage 0 — Setup (before any recording)
1. Launch the wall:
   ```
   cd app && python3 -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt
   streamlit run streamlit_app.py
   ```
2. Sidebar: **Demo stream**, Refresh `1–2s`, Window `8 min`, focus metric **temperature**.
3. **Let it run ~60s** before recording so every sparkline has a full window of history (no half-empty charts).
4. Confirm: dark theme, no Deploy button, **"◆ DEMO STREAM"** badge.
5. (Optional live) `./run.sh up`; wait until the Spark processor is writing Redis; then pick
   **"Redis (live)"** and confirm the badge flips to **"● LIVE · REDIS TIMESERIES"**.
6. Screen Studio: 16:9, retina, clean menu bar.

---

## Clips to record (in this recording order)

### CLIP A — The wall, moving (the hero)
- **What:** The 5 sensor cards with sparklines updating. Capture **at least 2–3 refresh ticks** so
  motion is obvious. Bonus: catch a value crossing out of band → the **dot turns red**.
- **Length:** ~14s raw.
- **Screen Studio:** slow push-in across the cards; don't over-zoom — the point is seeing all 5 move.

### CLIP B — Combined multi-sensor chart
- **What:** Scroll to the **combined chart**; let it refresh once or twice. The green **normal-band**
  shading should be visible with lines weaving through it.
- **Length:** ~10s raw.

### CLIP C — Pipeline-health strip
- **What:** The Simulator → Kafka → Spark → Redis strip. Hold on the **msg/s** number as it ticks;
  the "~80% kept" and "Redis TS keys" nodes read as the architecture.
- **Length:** ~8s raw.

### CLIP D — KPIs (b-roll / optional opener)
- **What:** The KPI row (active sensors · ingest rate · clean kept · anomalies rejected) updating.
  Good as a 2–3s establishing shot under caption 1.
- **Length:** ~6s raw.

### CLIP E — Real Grafana (OPTIONAL)
- **What:** If the stack is up, the provisioned Grafana dashboard at :3000. A 3s cut adds a "real
  ops tool" beat. Only if it's populated and clean.
- **Length:** ~6s raw.

### Title + End cards
- Built in the editor. Text from `01_caption_script_hero.md` (scenes 0 and 4).

---

## Assembly order (in the editor) = final scenes
`Title → CLIP D/A → CLIP A → CLIP B → CLIP C → End card`
Map to the script: 0 → 1 → 2 → 3 → 4. Design the end frame to loop back to the first.

---

## Screen Studio tips
- **Capture real refresh motion** — never simulate movement with a zoom; the live update *is* the shot.
- Keep the **badge** ("◆ DEMO STREAM" / "● LIVE · REDIS") in frame.
- **One motion per beat.** Captions lower-third.
- **Music:** clean tech bed, slightly more energy than the other promos (it's a fast, live system).
- Export **1080p MP4, 30–60fps**; make the cut **loopable** for LinkedIn autoplay.

## Final QC before you publish
- [ ] The first 5 seconds clearly show data **moving**.
- [ ] No container cold-start, logs, or connection errors on screen.
- [ ] The pipeline strip makes the architecture obvious without a caption.
- [ ] Under ~45s and loops cleanly.
- [ ] Ends with a clear "what is this + who made it" card.
