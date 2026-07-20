"""
Generates synthetic (fully fictional) streaming-media viewing events to
simulate a raw event feed landing from a streaming app — the kind of
input a feature-engineering pipeline would ingest into Bronze.

Run: python data/generate_raw_events.py
Writes newline-delimited JSON to data/raw/events_*.json (one file per
simulated ingestion batch, mimicking daily/hourly batch drops).
"""
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(7)

RAW_DIR = Path(__file__).resolve().parent / "raw"
RAW_DIR.mkdir(exist_ok=True)

CONTENT = [
    ("C001", "Northern Lights", "drama"),
    ("C002", "Comet Chasers", "sci-fi"),
    ("C003", "Kitchen Wars", "reality"),
    ("C004", "The Long Road", "drama"),
    ("C005", "Small Town Secrets", "mystery"),
    ("C006", "Orbit", "sci-fi"),
    ("C007", "Late Night Laughs", "comedy"),
    ("C008", "Deep Blue", "documentary"),
    ("C009", "Rivals", "sports"),
    ("C010", "After Hours", "comedy"),
]

DEVICES = ["mobile", "smart_tv", "web", "tablet", "game_console"]
EVENT_TYPES = ["play_start", "pause", "resume", "play_complete", "abandon"]

N_USERS = 2000
DAYS = 14
BATCHES_PER_DAY = 4  # simulate 4 ingestion batches per day

start_date = datetime(2026, 6, 1)

for day in range(DAYS):
    for batch in range(BATCHES_PER_DAY):
        batch_ts = start_date + timedelta(days=day, hours=batch * 6)
        events = []
        n_events = random.randint(800, 1500)
        for _ in range(n_events):
            user_id = f"U{random.randint(1, N_USERS):05d}"
            content_id, title, genre = random.choice(CONTENT)
            event_type = random.choices(
                EVENT_TYPES, weights=[30, 15, 10, 25, 20]
            )[0]
            watch_seconds = random.randint(0, 3600) if event_type != "play_start" else 0
            device = random.choice(DEVICES)
            event_ts = batch_ts + timedelta(seconds=random.randint(0, 6 * 3600))

            # Inject a small amount of realistic messiness on purpose:
            # occasional null device, occasional duplicate-looking event,
            # occasional late-arriving event from a prior batch window.
            if random.random() < 0.02:
                device = None

            events.append({
                "user_id": user_id,
                "content_id": content_id,
                "content_title": title,
                "genre": genre,
                "event_type": event_type,
                "watch_seconds": watch_seconds,
                "device": device,
                "event_ts": event_ts.isoformat(),
                "ingestion_batch_ts": batch_ts.isoformat(),
            })

        # duplicate ~1% of events to simulate at-least-once delivery,
        # which the Silver layer is responsible for deduping
        for _ in range(int(len(events) * 0.01)):
            events.append(random.choice(events))

        out_path = RAW_DIR / f"events_{batch_ts.strftime('%Y%m%d_%H%M')}.json"
        with open(out_path, "w") as f:
            for e in events:
                f.write(json.dumps(e) + "\n")

print(f"Generated {DAYS * BATCHES_PER_DAY} batch files in {RAW_DIR}")
