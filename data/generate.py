from pathlib import Path
import csv
import random

ROOT = Path(__file__).resolve().parents[1]
rng = random.Random(42)
rows = []
for i in range(1, 5001):
    rows.append(
        [
            f"E{i:05}",
            f"2026-09-{1 + i % 20:02}",
            f"S{1 + (i // 20) % 4:02}",
            f"P{1 + i % 15:03}",
            rng.randint(1, 5),
            rng.choice([549, 999, 1999, 4999]),
            "2026-09-21T00:00:00",
        ]
    )
duplicates = [r.copy() for r in rows[:100]]
bad = [r.copy() for r in rows[:30]]
for r in bad:
    r[0] = "BAD" + r[0]
    r[4] = 0
corrections = [r.copy() for r in rows[:20]]
for r in corrections:
    r[4] = 7
    r[6] = "2026-09-22T00:00:00"
p = ROOT / "data/sample/events.csv"
with p.open("w", newline="") as f:
    w = csv.writer(f)
    w.writerow(
        [
            "event_id",
            "event_date",
            "store_id",
            "product_id",
            "quantity",
            "unit_price_cents",
            "updated_at",
        ]
    )
    w.writerows(rows + duplicates + bad + corrections)
