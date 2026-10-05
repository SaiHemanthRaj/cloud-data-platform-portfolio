"""Immutable raw landing and transactional, replayable event ingestion."""

import argparse
import csv
import hashlib
import io
import json
import logging
import os
import sqlite3
import tempfile
from datetime import date, datetime
from pathlib import Path

LOG = logging.getLogger(__name__)
FIELDS = [
    "event_id",
    "event_date",
    "store_id",
    "product_id",
    "quantity",
    "unit_price_cents",
    "updated_at",
]
DDL = """
CREATE TABLE IF NOT EXISTS events (
event_id TEXT PRIMARY KEY,event_date TEXT NOT NULL,store_id TEXT NOT NULL,product_id TEXT NOT NULL,
quantity INTEGER NOT NULL CHECK(quantity>0),unit_price_cents INTEGER NOT NULL CHECK(unit_price_cents>=0),
updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS batches (batch_hash TEXT PRIMARY KEY,report_json TEXT NOT NULL);
"""


def immutable_write(root, key, payload):
    """Publish only fully flushed bytes. Existing content must match exactly."""
    dest = root / key
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=dest.parent, prefix=".partial-")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        try:
            os.link(name, dest)
        except FileExistsError:
            if dest.read_bytes() != payload:
                raise ValueError("Immutable object collision")
    finally:
        Path(name).unlink(missing_ok=True)
    return dest


def validate(row):
    if set(row) != set(FIELDS) or any(row.get(k) in (None, "") for k in FIELDS):
        raise ValueError("missing_or_extra_field")
    for k in ["event_id", "store_id", "product_id"]:
        if len(row[k]) > 64 or not all(c.isalnum() or c == "_" for c in row[k]):
            raise ValueError("invalid_identifier")
    day = date.fromisoformat(row["event_date"]).isoformat()
    ts = datetime.fromisoformat(row["updated_at"])
    if ts.tzinfo is not None:
        raise ValueError("updated_at_must_be_naive_utc")
    quantity = int(row["quantity"])
    price = int(row["unit_price_cents"])
    if quantity <= 0 or quantity > 1_000_000 or price < 0 or price > 1_000_000_000:
        raise ValueError("invalid_amount")
    return (
        row["event_id"],
        day,
        row["store_id"],
        row["product_id"],
        quantity,
        price,
        ts.isoformat(timespec="microseconds"),
    )


def ingest(source, root):
    source = Path(source)
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    payload = source.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    raw_key = f"objects/raw/{digest}.csv"
    immutable_write(root, raw_key, payload)
    db = sqlite3.connect(root / "journal.sqlite", timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript(DDL)
    try:
        db.execute("BEGIN IMMEDIATE")
        found = db.execute(
            "SELECT report_json FROM batches WHERE batch_hash=?", (digest,)
        ).fetchone()
        if found:
            db.rollback()
            report = json.loads(found[0])
            immutable_write(root, f"objects/manifests/{digest}.json", found[0].encode())
            report["status"] = "replayed"
            return report
        reader = csv.DictReader(io.StringIO(payload.decode("utf-8")))
        if reader.fieldnames != FIELDS:
            raise ValueError("unexpected_header")
        rejects = []
        accepted = []
        duplicates = 0
        stale = 0
        inserted = 0
        updated = 0
        total = 0
        for line, row in enumerate(reader, 2):
            total += 1
            try:
                event = validate(row)
            except (ValueError, TypeError) as exc:
                rejects.append({"line": line, "reason": str(exc), "row": row})
                continue
            previous = db.execute("SELECT * FROM events WHERE event_id=?", (event[0],)).fetchone()
            if previous and previous[-1] == event[-1]:
                if previous == event:
                    duplicates += 1
                else:
                    rejects.append({"line": line, "reason": "conflicting_version", "row": row})
                continue
            if previous and previous[-1] > event[-1]:
                stale += 1
                continue
            db.execute(
                """INSERT INTO events VALUES (?,?,?,?,?,?,?) ON CONFLICT(event_id) DO UPDATE SET
             event_date=excluded.event_date,store_id=excluded.store_id,product_id=excluded.product_id,
             quantity=excluded.quantity,unit_price_cents=excluded.unit_price_cents,updated_at=excluded.updated_at""",
                event,
            )
            if previous:
                updated += 1
            else:
                inserted += 1
            accepted.append(event)
        out = io.StringIO()
        writer = csv.writer(out, lineterminator="\n")
        writer.writerow(FIELDS)
        writer.writerows(accepted)
        curated = out.getvalue().encode()
        quarantine = (json.dumps(rejects, indent=2, sort_keys=True) + "\n").encode()
        curated_key = f"objects/curated/{hashlib.sha256(curated).hexdigest()}.csv"
        quarantine_key = f"objects/quarantine/{hashlib.sha256(quarantine).hexdigest()}.json"
        immutable_write(root, curated_key, curated)
        immutable_write(root, quarantine_key, quarantine)
        report = {
            "status": "loaded",
            "batch_hash": digest,
            "source_rows": total,
            "inserted": inserted,
            "updated": updated,
            "duplicates": duplicates,
            "stale": stale,
            "quarantined": len(rejects),
            "accepted_versions": len(accepted),
            "canonical_events": db.execute("SELECT COUNT(*) FROM events").fetchone()[0],
            "objects": [
                {"key": k, "sha256": hashlib.sha256(v).hexdigest(), "bytes": len(v)}
                for k, v in [
                    (raw_key, payload),
                    (curated_key, curated),
                    (quarantine_key, quarantine),
                ]
            ],
        }
        if inserted + updated + duplicates + stale + len(rejects) != total:
            raise RuntimeError("Row conservation failed")
        text = json.dumps(report, indent=2, sort_keys=True) + "\n"
        # The journal certifies completion. A missing manifest is repaired on replay.
        db.execute("INSERT INTO batches VALUES (?,?)", (digest, text))
        db.commit()
        immutable_write(root, f"objects/manifests/{digest}.json", text.encode())
        LOG.info(
            "batch_committed %s",
            json.dumps(
                {
                    k: report[k]
                    for k in ["batch_hash", "source_rows", "quarantined", "inserted", "updated"]
                }
            ),
        )
        return report
    except Exception:
        db.rollback()
        LOG.error("batch_failed hash=%s; journal determines commit state", digest)
        raise
    finally:
        db.close()


def export_canonical(root):
    """Write a consistent database snapshot; Spark reads this, not append-only batch files."""
    root = Path(root)
    db = sqlite3.connect(root / "journal.sqlite")
    try:
        rows = db.execute("SELECT * FROM events ORDER BY event_id").fetchall()
    finally:
        db.close()
    snapshot = root / "canonical.csv"
    fd, name = tempfile.mkstemp(dir=root, prefix=".snapshot-")
    try:
        with os.fdopen(fd, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(FIELDS)
            w.writerows(rows)
            f.flush()
            os.fsync(f.fileno())
        os.replace(name, snapshot)
    finally:
        Path(name).unlink(missing_ok=True)
    return snapshot


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--root", type=Path, default=Path(os.getenv("PIPELINE_ROOT", "artifacts")))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    report = ingest(args.source, args.root)
    export_canonical(args.root)
    (args.root / "run_report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
