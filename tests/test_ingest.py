import csv
import sqlite3
from pathlib import Path
import pytest
from pipeline.ingest import ingest, export_canonical, FIELDS, immutable_write

ROOT = Path(__file__).resolve().parents[1]


def make_file(tmp_path, rows, header=FIELDS):
    p = tmp_path / "events.csv"
    with p.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    return p


def row(**changes):
    r = dict(zip(FIELDS, ["E1", "2026-09-01", "S1", "P1", "2", "1999", "2026-09-02T00:00:00"]))
    r.update(changes)
    return list(r.values())


def test_demo_row_conservation_and_replay(tmp_path):
    a = ingest(ROOT / "data/sample/events.csv", tmp_path)
    assert a["source_rows"] == 5150 and a["canonical_events"] == 5000
    assert a["quarantined"] == 30 and a["duplicates"] == 100 and a["updated"] == 20
    b = ingest(ROOT / "data/sample/events.csv", tmp_path)
    assert b["status"] == "replayed"
    assert a["objects"] == b["objects"]
    p = export_canonical(tmp_path)
    assert len(list(csv.DictReader(p.open()))) == 5000


def test_out_of_order_and_conflicting_updates(tmp_path):
    p = make_file(
        tmp_path,
        [
            row(),
            row(quantity="3", updated_at="2026-09-03T00:00:00"),
            row(quantity="9", updated_at="2026-09-01T00:00:00"),
            row(quantity="4", updated_at="2026-09-03T00:00:00"),
        ],
    )
    r = ingest(p, tmp_path / "out")
    assert (r["inserted"], r["updated"], r["stale"], r["quarantined"]) == (1, 1, 1, 1)
    with sqlite3.connect(tmp_path / "out/journal.sqlite") as db:
        assert db.execute("SELECT quantity FROM events").fetchone()[0] == 3


@pytest.mark.parametrize(
    "changes",
    [
        {"quantity": "0"},
        {"unit_price_cents": "-1"},
        {"event_date": "bad"},
        {"event_id": ""},
        {"updated_at": "2026-09-02T00:00:00Z"},
    ],
)
def test_invalid_rows_are_quarantined(tmp_path, changes):
    r = ingest(make_file(tmp_path, [row(**changes)]), tmp_path / "out")
    assert r["quarantined"] == 1 and r["canonical_events"] == 0


def test_bad_header_fails_without_committing(tmp_path):
    p = make_file(tmp_path, [["1"]], ["wrong"])
    with pytest.raises(ValueError, match="header"):
        ingest(p, tmp_path / "out")
    with sqlite3.connect(tmp_path / "out/journal.sqlite") as db:
        assert db.execute("SELECT COUNT(*) FROM batches").fetchone()[0] == 0


def test_immutable_collision(tmp_path):
    immutable_write(tmp_path, "one", b"a")
    with pytest.raises(ValueError, match="collision"):
        immutable_write(tmp_path, "one", b"b")
    assert (tmp_path / "one").read_bytes() == b"a"


def test_failure_before_commit_can_be_retried(tmp_path, monkeypatch):
    import pipeline.ingest as m

    original = m.immutable_write

    def fail(root, key, payload):
        if "/curated/" in key:
            raise OSError("simulated disk failure")
        return original(root, key, payload)

    p = make_file(tmp_path, [row()])
    out = tmp_path / "out"
    monkeypatch.setattr(m, "immutable_write", fail)
    with pytest.raises(OSError):
        m.ingest(p, out)
    with sqlite3.connect(out / "journal.sqlite") as db:
        assert db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0
    monkeypatch.setattr(m, "immutable_write", original)
    assert m.ingest(p, out)["status"] == "loaded"


def test_missing_postcommit_manifest_is_repaired_on_replay(tmp_path, monkeypatch):
    import pipeline.ingest as m

    original = m.immutable_write

    def fail(root, key, payload):
        if "/manifests/" in key:
            raise OSError("simulated postcommit disk failure")
        return original(root, key, payload)

    p = make_file(tmp_path, [row()])
    out = tmp_path / "out"
    monkeypatch.setattr(m, "immutable_write", fail)
    with pytest.raises(OSError):
        m.ingest(p, out)
    with sqlite3.connect(out / "journal.sqlite") as db:
        assert db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
    monkeypatch.setattr(m, "immutable_write", original)
    report = m.ingest(p, out)
    assert report["status"] == "replayed"
    assert (out / f"objects/manifests/{report['batch_hash']}.json").exists()
