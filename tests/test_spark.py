import sqlite3
from pathlib import Path
import pytest

pytest.importorskip("pyspark")
from pipeline.ingest import ingest, export_canonical
from pipeline.spark_rollup import rollup


def test_local_spark_reconciles_and_reads_partitioned_parquet(tmp_path):
    root = Path(__file__).resolve().parents[1]
    ingest(root / "data/sample/events.csv", tmp_path)
    result = rollup(export_canonical(tmp_path), tmp_path / "daily_sales")
    with sqlite3.connect(tmp_path / "journal.sqlite") as db:
        expected = db.execute("SELECT SUM(quantity*unit_price_cents) FROM events").fetchone()[0]
    assert result["source_events"] == 5000 and result["revenue_cents"] == expected
    assert result["groups"] == 80 and result["reconciled"]
    assert list((tmp_path / "daily_sales").glob("event_date=*/*.parquet"))
