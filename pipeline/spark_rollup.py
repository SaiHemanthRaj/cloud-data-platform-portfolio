"""Local Spark daily store rollup from the journal's canonical snapshot."""

import argparse
import json
from pathlib import Path


def rollup(source, output):
    from pyspark.sql import SparkSession, functions as F, types as T

    schema = T.StructType(
        [
            T.StructField("event_id", T.StringType(), False),
            T.StructField("event_date", T.DateType(), False),
            T.StructField("store_id", T.StringType(), False),
            T.StructField("product_id", T.StringType(), False),
            T.StructField("quantity", T.LongType(), False),
            T.StructField("unit_price_cents", T.LongType(), False),
            T.StructField("updated_at", T.TimestampType(), False),
        ]
    )
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("retail-event-rollup")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        df = (
            spark.read.option("header", True)
            .option("mode", "FAILFAST")
            .schema(schema)
            .csv(str(source))
        )
        nulls = " OR ".join(f"{c} IS NULL" for c in df.columns)
        if df.filter(nulls).limit(1).count():
            raise ValueError("Null field in canonical snapshot")
        if df.filter((F.col("quantity") <= 0) | (F.col("unit_price_cents") < 0)).limit(1).count():
            raise ValueError("Invalid amount")
        if df.groupBy("event_id").count().filter("count>1").limit(1).count():
            raise ValueError("Duplicate canonical event")
        totals = df.agg(
            F.count("*").alias("events"),
            F.sum(F.col("quantity") * F.col("unit_price_cents")).alias("revenue_cents"),
        ).first()
        grouped = df.groupBy("event_date", "store_id").agg(
            F.count("*").alias("events"),
            F.sum(F.col("quantity") * F.col("unit_price_cents")).alias("revenue_cents"),
        )
        check = grouped.agg(
            F.sum("events").alias("events"), F.sum("revenue_cents").alias("revenue_cents")
        ).first()
        if totals != check:
            raise ValueError("Rollup reconciliation failed")
        grouped.write.mode("overwrite").partitionBy("event_date").parquet(str(output))
        readback = spark.read.parquet(str(output))
        report = {
            "spark_version": spark.version,
            "source_events": totals.events,
            "revenue_cents": totals.revenue_cents,
            "groups": readback.count(),
            "reconciled": True,
            "sample": [
                r.asDict() for r in readback.orderBy("event_date", "store_id").limit(5).collect()
            ],
        }
        for row in report["sample"]:
            row["event_date"] = str(row["event_date"])
        return report
    finally:
        spark.stop()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", default="artifacts/canonical.csv")
    p.add_argument("--output", default="artifacts/daily_sales")
    a = p.parse_args()
    report = rollup(Path(a.source), Path(a.output))
    dest = Path(a.output).parent / "spark_report.json"
    dest.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
