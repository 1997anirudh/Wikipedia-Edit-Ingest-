"""PySpark Structured Streaming job: Kafka -> windowed aggregates + lake.

Reads normalized wiki edits from the `wikipedia.edits` topic, computes
1-minute windowed aggregations, and writes them to DuckDB for the
dashboard. The curated event stream is also written to local Parquet,
partitioned by date, as a small data-lake sink.

Aggregations (mirrored in pandas in common/aggregations.py for testing):

    wiki_minute_stats : edits per wiki per minute, bot vs human split,
                        byte-change sum / absolute sum / average
    top_pages         : top 10 pages per minute by edit count
    top_editors       : top 10 editors per minute by edit count

DuckDB writes use foreachBatch with delete-then-insert per window, which
makes micro-batch retries idempotent: reprocessing a window replaces its
rows instead of duplicating them.

Top-N ranking is done inside foreachBatch, not on the stream itself:
Structured Streaming does not support window functions such as
ROW_NUMBER over streaming DataFrames, but the micro-batch DataFrame
handed to foreachBatch is a regular static DataFrame where ranking is
fully supported.

Configuration (environment variables):
    KAFKA_BROKER    default localhost:19092; compose sets redpanda:9092
    KAFKA_TOPIC     default wikipedia.edits
    DUCKDB_PATH     default data/duckdb/wikipedia.duckdb; compose /data/...
    PARQUET_PATH    default data/parquet; compose /data/parquet
    CHECKPOINT_PATH default data/checkpoints; compose /data/checkpoints
"""

from __future__ import annotations

import os

import duckdb
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    abs as spark_abs,
)
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    from_unixtime,
    row_number,
    to_date,
    window,
)
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Keep in sync with common/events.py (WikiEdit).
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", LongType()),
        StructField("type", StringType()),
        StructField("wiki", StringType()),
        StructField("title", StringType()),
        StructField("user", StringType()),
        StructField("bot", BooleanType()),
        StructField("minor", BooleanType()),
        StructField("namespace", IntegerType()),
        StructField("timestamp", LongType()),
        StructField("comment", StringType()),
        StructField("byte_change", LongType()),
        StructField("old_length", LongType()),
        StructField("new_length", LongType()),
        StructField("revision_old", LongType()),
        StructField("revision_new", LongType()),
        StructField("server_name", StringType()),
    ]
)

TOP_N = 10
WATERMARK = "2 minutes"

DUCKDB_SCHEMA = """
CREATE TABLE IF NOT EXISTS wiki_minute_stats (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    wiki VARCHAR,
    edits BIGINT,
    bot_edits BIGINT,
    human_edits BIGINT,
    total_byte_change BIGINT,
    abs_byte_change BIGINT,
    avg_byte_change DOUBLE
);
CREATE TABLE IF NOT EXISTS top_pages (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    wiki VARCHAR,
    title VARCHAR,
    edits BIGINT
);
CREATE TABLE IF NOT EXISTS top_editors (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    "user" VARCHAR,
    edits BIGINT
);
"""


def write_batch_to_duckdb(table: str, batch_df: DataFrame, db_path: str) -> None:
    """Idempotently upsert one micro-batch into a DuckDB table.

    Rows for the window(s) present in the batch are deleted first, then
    the batch is inserted, so a retried micro-batch cannot duplicate data.
    """
    pdf = batch_df.toPandas()
    if pdf.empty:
        return
    con = duckdb.connect(db_path)
    try:
        con.execute(DUCKDB_SCHEMA)
        window_starts = sorted(pdf["window_start"].unique())
        placeholders = ", ".join("?" for _ in window_starts)
        con.execute(
            f"DELETE FROM {table} WHERE window_start IN ({placeholders})",
            list(window_starts),
        )
        con.register("batch_pdf", pdf)
        columns = ", ".join(f'"{c}"' for c in pdf.columns)
        con.execute(f"INSERT INTO {table} ({columns}) SELECT {columns} FROM batch_pdf")
    finally:
        con.close()


def make_duckdb_writer(table: str, db_path: str):
    def writer(batch_df: DataFrame, _batch_id: int) -> None:
        write_batch_to_duckdb(table, batch_df, db_path)

    return writer


def make_topn_writer(table: str, db_path: str, group_cols: list[str]):
    """foreachBatch writer that ranks a static micro-batch and keeps top N.

    The streaming query only produces per-group counts; ranking happens
    here because ROW_NUMBER is not supported on streaming DataFrames.
    """

    def writer(batch_df: DataFrame, _batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        rank_window = Window.partitionBy("window_start").orderBy(
            col("edits").desc(), *[col(c).asc() for c in group_cols]
        )
        ranked = (
            batch_df.withColumn("rank", row_number().over(rank_window))
            .filter(col("rank") <= TOP_N)
            .drop("rank")
        )
        write_batch_to_duckdb(table, ranked, db_path)

    return writer


def build_aggregations(events: DataFrame) -> dict[str, DataFrame]:
    """Compute the three windowed aggregations from the parsed stream."""
    windowed = events.withWatermark("event_time", WATERMARK)

    wiki_stats = (
        windowed.groupBy(window("event_time", "1 minute"), "wiki")
        .agg(
            count("*").alias("edits"),
            spark_sum(col("bot").cast("long")).alias("bot_edits"),
            spark_sum("byte_change").alias("total_byte_change"),
            spark_sum(spark_abs("byte_change")).alias("abs_byte_change"),
            avg("byte_change").alias("avg_byte_change"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "wiki",
            "edits",
            "bot_edits",
            (col("edits") - col("bot_edits")).alias("human_edits"),
            "total_byte_change",
            "abs_byte_change",
            "avg_byte_change",
        )
    )

    def counts_by(group_cols: list[str]) -> DataFrame:
        # Per-group counts only. The top-N cut is applied per micro-batch
        # in make_topn_writer; see the module docstring for why.
        return (
            windowed.groupBy(window("event_time", "1 minute"), *group_cols)
            .agg(count("*").alias("edits"))
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                *group_cols,
                "edits",
            )
        )

    return {
        "wiki_minute_stats": wiki_stats,
        "top_pages": counts_by(["wiki", "title"]),
        "top_editors": counts_by(["user"]),
    }


# Which tables need top-N ranking in the sink, and the columns that
# define a group (also the ranking tie-break order).
TOPN_TABLES = {
    "top_pages": ["wiki", "title"],
    "top_editors": ["user"],
}


def main() -> None:
    broker = os.environ.get("KAFKA_BROKER", "localhost:19092")
    topic = os.environ.get("KAFKA_TOPIC", "wikipedia.edits")
    duckdb_path = os.environ.get("DUCKDB_PATH", "data/duckdb/wikipedia.duckdb")
    parquet_path = os.environ.get("PARQUET_PATH", "data/parquet")
    checkpoint_path = os.environ.get("CHECKPOINT_PATH", "data/checkpoints")

    os.makedirs(os.path.dirname(os.path.abspath(duckdb_path)), exist_ok=True)
    os.makedirs(parquet_path, exist_ok=True)

    spark = (
        SparkSession.builder.appName("wikipedia-edit-streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    events = (
        raw.select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .filter(col("wiki").isNotNull() & col("timestamp").isNotNull())
        .withColumn("event_time", from_unixtime(col("timestamp")).cast("timestamp"))
    )

    queries = []

    # Aggregations -> DuckDB (one streaming query per table).
    for table, agg_df in build_aggregations(events).items():
        if table in TOPN_TABLES:
            writer = make_topn_writer(table, duckdb_path, TOPN_TABLES[table])
        else:
            writer = make_duckdb_writer(table, duckdb_path)
        queries.append(
            agg_df.writeStream.outputMode("update")
            .foreachBatch(writer)
            .option("checkpointLocation", os.path.join(checkpoint_path, table))
            .start()
        )

    # Curated events -> Parquet data lake, partitioned by event date.
    lake_df = events.withColumn("date", to_date("event_time"))
    queries.append(
        lake_df.writeStream.outputMode("append")
        .format("parquet")
        .partitionBy("date")
        .option("path", parquet_path)
        .option("checkpointLocation", os.path.join(checkpoint_path, "parquet"))
        .start()
    )

    for query in queries:
        query.awaitTermination()


if __name__ == "__main__":
    main()
