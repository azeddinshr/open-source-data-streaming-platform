import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("gharchive-spark")

EVENT_SCHEMA = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("type", T.StringType()),
    T.StructField("actor", T.StructType([
        T.StructField("id", T.LongType()),
        T.StructField("login", T.StringType()),
    ])),
    T.StructField("repo", T.StructType([
        T.StructField("id", T.LongType()),
        T.StructField("name", T.StringType()),
    ])),
    T.StructField("org", T.StructType([
        T.StructField("id", T.LongType()),
        T.StructField("login", T.StringType()),
    ])),
    T.StructField("public", T.BooleanType()),
    T.StructField("created_at", T.StringType()),
])


def build_spark():
    return (
        SparkSession.builder.appName("gharchive-to-parquet")
        .config("spark.sql.session.timeZone", "UTC")
        # same null-on-parse-failure behaviour on Spark 3.5 and 4.x
        .config("spark.sql.ansi.enabled", "false")
        # overwrite only the partitions this run produces
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="/data/raw/gharchive/*.json.gz")
    parser.add_argument("--output", default="/data/parquet/gharchive_events")
    parser.add_argument("--rejected", default="/data/rejected/gharchive")
    args = parser.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    lines = spark.read.text(args.input).select(
        F.col("value"),
        F.col("_metadata.file_name").alias("source_file"),
    )

    parsed = (
        lines
        .withColumn("e", F.from_json("value", EVENT_SCHEMA))
        .withColumn("created_ts", F.to_timestamp(F.col("e.created_at")))
        .cache()
    )

    is_valid = (
        F.col("e.id").isNotNull()
        & F.col("e.type").isNotNull()
        & F.col("e.actor.login").isNotNull()
        & F.col("e.repo.name").isNotNull()
        & F.col("created_ts").isNotNull()
    )

    # file names look like 2026-10-12-9.json.gz (hour is not zero-padded)
    file_ts = F.to_timestamp(
        F.regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2}-\d{1,2})", 1),
        "yyyy-MM-dd-H",
    )

    valid = parsed.filter(is_valid).select(
        F.col("e.id").alias("event_id"),
        F.col("e.type").alias("event_type"),
        F.col("e.actor.id").alias("actor_id"),
        F.col("e.actor.login").alias("actor_login"),
        F.col("e.repo.id").alias("repo_id"),
        F.col("e.repo.name").alias("repo_name"),
        F.col("e.org.login").alias("org_login"),
        F.col("e.public").alias("is_public"),
        F.col("created_ts").alias("created_at"),
        F.get_json_object("value", "$.payload.action").alias("action"),
        F.to_date(file_ts).alias("archive_date"),
        F.hour(file_ts).alias("archive_hour"),
        F.col("source_file"),
    )

    # dedup scoped to one archive hour, so each hour's output depends only on its own file
    deduped = valid.dropDuplicates(["archive_date", "archive_hour", "event_id"]).cache()

    total = parsed.count()
    n_valid = valid.count()
    n_written = deduped.count()
    n_rejected = total - n_valid
    n_duplicates = n_valid - n_written

    log.info("Lines read per file:")
    parsed.groupBy("source_file").count().orderBy("source_file").show(truncate=False)
    log.info(
        "total=%d | rejected=%d | duplicates=%d | written=%d",
        total, n_rejected, n_duplicates, n_written,
    )

    if n_rejected:
        parsed.filter(~is_valid).select("value", "source_file") \
            .write.mode("overwrite").json(args.rejected)
        log.warning("Rejected lines written to %s", args.rejected)

    (
        deduped.repartition("archive_date", "archive_hour")
        .write.mode("overwrite")
        .partitionBy("archive_date", "archive_hour")
        .parquet(args.output)
    )
    log.info("Parquet written to %s", args.output)

    deduped.groupBy("event_type").count().orderBy(F.desc("count")).show(10, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()