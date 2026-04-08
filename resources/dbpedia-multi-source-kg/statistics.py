import os
import sys

from dotenv import load_dotenv
from pyodibel.management.spark_mgr import get_spark_session
from pyodibel.operations.rdf.rdf2 import rDF2
from pyspark.sql import functions as F


def compute_and_write_stats(input_path: str) -> None:

    spark = get_spark_session("CrossMultiSourceKGGenerator")

    # --- Load data ---
    rdf = rDF2.parse(spark, input_path)

    df = rdf.df.select("s", "p", "o")

    df = df.persist()

    triple_count_df = df.agg(F.count("*").alias("triple_count"))

    subject_counts_df = df.groupBy("s").count()
    predicate_counts_df = df.groupBy("p").count()
    object_counts_df = df.groupBy("o").count()

    subject_counts_df = subject_counts_df.orderBy(F.desc("count"))
    predicate_counts_df = predicate_counts_df.orderBy(F.desc("count"))
    object_counts_df = object_counts_df.orderBy(F.desc("count"))

    stats_path = os.path.join(os.path.dirname(input_path), "stats")
    triple_count_path = os.path.join(stats_path, "triple_count.csv")
    subject_count_path = os.path.join(stats_path, "subject_counts.csv")
    predicate_counts_path = os.path.join(stats_path, "predicate_counts.csv")
    object_counts_path = os.path.join(stats_path, "object_counts.csv")

    triple_count_df.coalesce(1).write.mode("overwrite").csv(triple_count_path, header=True)

    subject_counts_df.write.mode("overwrite").option("compression", "gzip").csv(subject_count_path, header=True)
    predicate_counts_df.write.mode("overwrite").option("compression", "gzip").csv(predicate_counts_path, header=True)
    object_counts_df.write.mode("overwrite").option("compression", "gzip").csv(object_counts_path, header=True)

    df.unpersist()

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: statistics.py <input_path>")
        sys.exit(1)

    input_path = sys.argv[1]

    compute_and_write_stats(input_path)
