import os
import sys

from dotenv import load_dotenv
from pyodibel.management.spark_mgr import get_spark_session
from pyodibel.operations.rdf.rdf2 import rDF2


def compute_and_write_stats(input_path: str) -> None:

    spark = get_spark_session("CrossMultiSourceKGGenerator")

    # --- Load data ---
    rdf = rDF2.parse(spark, input_path)
    df = rdf.df

    def partition_counter(rows):
        triple_count = 0
        s_counts = {}
        p_counts = {}
        o_counts = {}

        for row in rows:
            triple_count += 1

            s = row["s"]
            p = row["p"]
            o = row["o"]

            s_counts[s] = s_counts.get(s, 0) + 1
            p_counts[p] = p_counts.get(p, 0) + 1
            o_counts[o] = o_counts.get(o, 0) + 1

        yield triple_count, s_counts, p_counts, o_counts

    def reducer(a, b):
        def merge_dict(d1, d2):
            for k, v in d2.items():
                d1[k] = d1.get(k, 0) + v
            return d1

        return (
            a[0] + b[0],
            merge_dict(a[1], b[1]),
            merge_dict(a[2], b[2]),
            merge_dict(a[3], b[3]),
        )

    partial = df.rdd.mapPartitions(partition_counter)

    total_triples, s_counts, p_counts, o_counts = partial.reduce(reducer)

    stats_path = os.path.join(os.path.dirname(input_path), "stats")
    triple_count_path = os.path.join(stats_path, "triple_count.csv")
    subject_count_path = os.path.join(stats_path, "subject_counts.csv")
    predicate_counts_path = os.path.join(stats_path, "predicate_counts.csv")
    object_counts_path = os.path.join(stats_path, "object_counts.csv")

    triple_df = spark.createDataFrame(
        [(total_triples,)], ["triple_count"]
    )

    subject_df = spark.createDataFrame(
        [(k, v) for k, v in s_counts.items()],
        ["s", "count"]
    )

    predicate_df = spark.createDataFrame(
        [(k, v) for k, v in p_counts.items()],
        ["p", "count"]
    )

    object_df = spark.createDataFrame(
        [(k, v) for k, v in o_counts.items()],
        ["o", "count"]
    )

    triple_df.coalesce(1).write.mode("overwrite").csv(triple_count_path, header=True)

    subject_df.write.mode("overwrite").csv(subject_count_path, header=True)
    predicate_df.write.mode("overwrite").csv(predicate_counts_path, header=True)
    object_df.write.mode("overwrite").csv(object_counts_path, header=True)

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: statistics.py <input_path>")
        sys.exit(1)

    input_path = sys.argv[1]

    compute_and_write_stats(input_path)
