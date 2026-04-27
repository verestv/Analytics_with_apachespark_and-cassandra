# =============================================================================
# main.py  —  Amazon Reviews → Cassandra ETL
# PySpark 4.x: no spark-cassandra-connector needed
# Dataset: verified purchases only (46k rows) — same as previous homework
# Strategy: Spark transforms → toLocalIterator() → cassandra-driver batches
# =============================================================================

import os
import time
from pathlib import Path

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F

# ── Config ────────────────────────────────────────────────────────────────────
base_dir = Path(__file__).resolve().parent
load_dotenv(base_dir / ".env")

CASSANDRA_HOST     = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT     = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_USER     = os.getenv("CASSANDRA_USER", "")
CASSANDRA_PASSWORD = os.getenv("CASSANDRA_PASSWORD", "")
KEYSPACE           = "reviews"
CSV_PATH           = base_dir / "amazon_reviews.csv"
BATCH_SIZE         = 5


# ── Helpers ───────────────────────────────────────────────────────────────────
def fmt_time(s):
    return f"{s:.0f}s" if s < 60 else f"{int(s)//60}m {int(s)%60}s"


def section(title):
    print(f"\n{'═'*60}\n  {title}\n{'═'*60}")


def get_cassandra_session():
    auth = PlainTextAuthProvider(CASSANDRA_USER, CASSANDRA_PASSWORD) if CASSANDRA_USER else None
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT, auth_provider=auth)
    session = cluster.connect(KEYSPACE)
    return cluster, session


def insert_rows(session, prepared, df, label):
    """Insert rows one by one via toLocalIterator — avoids batch size limit on large text fields."""
    print(f"  → Inserting into {label}...")
    t = time.time()
    count = 0

    for row in df.toLocalIterator():
        session.execute(prepared, row)
        count += 1
        if count % 5_000 == 0:
            print(f"    {count:,} rows...  ⏱ {fmt_time(time.time()-t)}")

    print(f"  ✅  {label} done | {count:,} rows | ⏱ {fmt_time(time.time()-t)}")


# =============================================================================
# TABLE LOADER CLASSES
# All tables use df_verified — same dataset as previous homework
# =============================================================================

class ReviewsByProductLoader:
    """
    TABLE: reviews_by_product
    Endpoints 1 & 2 — reviews for product_id, optionally by star_rating
    Partition: product_id  |  Clustering: star_rating DESC, review_id ASC
    """
    CQL = """
        INSERT INTO reviews_by_product
        (product_id, star_rating, review_id, customer_id, product_title,
         review_headline, review_body, review_date,
         helpful_votes, total_votes, verified_purchase)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    def load(self, df, session):
        print(f"\n{'─'*60}")
        print("  [TABLE 1+2] reviews_by_product")
        prepared = session.prepare(self.CQL)
        insert_rows(session, prepared, df.select(
            "product_id", "star_rating", "review_id", "customer_id",
            "product_title", "review_headline", "review_body", "review_date",
            "helpful_votes", "total_votes", "verified_purchase"
        ), "reviews_by_product")


class ReviewsByCustomerLoader:
    """
    TABLE: reviews_by_customer
    Endpoint 3 — all reviews for a customer_id
    Partition: customer_id  |  Clustering: review_date DESC, review_id ASC
    Separate table — querying reviews_by_product by customer_id
    would require ALLOW FILTERING.
    """
    CQL = """
        INSERT INTO reviews_by_customer
        (customer_id, review_date, review_id, product_id, product_title,
         star_rating, review_headline, review_body,
         helpful_votes, total_votes, verified_purchase)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    def load(self, df, session):
        print(f"\n{'─'*60}")
        print("  [TABLE 3] reviews_by_customer")
        prepared = session.prepare(self.CQL)
        insert_rows(session, prepared, df.select(
            "customer_id", "review_date", "review_id", "product_id",
            "product_title", "star_rating", "review_headline", "review_body",
            "helpful_votes", "total_votes", "verified_purchase"
        ), "reviews_by_customer")


class TopProductsLoader:
    """
    TABLE: top_products_by_period
    Endpoint 4 — N most reviewed products for a given period
    Partition: year_month  |  Clustering: review_count DESC, product_id ASC
    Pre-aggregated by Spark — small result, safe to stream.
    """
    CQL = """
        INSERT INTO top_products_by_period
        (year_month, review_count, product_id, product_title)
        VALUES (?, ?, ?, ?)
    """

    def load(self, df, session):
        print(f"\n{'─'*60}")
        print("  [TABLE 4] top_products_by_period")
        prepared = session.prepare(self.CQL)
        agg = (
            df
            .withColumn("year_month", F.date_format("review_date", "yyyy-MM"))
            .groupBy("year_month", "product_id", "product_title")
            .agg(F.count("*").cast("int").alias("review_count"))
            .select("year_month", "review_count", "product_id", "product_title")
        )
        insert_rows(session, prepared, agg, "top_products_by_period")


class TopCustomersLoader:
    """
    TABLE: top_customers_by_period
    Endpoint 5 — N most productive customers (verified purchases only)
    Partition: year_month  |  Clustering: review_count DESC, customer_id ASC
    df_verified passed in — already filtered upstream.
    """
    CQL = """
        INSERT INTO top_customers_by_period
        (year_month, review_count, customer_id)
        VALUES (?, ?, ?)
    """

    def load(self, df, session):
        print(f"\n{'─'*60}")
        print("  [TABLE 5] top_customers_by_period")
        prepared = session.prepare(self.CQL)
        agg = (
            df
            .withColumn("year_month", F.date_format("review_date", "yyyy-MM"))
            .groupBy("year_month", "customer_id")
            .agg(F.count("*").cast("int").alias("review_count"))
            .select("year_month", "review_count", "customer_id")
        )
        insert_rows(session, prepared, agg, "top_customers_by_period")


class TopHatersLoader:
    """
    TABLE: top_haters_by_period
    Endpoint 6 — N customers with most 1 or 2 star reviews per period
    Partition: year_month  |  Clustering: low_star_count DESC, customer_id ASC
    Filtered to star_rating IN (1, 2) before aggregation.
    """
    CQL = """
        INSERT INTO top_haters_by_period
        (year_month, low_star_count, customer_id)
        VALUES (?, ?, ?)
    """

    def load(self, df, session):
        print(f"\n{'─'*60}")
        print("  [TABLE 6] top_haters_by_period")
        prepared = session.prepare(self.CQL)
        agg = (
            df
            .filter(F.col("star_rating").isin(1, 2))
            .withColumn("year_month", F.date_format("review_date", "yyyy-MM"))
            .groupBy("year_month", "customer_id")
            .agg(F.count("*").cast("int").alias("low_star_count"))
            .select("year_month", "low_star_count", "customer_id")
        )
        insert_rows(session, prepared, agg, "top_haters_by_period")


class TopBackersLoader:
    """
    TABLE: top_backers_by_period
    Endpoint 7 — N customers with most 4 or 5 star reviews per period
    Partition: year_month  |  Clustering: high_star_count DESC, customer_id ASC
    Filtered to star_rating IN (4, 5) before aggregation.
    """
    CQL = """
        INSERT INTO top_backers_by_period
        (year_month, high_star_count, customer_id)
        VALUES (?, ?, ?)
    """

    def load(self, df, session):
        print(f"\n{'─'*60}")
        print("  [TABLE 7] top_backers_by_period")
        prepared = session.prepare(self.CQL)
        agg = (
            df
            .filter(F.col("star_rating").isin(4, 5))
            .withColumn("year_month", F.date_format("review_date", "yyyy-MM"))
            .groupBy("year_month", "customer_id")
            .agg(F.count("*").cast("int").alias("high_star_count"))
            .select("year_month", "high_star_count", "customer_id")
        )
        insert_rows(session, prepared, agg, "top_backers_by_period")


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def main():
    t_start = time.time()
    section("Amazon Reviews → Cassandra ETL")

    # ── Stage 1: Spark ────────────────────────────────────────────────────────
    print("\n  [STAGE 1] Starting Spark session...")
    spark = (
        SparkSession.builder
        .appName("amazon-reviews-etl")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("  ✓ Spark ready")

    # ── Stage 2: Read CSV ─────────────────────────────────────────────────────
    print(f"\n  [STAGE 2] Reading CSV: {CSV_PATH}")
    t = time.time()
    df_raw = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("inferSchema", "false")
        .load(str(CSV_PATH))
    )
    print(f"  ✓ Raw rows: {df_raw.count():,} ({fmt_time(time.time()-t)})")

    # ── Stage 3: Clean — exact same logic as previous homework ────────────────
    print("\n  [STAGE 3] Cleaning data...")
    t = time.time()
    critical_cols = ["review_id", "product_id", "star_rating", "review_date"]
    df_clean = (
        df_raw
        .na.drop(subset=critical_cols)
        .withColumn("review_date",       F.to_date(F.col("review_date"), "yyyy-MM-dd"))
        .withColumn("star_rating",       F.col("star_rating").cast("int"))
        .withColumn("helpful_votes",     F.col("helpful_votes").cast("int"))
        .withColumn("total_votes",       F.col("total_votes").cast("int"))
        .withColumn("verified_purchase", F.col("verified_purchase").cast("int"))
    )
    print(f"  ✓ Clean rows: {df_clean.count():,} ({fmt_time(time.time()-t)})")

    # ── Stage 4: Verified only — same filter as previous homework ─────────────
    print("\n  [STAGE 4] Filtering verified purchases...")
    df_verified = df_clean.filter(F.col("verified_purchase") == 1)
    verified_count = df_verified.count()
    print(f"  ✓ Verified rows: {verified_count:,}")

    # ── Stage 5: Connect Cassandra ────────────────────────────────────────────
    print("\n  [STAGE 5] Connecting to Cassandra...")
    cluster, session = get_cassandra_session()
    print(f"  ✓ Connected → {CASSANDRA_HOST}:{CASSANDRA_PORT} / keyspace: {KEYSPACE}")

    # ── Stage 6: Load all 6 tables from df_verified ───────────────────────────
    section("STAGE 6 — Loading tables into Cassandra")

    try:
        # All loaders receive df_verified — same dataset as previous homework
        ReviewsByProductLoader().load(df_verified, session)
        ReviewsByCustomerLoader().load(df_verified, session)
        TopProductsLoader().load(df_verified, session)
        TopCustomersLoader().load(df_verified, session)   # already verified
        TopHatersLoader().load(df_verified, session)
        TopBackersLoader().load(df_verified, session)
    finally:
        cluster.shutdown()
        spark.stop()

    section(f"✅  All done! Total time: {fmt_time(time.time()-t_start)}")


if __name__ == "__main__":
    main()