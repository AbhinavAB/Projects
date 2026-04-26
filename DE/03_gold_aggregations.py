"""
=============================================================================
LAYER 3 — GOLD: Business Consumption Aggregations
=============================================================================
Reads from Silver DLT tables, performs domain-specific aggregations and
materialises business-ready Gold tables.

Gold tables produced
--------------------
sms_catalog.gold.agg_subscription_metrics_daily
sms_catalog.gold.agg_subscription_metrics_monthly
sms_catalog.gold.agg_revenue_by_plan
sms_catalog.gold.agg_user_cohort_analysis
sms_catalog.gold.agg_device_distribution
sms_catalog.gold.agg_payment_performance
sms_catalog.gold.dim_subscription_status_snapshot   (SCD Type-1 current view)
sms_catalog.gold.fact_subscription_lifecycle        (lifecycle events for churn)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import logging
from config import (
    CATALOG_NAME, SILVER_SCHEMA, GOLD_SCHEMA,
    GOLD_PATH, CHECKPOINT_PATH, TRIGGER_INTERVAL
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger("gold_aggregations")


# ─────────────────────────────────────────────────────────────────────────────
# 1. Spark Session
# ─────────────────────────────────────────────────────────────────────────────

def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SMS_Gold_Aggregations")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


SILVER_PREFIX = f"{CATALOG_NAME}.{SILVER_SCHEMA}"
GOLD_PREFIX   = f"{CATALOG_NAME}.{GOLD_SCHEMA}"


# ─────────────────────────────────────────────────────────────────────────────
# 2. Helpers
# ─────────────────────────────────────────────────────────────────────────────

def read_silver(spark: SparkSession, entity: str) -> DataFrame:
    return spark.table(f"{SILVER_PREFIX}.{entity}")


def upsert_gold(spark: SparkSession, df: DataFrame,
                table_name: str, merge_keys: list[str],
                partition_cols: list[str] = None):
    """
    Merge (upsert) aggregated DataFrame into a Gold Delta table.
    Creates the table if it does not exist.
    """
    full_table = f"{GOLD_PREFIX}.{table_name}"
    path       = f"{GOLD_PATH}/{table_name}"

    # Create table if missing
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table}
        USING DELTA
        LOCATION '{path}'
        TBLPROPERTIES (
            'quality_layer'           = 'gold',
            'delta.enableChangeDataFeed' = 'true',
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true',
            'owner_team'              = 'data_engineering',
            'pipeline'                = 'sms_gold_aggregations'
        )
        {f"PARTITIONED BY ({', '.join(partition_cols)})" if partition_cols else ""}
    """)

    from delta.tables import DeltaTable
    target = DeltaTable.forName(spark, full_table)

    match_condition = " AND ".join(
        [f"target.{k} = source.{k}" for k in merge_keys]
    )

    (
        target.alias("target")
        .merge(df.alias("source"), match_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    logger.info(f"Upserted {df.count()} rows into {full_table}")


def init_gold_schema(spark: SparkSession):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_PREFIX}")
    spark.sql(f"""
        ALTER SCHEMA {GOLD_PREFIX}
        SET TAGS (
            'layer'   = 'gold',
            'env'     = 'production',
            'team'    = 'data_engineering',
            'purpose' = 'business_consumption'
        )
    """)
    logger.info(f"Gold schema ready: {GOLD_PREFIX}")


# ─────────────────────────────────────────────────────────────────────────────
# 3. Gold Aggregations
# ─────────────────────────────────────────────────────────────────────────────

def build_subscription_metrics_daily(spark: SparkSession) -> DataFrame:
    """
    Daily subscription KPIs: new, churned, active, trial, cancelled counts.
    Grain: country_code × plan_code × calendar_date
    """
    sub = read_silver(spark, "subscription_enriched")
    return (
        sub
        .withColumn("metric_date", F.col("start_date"))
        .groupBy(
            "metric_date",
            F.col("u_country_code").alias("country_code"),
            F.col("c_plan_category").alias("plan_category"),
            F.col("plan_code"),
        )
        .agg(
            F.count("sub_ref_id")                                    .alias("total_subscriptions"),
            F.sum(F.when(F.col("status")     == "ACTIVE",   1).otherwise(0)).alias("active_count"),
            F.sum(F.when(F.col("status")     == "TRIALING", 1).otherwise(0)).alias("trialing_count"),
            F.sum(F.when(F.col("status")     == "CANCELLED",1).otherwise(0)).alias("cancelled_count"),
            F.sum(F.when(F.col("status")     == "EXPIRED",  1).otherwise(0)).alias("expired_count"),
            F.sum(F.when(F.col("status")     == "SUSPENDED",1).otherwise(0)).alias("suspended_count"),
            F.sum(F.when(F.col("trial_flag") == True, 1).otherwise(0))      .alias("trial_starts"),
            F.avg("days_remaining")                                  .alias("avg_days_remaining"),
            F.avg("device_utilisation_pct")                          .alias("avg_device_utilisation_pct"),
        )
        .withColumn("churn_rate",
                    F.round(
                        F.col("cancelled_count") / F.nullif(F.col("total_subscriptions"), F.lit(0)) * 100,
                        4
                    ))
        .withColumn("_computed_at", F.current_timestamp())
    )


def build_subscription_metrics_monthly(spark: SparkSession) -> DataFrame:
    """
    Monthly rollup: MRR, ARR, churn rate, net new subscriptions.
    Grain: country_code × plan_category × year_month
    """
    sub = read_silver(spark, "subscription_enriched")
    cat = read_silver(spark, "catalog")

    enriched = sub.join(
        cat.select("plan_code",
                   F.col("monthly_price").alias("cat_monthly_price"),
                   F.col("annual_price") .alias("cat_annual_price")),
        on="plan_code", how="left"
    )

    return (
        enriched
        .withColumn("year_month",   F.date_format("start_date", "yyyy-MM"))
        .groupBy(
            "year_month",
            F.col("u_country_code").alias("country_code"),
            F.col("c_plan_category").alias("plan_category"),
            F.col("renewal_type"),
        )
        .agg(
            F.countDistinct(
                F.when(F.col("status") == "ACTIVE", F.col("sub_ref_id"))
            ).alias("active_subscribers"),
            F.countDistinct(
                F.when(F.col("status") == "CANCELLED", F.col("sub_ref_id"))
            ).alias("churned_subscribers"),
            F.countDistinct(
                F.when(F.col("trial_flag") == True, F.col("sub_ref_id"))
            ).alias("trial_subscribers"),
            # MRR = active subs × monthly price
            F.sum(
                F.when(F.col("status") == "ACTIVE",
                       F.coalesce("cat_monthly_price", F.lit(0.0)))
                .otherwise(F.lit(0.0))
            ).alias("mrr"),
            F.avg("device_utilisation_pct").alias("avg_device_utilisation_pct"),
        )
        .withColumn("arr",          F.round(F.col("mrr") * 12, 2))
        .withColumn("churn_rate",
                    F.round(
                        F.col("churned_subscribers") /
                        F.nullif(
                            F.col("active_subscribers") + F.col("churned_subscribers"),
                            F.lit(0)
                        ) * 100,
                        4
                    ))
        .withColumn("_computed_at", F.current_timestamp())
    )


def build_revenue_by_plan(spark: SparkSession) -> DataFrame:
    """
    Cumulative revenue by plan: total paid, refunded, net, avg per subscriber.
    Grain: plan_code × payment_method × currency_code
    """
    sub = read_silver(spark, "subscription_enriched")
    ord_ = read_silver(spark, "order")
    pay  = read_silver(spark, "payment")

    joined = (
        ord_
        .join(
            pay.select(
                "payment_ref_id",
                F.col("payment_status")  .alias("p_status"),
                F.col("payment_amount")  .alias("p_amount"),
                F.col("payment_method")  .alias("p_method"),
                F.col("payment_currency").alias("p_currency"),
            ),
            on="payment_ref_id", how="left"
        )
        .join(
            sub.select(
                "sub_ref_id",
                "plan_code",
                F.col("c_plan_name")    .alias("plan_name"),
                F.col("c_plan_category").alias("plan_category"),
            ),
            on="sub_ref_id", how="left"
        )
    )

    return (
        joined
        .groupBy("plan_code", "plan_name", "plan_category", "p_method", "p_currency")
        .agg(
            F.count("order_ref_id")                              .alias("total_orders"),
            F.countDistinct("sub_ref_id")                        .alias("distinct_subscriptions"),
            F.sum("p_amount")                                    .alias("gross_revenue"),
            F.sum(
                F.when(F.col("p_status") == "COMPLETED",
                       F.col("p_amount")).otherwise(F.lit(0.0))
            ).alias("collected_revenue"),
            F.sum(
                F.when(F.col("p_status") == "REFUNDED",
                       F.col("p_amount")).otherwise(F.lit(0.0))
            ).alias("refunded_revenue"),
            F.sum(
                F.when(F.col("p_status") == "FAILED",
                       F.col("p_amount")).otherwise(F.lit(0.0))
            ).alias("failed_revenue"),
            F.avg(
                F.when(F.col("p_status") == "COMPLETED",
                       F.col("p_amount"))
            ).alias("avg_transaction_value"),
        )
        .withColumn("net_revenue",
                    F.round(F.col("collected_revenue") - F.col("refunded_revenue"), 2))
        .withColumn("collection_rate",
                    F.round(
                        F.col("collected_revenue") /
                        F.nullif(F.col("gross_revenue"), F.lit(0)) * 100,
                        4
                    ))
        .withColumn("_computed_at", F.current_timestamp())
    )


def build_user_cohort_analysis(spark: SparkSession) -> DataFrame:
    """
    User cohort table: signup month cohort retention metrics.
    Grain: cohort_month × months_since_signup × country_code
    """
    sub = read_silver(spark, "subscription_enriched")

    # Derive cohort month from the subscription start
    windowed = (
        sub
        .withColumn("cohort_month",
                    F.date_format("start_date", "yyyy-MM"))
        .withColumn("activity_month",
                    F.date_format("updated_at", "yyyy-MM"))
        .withColumn("months_since_signup",
                    F.months_between(
                        F.col("updated_at"),
                        F.col("start_date")
                    ).cast("int"))
    )

    return (
        windowed
        .groupBy(
            "cohort_month",
            "months_since_signup",
            F.col("u_country_code").alias("country_code"),
            F.col("u_user_tier").alias("user_tier"),
        )
        .agg(
            F.countDistinct("user_ref_id")       .alias("cohort_users"),
            F.sum(F.when(F.col("is_live") == True, 1).otherwise(0)).alias("retained_users"),
            F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("churned_users"),
            F.avg("c_monthly_price")             .alias("avg_plan_price"),
        )
        .withColumn("retention_rate",
                    F.round(
                        F.col("retained_users") /
                        F.nullif(F.col("cohort_users"), F.lit(0)) * 100,
                        4
                    ))
        .withColumn("_computed_at", F.current_timestamp())
    )


def build_device_distribution(spark: SparkSession) -> DataFrame:
    """
    Device distribution per subscription tier & country.
    Grain: device_type × os_type × plan_category × country_code
    """
    dev = read_silver(spark, "device")
    sub = read_silver(spark, "subscription_enriched")

    return (
        dev
        .join(
            sub.select(
                "sub_ref_id",
                F.col("c_plan_category") .alias("plan_category"),
                F.col("u_country_code")  .alias("country_code"),
                F.col("u_user_tier")     .alias("user_tier"),
            ),
            on="sub_ref_id", how="left"
        )
        .groupBy("device_type", "os_type", "plan_category", "country_code", "user_tier")
        .agg(
            F.count("device_ref_id")                             .alias("total_devices"),
            F.sum(F.when(F.col("is_active") == True, 1).otherwise(0)).alias("active_devices"),
            F.sum(F.when(F.col("is_recently_active") == True, 1).otherwise(0)).alias("recently_active_devices"),
            F.avg("days_since_last_seen")                        .alias("avg_days_since_last_seen"),
        )
        .withColumn("active_device_pct",
                    F.round(
                        F.col("active_devices") /
                        F.nullif(F.col("total_devices"), F.lit(0)) * 100,
                        2
                    ))
        .withColumn("_computed_at", F.current_timestamp())
    )


def build_payment_performance(spark: SparkSession) -> DataFrame:
    """
    Payment success rates, failure reasons, and gateway performance.
    Grain: payment_gateway × payment_method × payment_currency × month
    """
    pay = read_silver(spark, "payment")

    return (
        pay
        .withColumn("payment_month", F.date_format("payment_date", "yyyy-MM"))
        .groupBy("payment_gateway", "payment_method", "payment_currency", "payment_month")
        .agg(
            F.count("payment_ref_id")                            .alias("total_transactions"),
            F.sum(F.when(F.col("payment_status") == "COMPLETED",  1).otherwise(0)).alias("successful_transactions"),
            F.sum(F.when(F.col("payment_status") == "FAILED",     1).otherwise(0)).alias("failed_transactions"),
            F.sum(F.when(F.col("payment_status") == "REFUNDED",   1).otherwise(0)).alias("refunded_transactions"),
            F.sum(F.when(F.col("payment_status") == "DISPUTED",   1).otherwise(0)).alias("disputed_transactions"),
            F.sum(F.when(F.col("payment_status") == "COMPLETED",  F.col("payment_amount")).otherwise(F.lit(0.0))).alias("total_collected"),
            F.sum(F.when(F.col("payment_status") == "REFUNDED",   F.col("payment_amount")).otherwise(F.lit(0.0))).alias("total_refunded"),
            F.avg(F.when(F.col("payment_status") == "COMPLETED",  F.col("payment_amount"))).alias("avg_successful_amount"),
            F.avg("retry_count")                                 .alias("avg_retry_count"),
        )
        .withColumn("success_rate",
                    F.round(
                        F.col("successful_transactions") /
                        F.nullif(F.col("total_transactions"), F.lit(0)) * 100,
                        4
                    ))
        .withColumn("failure_rate",
                    F.round(
                        F.col("failed_transactions") /
                        F.nullif(F.col("total_transactions"), F.lit(0)) * 100,
                        4
                    ))
        .withColumn("net_collected",
                    F.round(F.col("total_collected") - F.col("total_refunded"), 2))
        .withColumn("_computed_at", F.current_timestamp())
    )


def build_subscription_status_snapshot(spark: SparkSession) -> DataFrame:
    """
    SCD Type-1 current snapshot of all subscriptions with full context.
    Used as a primary Gold dimension for BI tools.
    """
    return (
        read_silver(spark, "subscription_enriched")
        .filter(F.col("status").isin("ACTIVE", "TRIALING", "SUSPENDED"))
        .select(
            "sub_ref_id", "plan_code",
            "u_user_ref_id", "u_country_code", "u_user_tier", "u_age_band",
            "u_verified_flag", "u_email",          # masked in Gold via policy
            "c_plan_name", "c_plan_category",
            "c_monthly_price", "c_annual_price", "c_currency_code",
            "status", "start_date", "end_date",
            "renewal_type", "billing_cycle", "trial_flag",
            "is_live", "days_remaining",
            "active_device_count", "device_utilisation_pct",
            "pay_status", "pay_amount", "pay_method", "pay_date",
        )
        .withColumn("snapshot_at", F.current_timestamp())
    )


def build_subscription_lifecycle(spark: SparkSession) -> DataFrame:
    """
    Lifecycle event table: one row per status transition.
    Enables churn prediction model features.
    """
    sub = read_silver(spark, "subscription")
    ord_ = read_silver(spark, "order")

    # Event derivation: one row per subscription × status
    transitions = (
        sub
        .select(
            "sub_ref_id", "user_ref_id", "plan_code",
            "status", "trial_flag",
            "start_date", "end_date",
            "created_at", "updated_at",
        )
        .withColumn("lifecycle_event",
                    F.when(F.col("status") == "ACTIVE",    F.lit("SUBSCRIPTION_ACTIVATED"))
                    .when(F.col("status") == "TRIALING",   F.lit("TRIAL_STARTED"))
                    .when(F.col("status") == "CANCELLED",  F.lit("SUBSCRIPTION_CANCELLED"))
                    .when(F.col("status") == "EXPIRED",    F.lit("SUBSCRIPTION_EXPIRED"))
                    .when(F.col("status") == "SUSPENDED",  F.lit("SUBSCRIPTION_SUSPENDED"))
                    .otherwise(F.lit("UNKNOWN_TRANSITION")))
        .withColumn("event_date", F.col("updated_at").cast("date"))
    )

    # Enrich with order count per subscription
    order_counts = (
        ord_
        .groupBy("sub_ref_id")
        .agg(F.count("order_ref_id").alias("total_orders_to_date"))
    )

    return (
        transitions
        .join(order_counts, on="sub_ref_id", how="left")
        .withColumn("_computed_at", F.current_timestamp())
    )


# ─────────────────────────────────────────────────────────────────────────────
# 4. Gold APPLY CHANGES Streaming (micro-batch refresh via foreachBatch)
# ─────────────────────────────────────────────────────────────────────────────

def run_gold_refresh_batch(spark: SparkSession):
    """
    Batch computation of all Gold tables — triggered by scheduler or
    after each Silver DLT pipeline run. In a streaming architecture,
    wrap this in a Structured Streaming foreachBatch trigger.
    """
    logger.info("Running Gold layer refresh …")
    init_gold_schema(spark)

    aggregations = [
        (build_subscription_metrics_daily,   "agg_subscription_metrics_daily",
         ["metric_date", "country_code", "plan_code"],
         ["metric_date"]),

        (build_subscription_metrics_monthly, "agg_subscription_metrics_monthly",
         ["year_month", "country_code", "plan_category", "renewal_type"],
         ["year_month"]),

        (build_revenue_by_plan,              "agg_revenue_by_plan",
         ["plan_code", "p_method", "p_currency"],
         None),

        (build_user_cohort_analysis,         "agg_user_cohort_analysis",
         ["cohort_month", "months_since_signup", "country_code", "user_tier"],
         ["cohort_month"]),

        (build_device_distribution,          "agg_device_distribution",
         ["device_type", "os_type", "plan_category", "country_code", "user_tier"],
         None),

        (build_payment_performance,          "agg_payment_performance",
         ["payment_gateway", "payment_method", "payment_currency", "payment_month"],
         ["payment_month"]),

        (build_subscription_status_snapshot, "dim_subscription_status_snapshot",
         ["sub_ref_id"],
         None),

        (build_subscription_lifecycle,       "fact_subscription_lifecycle",
         ["sub_ref_id", "lifecycle_event", "event_date"],
         ["event_date"]),
    ]

    for build_fn, table_name, merge_keys, partition_cols in aggregations:
        try:
            logger.info(f"Building: {table_name} …")
            df = build_fn(spark)
            upsert_gold(spark, df, table_name, merge_keys, partition_cols)
        except Exception as ex:
            logger.error(f"Failed to build {table_name}: {ex}", exc_info=True)
            raise

    logger.info("Gold layer refresh complete.")


def run_gold_streaming(spark: SparkSession):
    """
    Continuous streaming mode: polls Silver CDF every 30 seconds and
    triggers incremental Gold refreshes using foreachBatch.
    """
    def foreach_gold_refresh(batch_df, batch_id):
        logger.info(f"Micro-batch {batch_id}: triggering Gold refresh …")
        run_gold_refresh_batch(spark)

    trigger_df = (
        spark.readStream
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", "0")
        .table(f"{CATALOG_NAME}.{SILVER_SCHEMA}.subscription_enriched")
    )

    query = (
        trigger_df.writeStream
        .foreachBatch(foreach_gold_refresh)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/gold/trigger")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    query.awaitTermination()


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    spark = get_spark()
    # Use "batch" for scheduled jobs, "streaming" for continuous mode
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "batch"
    if mode == "streaming":
        run_gold_streaming(spark)
    else:
        run_gold_refresh_batch(spark)
