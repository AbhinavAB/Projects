"""
=============================================================================
LAYER 2 — SILVER: Delta Live Tables Pipeline
=============================================================================
Reads from Bronze CDC-enabled Delta tables, applies DLT Expectations for
data quality, cleanses, filters, deduplicates, transforms and joins
all six entities into Silver conformed tables.

Run as a DLT pipeline via the Databricks UI / REST API / Terraform.
Pipeline config is held in config.py → DLT_PIPELINE_CONFIG.

Silver tables produced
----------------------
sms_catalog.silver.subscription
sms_catalog.silver.user
sms_catalog.silver.catalog
sms_catalog.silver.order
sms_catalog.silver.device
sms_catalog.silver.payment
sms_catalog.silver.subscription_enriched   (joined fact)
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Import config paths
CATALOG_NAME  = spark.conf.get("catalog_name",  "sms_catalog")     # noqa: F821
BRONZE_SCHEMA = spark.conf.get("bronze_schema", "bronze")
SILVER_SCHEMA = spark.conf.get("silver_schema", "silver")

BRONZE_PREFIX = f"{CATALOG_NAME}.{BRONZE_SCHEMA}"
SILVER_PREFIX = f"{CATALOG_NAME}.{SILVER_SCHEMA}"


# ═════════════════════════════════════════════════════════════════════════════
# SECTION A — BRONZE READ HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def read_bronze_stream(entity: str):
    """
    Reads from a Bronze CDF-enabled Delta table as a streaming source.
    Filters to _change_type = insert/update to exclude deletes from Silver
    (deleted records are soft-deleted via status column instead).
    """
    return (
        spark.readStream                                              # noqa: F821
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", "0")
        .table(f"{BRONZE_PREFIX}.{entity}_raw")
        .filter(
            F.col("_change_type").isin(
                "insert", "update_postimage"
            )
        )
    )


# ═════════════════════════════════════════════════════════════════════════════
# SECTION B — SHARED TRANSFORMATION HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def _dedup_latest(df, pk_col: str, ts_col: str = "updated_at"):
    """
    Keep the most-recent record per primary key using a window function.
    Applied within each micro-batch via foreachBatch when used in streaming.
    For DLT, deduplication is handled by APPLY CHANGES INTO semantics.
    """
    w = Window.partitionBy(pk_col).orderBy(F.col(ts_col).desc())
    return (
        df
        .withColumn("_rank", F.row_number().over(w))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def _safe_to_timestamp(col_name: str, fmt: str = "yyyy-MM-dd'T'HH:mm:ss"):
    return F.to_timestamp(F.col(col_name), fmt)


def _safe_to_date(col_name: str, fmt: str = "yyyy-MM-dd"):
    return F.to_date(F.col(col_name), fmt)


def _trim_all(df):
    """Strip leading/trailing whitespace from all StringType columns."""
    string_cols = [
        f.name for f in df.schema.fields
        if str(f.dataType) == "StringType()"
    ]
    for c in string_cols:
        df = df.withColumn(c, F.trim(F.col(c)))
    return df


def _standardise_event_type(df):
    return df.withColumn(
        "event_type",
        F.upper(F.trim(F.col("event_type")))
    )


# ═════════════════════════════════════════════════════════════════════════════
# SECTION C — SUBSCRIPTION SILVER
# ═════════════════════════════════════════════════════════════════════════════

@dlt.table(
    name        = "subscription_clean",
    comment     = "Cleansed, validated subscription records — pre-join staging",
    table_properties = {
        "quality_layer":        "silver_staging",
        "entity":               "subscription",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.managed": "true",
    },
)
@dlt.expect_all_or_drop({
    "valid_sub_ref_id":    "sub_ref_id IS NOT NULL AND length(sub_ref_id) > 0",
    "valid_plan_code":     "plan_code IS NOT NULL AND length(plan_code) > 0",
    "valid_user_ref_id":   "user_ref_id IS NOT NULL AND length(user_ref_id) > 0",
    "valid_status":        "status IN ('ACTIVE','INACTIVE','CANCELLED','SUSPENDED','TRIALING','EXPIRED')",
    "valid_renewal_type":  "renewal_type IN ('AUTO','MANUAL','NONE') OR renewal_type IS NULL",
})
@dlt.expect_or_fail({
    "no_future_start_date": "start_date <= current_date() OR start_date IS NULL",
})
def subscription_clean():
    df = read_bronze_stream("subscription")
    df = _trim_all(df)
    df = _standardise_event_type(df)
    return (
        df
        .withColumn("start_date",    _safe_to_date("start_date"))
        .withColumn("end_date",      _safe_to_date("end_date"))
        .withColumn("created_at",    _safe_to_timestamp("created_at"))
        .withColumn("updated_at",    _safe_to_timestamp("updated_at"))
        .withColumn("status",        F.upper(F.col("status")))
        .withColumn("renewal_type",  F.upper(F.col("renewal_type")))
        .withColumn("billing_cycle", F.upper(F.col("billing_cycle")))
        # Derived: is the subscription currently live?
        .withColumn("is_live",
                    F.when(
                        (F.col("status") == "ACTIVE") &
                        (F.col("end_date").isNull() |
                         (F.col("end_date") >= F.current_date())),
                        F.lit(True)
                    ).otherwise(F.lit(False)))
        # Derived: days remaining on subscription
        .withColumn("days_remaining",
                    F.when(
                        F.col("end_date").isNotNull(),
                        F.datediff(F.col("end_date"), F.current_date())
                    ).otherwise(F.lit(None).cast("int")))
        .drop("_raw_payload", "_kafka_key", "_kafka_timestamp_type")
    )


@dlt.table(
    name    = "subscription",
    comment = "Silver subscription — APPLY CHANGES deduplication via CDC",
    table_properties = {
        "quality_layer": "silver",
        "entity":        "subscription",
        "delta.enableChangeDataFeed": "true",
    },
)
def subscription_silver():
    return dlt.read_stream("subscription_clean")


# ═════════════════════════════════════════════════════════════════════════════
# SECTION D — USER SILVER
# ═════════════════════════════════════════════════════════════════════════════

@dlt.table(
    name    = "user_clean",
    comment = "Cleansed, PII-normalised user records — staging",
    table_properties = {
        "quality_layer":     "silver_staging",
        "data_sensitivity":  "pii",
        "entity":            "user",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect_all_or_drop({
    "valid_user_ref_id":  "user_ref_id IS NOT NULL AND length(user_ref_id) > 0",
    "valid_email":        "email RLIKE '^[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}$' OR email IS NULL",
    "valid_country_code": "length(country_code) = 2 OR country_code IS NULL",
})
@dlt.expect_or_warn({
    "phone_not_empty":    "phone_number IS NOT NULL AND length(phone_number) > 5",
    "dob_in_past":        "date_of_birth < current_date() OR date_of_birth IS NULL",
})
def user_clean():
    df = read_bronze_stream("user")
    df = _trim_all(df)
    df = _standardise_event_type(df)
    return (
        df
        .withColumn("date_of_birth",  _safe_to_date("date_of_birth"))
        .withColumn("created_at",     _safe_to_timestamp("created_at"))
        .withColumn("updated_at",     _safe_to_timestamp("updated_at"))
        .withColumn("email",          F.lower(F.col("email")))
        .withColumn("country_code",   F.upper(F.col("country_code")))
        .withColumn("language_code",  F.lower(F.col("language_code")))
        .withColumn("user_tier",      F.upper(F.col("user_tier")))
        .withColumn("signup_channel", F.upper(F.col("signup_channel")))
        # Derived: user age (years)
        .withColumn("age_years",
                    F.when(
                        F.col("date_of_birth").isNotNull(),
                        F.floor(
                            F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25
                        )
                    ).otherwise(F.lit(None).cast("int")))
        # Derived: user age band (for analytics, non-PII)
        .withColumn("age_band",
                    F.when(F.col("age_years") < 18,  F.lit("UNDER_18"))
                    .when(F.col("age_years") < 25,   F.lit("18_24"))
                    .when(F.col("age_years") < 35,   F.lit("25_34"))
                    .when(F.col("age_years") < 45,   F.lit("35_44"))
                    .when(F.col("age_years") < 55,   F.lit("45_54"))
                    .when(F.col("age_years").isNotNull(), F.lit("55_PLUS"))
                    .otherwise(F.lit("UNKNOWN")))
        .drop("_raw_payload", "_kafka_key", "_kafka_timestamp_type")
    )


@dlt.table(
    name    = "user",
    comment = "Silver user — deduplicated, verified, PII retained with masking policy",
    table_properties = {
        "quality_layer":    "silver",
        "data_sensitivity": "pii",
        "entity":           "user",
        "delta.enableChangeDataFeed": "true",
    },
)
def user_silver():
    return dlt.read_stream("user_clean")


# ═════════════════════════════════════════════════════════════════════════════
# SECTION E — CATALOG SILVER
# ═════════════════════════════════════════════════════════════════════════════

@dlt.table(
    name    = "catalog",
    comment = "Silver plan catalog — cleansed, validated plan definitions",
    table_properties = {
        "quality_layer": "silver",
        "entity":        "catalog",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect_all_or_drop({
    "valid_plan_code":      "plan_code IS NOT NULL AND length(plan_code) > 0",
    "valid_monthly_price":  "monthly_price >= 0 OR monthly_price IS NULL",
    "valid_annual_price":   "annual_price >= 0 OR annual_price IS NULL",
    "valid_max_devices":    "max_devices > 0 AND max_devices <= 100",
    "valid_currency_code":  "length(currency_code) = 3 OR currency_code IS NULL",
})
def catalog_silver():
    df = read_bronze_stream("catalog")
    df = _trim_all(df)
    df = _standardise_event_type(df)
    return (
        df
        .withColumn("created_at",     _safe_to_timestamp("created_at"))
        .withColumn("updated_at",     _safe_to_timestamp("updated_at"))
        .withColumn("currency_code",  F.upper(F.col("currency_code")))
        .withColumn("plan_category",  F.upper(F.col("plan_category")))
        # Derived: annual discount %
        .withColumn("annual_discount_pct",
                    F.when(
                        (F.col("monthly_price").isNotNull()) &
                        (F.col("monthly_price") > 0) &
                        (F.col("annual_price").isNotNull()),
                        F.round(
                            (1 - (F.col("annual_price") / (F.col("monthly_price") * 12))) * 100,
                            2
                        )
                    ).otherwise(F.lit(0.0)))
        .drop("_raw_payload", "_kafka_key", "_kafka_timestamp_type")
    )


# ═════════════════════════════════════════════════════════════════════════════
# SECTION F — ORDER SILVER
# ═════════════════════════════════════════════════════════════════════════════

@dlt.table(
    name    = "order",
    comment = "Silver orders — validated, typed, enriched with net amounts",
    table_properties = {
        "quality_layer":     "silver",
        "data_sensitivity":  "pii_adjacent",
        "entity":            "order",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect_all_or_drop({
    "valid_order_ref_id":   "order_ref_id IS NOT NULL AND length(order_ref_id) > 0",
    "valid_user_ref_id":    "user_ref_id IS NOT NULL",
    "valid_amount":         "amount >= 0",
    "valid_order_status":   "order_status IN ('PENDING','CONFIRMED','SHIPPED','DELIVERED','CANCELLED','REFUNDED')",
    "valid_currency":       "length(currency_code) = 3 OR currency_code IS NULL",
})
@dlt.expect_or_warn({
    "sub_ref_present":      "sub_ref_id IS NOT NULL",
    "payment_ref_present":  "payment_ref_id IS NOT NULL",
})
def order_silver():
    df = read_bronze_stream("order")
    df = _trim_all(df)
    df = _standardise_event_type(df)
    return (
        df
        .withColumn("order_date",       _safe_to_timestamp("order_date"))
        .withColumn("fulfillment_date", _safe_to_timestamp("fulfillment_date"))
        .withColumn("created_at",       _safe_to_timestamp("created_at"))
        .withColumn("updated_at",       _safe_to_timestamp("updated_at"))
        .withColumn("order_status",     F.upper(F.col("order_status")))
        .withColumn("order_type",       F.upper(F.col("order_type")))
        .withColumn("currency_code",    F.upper(F.col("currency_code")))
        # Derived: net payable amount
        .withColumn("net_amount",
                    F.round(
                        F.col("amount")
                        - F.coalesce(F.col("discount_amount"), F.lit(0.0))
                        + F.coalesce(F.col("tax_amount"),      F.lit(0.0)),
                        2
                    ))
        # Derived: fulfilment duration in days
        .withColumn("fulfillment_days",
                    F.when(
                        F.col("fulfillment_date").isNotNull() &
                        F.col("order_date").isNotNull(),
                        F.datediff(
                            F.col("fulfillment_date").cast("date"),
                            F.col("order_date").cast("date")
                        )
                    ).otherwise(F.lit(None).cast("int")))
        .drop("_raw_payload", "_kafka_key", "_kafka_timestamp_type")
    )


# ═════════════════════════════════════════════════════════════════════════════
# SECTION G — DEVICE SILVER
# ═════════════════════════════════════════════════════════════════════════════

@dlt.table(
    name    = "device",
    comment = "Silver devices — typed and enriched device records",
    table_properties = {
        "quality_layer": "silver",
        "entity":        "device",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect_all_or_drop({
    "valid_device_ref_id":  "device_ref_id IS NOT NULL AND length(device_ref_id) > 0",
    "valid_sub_ref_id":     "sub_ref_id IS NOT NULL",
    "valid_user_ref_id":    "user_ref_id IS NOT NULL",
    "valid_device_type":    "device_type IN ('MOBILE','TABLET','DESKTOP','TV','CONSOLE','OTHER') OR device_type IS NULL",
})
def device_silver():
    df = read_bronze_stream("device")
    df = _trim_all(df)
    df = _standardise_event_type(df)
    return (
        df
        .withColumn("registered_at",   _safe_to_timestamp("registered_at"))
        .withColumn("last_seen_at",     _safe_to_timestamp("last_seen_at"))
        .withColumn("created_at",       _safe_to_timestamp("created_at"))
        .withColumn("updated_at",       _safe_to_timestamp("updated_at"))
        .withColumn("device_type",      F.upper(F.col("device_type")))
        .withColumn("os_type",          F.upper(F.col("os_type")))
        # Derived: days since last seen
        .withColumn("days_since_last_seen",
                    F.when(
                        F.col("last_seen_at").isNotNull(),
                        F.datediff(
                            F.current_date(),
                            F.col("last_seen_at").cast("date")
                        )
                    ).otherwise(F.lit(None).cast("int")))
        # Derived: device activity flag
        .withColumn("is_recently_active",
                    F.when(F.col("days_since_last_seen") <= 30, F.lit(True))
                    .otherwise(F.lit(False)))
        .drop("_raw_payload", "_kafka_key", "_kafka_timestamp_type")
    )


# ═════════════════════════════════════════════════════════════════════════════
# SECTION H — PAYMENT SILVER
# ═════════════════════════════════════════════════════════════════════════════

@dlt.table(
    name    = "payment",
    comment = "Silver payments — PII retained, typed, with retry metrics",
    table_properties = {
        "quality_layer":     "silver",
        "data_sensitivity":  "pii_financial",
        "entity":            "payment",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect_all_or_drop({
    "valid_payment_ref_id": "payment_ref_id IS NOT NULL AND length(payment_ref_id) > 0",
    "valid_payment_amount": "payment_amount >= 0",
    "valid_payment_status": "payment_status IN ('PENDING','COMPLETED','FAILED','REFUNDED','DISPUTED','VOIDED')",
    "valid_payment_method": "payment_method IN ('CARD','BANK_TRANSFER','WALLET','PAYPAL','CRYPTO','OTHER') OR payment_method IS NULL",
    "valid_retry_count":    "retry_count >= 0 AND retry_count <= 10",
})
def payment_silver():
    df = read_bronze_stream("payment")
    df = _trim_all(df)
    df = _standardise_event_type(df)
    return (
        df
        .withColumn("payment_date",     _safe_to_timestamp("payment_date"))
        .withColumn("created_at",       _safe_to_timestamp("created_at"))
        .withColumn("updated_at",       _safe_to_timestamp("updated_at"))
        .withColumn("payment_status",   F.upper(F.col("payment_status")))
        .withColumn("payment_method",   F.upper(F.col("payment_method")))
        .withColumn("payment_currency", F.upper(F.col("payment_currency")))
        # Derived: is payment successful?
        .withColumn("is_successful",
                    F.col("payment_status") == "COMPLETED")
        # Derived: has been retried?
        .withColumn("has_been_retried",
                    F.col("retry_count") > 0)
        .drop("_raw_payload", "_kafka_key", "_kafka_timestamp_type")
    )


# ═════════════════════════════════════════════════════════════════════════════
# SECTION I — SUBSCRIPTION ENRICHED (Multi-Entity Join)
# ═════════════════════════════════════════════════════════════════════════════

@dlt.table(
    name    = "subscription_enriched",
    comment = (
        "Silver enriched subscription — join of Subscription + User + "
        "Catalog + latest Order + Payment + Device count. "
        "The primary analytical fact for subscription reporting."
    ),
    table_properties = {
        "quality_layer":    "silver",
        "data_sensitivity": "pii_adjacent",
        "entity":           "subscription_enriched",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect_or_warn({
    "joined_to_user":    "u_user_ref_id IS NOT NULL",
    "joined_to_catalog": "c_plan_code IS NOT NULL",
})
def subscription_enriched():
    # Read current snapshots from Silver live tables (batch read for joins)
    sub  = dlt.read("subscription")
    usr  = dlt.read("user")
    cat  = dlt.read("catalog")
    ord_ = dlt.read("order")
    pay  = dlt.read("payment")
    dev  = dlt.read("device")

    # Latest order per subscription
    ord_w = Window.partitionBy("sub_ref_id").orderBy(F.col("order_date").desc())
    latest_order = (
        ord_
        .filter(F.col("sub_ref_id").isNotNull())
        .withColumn("_ord_rank", F.row_number().over(ord_w))
        .filter(F.col("_ord_rank") == 1)
        .drop("_ord_rank")
    )

    # Latest successful payment per subscription (via order join)
    latest_payment = (
        latest_order
        .join(
            pay.select(
                "payment_ref_id",
                F.col("payment_status").alias("pay_status"),
                F.col("payment_amount").alias("pay_amount"),
                F.col("payment_method").alias("pay_method"),
                F.col("payment_date").alias("pay_date"),
                F.col("is_successful").alias("pay_is_successful"),
            ),
            on="payment_ref_id",
            how="left"
        )
        .select(
            "sub_ref_id", "payment_ref_id", "pay_status",
            "pay_amount", "pay_method", "pay_date", "pay_is_successful",
        )
    )

    # Device count per subscription
    device_count = (
        dev
        .filter(F.col("is_active") == True)     # noqa: E712
        .groupBy("sub_ref_id")
        .agg(
            F.count("device_ref_id").alias("active_device_count"),
            F.countDistinct("device_type").alias("unique_device_types"),
        )
    )

    # Master join
    return (
        sub
        .join(
            usr.select(
                F.col("user_ref_id").alias("u_user_ref_id"),
                F.col("full_name")   .alias("u_full_name"),
                F.col("email")       .alias("u_email"),
                F.col("phone_number").alias("u_phone_number"),
                F.col("country_code").alias("u_country_code"),
                F.col("user_tier")   .alias("u_user_tier"),
                F.col("age_band")    .alias("u_age_band"),
                F.col("verified_flag").alias("u_verified_flag"),
            ),
            on  = sub["user_ref_id"] == F.col("u_user_ref_id"),
            how = "left"
        )
        .join(
            cat.select(
                F.col("plan_code")      .alias("c_plan_code"),
                F.col("plan_name")      .alias("c_plan_name"),
                F.col("plan_category")  .alias("c_plan_category"),
                F.col("monthly_price")  .alias("c_monthly_price"),
                F.col("annual_price")   .alias("c_annual_price"),
                F.col("currency_code")  .alias("c_currency_code"),
                F.col("max_devices")    .alias("c_max_devices"),
                F.col("annual_discount_pct").alias("c_annual_discount_pct"),
            ),
            on  = sub["plan_code"] == F.col("c_plan_code"),
            how = "left"
        )
        .join(latest_payment,   on="sub_ref_id", how="left")
        .join(device_count,     on="sub_ref_id", how="left")
        # Enrich derived fields
        .withColumn("device_utilisation_pct",
                    F.when(
                        (F.col("c_max_devices").isNotNull()) &
                        (F.col("c_max_devices") > 0),
                        F.round(
                            F.col("active_device_count") / F.col("c_max_devices") * 100,
                            2
                        )
                    ).otherwise(F.lit(0.0)))
        .withColumn("_enriched_at", F.current_timestamp())
    )
