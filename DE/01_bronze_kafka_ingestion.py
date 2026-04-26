"""
=============================================================================
LAYER 1 — BRONZE: Real-Time Kafka Streaming Ingestion
=============================================================================
Reads raw Kafka events for all 6 SMS entities and lands them into Delta
Bronze tables with zero transformation.  Every record is preserved exactly
as received, with Kafka metadata appended for full auditability.

Tables created
--------------
sms_catalog.bronze.subscription_raw
sms_catalog.bronze.user_raw
sms_catalog.bronze.catalog_raw
sms_catalog.bronze.order_raw
sms_catalog.bronze.device_raw
sms_catalog.bronze.payment_raw
"""

import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, IntegerType, BooleanType, TimestampType
)
from config import (
    KAFKA_CONFIG, KAFKA_TOPICS,
    CATALOG_NAME, BRONZE_SCHEMA, BRONZE_PATH, CHECKPOINT_PATH,
    TRIGGER_INTERVAL
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger("bronze_ingestion")


# ─────────────────────────────────────────────────────────────────────────────
# 1. Spark Session
# ─────────────────────────────────────────────────────────────────────────────

def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SMS_Bronze_Kafka_Ingestion")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        # Kafka consumer settings surfaced to Spark
        .config("spark.kafka.sasl.jaas.config",
                (f"org.apache.kafka.common.security.plain.PlainLoginModule required "
                 f"username='{KAFKA_CONFIG['sasl_username']}' "
                 f"password='{KAFKA_CONFIG['sasl_password']}';"))
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
# 2. Raw Kafka Message Schemas (value payload)
# ─────────────────────────────────────────────────────────────────────────────

SUBSCRIPTION_SCHEMA = StructType([
    StructField("sub_ref_id",        StringType(), False),   # PK
    StructField("plan_code",         StringType(), False),   # FK → Catalog
    StructField("user_ref_id",       StringType(), False),
    StructField("status",            StringType(), True),
    StructField("start_date",        StringType(), True),
    StructField("end_date",          StringType(), True),
    StructField("renewal_type",      StringType(), True),
    StructField("billing_cycle",     StringType(), True),
    StructField("trial_flag",        BooleanType(), True),
    StructField("created_at",        StringType(), True),
    StructField("updated_at",        StringType(), True),
    StructField("event_type",        StringType(), True),    # INSERT / UPDATE / DELETE
])

USER_SCHEMA = StructType([
    StructField("user_ref_id",       StringType(), False),   # PK
    StructField("full_name",         StringType(), True),    # PII
    StructField("email",             StringType(), True),    # PII
    StructField("phone_number",      StringType(), True),    # PII
    StructField("date_of_birth",     StringType(), True),    # PII
    StructField("address",           StringType(), True),    # PII
    StructField("country_code",      StringType(), True),
    StructField("language_code",     StringType(), True),
    StructField("user_tier",         StringType(), True),
    StructField("signup_channel",    StringType(), True),
    StructField("verified_flag",     BooleanType(), True),
    StructField("created_at",        StringType(), True),
    StructField("updated_at",        StringType(), True),
    StructField("event_type",        StringType(), True),
])

CATALOG_SCHEMA = StructType([
    StructField("plan_code",         StringType(), False),   # PK
    StructField("plan_name",         StringType(), True),
    StructField("plan_category",     StringType(), True),
    StructField("monthly_price",     DoubleType(), True),
    StructField("annual_price",      DoubleType(), True),
    StructField("currency_code",     StringType(), True),
    StructField("max_devices",       IntegerType(), True),
    StructField("features",          StringType(), True),    # JSON array
    StructField("is_active",         BooleanType(), True),
    StructField("created_at",        StringType(), True),
    StructField("updated_at",        StringType(), True),
    StructField("event_type",        StringType(), True),
])

ORDER_SCHEMA = StructType([
    StructField("order_ref_id",      StringType(), False),   # PK
    StructField("payment_ref_id",    StringType(), True),    # FK → Payment
    StructField("sub_ref_id",        StringType(), True),    # FK → Subscription
    StructField("user_ref_id",       StringType(), True),    # FK → User
    StructField("order_status",      StringType(), True),
    StructField("order_type",        StringType(), True),
    StructField("amount",            DoubleType(), True),
    StructField("discount_amount",   DoubleType(), True),
    StructField("tax_amount",        DoubleType(), True),
    StructField("currency_code",     StringType(), True),
    StructField("shipping_address",  StringType(), True),    # PII
    StructField("order_date",        StringType(), True),
    StructField("fulfillment_date",  StringType(), True),
    StructField("created_at",        StringType(), True),
    StructField("updated_at",        StringType(), True),
    StructField("event_type",        StringType(), True),
])

DEVICE_SCHEMA = StructType([
    StructField("device_ref_id",     StringType(), False),   # PK
    StructField("sub_ref_id",        StringType(), True),    # FK → Subscription
    StructField("user_ref_id",       StringType(), True),    # FK → User
    StructField("device_type",       StringType(), True),
    StructField("os_type",           StringType(), True),
    StructField("os_version",        StringType(), True),
    StructField("device_model",      StringType(), True),
    StructField("device_fingerprint",StringType(), True),
    StructField("is_active",         BooleanType(), True),
    StructField("registered_at",     StringType(), True),
    StructField("last_seen_at",      StringType(), True),
    StructField("created_at",        StringType(), True),
    StructField("updated_at",        StringType(), True),
    StructField("event_type",        StringType(), True),
])

PAYMENT_SCHEMA = StructType([
    StructField("payment_ref_id",    StringType(), False),   # PK
    StructField("card_number",       StringType(), True),    # PII
    StructField("bank_account",      StringType(), True),    # PII
    StructField("billing_address",   StringType(), True),    # PII
    StructField("payment_method",    StringType(), True),
    StructField("payment_status",    StringType(), True),
    StructField("payment_amount",    DoubleType(), True),
    StructField("payment_currency",  StringType(), True),
    StructField("payment_gateway",   StringType(), True),
    StructField("transaction_id",    StringType(), True),
    StructField("payment_date",      StringType(), True),
    StructField("failure_reason",    StringType(), True),
    StructField("retry_count",       IntegerType(), True),
    StructField("created_at",        StringType(), True),
    StructField("updated_at",        StringType(), True),
    StructField("event_type",        StringType(), True),
])

ENTITY_SCHEMAS = {
    "subscription": SUBSCRIPTION_SCHEMA,
    "user":         USER_SCHEMA,
    "catalog":      CATALOG_SCHEMA,
    "order":        ORDER_SCHEMA,
    "device":       DEVICE_SCHEMA,
    "payment":      PAYMENT_SCHEMA,
}


# ─────────────────────────────────────────────────────────────────────────────
# 3. Kafka Reader Factory
# ─────────────────────────────────────────────────────────────────────────────

def build_kafka_reader(spark: SparkSession, topic: str):
    """
    Returns a streaming DataFrame reading raw bytes from a single Kafka topic.
    Includes all Kafka metadata columns for auditability.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",          KAFKA_CONFIG["bootstrap_servers"])
        .option("kafka.security.protocol",          KAFKA_CONFIG["security_protocol"])
        .option("kafka.sasl.mechanism",             KAFKA_CONFIG["sasl_mechanism"])
        .option("kafka.sasl.jaas.config",
                (f"org.apache.kafka.common.security.plain.PlainLoginModule required "
                 f"username=\"{KAFKA_CONFIG['sasl_username']}\" "
                 f"password=\"{KAFKA_CONFIG['sasl_password']}\";"))
        .option("subscribe",                        topic)
        .option("startingOffsets",                  "earliest")
        .option("failOnDataLoss",                   "false")
        .option("maxOffsetsPerTrigger",             10000)
        .option("kafka.group.id",                   KAFKA_CONFIG["group_id"])
        .load()
    )


# ─────────────────────────────────────────────────────────────────────────────
# 4. Bronze Landing — Parse + Append Metadata
# ─────────────────────────────────────────────────────────────────────────────

def parse_kafka_message(raw_df, value_schema: StructType, entity_name: str):
    """
    Deserialises the Kafka value bytes as JSON using the provided schema,
    and enriches with Kafka metadata + pipeline audit columns.
    """
    return (
        raw_df
        # Cast binary → string
        .withColumn("_kafka_value_str", F.col("value").cast(StringType()))
        .withColumn("_kafka_key_str",   F.col("key").cast(StringType()))

        # Parse JSON payload
        .withColumn("_payload",
                    F.from_json(F.col("_kafka_value_str"), value_schema))

        # Flatten payload fields
        .select(
            # Kafka metadata (kept for auditability / replay)
            F.col("topic")                      .alias("_kafka_topic"),
            F.col("partition")                  .alias("_kafka_partition"),
            F.col("offset")                     .alias("_kafka_offset"),
            F.col("timestamp")                  .alias("_kafka_timestamp"),
            F.col("timestampType")              .alias("_kafka_timestamp_type"),
            F.col("_kafka_key_str")             .alias("_kafka_key"),

            # Raw payload (preserved for forensics / schema evolution)
            F.col("_kafka_value_str")           .alias("_raw_payload"),

            # Parsed entity fields
            F.col("_payload.*"),

            # Pipeline audit
            F.lit(entity_name)                  .alias("_entity_name"),
            F.current_timestamp()               .alias("_ingested_at"),
            F.lit("kafka_stream")               .alias("_source_system"),
            F.expr("md5(_kafka_value_str)")     .alias("_row_hash"),
        )
    )


def create_bronze_table(spark: SparkSession, entity_name: str, table_path: str):
    """
    Idempotently creates the Bronze Delta table with required properties.
    """
    full_table = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.{entity_name}_raw"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table}
        USING DELTA
        LOCATION '{table_path}/{entity_name}_raw'
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true',
            'quality_layer' = 'bronze',
            'entity' = '{entity_name}',
            'data_classification' = 'raw',
            'owner_team' = 'data_engineering',
            'pipeline' = 'sms_kafka_ingestion'
        )
    """)
    logger.info(f"Ensured Bronze table: {full_table}")


def write_bronze_stream(parsed_df, entity_name: str):
    """
    Appends parsed records to the Bronze Delta table.
    append-only — no deduplication at this layer.
    """
    table_path  = f"{BRONZE_PATH}/{entity_name}_raw"
    ckpt_path   = f"{CHECKPOINT_PATH}/bronze/{entity_name}"
    full_table  = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.{entity_name}_raw"

    return (
        parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation",   ckpt_path)
        .option("path",                 table_path)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName(f"bronze_{entity_name}_writer")
        .toTable(full_table)
    )


# ─────────────────────────────────────────────────────────────────────────────
# 5. Dead-Letter Queue — malformed / unparseable records
# ─────────────────────────────────────────────────────────────────────────────

def write_dlq_stream(raw_df, entity_name: str):
    """
    Routes records that failed JSON parsing to a Dead-Letter Queue table.
    Identified by a null _payload after from_json().
    """
    dlq_table = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.{entity_name}_dlq"
    ckpt_path = f"{CHECKPOINT_PATH}/bronze/dlq/{entity_name}"

    dlq_df = (
        raw_df
        .withColumn("_kafka_value_str", F.col("value").cast(StringType()))
        .withColumn("_parse_error",
                    F.when(F.col("_kafka_value_str").isNull(),
                           F.lit("null_payload"))
                    .otherwise(F.lit("json_parse_error")))
        .withColumn("_failed_at", F.current_timestamp())
        .select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp"),
            F.col("_kafka_value_str").alias("_raw_value"),
            F.col("_parse_error"),
            F.col("_failed_at"),
            F.lit(entity_name).alias("_entity_name"),
        )
    )

    return (
        dlq_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", ckpt_path)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName(f"dlq_{entity_name}_writer")
        .toTable(dlq_table)
    )


# ─────────────────────────────────────────────────────────────────────────────
# 6. Bronze Schema Initialisation
# ─────────────────────────────────────────────────────────────────────────────

def init_bronze_schema(spark: SparkSession):
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{BRONZE_SCHEMA}")

    # Apply schema-level tags
    spark.sql(f"""
        ALTER SCHEMA {CATALOG_NAME}.{BRONZE_SCHEMA}
        SET TAGS (
            'layer' = 'bronze',
            'env'   = 'production',
            'pii'   = 'true',
            'team'  = 'data_engineering'
        )
    """)
    logger.info(f"Bronze schema initialised: {CATALOG_NAME}.{BRONZE_SCHEMA}")


# ─────────────────────────────────────────────────────────────────────────────
# 7. Main Orchestration
# ─────────────────────────────────────────────────────────────────────────────

def run_bronze_pipeline():
    spark  = get_spark()
    logger.info("Starting SMS Bronze Kafka ingestion pipeline …")

    init_bronze_schema(spark)

    active_queries = []

    for entity_name, topic in KAFKA_TOPICS.items():
        logger.info(f"Starting stream: {entity_name} ← {topic}")
        schema = ENTITY_SCHEMAS[entity_name]

        # Read from Kafka
        raw_df = build_kafka_reader(spark, topic)

        # Parse JSON payload
        parsed_df = parse_kafka_message(raw_df, schema, entity_name)

        # Ensure Bronze table exists
        create_bronze_table(spark, entity_name, BRONZE_PATH)

        # Write good records to Bronze
        good_query = write_bronze_stream(
            parsed_df.filter(F.col("_row_hash").isNotNull()),
            entity_name
        )

        # Write malformed records to DLQ
        dlq_query = write_dlq_stream(
            raw_df.filter(
                F.from_json(
                    F.col("value").cast(StringType()),
                    ENTITY_SCHEMAS[entity_name]
                ).isNull()
            ),
            entity_name
        )

        active_queries.extend([good_query, dlq_query])

    logger.info(f"Launched {len(active_queries)} streaming queries.")

    # Await termination of all queries
    for q in active_queries:
        q.awaitTermination()


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run_bronze_pipeline()
