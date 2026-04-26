"""
=============================================================================
Subscription Management System (SMS) - Databricks Kafka Pipeline
Configuration & Constants
=============================================================================
"""

# ─── Kafka Configuration ────────────────────────────────────────────────────

KAFKA_CONFIG = {
    "bootstrap_servers": "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092",
    "security_protocol": "SASL_SSL",
    "sasl_mechanism": "PLAIN",
    "sasl_username": "{{secrets/kafka/username}}",       # Databricks Secret
    "sasl_password": "{{secrets/kafka/password}}",       # Databricks Secret
    "group_id": "sms-databricks-consumer-group",
    "auto_offset_reset": "earliest",
    "enable_auto_commit": False,
    "max_poll_records": 500,
    "session_timeout_ms": 30000,
    "heartbeat_interval_ms": 10000,
}

# ─── Kafka Topic Mapping ─────────────────────────────────────────────────────

KAFKA_TOPICS = {
    "subscription": "sms.subscription.events",
    "user":         "sms.user.events",
    "catalog":      "sms.catalog.events",
    "order":        "sms.order.events",
    "device":       "sms.device.events",
    "payment":      "sms.payment.events",
}

# ─── Unity Catalog / Storage Configuration ──────────────────────────────────

CATALOG_NAME       = "sms_catalog"
BRONZE_SCHEMA      = "bronze"
SILVER_SCHEMA      = "silver"
GOLD_SCHEMA        = "gold"

STORAGE_BASE_PATH  = "abfss://sms-datalake@storageaccount.dfs.core.windows.net"
BRONZE_PATH        = f"{STORAGE_BASE_PATH}/bronze"
SILVER_PATH        = f"{STORAGE_BASE_PATH}/silver"
GOLD_PATH          = f"{STORAGE_BASE_PATH}/gold"
CHECKPOINT_PATH    = f"{STORAGE_BASE_PATH}/checkpoints"

# ─── Delta Sharing ───────────────────────────────────────────────────────────

DELTA_SHARE_NAME   = "sms_external_share"
SHARE_RECIPIENT_CONFIGS = {
    "analytics_team":   {"ip_whitelist": ["10.0.1.0/24"], "expiry_days": 365},
    "finance_team":     {"ip_whitelist": ["10.0.2.0/24"], "expiry_days": 365},
    "external_partner": {"ip_whitelist": ["203.0.113.0/24"], "expiry_days": 90},
}

# ─── Access Control ──────────────────────────────────────────────────────────

ACCESS_CONTROL = {
    "groups": {
        "sms_admins":           ["SELECT", "MODIFY", "CREATE", "USAGE"],
        "sms_data_engineers":   ["SELECT", "MODIFY", "USAGE"],
        "sms_analysts":         ["SELECT", "USAGE"],
        "sms_finance":          ["SELECT", "USAGE"],
        "sms_support":          ["SELECT", "USAGE"],
        "sms_external":         ["SELECT"],
    },
    "schema_grants": {
        "bronze":  ["sms_admins", "sms_data_engineers"],
        "silver":  ["sms_admins", "sms_data_engineers", "sms_analysts"],
        "gold":    ["sms_admins", "sms_data_engineers", "sms_analysts", "sms_finance", "sms_support"],
    },
}

# ─── PII / Sensitive Column Definitions ─────────────────────────────────────

PII_COLUMNS = {
    "user":    ["email", "phone_number", "date_of_birth", "address", "full_name"],
    "payment": ["card_number", "bank_account", "billing_address"],
    "order":   ["shipping_address"],
}

MASKED_COLUMN_ACCESS = {
    "unmasked_roles": ["sms_admins", "sms_data_engineers"],
    "partial_mask_roles": ["sms_analysts", "sms_finance"],
    "full_mask_roles":  ["sms_support", "sms_external"],
}

# ─── DLT Pipeline Settings ───────────────────────────────────────────────────

DLT_PIPELINE_CONFIG = {
    "name": "SMS_Silver_DLT_Pipeline",
    "continuous": True,
    "channel": "CURRENT",
    "photon": True,
    "development": False,
    "configuration": {
        "pipelines.tableManagedByExternalSystem": "false",
        "spark.databricks.delta.retentionDurationCheck.enabled": "true",
    },
}

# ─── Data Quality Thresholds ─────────────────────────────────────────────────

DQ_THRESHOLDS = {
    "max_null_ratio":       0.05,   # 5% max null rate for critical fields
    "max_duplicate_ratio":  0.01,   # 1% max duplicate rate
    "max_late_arrival_hrs": 24,     # Records older than 24h flagged
}

# ─── Watermark & Trigger Settings ────────────────────────────────────────────

WATERMARK_DELAY     = "10 minutes"
TRIGGER_INTERVAL    = "30 seconds"
MAX_FILES_PER_BATCH = 1000
