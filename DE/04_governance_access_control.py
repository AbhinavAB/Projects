"""
=============================================================================
GOVERNANCE: Delta Sharing + Unity Catalog Access Control + Column Masking
=============================================================================
Configures:
  1. Unity Catalog Tags on all layers (Bronze / Silver / Gold)
  2. Column-level security policies (row filters + column masks)
  3. Group/Role-based GRANT statements (policy-driven)
  4. Delta Sharing: share, recipient, and table grants
  5. Column Masking Functions for PII fields

Run this notebook ONCE after pipeline bootstrap, then on any schema change.
Idempotent — safe to re-run.
"""

from pyspark.sql import SparkSession
import logging
from config import (
    CATALOG_NAME, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA,
    ACCESS_CONTROL, DELTA_SHARE_NAME, SHARE_RECIPIENT_CONFIGS,
    PII_COLUMNS, MASKED_COLUMN_ACCESS
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger("governance")


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("SMS_Governance").getOrCreate()


# ═════════════════════════════════════════════════════════════════════════════
# 1. UNITY CATALOG TAGS
# ═════════════════════════════════════════════════════════════════════════════

def apply_catalog_tags(spark: SparkSession):
    """
    Tag the Unity Catalog, schemas, and all tables for discovery,
    lineage, and data classification.
    """
    logger.info("Applying Unity Catalog tags …")

    # ── Catalog-level tags ──────────────────────────────────────────────────
    spark.sql(f"""
        ALTER CATALOG {CATALOG_NAME}
        SET TAGS (
            'domain'        = 'subscription_management',
            'env'           = 'production',
            'owner'         = 'data_platform_team',
            'data_product'  = 'sms',
            'compliance'    = 'gdpr_ccpa',
            'retention_yrs' = '7'
        )
    """)

    # ── Schema-level tags ───────────────────────────────────────────────────
    schema_tags = {
        BRONZE_SCHEMA: "layer=bronze,sensitivity=raw,pii=true",
        SILVER_SCHEMA: "layer=silver,sensitivity=conformd,pii=true",
        GOLD_SCHEMA:   "layer=gold,sensitivity=aggregated,pii=partial",
    }
    for schema, tag_str in schema_tags.items():
        tags_kv = ",\n            ".join(
            f"'{k}' = '{v}'" for pair in tag_str.split(",")
            for k, v in [pair.split("=")]
        )
        spark.sql(f"""
            ALTER SCHEMA {CATALOG_NAME}.{schema}
            SET TAGS ({tags_kv})
        """)

    # ── Table-level tags ────────────────────────────────────────────────────
    table_tag_map = {
        # Bronze
        f"{BRONZE_SCHEMA}.subscription_raw": {"entity": "subscription", "pii": "false", "criticality": "high"},
        f"{BRONZE_SCHEMA}.user_raw":         {"entity": "user",         "pii": "true",  "criticality": "critical"},
        f"{BRONZE_SCHEMA}.catalog_raw":      {"entity": "catalog",      "pii": "false", "criticality": "medium"},
        f"{BRONZE_SCHEMA}.order_raw":        {"entity": "order",        "pii": "partial","criticality": "high"},
        f"{BRONZE_SCHEMA}.device_raw":       {"entity": "device",       "pii": "false", "criticality": "medium"},
        f"{BRONZE_SCHEMA}.payment_raw":      {"entity": "payment",      "pii": "true",  "criticality": "critical"},
        # Silver
        f"{SILVER_SCHEMA}.subscription":     {"entity": "subscription", "pii": "false", "criticality": "high"},
        f"{SILVER_SCHEMA}.user":             {"entity": "user",         "pii": "true",  "criticality": "critical"},
        f"{SILVER_SCHEMA}.catalog":          {"entity": "catalog",      "pii": "false", "criticality": "medium"},
        f"{SILVER_SCHEMA}.order":            {"entity": "order",        "pii": "partial","criticality": "high"},
        f"{SILVER_SCHEMA}.device":           {"entity": "device",       "pii": "false", "criticality": "medium"},
        f"{SILVER_SCHEMA}.payment":          {"entity": "payment",      "pii": "true",  "criticality": "critical"},
        f"{SILVER_SCHEMA}.subscription_enriched": {"entity": "subscription_enriched", "pii": "true", "criticality": "high"},
        # Gold
        f"{GOLD_SCHEMA}.agg_subscription_metrics_daily":   {"entity": "agg_sub_daily",   "pii": "false", "criticality": "medium"},
        f"{GOLD_SCHEMA}.agg_subscription_metrics_monthly": {"entity": "agg_sub_monthly", "pii": "false", "criticality": "medium"},
        f"{GOLD_SCHEMA}.agg_revenue_by_plan":               {"entity": "agg_revenue",     "pii": "false", "criticality": "high"},
        f"{GOLD_SCHEMA}.agg_user_cohort_analysis":          {"entity": "agg_cohort",      "pii": "false", "criticality": "medium"},
        f"{GOLD_SCHEMA}.agg_device_distribution":           {"entity": "agg_device",      "pii": "false", "criticality": "low"},
        f"{GOLD_SCHEMA}.agg_payment_performance":           {"entity": "agg_payment",     "pii": "false", "criticality": "high"},
        f"{GOLD_SCHEMA}.dim_subscription_status_snapshot":  {"entity": "dim_sub_status",  "pii": "partial","criticality": "high"},
        f"{GOLD_SCHEMA}.fact_subscription_lifecycle":       {"entity": "fact_lifecycle",  "pii": "false", "criticality": "high"},
    }

    for table_path, tags in table_tag_map.items():
        tags_sql = ",\n            ".join(f"'{k}' = '{v}'" for k, v in tags.items())
        spark.sql(f"""
            ALTER TABLE {CATALOG_NAME}.{table_path}
            SET TAGS ({tags_sql})
        """)

    # ── Column-level tags for PII fields ────────────────────────────────────
    pii_column_tags = {
        f"{SILVER_SCHEMA}.user": {
            "email":          {"pii_type": "email",         "gdpr_subject": "true"},
            "full_name":      {"pii_type": "name",          "gdpr_subject": "true"},
            "phone_number":   {"pii_type": "phone",         "gdpr_subject": "true"},
            "date_of_birth":  {"pii_type": "dob",           "gdpr_subject": "true"},
            "address":        {"pii_type": "address",       "gdpr_subject": "true"},
        },
        f"{SILVER_SCHEMA}.payment": {
            "card_number":    {"pii_type": "financial_pci", "pci_dss": "true"},
            "bank_account":   {"pii_type": "financial_pci", "pci_dss": "true"},
            "billing_address":{"pii_type": "address",       "gdpr_subject": "true"},
        },
        f"{SILVER_SCHEMA}.order": {
            "shipping_address":{"pii_type": "address",      "gdpr_subject": "true"},
        },
    }

    for table_path, cols in pii_column_tags.items():
        for col_name, col_tags in cols.items():
            col_tags_sql = ", ".join(f"'{k}' = '{v}'" for k, v in col_tags.items())
            spark.sql(f"""
                ALTER TABLE {CATALOG_NAME}.{table_path}
                ALTER COLUMN {col_name}
                SET TAGS ({col_tags_sql})
            """)

    logger.info("Unity Catalog tags applied.")


# ═════════════════════════════════════════════════════════════════════════════
# 2. COLUMN MASKING FUNCTIONS
# ═════════════════════════════════════════════════════════════════════════════

def create_masking_functions(spark: SparkSession):
    """
    Create reusable SQL masking functions in Unity Catalog.
    These are referenced by column masking policies on Silver/Gold tables.

    Masking logic:
      sms_admins / sms_data_engineers → full value (no mask)
      sms_analysts / sms_finance      → partial mask
      sms_support / sms_external      → fully masked (stars/hash)
    """
    logger.info("Creating column masking functions …")

    # ── Email masking: user@domain.com → u***@domain.com ────────────────────
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {CATALOG_NAME}.{SILVER_SCHEMA}.mask_email(email STRING)
        RETURNS STRING
        LANGUAGE SQL
        COMMENT 'Masks email address — partial for analysts, full for support/external'
        RETURN
            CASE
                WHEN is_member('sms_admins')
                  OR is_member('sms_data_engineers')       THEN email
                WHEN is_member('sms_analysts')
                  OR is_member('sms_finance')
                  THEN concat(
                      left(split(email, '@')[0], 1),
                      '***@',
                      split(email, '@')[1]
                  )
                ELSE '***@***.***'
            END
    """)

    # ── Phone masking: +44xxxxxxx7890 → +44*****7890 ────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {CATALOG_NAME}.{SILVER_SCHEMA}.mask_phone(phone STRING)
        RETURNS STRING
        LANGUAGE SQL
        COMMENT 'Masks phone number — last 4 digits visible for partial access'
        RETURN
            CASE
                WHEN is_member('sms_admins')
                  OR is_member('sms_data_engineers')       THEN phone
                WHEN is_member('sms_analysts')
                  OR is_member('sms_finance')
                  THEN concat('*****', right(phone, 4))
                ELSE '**********'
            END
    """)

    # ── Full name masking: John Smith → J*** S*** ────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {CATALOG_NAME}.{SILVER_SCHEMA}.mask_full_name(name STRING)
        RETURNS STRING
        LANGUAGE SQL
        COMMENT 'Masks full name — initials only for partial access'
        RETURN
            CASE
                WHEN is_member('sms_admins')
                  OR is_member('sms_data_engineers')       THEN name
                WHEN is_member('sms_analysts')
                  OR is_member('sms_finance')
                  THEN concat(left(name, 1), '***')
                ELSE '***'
            END
    """)

    # ── Date of birth masking: 1990-05-21 → 1990-**-** ───────────────────────
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {CATALOG_NAME}.{SILVER_SCHEMA}.mask_dob(dob DATE)
        RETURNS STRING
        LANGUAGE SQL
        COMMENT 'Returns birth year only for partial access, fully masked otherwise'
        RETURN
            CASE
                WHEN is_member('sms_admins')
                  OR is_member('sms_data_engineers')       THEN cast(dob AS STRING)
                WHEN is_member('sms_analysts')
                  OR is_member('sms_finance')
                  THEN concat(year(dob), '-**-**')
                ELSE '****-**-**'
            END
    """)

    # ── Address masking: full address → city + country only ──────────────────
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {CATALOG_NAME}.{SILVER_SCHEMA}.mask_address(addr STRING)
        RETURNS STRING
        LANGUAGE SQL
        COMMENT 'Returns masked address — *** for lower-privilege roles'
        RETURN
            CASE
                WHEN is_member('sms_admins')
                  OR is_member('sms_data_engineers')       THEN addr
                ELSE '*** MASKED ***'
            END
    """)

    # ── PCI: Card number → **** **** **** 1234 ───────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {CATALOG_NAME}.{SILVER_SCHEMA}.mask_card_number(card STRING)
        RETURNS STRING
        LANGUAGE SQL
        COMMENT 'PCI-DSS compliant card masking — last 4 digits only for admins'
        RETURN
            CASE
                WHEN is_member('sms_admins')               THEN card
                ELSE concat('**** **** **** ', right(regexp_replace(card, '[^0-9]', ''), 4))
            END
    """)

    # ── Bank account masking ──────────────────────────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {CATALOG_NAME}.{SILVER_SCHEMA}.mask_bank_account(acct STRING)
        RETURNS STRING
        LANGUAGE SQL
        COMMENT 'Masks bank account — admins only see full value'
        RETURN
            CASE
                WHEN is_member('sms_admins')               THEN acct
                ELSE concat('****', right(acct, 4))
            END
    """)

    logger.info("Masking functions created.")


# ═════════════════════════════════════════════════════════════════════════════
# 3. APPLY COLUMN MASKS TO SILVER TABLES
# ═════════════════════════════════════════════════════════════════════════════

def apply_column_masks(spark: SparkSession):
    """
    Bind masking functions to individual columns on Silver tables.
    Unity Catalog applies these transparently at query time.
    """
    logger.info("Applying column-level masking policies …")

    mask_fn_prefix = f"{CATALOG_NAME}.{SILVER_SCHEMA}"

    column_masks = [
        # (table, column, masking_function)
        (f"{SILVER_SCHEMA}.user",    "email",           f"{mask_fn_prefix}.mask_email(email)"),
        (f"{SILVER_SCHEMA}.user",    "full_name",        f"{mask_fn_prefix}.mask_full_name(full_name)"),
        (f"{SILVER_SCHEMA}.user",    "phone_number",     f"{mask_fn_prefix}.mask_phone(phone_number)"),
        (f"{SILVER_SCHEMA}.user",    "date_of_birth",    f"{mask_fn_prefix}.mask_dob(date_of_birth)"),
        (f"{SILVER_SCHEMA}.user",    "address",          f"{mask_fn_prefix}.mask_address(address)"),
        (f"{SILVER_SCHEMA}.payment", "card_number",      f"{mask_fn_prefix}.mask_card_number(card_number)"),
        (f"{SILVER_SCHEMA}.payment", "bank_account",     f"{mask_fn_prefix}.mask_bank_account(bank_account)"),
        (f"{SILVER_SCHEMA}.payment", "billing_address",  f"{mask_fn_prefix}.mask_address(billing_address)"),
        (f"{SILVER_SCHEMA}.order",   "shipping_address", f"{mask_fn_prefix}.mask_address(shipping_address)"),
        # Gold snapshot (denormalised PII fields)
        (f"{GOLD_SCHEMA}.dim_subscription_status_snapshot", "u_email",
         f"{mask_fn_prefix}.mask_email(u_email)"),
    ]

    for table_path, col_name, mask_expr in column_masks:
        spark.sql(f"""
            ALTER TABLE {CATALOG_NAME}.{table_path}
            ALTER COLUMN {col_name}
            SET MASK {mask_expr}
        """)
        logger.info(f"  Masked {table_path}.{col_name}")

    logger.info("Column masking policies applied.")


# ═════════════════════════════════════════════════════════════════════════════
# 4. ROW-LEVEL SECURITY (Optional — support tier isolation)
# ═════════════════════════════════════════════════════════════════════════════

def apply_row_filters(spark: SparkSession):
    """
    Row-level security: support staff can only see users in their
    assigned country group.  Admins/engineers see all rows.
    """
    logger.info("Applying row-level security filters …")

    # Create a country-access mapping table (maintained by security team)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{SILVER_SCHEMA}.user_country_access_map (
            group_name   STRING,
            country_code STRING
        )
        USING DELTA
        COMMENT 'Maps support groups to allowed country codes for row-level access'
    """)

    # Row filter function for user table
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {CATALOG_NAME}.{SILVER_SCHEMA}.user_row_filter(country_code STRING)
        RETURNS BOOLEAN
        LANGUAGE SQL
        COMMENT 'Row-level filter: admins/engineers see all; support sees only their countries'
        RETURN
            is_member('sms_admins')
            OR is_member('sms_data_engineers')
            OR is_member('sms_analysts')
            OR is_member('sms_finance')
            OR EXISTS (
                SELECT 1
                FROM {CATALOG_NAME}.{SILVER_SCHEMA}.user_country_access_map m
                WHERE m.group_name   = current_user()
                  AND m.country_code = country_code
            )
    """)

    # Apply row filter to user table
    spark.sql(f"""
        ALTER TABLE {CATALOG_NAME}.{SILVER_SCHEMA}.user
        SET ROW FILTER {CATALOG_NAME}.{SILVER_SCHEMA}.user_row_filter
        ON (u_country_code)
    """)

    logger.info("Row-level security filters applied.")


# ═════════════════════════════════════════════════════════════════════════════
# 5. GRANT STATEMENTS — GROUP-BASED ACCESS CONTROL
# ═════════════════════════════════════════════════════════════════════════════

def apply_access_grants(spark: SparkSession):
    """
    Applies GRANT statements to implement least-privilege access control
    across catalog, schemas, and tables.
    """
    logger.info("Applying Unity Catalog GRANT statements …")

    # ── Catalog-level: USAGE to all groups ──────────────────────────────────
    for group in ACCESS_CONTROL["groups"]:
        spark.sql(f"""
            GRANT USAGE ON CATALOG {CATALOG_NAME}
            TO `{group}`
        """)

    # ── Schema-level grants ──────────────────────────────────────────────────
    for schema, groups in ACCESS_CONTROL["schema_grants"].items():
        for group in groups:
            privileges = ACCESS_CONTROL["groups"][group]
            for priv in privileges:
                spark.sql(f"""
                    GRANT {priv} ON SCHEMA {CATALOG_NAME}.{schema}
                    TO `{group}`
                """)

    # ── Table-level refined grants ───────────────────────────────────────────
    # Bronze: restricted to admins + engineers only
    bronze_tables = [
        "subscription_raw", "user_raw", "catalog_raw",
        "order_raw", "device_raw", "payment_raw"
    ]
    for tbl in bronze_tables:
        spark.sql(f"""
            GRANT SELECT ON TABLE {CATALOG_NAME}.{BRONZE_SCHEMA}.{tbl}
            TO `sms_admins`
        """)
        spark.sql(f"""
            GRANT SELECT ON TABLE {CATALOG_NAME}.{BRONZE_SCHEMA}.{tbl}
            TO `sms_data_engineers`
        """)

    # Silver: analysts get SELECT on non-PII tables only; PII tables masked
    silver_non_pii = ["subscription", "catalog", "device"]
    silver_pii     = ["user", "payment", "order", "subscription_enriched"]

    for tbl in silver_non_pii:
        for group in ["sms_analysts", "sms_finance", "sms_support"]:
            spark.sql(f"""
                GRANT SELECT ON TABLE {CATALOG_NAME}.{SILVER_SCHEMA}.{tbl}
                TO `{group}`
            """)

    for tbl in silver_pii:
        for group in ["sms_analysts", "sms_finance"]:
            # SELECT granted but column masks apply transparently
            spark.sql(f"""
                GRANT SELECT ON TABLE {CATALOG_NAME}.{SILVER_SCHEMA}.{tbl}
                TO `{group}`
            """)
        # Support: only on enriched (masked), not raw PII tables
        spark.sql(f"""
            GRANT SELECT ON TABLE {CATALOG_NAME}.{SILVER_SCHEMA}.subscription_enriched
            TO `sms_support`
        """)

    # Gold: all analysts, finance, support, external get SELECT
    gold_tables = [
        "agg_subscription_metrics_daily",
        "agg_subscription_metrics_monthly",
        "agg_revenue_by_plan",
        "agg_user_cohort_analysis",
        "agg_device_distribution",
        "agg_payment_performance",
        "dim_subscription_status_snapshot",
        "fact_subscription_lifecycle",
    ]
    gold_finance_only = ["agg_revenue_by_plan", "agg_payment_performance"]

    for tbl in gold_tables:
        for group in ["sms_admins", "sms_data_engineers", "sms_analysts"]:
            spark.sql(f"""
                GRANT SELECT ON TABLE {CATALOG_NAME}.{GOLD_SCHEMA}.{tbl}
                TO `{group}`
            """)

    for tbl in gold_finance_only:
        spark.sql(f"""
            GRANT SELECT ON TABLE {CATALOG_NAME}.{GOLD_SCHEMA}.{tbl}
            TO `sms_finance`
        """)

    # Support: limited Gold access
    support_gold = ["dim_subscription_status_snapshot",
                    "agg_subscription_metrics_daily"]
    for tbl in support_gold:
        spark.sql(f"""
            GRANT SELECT ON TABLE {CATALOG_NAME}.{GOLD_SCHEMA}.{tbl}
            TO `sms_support`
        """)

    # External: only aggregated, non-PII Gold tables
    external_gold = [
        "agg_subscription_metrics_monthly",
        "agg_device_distribution"
    ]
    for tbl in external_gold:
        spark.sql(f"""
            GRANT SELECT ON TABLE {CATALOG_NAME}.{GOLD_SCHEMA}.{tbl}
            TO `sms_external`
        """)

    # Grant EXECUTE on masking functions to all groups (the function itself
    # enforces access — without EXECUTE, queries would fail silently)
    for group in ACCESS_CONTROL["groups"]:
        spark.sql(f"""
            GRANT EXECUTE ON FUNCTION {CATALOG_NAME}.{SILVER_SCHEMA}.mask_email
            TO `{group}`
        """)
        spark.sql(f"""
            GRANT EXECUTE ON FUNCTION {CATALOG_NAME}.{SILVER_SCHEMA}.mask_phone
            TO `{group}`
        """)

    logger.info("All GRANT statements applied.")


# ═════════════════════════════════════════════════════════════════════════════
# 6. DELTA SHARING CONFIGURATION
# ═════════════════════════════════════════════════════════════════════════════

def configure_delta_sharing(spark: SparkSession):
    """
    Creates a Delta Share and adds the Gold non-PII tables to it.
    Registers recipients with token-based authentication.
    Assigns tables to shares with partition filtering where applicable.
    """
    logger.info("Configuring Delta Sharing …")

    # ── Create the share ─────────────────────────────────────────────────────
    spark.sql(f"""
        CREATE SHARE IF NOT EXISTS {DELTA_SHARE_NAME}
        COMMENT 'SMS Gold layer — consumable datasets for analytics and partners'
    """)

    # ── Add Gold tables to the share ─────────────────────────────────────────
    shared_tables = {
        "agg_subscription_metrics_daily":   "Subscription KPIs aggregated by day",
        "agg_subscription_metrics_monthly": "Subscription KPIs aggregated by month",
        "agg_revenue_by_plan":              "Revenue breakdown by plan and payment method",
        "agg_user_cohort_analysis":         "User retention cohort analysis",
        "agg_device_distribution":          "Device type and OS distribution",
        "agg_payment_performance":          "Payment gateway success and failure metrics",
        "fact_subscription_lifecycle":      "Subscription lifecycle events for churn modelling",
    }

    for tbl_name, comment in shared_tables.items():
        spark.sql(f"""
            ALTER SHARE {DELTA_SHARE_NAME}
            ADD TABLE {CATALOG_NAME}.{GOLD_SCHEMA}.{tbl_name}
            COMMENT '{comment}'
        """)

    # ── Create recipients ────────────────────────────────────────────────────
    for recipient_name, cfg in SHARE_RECIPIENT_CONFIGS.items():
        try:
            spark.sql(f"""
                CREATE RECIPIENT IF NOT EXISTS {recipient_name}
                COMMENT 'Delta Share recipient: {recipient_name}'
                PROPERTIES (
                    'ip_access_list' = '{",".join(cfg["ip_whitelist"])}'
                )
            """)
            logger.info(f"  Recipient created: {recipient_name}")
        except Exception as e:
            logger.warning(f"  Recipient {recipient_name} may already exist: {e}")

    # ── Grant share access to recipients ─────────────────────────────────────
    # Full Gold share for internal analytics team
    spark.sql(f"""
        GRANT SELECT ON SHARE {DELTA_SHARE_NAME}
        TO RECIPIENT analytics_team
    """)

    # Finance gets revenue and payment tables only — use separate share or
    # use a filtered share view:
    spark.sql(f"""
        CREATE SHARE IF NOT EXISTS sms_finance_share
        COMMENT 'SMS Finance subset share'
    """)
    for tbl in ["agg_revenue_by_plan", "agg_payment_performance",
                "agg_subscription_metrics_monthly"]:
        spark.sql(f"""
            ALTER SHARE sms_finance_share
            ADD TABLE {CATALOG_NAME}.{GOLD_SCHEMA}.{tbl}
        """)
    spark.sql("""
        GRANT SELECT ON SHARE sms_finance_share
        TO RECIPIENT finance_team
    """)

    # External partners get only public aggregates
    spark.sql(f"""
        CREATE SHARE IF NOT EXISTS sms_partner_share
        COMMENT 'SMS external partner share — non-PII aggregates only'
    """)
    for tbl in ["agg_subscription_metrics_monthly", "agg_device_distribution"]:
        spark.sql(f"""
            ALTER SHARE sms_partner_share
            ADD TABLE {CATALOG_NAME}.{GOLD_SCHEMA}.{tbl}
        """)
    spark.sql("""
        GRANT SELECT ON SHARE sms_partner_share
        TO RECIPIENT external_partner
    """)

    logger.info("Delta Sharing configured.")


# ═════════════════════════════════════════════════════════════════════════════
# 7. AUDIT LOGGING SETUP
# ═════════════════════════════════════════════════════════════════════════════

def setup_audit_logging(spark: SparkSession):
    """
    Creates an audit log Delta table to capture governance events.
    Databricks workspace audit logs are consumed and parsed here.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{GOLD_SCHEMA}.governance_audit_log (
            event_time       TIMESTAMP,
            user_identity    STRING,
            service_name     STRING,
            action_name      STRING,
            resource_name    STRING,
            request_params   STRING,
            response_status  STRING,
            source_ip        STRING,
            workspace_id     STRING,
            _ingested_at     TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (event_time)
        TBLPROPERTIES (
            'quality_layer' = 'gold',
            'purpose'       = 'audit',
            'retention'     = '7_years'
        )
        COMMENT 'Governance and data access audit log'
    """)
    logger.info("Audit log table created.")


# ═════════════════════════════════════════════════════════════════════════════
# 8. ORCHESTRATION
# ═════════════════════════════════════════════════════════════════════════════

def run_governance_setup():
    spark = get_spark()
    logger.info("Starting governance setup …")

    steps = [
        ("Catalog Tags",        apply_catalog_tags),
        ("Masking Functions",   create_masking_functions),
        ("Column Masks",        apply_column_masks),
        ("Row Filters",         apply_row_filters),
        ("Access Grants",       apply_access_grants),
        ("Delta Sharing",       configure_delta_sharing),
        ("Audit Logging",       setup_audit_logging),
    ]

    for step_name, step_fn in steps:
        try:
            logger.info(f"Step: {step_name} …")
            step_fn(spark)
            logger.info(f"  ✓ {step_name} complete")
        except Exception as ex:
            logger.error(f"  ✗ {step_name} FAILED: {ex}", exc_info=True)
            raise

    logger.info("Governance setup complete.")


if __name__ == "__main__":
    run_governance_setup()
