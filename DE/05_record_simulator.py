"""
=============================================================================
SIMULATOR: Kafka & File-based Record Generator for SMS Pipeline Testing
=============================================================================
Generates realistic synthetic records for all 6 SMS entities and either:
  A) Publishes them directly to Kafka topics (real-time streaming test)
  B) Writes them to JSON/Parquet files (batch landing test)

Usage
-----
  # Stream to Kafka (requires kafka-python):
  python 05_record_simulator.py --mode kafka --entities all --rate 50 --duration 300

  # Write to files:
  python 05_record_simulator.py --mode files --entities all --records 10000 --output ./test_data

  # Single entity:
  python 05_record_simulator.py --mode kafka --entities subscription,user --rate 10

  # Chaos mode (includes malformed records, duplicates, late arrivals):
  python 05_record_simulator.py --mode kafka --entities all --rate 100 --chaos

Dependencies:
  pip install kafka-python faker tqdm
"""

import argparse
import json
import random
import time
import uuid
import os
from datetime import datetime, timedelta, date
from typing import Optional
import threading
from pathlib import Path

try:
    from faker import Faker
except ImportError:
    raise SystemExit("Install faker: pip install faker")

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("[WARN] kafka-python not installed. Kafka mode disabled. pip install kafka-python")

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPICS = {
    "subscription": "sms.subscription.events",
    "user":         "sms.user.events",
    "catalog":      "sms.catalog.events",
    "order":        "sms.order.events",
    "device":       "sms.device.events",
    "payment":      "sms.payment.events",
}

fake = Faker()
Faker.seed(42)
random.seed(42)

# ─── Shared Reference Data Pools ─────────────────────────────────────────────

PLAN_CODES = [
    "BASIC_MONTHLY", "BASIC_ANNUAL",
    "STANDARD_MONTHLY", "STANDARD_ANNUAL",
    "PREMIUM_MONTHLY", "PREMIUM_ANNUAL",
    "ENTERPRISE_MONTHLY", "ENTERPRISE_ANNUAL",
    "STUDENT_MONTHLY", "FAMILY_ANNUAL",
]

SUBSCRIPTION_STATUSES = ["ACTIVE", "INACTIVE", "CANCELLED", "SUSPENDED", "TRIALING", "EXPIRED"]
ORDER_STATUSES         = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED", "REFUNDED"]
PAYMENT_STATUSES       = ["PENDING", "COMPLETED", "FAILED", "REFUNDED", "DISPUTED", "VOIDED"]
PAYMENT_METHODS        = ["CARD", "BANK_TRANSFER", "WALLET", "PAYPAL", "CRYPTO", "OTHER"]
PAYMENT_GATEWAYS       = ["STRIPE", "PAYPAL", "ADYEN", "BRAINTREE", "SQUARE"]
DEVICE_TYPES           = ["MOBILE", "TABLET", "DESKTOP", "TV", "CONSOLE", "OTHER"]
OS_TYPES               = ["ANDROID", "IOS", "WINDOWS", "MACOS", "LINUX", "TIZEN", "WEBOS"]
SIGNUP_CHANNELS        = ["WEB", "MOBILE_APP", "REFERRAL", "ORGANIC", "PAID_SEARCH", "SOCIAL"]
USER_TIERS             = ["FREE", "STANDARD", "PREMIUM", "ENTERPRISE"]
RENEWAL_TYPES          = ["AUTO", "MANUAL", "NONE"]
BILLING_CYCLES         = ["MONTHLY", "ANNUAL", "QUARTERLY"]
EVENT_TYPES            = ["INSERT", "UPDATE", "DELETE"]
COUNTRY_CODES          = ["US", "GB", "DE", "FR", "CA", "AU", "IN", "BR", "SG", "JP"]
CURRENCY_CODES         = ["USD", "GBP", "EUR", "CAD", "AUD", "INR", "BRL"]
ORDER_TYPES            = ["NEW", "RENEWAL", "UPGRADE", "DOWNGRADE", "ADDON"]
DEVICE_MODELS          = ["iPhone 15", "Samsung Galaxy S24", "iPad Pro", "MacBook Air",
                          "Dell XPS", "OnePlus 12", "Pixel 8", "Surface Pro"]

# ─────────────────────────────────────────────────────────────────────────────
# In-memory pools (so FK references are valid)
# ─────────────────────────────────────────────────────────────────────────────

_user_pool:          list[str] = []
_sub_pool:           list[dict] = []   # {sub_ref_id, user_ref_id, plan_code}
_payment_pool:       list[str] = []

MAX_POOL = 5000


def _ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def _date(d: date) -> str:
    return d.strftime("%Y-%m-%d")


# ─────────────────────────────────────────────────────────────────────────────
# Record Generators
# ─────────────────────────────────────────────────────────────────────────────

def generate_user(chaos: bool = False) -> dict:
    uid = str(uuid.uuid4())
    _user_pool.append(uid)
    if len(_user_pool) > MAX_POOL:
        _user_pool.pop(0)

    created = fake.date_time_between(start_date="-3y", end_date="-1d")
    record = {
        "user_ref_id":    uid,
        "full_name":      fake.name(),
        "email":          fake.email(),
        "phone_number":   fake.phone_number(),
        "date_of_birth":  _date(fake.date_of_birth(minimum_age=18, maximum_age=75)),
        "address":        fake.address().replace("\n", ", "),
        "country_code":   random.choice(COUNTRY_CODES),
        "language_code":  random.choice(["en", "de", "fr", "es", "pt", "ja", "zh"]),
        "user_tier":      random.choice(USER_TIERS),
        "signup_channel": random.choice(SIGNUP_CHANNELS),
        "verified_flag":  random.random() > 0.1,
        "created_at":     _ts(created),
        "updated_at":     _ts(created + timedelta(hours=random.randint(0, 1000))),
        "event_type":     "INSERT",
    }

    if chaos:
        _inject_chaos(record)
    return record


def generate_catalog() -> dict:
    plan = random.choice(PLAN_CODES)
    is_annual = "ANNUAL" in plan
    monthly_price = round(random.choice([4.99, 9.99, 14.99, 24.99, 49.99, 99.99]), 2)
    created = fake.date_time_between(start_date="-5y", end_date="-6m")
    return {
        "plan_code":      plan,
        "plan_name":      plan.replace("_", " ").title(),
        "plan_category":  plan.split("_")[0],
        "monthly_price":  monthly_price,
        "annual_price":   round(monthly_price * 12 * random.uniform(0.7, 0.9), 2),
        "currency_code":  "USD",
        "max_devices":    random.choice([1, 2, 3, 5, 10]),
        "features":       json.dumps(random.sample(
            ["4K_STREAMING", "DOWNLOADS", "FAMILY_SHARING",
             "PRIORITY_SUPPORT", "API_ACCESS", "CUSTOM_BRANDING"],
            k=random.randint(1, 4)
        )),
        "is_active":      random.random() > 0.05,
        "created_at":     _ts(created),
        "updated_at":     _ts(created + timedelta(days=random.randint(0, 365))),
        "event_type":     random.choice(["INSERT", "UPDATE"]),
    }


def generate_subscription(chaos: bool = False) -> dict:
    sid     = str(uuid.uuid4())
    uid     = random.choice(_user_pool) if _user_pool else str(uuid.uuid4())
    plan    = random.choice(PLAN_CODES)
    status  = random.choices(
        SUBSCRIPTION_STATUSES,
        weights=[50, 5, 15, 5, 10, 15]  # ACTIVE most common
    )[0]

    start = fake.date_time_between(start_date="-2y", end_date="-1d")
    end   = start + timedelta(days=365 if "ANNUAL" in plan else 30)

    _sub_pool.append({"sub_ref_id": sid, "user_ref_id": uid, "plan_code": plan})
    if len(_sub_pool) > MAX_POOL:
        _sub_pool.pop(0)

    record = {
        "sub_ref_id":    sid,
        "plan_code":     plan,
        "user_ref_id":   uid,
        "status":        status,
        "start_date":    _date(start.date()),
        "end_date":      _date(end.date()) if status != "ACTIVE" else None,
        "renewal_type":  random.choice(RENEWAL_TYPES),
        "billing_cycle": "ANNUAL" if "ANNUAL" in plan else "MONTHLY",
        "trial_flag":    status == "TRIALING" or (random.random() < 0.1),
        "created_at":    _ts(start),
        "updated_at":    _ts(start + timedelta(hours=random.randint(0, 500))),
        "event_type":    "INSERT",
    }
    if chaos:
        _inject_chaos(record)
    return record


def generate_payment(chaos: bool = False) -> dict:
    pid = str(uuid.uuid4())
    _payment_pool.append(pid)
    if len(_payment_pool) > MAX_POOL:
        _payment_pool.pop(0)

    pay_dt   = fake.date_time_between(start_date="-2y", end_date="now")
    status   = random.choices(
        PAYMENT_STATUSES,
        weights=[5, 70, 10, 8, 4, 3]
    )[0]
    amount   = round(random.choice([4.99, 9.99, 14.99, 24.99, 49.99, 99.99, 199.99]), 2)
    record   = {
        "payment_ref_id":   pid,
        "card_number":      fake.credit_card_number(card_type=None),
        "bank_account":     fake.bban(),
        "billing_address":  fake.address().replace("\n", ", "),
        "payment_method":   random.choice(PAYMENT_METHODS),
        "payment_status":   status,
        "payment_amount":   amount,
        "payment_currency": random.choice(CURRENCY_CODES),
        "payment_gateway":  random.choice(PAYMENT_GATEWAYS),
        "transaction_id":   str(uuid.uuid4()).replace("-", "")[:20].upper(),
        "payment_date":     _ts(pay_dt),
        "failure_reason":   random.choice([
            "INSUFFICIENT_FUNDS", "CARD_EXPIRED",
            "FRAUD_SUSPECTED", None, None, None
        ]) if status == "FAILED" else None,
        "retry_count":      random.randint(0, 3) if status in ["FAILED", "COMPLETED"] else 0,
        "created_at":       _ts(pay_dt),
        "updated_at":       _ts(pay_dt + timedelta(minutes=random.randint(0, 60))),
        "event_type":       "INSERT",
    }
    if chaos:
        _inject_chaos(record)
    return record


def generate_order(chaos: bool = False) -> dict:
    oid    = str(uuid.uuid4())
    sub    = random.choice(_sub_pool) if _sub_pool else {
        "sub_ref_id": str(uuid.uuid4()),
        "user_ref_id": str(uuid.uuid4()),
        "plan_code": random.choice(PLAN_CODES)
    }
    pay_id = random.choice(_payment_pool) if _payment_pool else str(uuid.uuid4())
    amount = round(random.choice([4.99, 9.99, 14.99, 24.99, 49.99, 99.99]), 2)
    ord_dt = fake.date_time_between(start_date="-2y", end_date="now")

    record = {
        "order_ref_id":     oid,
        "payment_ref_id":   pay_id,
        "sub_ref_id":       sub["sub_ref_id"],
        "user_ref_id":      sub["user_ref_id"],
        "order_status":     random.choices(
            ORDER_STATUSES,
            weights=[10, 40, 5, 35, 5, 5]
        )[0],
        "order_type":       random.choice(ORDER_TYPES),
        "amount":           amount,
        "discount_amount":  round(amount * random.choice([0, 0, 0, 0.1, 0.2, 0.3]), 2),
        "tax_amount":       round(amount * random.choice([0.08, 0.1, 0.15, 0.2]), 2),
        "currency_code":    random.choice(CURRENCY_CODES),
        "shipping_address": fake.address().replace("\n", ", "),
        "order_date":       _ts(ord_dt),
        "fulfillment_date": _ts(ord_dt + timedelta(days=random.randint(0, 7)))
                            if random.random() > 0.2 else None,
        "created_at":       _ts(ord_dt),
        "updated_at":       _ts(ord_dt + timedelta(hours=random.randint(0, 48))),
        "event_type":       "INSERT",
    }
    if chaos:
        _inject_chaos(record)
    return record


def generate_device(chaos: bool = False) -> dict:
    did = str(uuid.uuid4())
    sub = random.choice(_sub_pool) if _sub_pool else {
        "sub_ref_id": str(uuid.uuid4()),
        "user_ref_id": str(uuid.uuid4()),
    }
    reg_dt = fake.date_time_between(start_date="-3y", end_date="-1d")

    record = {
        "device_ref_id":      did,
        "sub_ref_id":         sub["sub_ref_id"],
        "user_ref_id":        sub["user_ref_id"],
        "device_type":        random.choice(DEVICE_TYPES),
        "os_type":            random.choice(OS_TYPES),
        "os_version":         f"{random.randint(10, 17)}.{random.randint(0, 9)}",
        "device_model":       random.choice(DEVICE_MODELS),
        "device_fingerprint": uuid.uuid4().hex,
        "is_active":          random.random() > 0.15,
        "registered_at":      _ts(reg_dt),
        "last_seen_at":       _ts(reg_dt + timedelta(days=random.randint(0, 700))),
        "created_at":         _ts(reg_dt),
        "updated_at":         _ts(reg_dt + timedelta(hours=random.randint(0, 1000))),
        "event_type":         "INSERT",
    }
    if chaos:
        _inject_chaos(record)
    return record


# ─────────────────────────────────────────────────────────────────────────────
# Chaos Injection
# ─────────────────────────────────────────────────────────────────────────────

def _inject_chaos(record: dict):
    """Randomly inject anomalies to test DLT expectations and DLQ routing."""
    chaos_type = random.choices(
        ["none", "null_pk", "bad_status", "future_date", "duplicate",
         "late_arrival", "extra_field", "malformed_json"],
        weights=[60, 5, 5, 5, 5, 5, 10, 5]
    )[0]

    if chaos_type == "null_pk":
        # Null out a PK field — should be dropped by DLT
        pk_fields = [k for k in record if k.endswith("_ref_id")]
        if pk_fields:
            record[random.choice(pk_fields)] = None

    elif chaos_type == "bad_status":
        if "status" in record:
            record["status"] = "UNKNOWN_STATUS_XYZ"
        elif "payment_status" in record:
            record["payment_status"] = "ZOMBIE"

    elif chaos_type == "future_date":
        if "start_date" in record:
            future = datetime.now() + timedelta(days=random.randint(1, 365))
            record["start_date"] = _date(future.date())

    elif chaos_type == "late_arrival":
        # Timestamp from 2 years ago — simulates late-arriving data
        old_ts = datetime.now() - timedelta(days=random.randint(365, 730))
        if "created_at" in record:
            record["created_at"]  = _ts(old_ts)
            record["updated_at"]  = _ts(old_ts)

    elif chaos_type == "extra_field":
        record["_unexpected_field"] = "chaos_test_value_" + str(random.randint(1000, 9999))

    # "duplicate" and "malformed_json" handled at producer level


def generate_malformed_payload() -> bytes:
    """Returns a byte string that will fail JSON parsing."""
    options = [
        b"{broken json",
        b"",
        b"null",
        b'{"incomplete": ',
        b"not json at all",
        b'{"nested": {"unclosed": {"very": "deep"}}',
    ]
    return random.choice(options)


# ─────────────────────────────────────────────────────────────────────────────
# Generation Dispatch
# ─────────────────────────────────────────────────────────────────────────────

GENERATORS = {
    "user":         generate_user,
    "catalog":      generate_catalog,
    "subscription": generate_subscription,
    "payment":      generate_payment,
    "order":        generate_order,
    "device":       generate_device,
}

# Dependency order ensures FK pools are populated before dependent entities
GENERATION_ORDER = ["user", "catalog", "payment", "subscription", "order", "device"]


# ─────────────────────────────────────────────────────────────────────────────
# Kafka Producer Mode
# ─────────────────────────────────────────────────────────────────────────────

def create_kafka_producer() -> "KafkaProducer":
    if not KAFKA_AVAILABLE:
        raise RuntimeError("kafka-python not installed.")

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k else None,
        acks="all",
        retries=3,
        compression_type="gzip",
        batch_size=16384,
        linger_ms=10,
    )


def stream_to_kafka(entities: list[str], rate_per_sec: int,
                    duration_sec: int, chaos: bool = False):
    """
    Produces records to Kafka at the given rate for the given duration.
    Spreads events evenly across the listed entities.
    """
    producer = create_kafka_producer()
    sent     = {e: 0 for e in entities}
    errors   = 0
    start    = time.time()
    interval = 1.0 / rate_per_sec

    print(f"\n▶ Streaming {rate_per_sec} records/s to Kafka "
          f"for {duration_sec}s across: {', '.join(entities)}")

    # Seed pools if needed
    if not _user_pool:
        for _ in range(200):
            generate_user()
    if not _sub_pool:
        for _ in range(200):
            if _user_pool:
                generate_subscription()
    if not _payment_pool:
        for _ in range(200):
            generate_payment()

    try:
        while (time.time() - start) < duration_sec:
            loop_start = time.time()

            for entity in entities:
                topic = KAFKA_TOPICS[entity]
                try:
                    # Inject malformed record occasionally in chaos mode
                    if chaos and random.random() < 0.02:
                        producer.send(topic, value=generate_malformed_payload(),
                                      key=None)
                    # Duplicate record in chaos mode
                    elif chaos and random.random() < 0.02 and entity in GENERATORS:
                        rec = GENERATORS[entity](chaos=False)
                        producer.send(topic, value=rec, key=rec.get(f"{entity}_ref_id"))
                        producer.send(topic, value=rec, key=rec.get(f"{entity}_ref_id"))
                        sent[entity] += 2
                    else:
                        fn   = GENERATORS[entity]
                        # catalog doesn't accept chaos kwarg
                        rec  = fn(chaos=chaos) if entity != "catalog" else fn()
                        key  = rec.get(f"{entity}_ref_id") or rec.get("plan_code")
                        producer.send(topic, value=rec, key=key)
                        sent[entity] += 1
                except Exception as ex:
                    errors += 1
                    print(f"  [ERROR] {entity}: {ex}")

            elapsed = time.time() - loop_start
            sleep   = max(0, interval - elapsed)
            time.sleep(sleep)

    except KeyboardInterrupt:
        print("\n⏹ Interrupted by user.")
    finally:
        producer.flush()
        producer.close()

    total = sum(sent.values())
    elapsed = time.time() - start
    print(f"\n✔ Done. {total:,} records sent in {elapsed:.1f}s "
          f"({total/elapsed:.0f} rec/s). Errors: {errors}")
    print("  By entity:", {k: f"{v:,}" for k, v in sent.items()})


# ─────────────────────────────────────────────────────────────────────────────
# File Output Mode
# ─────────────────────────────────────────────────────────────────────────────

def write_to_files(entities: list[str], num_records: int,
                   output_dir: str, chaos: bool = False,
                   format: str = "json"):
    """
    Writes records to JSON or NDJSON files — useful for testing
    file-based Auto Loader → Bronze pipeline.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Seed pools
    print("Seeding reference pools …")
    for _ in range(min(500, num_records // 10)):
        generate_user()
        generate_payment()
        generate_subscription()

    per_entity = num_records // len(entities)

    for entity in entities:
        fn       = GENERATORS[entity]
        filepath = out / f"{entity}_records.ndjson"
        print(f"Writing {per_entity:,} {entity} records to {filepath} …")

        records_written = 0
        with open(filepath, "w") as f:
            iter_range = range(per_entity)
            if TQDM_AVAILABLE:
                iter_range = tqdm(iter_range, desc=entity, unit="rec")
            for _ in iter_range:
                try:
                    rec = fn(chaos=chaos) if entity != "catalog" else fn()
                    f.write(json.dumps(rec) + "\n")
                    records_written += 1
                except Exception as ex:
                    print(f"  [ERROR] {entity}: {ex}")

        print(f"  ✓ {records_written:,} records written")

    print(f"\n✔ All files written to: {out.resolve()}")


# ─────────────────────────────────────────────────────────────────────────────
# Databricks Auto Loader Trigger Mode
# ─────────────────────────────────────────────────────────────────────────────

def write_rolling_files(entities: list[str], records_per_file: int,
                        num_files: int, output_dir: str,
                        interval_sec: float = 5.0, chaos: bool = False):
    """
    Writes rolling batches of files to simulate continuous file drops
    for Auto Loader testing (alternative to direct Kafka).
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Seed pools
    for _ in range(200):
        generate_user()
        generate_payment()
        generate_subscription()

    for file_idx in range(num_files):
        batch_ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        for entity in entities:
            fn       = GENERATORS[entity]
            filepath = out / entity / f"batch_{batch_ts}_{file_idx:05d}.ndjson"
            filepath.parent.mkdir(parents=True, exist_ok=True)

            with open(filepath, "w") as f:
                for _ in range(records_per_file):
                    rec = fn(chaos=chaos) if entity != "catalog" else fn()
                    f.write(json.dumps(rec) + "\n")

        print(f"[{datetime.now():%H:%M:%S}] File batch {file_idx+1}/{num_files} written.")
        if file_idx < num_files - 1:
            time.sleep(interval_sec)

    print(f"✔ {num_files} file batches written to {out.resolve()}")


# ─────────────────────────────────────────────────────────────────────────────
# Summary Statistics
# ─────────────────────────────────────────────────────────────────────────────

def generate_stats_report(entities: list[str], num_samples: int = 100):
    """
    Generates a small sample and prints a brief stats summary.
    Useful for sanity-checking record shapes before a large run.
    """
    # Seed
    for _ in range(50):
        generate_user()
        generate_payment()
        generate_subscription()

    print(f"\n{'─'*60}")
    print("  RECORD STATS PREVIEW")
    print(f"{'─'*60}")
    for entity in entities:
        fn      = GENERATORS[entity]
        samples = []
        for _ in range(num_samples):
            rec = fn() if entity != "catalog" else fn()
            samples.append(rec)

        fields  = list(samples[0].keys())
        null_counts = {f: sum(1 for s in samples if s.get(f) is None) for f in fields}
        null_pct    = {f: round(c / num_samples * 100, 1) for f, c in null_counts.items()}

        print(f"\n  {entity.upper()} ({len(fields)} fields, {num_samples} samples)")
        print(f"  {'Field':<30} {'Null%':>6}")
        print(f"  {'─'*40}")
        for f in fields:
            if null_pct[f] > 0:
                print(f"  {f:<30} {null_pct[f]:>5}%  ← nullable")
            else:
                print(f"  {f:<30} {0:>5}%")
    print(f"\n{'─'*60}\n")


# ─────────────────────────────────────────────────────────────────────────────
# CLI Entry Point
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="SMS Pipeline Record Simulator",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=__doc__
    )
    p.add_argument("--mode", choices=["kafka", "files", "rolling", "stats"],
                   default="stats",
                   help="Output mode: kafka | files | rolling | stats")
    p.add_argument("--entities", default="all",
                   help="Comma-separated entity names or 'all'. "
                        "Order: user,catalog,payment,subscription,order,device")
    p.add_argument("--rate",     type=int,   default=20,
                   help="Records per second (kafka mode)")
    p.add_argument("--duration", type=int,   default=60,
                   help="Streaming duration in seconds (kafka mode)")
    p.add_argument("--records",  type=int,   default=5000,
                   help="Total records per entity (files mode)")
    p.add_argument("--files",    type=int,   default=10,
                   help="Number of file batches (rolling mode)")
    p.add_argument("--recs-per-file", type=int, default=500,
                   help="Records per file (rolling mode)")
    p.add_argument("--interval", type=float, default=5.0,
                   help="Seconds between file drops (rolling mode)")
    p.add_argument("--output",   default="./test_data",
                   help="Output directory for file modes")
    p.add_argument("--format",   choices=["json", "ndjson"], default="ndjson",
                   help="Output file format")
    p.add_argument("--chaos",    action="store_true",
                   help="Inject malformed, duplicate, and late-arriving records")
    p.add_argument("--kafka-broker", default=KAFKA_BOOTSTRAP,
                   help=f"Kafka bootstrap server (default: {KAFKA_BOOTSTRAP})")
    return p.parse_args()


def main():
    args    = parse_args()
    global KAFKA_BOOTSTRAP
    KAFKA_BOOTSTRAP = args.kafka_broker

    if args.entities.lower() == "all":
        entities = GENERATION_ORDER
    else:
        entities = [e.strip() for e in args.entities.split(",")]
        # Validate
        for e in entities:
            if e not in GENERATORS:
                raise ValueError(f"Unknown entity: {e}. "
                                 f"Valid: {list(GENERATORS.keys())}")

    chaos_label = " [CHAOS MODE]" if args.chaos else ""
    print(f"\n{'═'*60}")
    print(f"  SMS Pipeline Record Simulator{chaos_label}")
    print(f"  Mode: {args.mode.upper()}  |  Entities: {', '.join(entities)}")
    print(f"{'═'*60}\n")

    if args.mode == "kafka":
        stream_to_kafka(
            entities    = entities,
            rate_per_sec= args.rate,
            duration_sec= args.duration,
            chaos       = args.chaos,
        )
    elif args.mode == "files":
        write_to_files(
            entities    = entities,
            num_records = args.records,
            output_dir  = args.output,
            chaos       = args.chaos,
            format      = args.format,
        )
    elif args.mode == "rolling":
        write_rolling_files(
            entities        = entities,
            records_per_file= args.recs_per_file,
            num_files       = args.files,
            output_dir      = args.output,
            interval_sec    = args.interval,
            chaos           = args.chaos,
        )
    elif args.mode == "stats":
        generate_stats_report(entities, num_samples=100)


if __name__ == "__main__":
    main()
