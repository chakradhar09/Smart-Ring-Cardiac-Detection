import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Optional

import psycopg
import redis
from kafka import KafkaConsumer, KafkaProducer
from psycopg.types.json import Json


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] anomaly-engine - %(message)s",
)


def env(name: str, fallback: str) -> str:
    value = os.getenv(name)
    return value if value else fallback


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def wait_for_database(database_url: str) -> psycopg.Connection:
    while True:
        try:
            connection = psycopg.connect(database_url)
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            logging.info("connected to database")
            return connection
        except Exception as error:
            logging.warning("database unavailable, retrying: %s", error)
            time.sleep(2)


def wait_for_redis(redis_url: str) -> redis.Redis:
    while True:
        try:
            client = redis.Redis.from_url(redis_url, decode_responses=True)
            client.ping()
            logging.info("connected to redis")
            return client
        except Exception as error:
            logging.warning("redis unavailable, retrying: %s", error)
            time.sleep(2)


def wait_for_kafka_consumer(brokers: str, topic: str) -> KafkaConsumer:
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=[broker.strip() for broker in brokers.split(",")],
                auto_offset_reset="earliest",
                group_id="anomaly-engine",
                enable_auto_commit=True,
                consumer_timeout_ms=2000,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            logging.info("connected to Kafka as consumer on topic %s", topic)
            return consumer
        except Exception as error:
            logging.warning("kafka consumer unavailable, retrying: %s", error)
            time.sleep(2)


def wait_for_kafka_producer(brokers: str) -> KafkaProducer:
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[broker.strip() for broker in brokers.split(",")],
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            )
            logging.info("connected to Kafka as producer")
            return producer
        except Exception as error:
            logging.warning("kafka producer unavailable, retrying: %s", error)
            time.sleep(2)


def load_baseline(cache: redis.Redis, user_id: str) -> Optional[Dict[str, float]]:
    data = cache.hgetall(f"baseline:{user_id}")
    if not data:
        return None
    return {
        "heart_rate": float(data.get("heart_rate", 0.0)),
        "hrv": float(data.get("hrv", 0.0)),
        "spo2": float(data.get("spo2", 0.0)),
        "count": float(data.get("count", 0.0)),
    }


def save_baseline(cache: redis.Redis, user_id: str, baseline: Dict[str, float]) -> None:
    cache.hset(
        f"baseline:{user_id}",
        mapping={
            "heart_rate": baseline["heart_rate"],
            "hrv": baseline["hrv"],
            "spo2": baseline["spo2"],
            "count": baseline["count"],
            "updated_at": now_iso(),
        },
    )


def update_baseline(current: Optional[Dict[str, float]], reading: Dict[str, float]) -> Dict[str, float]:
    heart_rate = float(reading.get("heart_rate", 0.0))
    hrv = float(reading.get("hrv", 0.0))
    spo2 = float(reading.get("spo2", 0.0))

    if current is None or current.get("count", 0.0) <= 0:
        return {
            "heart_rate": heart_rate,
            "hrv": hrv,
            "spo2": spo2,
            "count": 1.0,
        }

    alpha = 0.12
    return {
        "heart_rate": current["heart_rate"] + alpha * (heart_rate - current["heart_rate"]),
        "hrv": current["hrv"] + alpha * (hrv - current["hrv"]),
        "spo2": current["spo2"] + alpha * (spo2 - current["spo2"]),
        "count": current["count"] + 1.0,
    }


def classify_event_type(event: Dict) -> str:
    scores = {
        "arrhythmia": float(event.get("arrhythmia_score", 0.0)),
        "hypertension": float(event.get("hypertension_score", 0.0)),
        "ischemia": float(event.get("ischemic_score", 0.0)),
    }
    return max(scores, key=scores.get)


def evaluate_severity(event: Dict, baseline: Optional[Dict[str, float]]) -> tuple[str, float]:
    heart_rate = float(event.get("heart_rate", 0.0))
    spo2 = float(event.get("spo2", 0.0))
    confidence = float(event.get("confidence_score", 0.0))
    arrhythmia = float(event.get("arrhythmia_score", 0.0))
    ischemic = float(event.get("ischemic_score", 0.0))

    deviation = 0.0
    if baseline:
        baseline_hr = max(baseline.get("heart_rate", 1.0), 1.0)
        baseline_hrv = max(baseline.get("hrv", 1.0), 1.0)
        baseline_spo2 = max(baseline.get("spo2", 1.0), 1.0)

        hr_delta = abs(heart_rate - baseline_hr) / baseline_hr
        hrv_delta = abs(float(event.get("hrv", 0.0)) - baseline_hrv) / baseline_hrv
        spo2_delta = abs(spo2 - baseline_spo2) / baseline_spo2
        deviation = max(hr_delta, hrv_delta, spo2_delta)

    if arrhythmia >= 0.85 or spo2 <= 88.0 or heart_rate >= 170.0:
        return "critical", deviation

    if arrhythmia >= 0.70 or ischemic >= 0.65 or heart_rate >= 150.0 or deviation >= 0.45:
        return "high", deviation

    if confidence >= 0.78 or deviation >= 0.30:
        return "medium", deviation

    return "normal", deviation


def persist_event(connection: psycopg.Connection, event_record: Dict) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO cardiac_events (
                event_id,
                user_id,
                device_id,
                event_type,
                confidence_score,
                severity,
                raw_features,
                detected_at
            ) VALUES (
                %(event_id)s,
                %(user_id)s,
                %(device_id)s,
                %(event_type)s,
                %(confidence_score)s,
                %(severity)s,
                %(raw_features)s,
                %(detected_at)s
            )
            """,
            {
                **event_record,
                "raw_features": Json(event_record["raw_features"]),
            },
        )

        cursor.execute(
            """
            INSERT INTO audit_logs (
                audit_id,
                actor,
                action,
                target_type,
                target_id,
                details
            ) VALUES (
                %(audit_id)s,
                %(actor)s,
                %(action)s,
                %(target_type)s,
                %(target_id)s,
                %(details)s
            )
            """,
            {
                "audit_id": str(uuid.uuid4()),
                "actor": "anomaly-engine",
                "action": "cardiac_event_detected",
                "target_type": "cardiac_event",
                "target_id": event_record["event_id"],
                "details": Json(
                    {
                        "severity": event_record["severity"],
                        "event_type": event_record["event_type"],
                        "user_id": event_record["user_id"],
                    }
                ),
            },
        )


def main() -> None:
    kafka_brokers = env("KAFKA_BROKERS", "localhost:9092")
    topic_in = env("KAFKA_TOPIC_IN", "biosignal.classified")
    topic_out = env("KAFKA_TOPIC_OUT", "cardiac.anomaly")
    redis_url = env("REDIS_URL", "redis://localhost:6379/0")
    database_url = env("DATABASE_URL", "postgresql://ring:ringpass@localhost:5432/cardioring")

    db_connection = wait_for_database(database_url)
    cache = wait_for_redis(redis_url)
    consumer = wait_for_kafka_consumer(kafka_brokers, topic_in)
    producer = wait_for_kafka_producer(kafka_brokers)

    while True:
        try:
            had_messages = False
            for message in consumer:
                had_messages = True
                event = message.value

                user_id = event.get("user_id")
                device_id = event.get("device_id")
                if not user_id or not device_id:
                    logging.warning("skipping malformed classified event: %s", event)
                    continue

                baseline = load_baseline(cache, user_id)
                severity, deviation = evaluate_severity(event, baseline)

                # Update baseline only when reading appears normal to avoid poisoning baseline with crises.
                if severity == "normal":
                    updated_baseline = update_baseline(baseline, event)
                    save_baseline(cache, user_id, updated_baseline)
                    logging.info("updated baseline for user=%s", user_id)
                    continue

                event_id = str(uuid.uuid4())
                event_type = classify_event_type(event)
                detected_at = event.get("classified_at", now_iso())

                anomaly_event = {
                    "event_id": event_id,
                    "user_id": user_id,
                    "device_id": device_id,
                    "event_type": event_type,
                    "severity": severity,
                    "confidence_score": float(event.get("confidence_score", 0.0)),
                    "detected_at": detected_at,
                    "location": event.get("location", {}),
                    "raw_features": {
                        "captured_at": event.get("recorded_at"),
                        "classified_at": event.get("classified_at"),
                        "heart_rate": event.get("heart_rate"),
                        "hrv": event.get("hrv"),
                        "spo2": event.get("spo2"),
                        "ecg_value": event.get("ecg_value"),
                        "ppg_value": event.get("ppg_value"),
                        "confidence_score": event.get("confidence_score"),
                        "arrhythmia_score": event.get("arrhythmia_score"),
                        "hypertension_score": event.get("hypertension_score"),
                        "ischemic_score": event.get("ischemic_score"),
                        "deviation_score": round(float(deviation), 3),
                        "baseline_snapshot": {
                            "heart_rate": round(float(baseline.get("heart_rate", 0.0)), 2) if baseline else None,
                            "hrv": round(float(baseline.get("hrv", 0.0)), 2) if baseline else None,
                            "spo2": round(float(baseline.get("spo2", 0.0)), 2) if baseline else None,
                            "count": int(float(baseline.get("count", 0.0))) if baseline else 0,
                        },
                        "location": event.get("location", {}),
                        "metadata": event.get("metadata", {}),
                    },
                }

                persist_event(db_connection, anomaly_event)
                producer.send(topic_out, key=user_id.encode("utf-8"), value=anomaly_event)

                logging.warning(
                    "detected event=%s user=%s severity=%s type=%s",
                    event_id,
                    user_id,
                    severity,
                    event_type,
                )

            if not had_messages:
                time.sleep(0.5)
        except Exception as error:
            logging.exception("pipeline error, continuing: %s", error)
            time.sleep(1)


if __name__ == "__main__":
    main()
