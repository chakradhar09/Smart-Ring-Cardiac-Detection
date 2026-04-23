import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Dict

import psycopg
from kafka import KafkaConsumer, KafkaProducer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] ai-analysis-engine - %(message)s",
)


def env(name: str, fallback: str) -> str:
    value = os.getenv(name)
    return value if value else fallback


def clamp(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
    return max(minimum, min(maximum, value))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def compute_scores(reading: Dict) -> Dict[str, float]:
    heart_rate = float(reading.get("heart_rate", 0.0))
    hrv = float(reading.get("hrv", 0.0))
    spo2 = float(reading.get("spo2", 0.0))
    ecg = float(reading.get("ecg_value", 0.0))
    ppg = float(reading.get("ppg_value", 0.0))

    arrhythmia = clamp(
        max(
            abs(heart_rate - 72.0) / 70.0,
            (35.0 - hrv) / 35.0 if hrv < 35.0 else 0.0,
            (92.0 - spo2) / 15.0 if spo2 < 92.0 else 0.0,
        )
    )

    hypertension = clamp(
        max(
            (heart_rate - 100.0) / 55.0 if heart_rate > 100.0 else 0.0,
            (ppg - 1.2) / 0.8 if ppg > 1.2 else 0.0,
        )
    )

    ischemic = clamp(
        max(
            (91.0 - spo2) / 8.0 if spo2 < 91.0 else 0.0,
            (ecg - 1.4) / 0.9 if ecg > 1.4 else 0.0,
        )
    )

    confidence = round(min(0.995, 0.62 + max(arrhythmia, hypertension, ischemic) * 0.36), 3)

    return {
        "arrhythmia_score": round(arrhythmia, 3),
        "hypertension_score": round(hypertension, 3),
        "ischemic_score": round(ischemic, 3),
        "confidence_score": confidence,
    }


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


def wait_for_kafka_consumer(brokers: str, topic: str) -> KafkaConsumer:
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=[broker.strip() for broker in brokers.split(",")],
                auto_offset_reset="earliest",
                group_id="ai-analysis-engine",
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


def persist_reading(connection: psycopg.Connection, event: Dict) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO biosignal_readings (
                reading_id,
                user_id,
                device_id,
                ecg_value,
                ppg_value,
                heart_rate,
                hrv,
                spo2,
                arrhythmia_score,
                hypertension_score,
                ischemic_score,
                confidence_score,
                recorded_at
            ) VALUES (
                %(reading_id)s,
                %(user_id)s,
                %(device_id)s,
                %(ecg_value)s,
                %(ppg_value)s,
                %(heart_rate)s,
                %(hrv)s,
                %(spo2)s,
                %(arrhythmia_score)s,
                %(hypertension_score)s,
                %(ischemic_score)s,
                %(confidence_score)s,
                %(recorded_at)s
            )
            ON CONFLICT DO NOTHING
            """,
            event,
        )


def main() -> None:
    kafka_brokers = env("KAFKA_BROKERS", "localhost:9092")
    topic_in = env("KAFKA_TOPIC_IN", "biosignal.raw")
    topic_out = env("KAFKA_TOPIC_OUT", "biosignal.classified")
    database_url = env("DATABASE_URL", "postgresql://ring:ringpass@localhost:5432/cardioring")

    db_connection = wait_for_database(database_url)
    consumer = wait_for_kafka_consumer(kafka_brokers, topic_in)
    producer = wait_for_kafka_producer(kafka_brokers)

    while True:
        try:
            had_messages = False
            for message in consumer:
                had_messages = True
                reading = message.value
                reading.setdefault("reading_id", str(uuid.uuid4()))
                reading.setdefault("recorded_at", now_iso())

                scores = compute_scores(reading)
                classified_event = {
                    **reading,
                    **scores,
                    "classified_at": now_iso(),
                    "model_versions": {
                        "arrhythmia": "arrhythmia-v1.0-demo",
                        "hypertension": "hypertension-v1.0-demo",
                        "ischemic": "ischemic-v1.0-demo",
                    },
                }

                persist_reading(db_connection, classified_event)
                producer.send(topic_out, key=reading["user_id"].encode("utf-8"), value=classified_event)

                logging.info(
                    "classified reading=%s user=%s confidence=%.3f",
                    classified_event["reading_id"],
                    classified_event["user_id"],
                    classified_event["confidence_score"],
                )

            if not had_messages:
                time.sleep(0.5)
        except Exception as error:
            logging.exception("pipeline error, continuing: %s", error)
            time.sleep(1)


if __name__ == "__main__":
    main()
