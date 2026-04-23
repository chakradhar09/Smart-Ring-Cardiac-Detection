import os
from datetime import datetime, timezone
from pathlib import Path

import httpx
import psycopg
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://ring:ringpass@localhost:5432/cardioring")
INGESTION_URL = os.getenv("INGESTION_URL", "http://localhost:8080/ingest")
PORT = int(os.getenv("PORT", "8090"))

app = FastAPI(title="CardioRing Demo API", version="1.0.0")


def db_connection() -> psycopg.Connection:
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def severity_rank(severity: str | None) -> int:
    if severity == "critical":
        return 3
    if severity == "high":
        return 2
    if severity == "medium":
        return 1
    return 0


@app.get("/health")
def health() -> dict:
    try:
        with db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
        return {"status": "ok"}
    except Exception as error:
        return JSONResponse(status_code=503, content={"status": "degraded", "error": str(error)})


@app.get("/api/users")
def list_users() -> dict:
    with db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, name, age, email, emergency_contact, subscription_tier, created_at
                FROM users
                ORDER BY created_at DESC
                """
            )
            users = cursor.fetchall()
    return {"users": users}


@app.get("/api/demo/patient-profiles")
def list_patient_profiles(limit: int = 200) -> dict:
    with db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    u.user_id,
                    u.name,
                    u.age,
                    u.subscription_tier,
                    u.emergency_contact,
                    pp.profile_label,
                    pp.risk_level,
                    pp.primary_condition,
                    pp.anomaly_every,
                    pp.baseline_heart_rate,
                    pp.baseline_hrv,
                    pp.baseline_spo2,
                    latest.event_type AS latest_event_type,
                    latest.severity AS latest_event_severity,
                    latest.detected_at AS latest_event_at
                FROM users u
                LEFT JOIN patient_profiles pp ON pp.user_id = u.user_id
                LEFT JOIN LATERAL (
                    SELECT event_type, severity, detected_at
                    FROM cardiac_events ce
                    WHERE ce.user_id = u.user_id
                    ORDER BY detected_at DESC
                    LIMIT 1
                ) latest ON TRUE
                ORDER BY u.user_id
                LIMIT %s
                """,
                (max(1, min(limit, 1000)),),
            )
            profiles = cursor.fetchall()

    return {"profiles": profiles}


@app.get("/api/doctor/overview")
def doctor_overview(limit: int = 200) -> dict:
    with db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    u.user_id,
                    u.name,
                    u.age,
                    u.subscription_tier,
                    pp.profile_label,
                    pp.risk_level,
                    pp.primary_condition,
                    latest_reading.heart_rate,
                    latest_reading.hrv,
                    latest_reading.spo2,
                    latest_reading.confidence_score,
                    latest_reading.recorded_at AS latest_reading_at,
                    latest_event.event_type AS latest_event_type,
                    latest_event.severity AS latest_event_severity,
                    latest_event.detected_at AS latest_event_at,
                    COALESCE(alerts_24h.alert_count, 0) AS alerts_24h,
                    CASE
                        WHEN latest_event.severity = 'critical' THEN 'critical'
                        WHEN latest_event.severity = 'high' THEN 'high'
                        WHEN latest_event.severity = 'medium' THEN 'medium'
                        WHEN latest_reading.spo2 < 92 OR latest_reading.heart_rate > 120 THEN 'high'
                        WHEN latest_reading.spo2 < 95 OR latest_reading.heart_rate > 100 THEN 'medium'
                        ELSE 'stable'
                    END AS status
                FROM users u
                LEFT JOIN patient_profiles pp ON pp.user_id = u.user_id
                LEFT JOIN LATERAL (
                    SELECT heart_rate, hrv, spo2, confidence_score, recorded_at
                    FROM biosignal_readings br
                    WHERE br.user_id = u.user_id
                    ORDER BY recorded_at DESC
                    LIMIT 1
                ) latest_reading ON TRUE
                LEFT JOIN LATERAL (
                    SELECT event_type, severity, detected_at
                    FROM cardiac_events ce
                    WHERE ce.user_id = u.user_id
                    ORDER BY detected_at DESC
                    LIMIT 1
                ) latest_event ON TRUE
                LEFT JOIN LATERAL (
                    SELECT COUNT(*) AS alert_count
                    FROM alerts a
                    JOIN cardiac_events ce ON ce.event_id = a.event_id
                    WHERE ce.user_id = u.user_id
                    AND a.dispatched_at >= NOW() - INTERVAL '24 hours'
                ) alerts_24h ON TRUE
                ORDER BY
                    CASE
                        WHEN latest_event.severity = 'critical' THEN 3
                        WHEN latest_event.severity = 'high' THEN 2
                        WHEN latest_event.severity = 'medium' THEN 1
                        ELSE 0
                    END DESC,
                    latest_event.detected_at DESC NULLS LAST,
                    u.user_id
                LIMIT %s
                """,
                (max(1, min(limit, 1000)),),
            )
            patients = cursor.fetchall()

    status_summary = {"critical": 0, "high": 0, "medium": 0, "stable": 0}
    for patient in patients:
        status = patient.get("status") or "stable"
        if status not in status_summary:
            status = "stable"
        status_summary[status] += 1
        patient["status_rank"] = severity_rank(patient.get("latest_event_severity"))

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status_summary": status_summary,
        "patients": patients,
    }


@app.get("/api/doctor/patients/{user_id}/detail")
def doctor_patient_detail(user_id: str, event_limit: int = 25, alert_limit: int = 25) -> dict:
    safe_event_limit = max(1, min(event_limit, 200))
    safe_alert_limit = max(1, min(alert_limit, 200))

    with db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    u.user_id,
                    u.name,
                    u.age,
                    u.email,
                    u.emergency_contact,
                    u.subscription_tier,
                    pp.profile_label,
                    pp.risk_level,
                    pp.primary_condition,
                    pp.anomaly_every,
                    pp.baseline_heart_rate,
                    pp.baseline_hrv,
                    pp.baseline_spo2
                FROM users u
                LEFT JOIN patient_profiles pp ON pp.user_id = u.user_id
                WHERE u.user_id = %s
                """,
                (user_id,),
            )
            patient = cursor.fetchone()

            if patient is None:
                raise HTTPException(status_code=404, detail="user not found")

            cursor.execute(
                """
                SELECT
                    heart_rate,
                    hrv,
                    spo2,
                    arrhythmia_score,
                    hypertension_score,
                    ischemic_score,
                    confidence_score,
                    recorded_at
                FROM biosignal_readings
                WHERE user_id = %s
                ORDER BY recorded_at DESC
                LIMIT 1
                """,
                (user_id,),
            )
            latest_reading = cursor.fetchone()

            cursor.execute(
                """
                SELECT
                    AVG(heart_rate) AS baseline_heart_rate,
                    AVG(hrv) AS baseline_hrv,
                    AVG(spo2) AS baseline_spo2
                FROM biosignal_readings
                WHERE user_id = %s
                AND recorded_at >= NOW() - INTERVAL '15 minutes'
                """,
                (user_id,),
            )
            baseline_window = cursor.fetchone()

            cursor.execute(
                """
                SELECT severity, COUNT(*) AS count
                FROM cardiac_events
                WHERE user_id = %s
                AND detected_at >= NOW() - INTERVAL '24 hours'
                GROUP BY severity
                """,
                (user_id,),
            )
            event_distribution = cursor.fetchall()

            cursor.execute(
                """
                SELECT
                    event_id,
                    user_id,
                    device_id,
                    event_type,
                    confidence_score,
                    severity,
                    raw_features,
                    detected_at
                FROM cardiac_events
                WHERE user_id = %s
                ORDER BY detected_at DESC
                LIMIT %s
                """,
                (user_id, safe_event_limit),
            )
            events = cursor.fetchall()

            cursor.execute(
                """
                SELECT
                    a.alert_id,
                    a.event_id,
                    a.channel,
                    a.recipient,
                    a.status,
                    a.provider_reference,
                    a.dispatched_at,
                    a.acknowledged_at,
                    e.event_type,
                    e.severity,
                    e.detected_at
                FROM alerts a
                JOIN cardiac_events e ON e.event_id = a.event_id
                WHERE e.user_id = %s
                ORDER BY a.dispatched_at DESC
                LIMIT %s
                """,
                (user_id, safe_alert_limit),
            )
            alerts = cursor.fetchall()

    return {
        "patient": patient,
        "latest_reading": latest_reading,
        "baseline_window": baseline_window,
        "event_distribution_24h": event_distribution,
        "events": events,
        "alerts": alerts,
    }


@app.get("/api/users/{user_id}/dashboard")
def get_dashboard(user_id: str) -> dict:
    with db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    heart_rate,
                    hrv,
                    spo2,
                    arrhythmia_score,
                    hypertension_score,
                    ischemic_score,
                    confidence_score,
                    recorded_at
                FROM biosignal_readings
                WHERE user_id = %s
                ORDER BY recorded_at DESC
                LIMIT 1
                """,
                (user_id,),
            )
            latest_reading = cursor.fetchone()

            cursor.execute(
                """
                SELECT
                    AVG(heart_rate) AS baseline_heart_rate,
                    AVG(hrv) AS baseline_hrv,
                    AVG(spo2) AS baseline_spo2
                FROM biosignal_readings
                WHERE user_id = %s
                AND recorded_at >= NOW() - INTERVAL '15 minutes'
                """,
                (user_id,),
            )
            baseline_window = cursor.fetchone()

            cursor.execute(
                """
                SELECT severity, COUNT(*) AS count
                FROM cardiac_events
                WHERE user_id = %s
                AND detected_at >= NOW() - INTERVAL '24 hours'
                GROUP BY severity
                """,
                (user_id,),
            )
            event_distribution = cursor.fetchall()

            cursor.execute(
                """
                SELECT COUNT(*) AS total_alerts
                FROM alerts a
                JOIN cardiac_events e ON e.event_id = a.event_id
                WHERE e.user_id = %s
                AND a.dispatched_at >= NOW() - INTERVAL '24 hours'
                """,
                (user_id,),
            )
            alert_count = cursor.fetchone()

    if latest_reading is None:
        raise HTTPException(status_code=404, detail="no readings available for this user")

    return {
        "user_id": user_id,
        "latest_reading": latest_reading,
        "baseline_window": baseline_window,
        "event_distribution_24h": event_distribution,
        "alert_count_24h": alert_count["total_alerts"],
    }


@app.get("/api/users/{user_id}/events")
def get_events(user_id: str, limit: int = 25) -> dict:
    with db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    event_id,
                    user_id,
                    device_id,
                    event_type,
                    confidence_score,
                    severity,
                    raw_features,
                    detected_at
                FROM cardiac_events
                WHERE user_id = %s
                ORDER BY detected_at DESC
                LIMIT %s
                """,
                (user_id, max(1, min(limit, 200))),
            )
            events = cursor.fetchall()
    return {"events": events}


@app.get("/api/users/{user_id}/alerts")
def get_alerts(user_id: str, limit: int = 25) -> dict:
    with db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    a.alert_id,
                    a.event_id,
                    a.channel,
                    a.recipient,
                    a.status,
                    a.provider_reference,
                    a.dispatched_at,
                    a.acknowledged_at,
                    e.event_type,
                    e.severity,
                    e.detected_at
                FROM alerts a
                JOIN cardiac_events e ON e.event_id = a.event_id
                WHERE e.user_id = %s
                ORDER BY a.dispatched_at DESC
                LIMIT %s
                """,
                (user_id, max(1, min(limit, 200))),
            )
            alerts = cursor.fetchall()
    return {"alerts": alerts}


@app.get("/api/system/metrics")
def get_system_metrics() -> dict:
    with db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS count FROM users")
            users = cursor.fetchone()["count"]

            cursor.execute("SELECT COUNT(*) AS count FROM devices")
            devices = cursor.fetchone()["count"]

            cursor.execute("SELECT COUNT(*) AS count FROM biosignal_readings")
            readings = cursor.fetchone()["count"]

            cursor.execute("SELECT COUNT(*) AS count FROM cardiac_events")
            events = cursor.fetchone()["count"]

            cursor.execute("SELECT COUNT(*) AS count FROM alerts")
            alerts = cursor.fetchone()["count"]

            cursor.execute(
                """
                SELECT
                    PERCENTILE_CONT(0.95)
                    WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (a.dispatched_at - e.detected_at)))
                    AS p95_seconds
                FROM alerts a
                JOIN cardiac_events e ON e.event_id = a.event_id
                """
            )
            latency = cursor.fetchone()

    return {
        "entities": {
            "users": users,
            "devices": devices,
            "biosignal_readings": readings,
            "cardiac_events": events,
            "alerts": alerts,
        },
        "pipeline_sla": {
            "target_alert_latency_seconds": 30,
            "p95_observed_latency_seconds": round(float(latency["p95_seconds"] or 0.0), 3),
        },
    }


@app.post("/api/demo/trigger-critical")
async def trigger_critical(user_id: str = "user-001", device_id: str = "device-001") -> dict:
    payload = {
        "user_id": user_id,
        "device_id": device_id,
        "ecg_value": 2.0,
        "ppg_value": 1.8,
        "heart_rate": 182.0,
        "hrv": 12.0,
        "spo2": 84.0,
        "location": {"lat": 19.0760, "lon": 72.8777},
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "metadata": {
            "source": "manual-demo-trigger",
            "stream_label": "critical-pattern",
        },
    }

    async with httpx.AsyncClient(timeout=5) as client:
        response = await client.post(INGESTION_URL, json=payload)

    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"ingestion failed: {response.text}")

    return {"status": "queued", "ingestion_response": response.json()}


static_root = Path(__file__).parent / "static"
app.mount("/", StaticFiles(directory=static_root, html=True), name="static")
