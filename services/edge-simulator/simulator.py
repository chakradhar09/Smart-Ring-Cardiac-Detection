import os
import json
import random
import time
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse
from typing import Dict, List

import requests


def env(name: str, fallback: str) -> str:
    value = os.getenv(name)
    return value if value else fallback


def healthcheck_url(ingestion_url: str) -> str:
    parsed = urlparse(ingestion_url)
    path = parsed.path or ""
    if path.endswith("/ingest"):
        path = path[:-7] + "/health"
    elif path.endswith("/"):
        path = path + "health"
    elif path:
        path = path + "/health"
    else:
        path = "/health"

    return urlunparse(parsed._replace(path=path, params="", query="", fragment=""))


def wait_for_ingestion_ready(ingestion_url: str, timeout_seconds: int = 90) -> None:
    deadline = time.time() + timeout_seconds
    check_url = healthcheck_url(ingestion_url)

    while time.time() < deadline:
        try:
            response = requests.get(check_url, timeout=3)
            if response.ok:
                print(f"[simulator] ingestion readiness confirmed at {check_url}")
                return
        except requests.RequestException:
            pass
        time.sleep(1.0)

    print(f"[simulator] ingestion not ready after {timeout_seconds}s; continuing with best effort")


def post_with_retry(ingestion_url: str, payload: Dict, attempts: int = 3) -> requests.Response:
    last_error: requests.RequestException | None = None

    for attempt in range(1, attempts + 1):
        try:
            response = requests.post(ingestion_url, json=payload, timeout=5)
            if response.ok or attempt == attempts:
                return response
        except requests.RequestException as error:
            last_error = error
            if attempt == attempts:
                raise

        time.sleep(0.35 * attempt)

    if last_error is not None:
        raise last_error

    # Should be unreachable, but keeps return type explicit.
    return requests.post(ingestion_url, json=payload, timeout=5)


def load_profiles(file_path: str) -> List[Dict]:
    with open(file_path, "r", encoding="utf-8") as handle:
        profiles = json.load(handle)

    if not isinstance(profiles, list) or not profiles:
        raise ValueError("profiles file must contain a non-empty list")

    required = {"user_id", "device_id", "name", "risk_level", "primary_condition", "anomaly_every"}
    for profile in profiles:
        missing = [key for key in required if key not in profile]
        if missing:
            raise ValueError(f"profile missing required keys: {missing}")
        if "home" not in profile:
            profile["home"] = {"lat": 19.0760, "lon": 72.8777}

    return profiles


def jitter_location(home: Dict[str, float]) -> Dict[str, float]:
    return {
        "lat": float(home.get("lat", 19.0760)) + random.uniform(-0.0009, 0.0009),
        "lon": float(home.get("lon", 72.8777)) + random.uniform(-0.0009, 0.0009),
    }


def baseline_metrics(risk_level: str) -> Dict[str, float]:
    if risk_level == "high":
        return {
            "heart_rate": random.uniform(74.0, 102.0),
            "hrv": random.uniform(24.0, 42.0),
            "spo2": random.uniform(94.0, 98.5),
            "ecg_value": random.uniform(0.9, 1.35),
            "ppg_value": random.uniform(0.8, 1.35),
        }

    if risk_level == "low":
        return {
            "heart_rate": random.uniform(60.0, 86.0),
            "hrv": random.uniform(38.0, 58.0),
            "spo2": random.uniform(96.0, 99.0),
            "ecg_value": random.uniform(0.7, 1.15),
            "ppg_value": random.uniform(0.65, 1.15),
        }

    return {
        "heart_rate": random.uniform(68.0, 94.0),
        "hrv": random.uniform(30.0, 50.0),
        "spo2": random.uniform(95.0, 99.0),
        "ecg_value": random.uniform(0.75, 1.2),
        "ppg_value": random.uniform(0.7, 1.2),
    }


def condition_event_metrics(condition: str) -> Dict[str, float]:
    if condition == "hypertension":
        return {
            "heart_rate": random.uniform(112.0, 136.0),
            "hrv": random.uniform(26.0, 38.0),
            "spo2": random.uniform(95.0, 99.0),
            "ecg_value": random.uniform(1.15, 1.55),
            "ppg_value": random.uniform(1.9, 2.3),
        }

    if condition == "ischemia":
        return {
            "heart_rate": random.uniform(118.0, 148.0),
            "hrv": random.uniform(14.0, 30.0),
            "spo2": random.uniform(82.0, 88.0),
            "ecg_value": random.uniform(1.85, 2.45),
            "ppg_value": random.uniform(1.05, 1.55),
        }

    return {
        "heart_rate": random.uniform(155.0, 186.0),
        "hrv": random.uniform(8.0, 22.0),
        "spo2": random.uniform(84.0, 91.0),
        "ecg_value": random.uniform(1.7, 2.35),
        "ppg_value": random.uniform(1.25, 1.9),
    }


def build_reading(profile: Dict, sequence: int) -> Dict:
    condition = str(profile.get("primary_condition", "arrhythmia"))
    anomaly_every = int(profile.get("anomaly_every", 25))
    is_event = anomaly_every > 0 and sequence > 0 and sequence % anomaly_every == 0

    if is_event:
        metrics = condition_event_metrics(condition)
        stream_label = f"critical-{condition}"
        simulated_event = condition
    else:
        metrics = baseline_metrics(str(profile.get("risk_level", "medium")))
        stream_label = "normal-pattern"
        simulated_event = "none"

    return {
        "user_id": profile["user_id"],
        "device_id": profile["device_id"],
        "ecg_value": round(metrics["ecg_value"], 4),
        "ppg_value": round(metrics["ppg_value"], 4),
        "heart_rate": round(metrics["heart_rate"], 2),
        "hrv": round(metrics["hrv"], 2),
        "spo2": round(metrics["spo2"], 2),
        "location": jitter_location(profile.get("home", {})),
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "metadata": {
            "stream_label": stream_label,
            "source": "population-edge-simulator",
            "sequence": sequence,
            "profile_name": profile.get("name", profile["user_id"]),
            "risk_level": profile.get("risk_level", "unknown"),
            "primary_condition": condition,
            "simulated_event": simulated_event,
        },
    }


def main() -> None:
    ingestion_url = env("INGESTION_URL", "http://localhost:8080/ingest")
    profiles_file = env("PROFILES_FILE", "/app/patient_profiles.json")
    interval_ms = int(env("STREAM_INTERVAL_MS", "1200"))
    per_patient_stagger_ms = int(env("PER_PATIENT_STAGGER_MS", "120"))
    profiles = load_profiles(profiles_file)
    counters = {profile["user_id"]: 0 for profile in profiles}

    print(f"[simulator] streaming {len(profiles)} profiles to {ingestion_url}")
    wait_for_ingestion_ready(ingestion_url)

    while True:
        for profile in profiles:
            sequence = counters[profile["user_id"]]
            payload = build_reading(profile, sequence)
            mode = payload["metadata"]["stream_label"]
            event = payload["metadata"]["simulated_event"]

            try:
                response = post_with_retry(ingestion_url, payload)
                if response.ok:
                    print(
                        f"[simulator] user={profile['user_id']} seq={sequence} mode={mode} event={event} hr={payload['heart_rate']} spo2={payload['spo2']} -> {response.status_code}"
                    )
                else:
                    print(
                        f"[simulator] user={profile['user_id']} seq={sequence} mode={mode} failed status={response.status_code} body={response.text}"
                    )
            except requests.RequestException as error:
                print(f"[simulator] user={profile['user_id']} seq={sequence} request failed: {error}")

            counters[profile["user_id"]] = sequence + 1
            if per_patient_stagger_ms > 0:
                time.sleep(per_patient_stagger_ms / 1000.0)

        time.sleep(interval_ms / 1000.0)


if __name__ == "__main__":
    main()
