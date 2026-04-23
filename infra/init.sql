CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    age INT NOT NULL,
    email TEXT NOT NULL,
    emergency_contact TEXT NOT NULL,
    subscription_tier TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS devices (
    device_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(user_id),
    firmware_version TEXT NOT NULL,
    ble_mac_address TEXT NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS biosignal_readings (
    reading_id UUID NOT NULL,
    user_id TEXT NOT NULL REFERENCES users(user_id),
    device_id TEXT NOT NULL REFERENCES devices(device_id),
    ecg_value DOUBLE PRECISION NOT NULL,
    ppg_value DOUBLE PRECISION NOT NULL,
    heart_rate DOUBLE PRECISION NOT NULL,
    hrv DOUBLE PRECISION NOT NULL,
    spo2 DOUBLE PRECISION NOT NULL,
    arrhythmia_score DOUBLE PRECISION,
    hypertension_score DOUBLE PRECISION,
    ischemic_score DOUBLE PRECISION,
    confidence_score DOUBLE PRECISION,
    recorded_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (recorded_at, reading_id)
);

DO $$
BEGIN
    PERFORM create_hypertable(
        'biosignal_readings',
        'recorded_at',
        if_not_exists => TRUE,
        migrate_data => TRUE
    );
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'create_hypertable skipped: %', SQLERRM;
END $$;

CREATE INDEX IF NOT EXISTS idx_biosignal_user_recorded_at
ON biosignal_readings (user_id, recorded_at DESC);

CREATE TABLE IF NOT EXISTS cardiac_events (
    event_id UUID PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(user_id),
    device_id TEXT NOT NULL REFERENCES devices(device_id),
    event_type TEXT NOT NULL,
    confidence_score DOUBLE PRECISION NOT NULL,
    severity TEXT NOT NULL,
    raw_features JSONB NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS alerts (
    alert_id UUID PRIMARY KEY,
    event_id UUID NOT NULL REFERENCES cardiac_events(event_id),
    channel TEXT NOT NULL,
    recipient TEXT NOT NULL,
    status TEXT NOT NULL,
    provider_reference TEXT,
    dispatched_at TIMESTAMPTZ NOT NULL,
    acknowledged_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS audit_logs (
    audit_id UUID PRIMARY KEY,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    details JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS patient_profiles (
    user_id TEXT PRIMARY KEY REFERENCES users(user_id),
    profile_label TEXT NOT NULL,
    risk_level TEXT NOT NULL,
    primary_condition TEXT NOT NULL,
    anomaly_every INT NOT NULL,
    baseline_heart_rate DOUBLE PRECISION NOT NULL,
    baseline_hrv DOUBLE PRECISION NOT NULL,
    baseline_spo2 DOUBLE PRECISION NOT NULL,
    home_lat DOUBLE PRECISION NOT NULL,
    home_lon DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO users (user_id, name, age, email, emergency_contact, subscription_tier)
VALUES
    ('user-001', 'Priya Nair', 42, 'priya.nair@example.com', '+1555000101', 'premium'),
    ('user-002', 'Daniel Brooks', 57, 'daniel.brooks@example.com', '+1555000102', 'premium'),
    ('user-003', 'Meera Shah', 64, 'meera.shah@example.com', '+1555000103', 'enterprise'),
    ('user-004', 'Carlos Mendes', 48, 'carlos.mendes@example.com', '+1555000104', 'standard'),
    ('user-005', 'Aisha Khan', 51, 'aisha.khan@example.com', '+1555000105', 'premium'),
    ('user-006', 'Noah Reed', 59, 'noah.reed@example.com', '+1555000106', 'enterprise'),
    ('user-007', 'Emma Clarke', 34, 'emma.clarke@example.com', '+1555000107', 'standard'),
    ('user-008', 'Ravi Patel', 39, 'ravi.patel@example.com', '+1555000108', 'premium')
ON CONFLICT (user_id) DO NOTHING;

INSERT INTO devices (device_id, user_id, firmware_version, ble_mac_address)
VALUES
    ('device-001', 'user-001', '1.0.0-demo', '00:1A:7D:DA:71:13'),
    ('device-002', 'user-002', '1.0.0-demo', '00:1A:7D:DA:71:14'),
    ('device-003', 'user-003', '1.0.0-demo', '00:1A:7D:DA:71:15'),
    ('device-004', 'user-004', '1.0.0-demo', '00:1A:7D:DA:71:16'),
    ('device-005', 'user-005', '1.0.0-demo', '00:1A:7D:DA:71:17'),
    ('device-006', 'user-006', '1.0.0-demo', '00:1A:7D:DA:71:18'),
    ('device-007', 'user-007', '1.0.0-demo', '00:1A:7D:DA:71:19'),
    ('device-008', 'user-008', '1.0.0-demo', '00:1A:7D:DA:71:20')
ON CONFLICT (device_id) DO NOTHING;

INSERT INTO patient_profiles (
    user_id,
    profile_label,
    risk_level,
    primary_condition,
    anomaly_every,
    baseline_heart_rate,
    baseline_hrv,
    baseline_spo2,
    home_lat,
    home_lon
)
VALUES
    ('user-001', 'Athlete With Arrhythmia Risk', 'high', 'arrhythmia', 16, 74.0, 36.0, 97.0, 19.0760, 72.8777),
    ('user-002', 'Hypertension Monitoring Profile', 'medium', 'hypertension', 20, 82.0, 34.0, 97.5, 19.1197, 72.8468),
    ('user-003', 'Post-Ischemic Recovery Profile', 'high', 'ischemia', 18, 86.0, 28.0, 95.0, 19.2183, 72.9781),
    ('user-004', 'Intermittent Arrhythmia Profile', 'medium', 'arrhythmia', 24, 78.0, 40.0, 98.0, 19.0552, 72.8305),
    ('user-005', 'Stage-2 Hypertension Profile', 'high', 'hypertension', 14, 88.0, 30.0, 96.5, 19.1712, 72.9124),
    ('user-006', 'Coronary Ischemia Watch Profile', 'high', 'ischemia', 22, 84.0, 31.0, 95.5, 19.1014, 72.9011),
    ('user-007', 'Low-Risk Stable Wellness Profile', 'low', 'stable', 0, 69.0, 48.0, 98.4, 19.1462, 72.8751),
    ('user-008', 'Preventive Stable Monitoring Profile', 'low', 'stable', 0, 66.0, 51.0, 98.7, 19.0836, 72.8453)
ON CONFLICT (user_id) DO UPDATE SET
    profile_label = EXCLUDED.profile_label,
    risk_level = EXCLUDED.risk_level,
    primary_condition = EXCLUDED.primary_condition,
    anomaly_every = EXCLUDED.anomaly_every,
    baseline_heart_rate = EXCLUDED.baseline_heart_rate,
    baseline_hrv = EXCLUDED.baseline_hrv,
    baseline_spo2 = EXCLUDED.baseline_spo2,
    home_lat = EXCLUDED.home_lat,
    home_lon = EXCLUDED.home_lon;
