# CardioRing Architecture Demo Project

This repository is a runnable demo that replicates the architecture in the design document for:

- continuous smart-ring biosignal streaming
- event-driven AI classification and anomaly detection
- multi-channel emergency alert dispatch
- mobile and doctor-facing dashboards

## Documentation

- Implementation-focused deep dive: [Docs/project-technical-deep-dive.md](Docs/project-technical-deep-dive.md)
- Architecture reference: [Docs/architecture-smart-ring-cardiac-detection.md](Docs/architecture-smart-ring-cardiac-detection.md)
- Demo flow walkthrough: [Docs/demo-walkthrough.md](Docs/demo-walkthrough.md)

## What this demo includes

### Event-driven microservices

- Edge simulator generates concurrent multi-patient biosignal streams with profile-based condition events
- Ingestion service accepts device frames and publishes to Kafka
- AI analysis engine scores arrhythmia, hypertension, and ischemic risk
- Anomaly engine compares against rolling baseline from Redis
- Alert dispatch service triggers ring buzz command + notification workflow
- Notification gateway simulates push, SMS, and EMS dispatch
- API backend serves dashboard APIs and static demo UIs

### Multi-patient demo profiles

- 8 seeded demo patients with unique demographics, risk levels, and primary conditions (including stable low-risk profiles)
- Condition-specific simulation injection for arrhythmia, hypertension, and ischemia
- Per-profile event cadence control (`anomaly_every`) to stagger cardiac event generation
- Population profile visibility endpoint: `/api/demo/patient-profiles`
- Profile definitions are loaded from `services/edge-simulator/patient_profiles.json`
- Simulation cadence can be tuned with `SIM_STREAM_INTERVAL_MS` and `SIM_PER_PATIENT_STAGGER_MS`

### Doctor portal live monitor

- At-a-glance overview of all monitored patients with clinical status (`critical`, `high`, `medium`, `stable`)
- Search and status filtering for rapid triage from one table
- Click-through detailed patient dashboard with latest vitals, 24h event mix, event timeline, and alert timeline
- Click any item in Recent Cardiac Events to open a dialog with trigger-time snapshot details (HR, SpO2, HRV, ECG, PPG, scores, baseline, and metadata)
- Population overview API: `/api/doctor/overview`
- Detailed patient drill-down API: `/api/doctor/patients/{user_id}/detail`

### Data layer

- TimescaleDB (on PostgreSQL) stores high-frequency biosignal readings
- PostgreSQL tables store users, devices, cardiac events, alerts, and audit logs
- Redis stores user baseline metrics for fast anomaly comparisons

## Architecture mapping

| Architecture Component | Demo Implementation |
|---|---|
| Smart Ring Firmware + Edge Gateway | `services/edge-simulator` |
| Biosignal Ingestion Service (Go) | `services/ingestion-service` |
| Kafka Stream Bus | Redpanda in `docker-compose.yml` |
| AI Analysis Engine (Python) | `services/ai-analysis-engine` |
| Anomaly Detection Engine (Python + Redis baseline) | `services/anomaly-engine` |
| Alert Dispatch Service (Node.js) | `services/alert-dispatch-service` |
| Notification Gateway (Node.js) | `services/notification-gateway` |
| Patient Data Store (TimescaleDB + PostgreSQL) | `infra/init.sql` + Timescale container |
| Mobile App demo view | `services/api-backend/app/static/mobile.html` |
| Doctor Portal demo view | `services/api-backend/app/static/doctor.html` |

## Quick start

### Prerequisites

- Docker Desktop with Compose

### Run everything

```powershell
docker compose up --build
```

### Open the demo

- Control deck: http://localhost:18090
- Mobile view: http://localhost:18090/mobile.html
- Doctor portal: http://localhost:18090/doctor.html
- API health: http://localhost:18090/health
- Profile feed: http://localhost:18090/api/demo/patient-profiles
- Doctor overview API: http://localhost:18090/api/doctor/overview

If port `18090` is also busy, set `API_HOST_PORT` in a `.env` file before running Compose.

## Suggested live demo flow

1. Start the stack and open the control deck.
2. Call `/api/demo/patient-profiles` to show concurrent simulated patient identities and risks.
3. Open mobile and doctor pages in separate tabs.
4. Let simulator stream for ~30-45 seconds and observe mixed event types over multiple users.
5. Click Trigger Critical Event on the control deck for a manual emergency override.
6. Show new cardiac events in doctor view and alert records in dispatch log.
7. Call out ring buzz log from `alert-dispatch-service` container logs.

## Useful commands

```powershell
# Follow logs for core event path
docker compose logs -f ingestion-service ai-analysis-engine anomaly-engine alert-dispatch-service notification-gateway

# Reset and rerun clean demo
docker compose down -v
docker compose up --build
```

## Notes for demo realism

- AI models are deterministic heuristic scorers that emulate condition-specific model outputs.
- Notification channels are mocked and persisted in DB for auditable demo evidence.
- The design preserves asynchronous, decoupled message flow for resilience and scalability.
