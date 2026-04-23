const crypto = require("crypto");
const express = require("express");
const { Pool } = require("pg");

const app = express();
app.use(express.json());

const port = Number(process.env.PORT || 8083);
const databaseUrl = process.env.DATABASE_URL || "postgresql://ring:ringpass@localhost:5432/cardioring";
const pool = new Pool({ connectionString: databaseUrl });

function recipientForChannel(channel, event) {
  if (channel === "push") {
    return `mobile-app:${event.user_id}`;
  }
  if (channel === "sms") {
    return event.family_contact || "+1555000101";
  }
  if (channel === "ems") {
    return "ems://city-hospital";
  }
  return "unknown";
}

async function writeAudit(action, targetType, targetId, details) {
  await pool.query(
    `
      INSERT INTO audit_logs (
        audit_id,
        actor,
        action,
        target_type,
        target_id,
        details
      )
      VALUES ($1, $2, $3, $4, $5, $6::jsonb)
    `,
    [
      crypto.randomUUID(),
      "notification-gateway",
      action,
      targetType,
      String(targetId),
      JSON.stringify(details),
    ]
  );
}

app.get("/health", async (_req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ status: "ok" });
  } catch (error) {
    res.status(503).json({ status: "degraded", error: error.message });
  }
});

app.post("/dispatch", async (req, res) => {
  const event = req.body?.event;
  const channels = Array.isArray(req.body?.channels) ? req.body.channels : [];

  if (!event?.event_id || !event?.user_id) {
    return res.status(400).json({ status: "error", message: "event.event_id and event.user_id are required" });
  }
  if (channels.length === 0) {
    return res.status(400).json({ status: "error", message: "at least one channel is required" });
  }

  try {
    const dispatchedAt = new Date();
    const results = [];

    for (const channel of channels) {
      const success = Math.random() > 0.04;
      const status = channel === "ems" && success ? "ACKNOWLEDGED" : success ? "SENT" : "FAILED";
      const providerReference = `${channel}-${Date.now()}-${Math.floor(Math.random() * 10000)}`;
      const recipient = recipientForChannel(channel, event);
      const acknowledgedAt = channel === "ems" && success ? new Date() : null;

      await pool.query(
        `
          INSERT INTO alerts (
            alert_id,
            event_id,
            channel,
            recipient,
            status,
            provider_reference,
            dispatched_at,
            acknowledged_at
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        `,
        [
          crypto.randomUUID(),
          event.event_id,
          channel,
          recipient,
          status,
          providerReference,
          dispatchedAt,
          acknowledgedAt,
        ]
      );

      results.push({ channel, status, recipient, providerReference });
    }

    await writeAudit("alerts_dispatched", "cardiac_event", event.event_id, {
      user_id: event.user_id,
      severity: event.severity,
      channels,
    });

    return res.status(202).json({
      status: "accepted",
      event_id: event.event_id,
      dispatch_count: results.length,
      results,
    });
  } catch (error) {
    console.error("dispatch error", error);
    return res.status(500).json({ status: "error", message: "dispatch failed" });
  }
});

app.listen(port, () => {
  console.log(`notification-gateway listening on :${port}`);
});
