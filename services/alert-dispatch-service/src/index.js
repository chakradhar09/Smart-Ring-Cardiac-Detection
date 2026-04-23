const axios = require("axios");
const express = require("express");
const { Kafka, logLevel } = require("kafkajs");

const app = express();
const port = Number(process.env.PORT || 8082);
const brokers = (process.env.KAFKA_BROKERS || "localhost:9092").split(",").map((value) => value.trim());
const topicIn = process.env.KAFKA_TOPIC_IN || "cardiac.anomaly";
const notificationGatewayUrl = process.env.NOTIFICATION_GATEWAY_URL || "http://localhost:8083";

app.get("/health", (_req, res) => {
  res.json({ status: "ok" });
});

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function channelsForSeverity(severity) {
  if (severity === "critical") {
    return ["ring-buzz", "push", "sms", "ems"];
  }
  if (severity === "high") {
    return ["ring-buzz", "push", "sms"];
  }
  if (severity === "medium") {
    return ["push"];
  }
  return [];
}

async function dispatchAlert(event) {
  const channels = channelsForSeverity(event.severity);
  if (channels.length === 0) {
    return;
  }

  if (channels.includes("ring-buzz")) {
    console.log(`[ring-command] user=${event.user_id} event=${event.event_id} action=buzz`);
  }

  const externalChannels = channels.filter((channel) => channel !== "ring-buzz");
  if (externalChannels.length === 0) {
    return;
  }

  const response = await axios.post(
    `${notificationGatewayUrl}/dispatch`,
    {
      event,
      channels: externalChannels,
    },
    {
      timeout: 5000,
    }
  );

  console.log(
    `[alert-dispatch] event=${event.event_id} severity=${event.severity} channels=${externalChannels.join(",")} status=${response.status}`
  );
}

async function runConsumer() {
  const kafka = new Kafka({
    clientId: "alert-dispatch-service",
    brokers,
    logLevel: logLevel.NOTHING,
  });

  const consumer = kafka.consumer({ groupId: "alert-dispatch-service-group" });

  while (true) {
    try {
      await consumer.connect();
      await consumer.subscribe({ topic: topicIn, fromBeginning: true });
      console.log(`[alert-dispatch] connected to topic ${topicIn}`);

      await consumer.run({
        eachMessage: async ({ message }) => {
          const raw = message.value ? message.value.toString() : "{}";
          let event;

          try {
            event = JSON.parse(raw);
          } catch (error) {
            console.error("failed to parse event", raw, error);
            return;
          }

          if (!event.event_id || !event.user_id) {
            console.warn("skipping malformed event", event);
            return;
          }

          let attempt = 0;
          while (attempt < 3) {
            try {
              await dispatchAlert(event);
              return;
            } catch (error) {
              attempt += 1;
              console.error(
                `[alert-dispatch] dispatch retry=${attempt} event=${event.event_id} error=${error.message}`
              );
              await sleep(500 * attempt);
            }
          }
        },
      });

      return;
    } catch (error) {
      console.error("consumer failed, reconnecting", error);
      await sleep(2000);
    }
  }
}

app.listen(port, () => {
  console.log(`alert-dispatch-service listening on :${port}`);
  runConsumer().catch((error) => {
    console.error("fatal consumer error", error);
    process.exit(1);
  });
});
