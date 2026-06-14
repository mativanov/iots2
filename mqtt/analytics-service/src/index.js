const mqtt = require("mqtt");

function readNumberEnv(name, fallback, { minExclusive = null } = {}) {
  const value = process.env[name];

  if (value === undefined || value === "") {
    return fallback;
  }

  const parsed = Number(value);

  if (!Number.isFinite(parsed) || (minExclusive !== null && parsed <= minExclusive)) {
    throw new Error(`${name} must be a number greater than ${minExclusive}.`);
  }

  return parsed;
}

function readIntegerEnv(name, fallback, { min = 0 } = {}) {
  const value = process.env[name];

  if (value === undefined || value === "") {
    return fallback;
  }

  const parsed = Number.parseInt(value, 10);

  if (!Number.isInteger(parsed) || parsed < min) {
    throw new Error(`${name} must be an integer greater than or equal to ${min}.`);
  }

  return parsed;
}

function readQos() {
  const qos = readIntegerEnv("MQTT_QOS", 0, { min: 0 });

  if (![0, 1, 2].includes(qos)) {
    throw new Error("MQTT_QOS must be 0, 1, or 2.");
  }

  return qos;
}

function closeMqtt(client) {
  return new Promise((resolve) => {
    client.end(false, {}, resolve);
  });
}

function main() {
  const mqttUrl = process.env.MQTT_URL || "mqtt://mosquitto:1883";
  const topic = process.env.MQTT_TOPIC || "iot/readings";
  const qos = readQos();
  const alertThreshold = readNumberEnv("ALERT_THRESHOLD", 50);
  const windowSeconds = readIntegerEnv("WINDOW_SECONDS", 10, { min: 1 });
  const windowMs = windowSeconds * 1000;
  const client = mqtt.connect(mqttUrl, {
    clientId: "mqtt-analytics-service",  // Stable ID for persistent session
    clean: false,  // Persistent session - broker queues messages during disconnect
    reconnectPeriod: 1000,
    connectTimeout: 30000
  });
  let windowStart = Date.now();
  let messageCount = 0;
  let temperatureSum = 0;
  let skippedMessages = 0;
  let shuttingDown = false;

  function logWindow(windowEnd = Date.now()) {
    const averageTemperature = messageCount > 0 ? temperatureSum / messageCount : 0;
    const alert = messageCount > 0 && averageTemperature > alertThreshold;

    if (alert) {
      console.log("ALERT MQTT average temperature exceeded threshold");
    }

    console.log("MQTT analytics window", {
      windowStartTime: new Date(windowStart).toISOString(),
      windowEndTime: new Date(windowEnd).toISOString(),
      messageCount,
      averageTemperature: Number(averageTemperature.toFixed(2)),
      alert
    });

    windowStart = windowEnd;
    messageCount = 0;
    temperatureSum = 0;
  }

  const windowInterval = setInterval(() => {
    logWindow(windowStart + windowMs);
  }, windowMs);

  client.on("connect", () => {
    client.subscribe(topic, { qos }, (error) => {
      if (error) {
        console.error("MQTT analytics subscribe failed", error);
        return;
      }

      console.log("MQTT analytics service subscribed", {
        mqttUrl,
        topic,
        qos,
        alertThreshold,
        windowSeconds
      });
    });
  });

  client.on("message", (_topic, payload) => {
    if (shuttingDown) {
      return;
    }

    try {
      const event = JSON.parse(payload.toString("utf8"));
      const temperature = Number(event.temperature);

      if (!Number.isFinite(temperature)) {
        skippedMessages += 1;
        return;
      }

      messageCount += 1;
      temperatureSum += temperature;
    } catch (error) {
      skippedMessages += 1;
      console.error("MQTT analytics skipped malformed message", {
        error: error.message
      });
    }
  });

  client.on("error", (error) => {
    console.error("MQTT analytics client error", error);
  });

  async function shutdown(signal) {
    if (shuttingDown) {
      return;
    }

    shuttingDown = true;
    console.log("MQTT analytics service shutting down", {
      signal,
      skippedMessages
    });
    clearInterval(windowInterval);
    logWindow(Date.now());
    await closeMqtt(client);
  }

  process.on("SIGINT", () => {
    shutdown("SIGINT").then(() => process.exit(0));
  });

  process.on("SIGTERM", () => {
    shutdown("SIGTERM").then(() => process.exit(0));
  });
}

try {
  main();
} catch (error) {
  console.error("MQTT analytics service failed", error);
  process.exitCode = 1;
}
