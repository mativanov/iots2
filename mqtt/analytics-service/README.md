# MQTT Analytics Service

Subscribes to `iot/readings` and calculates average temperature in tumbling windows.

If the window average is greater than `ALERT_THRESHOLD`, the service logs:

```text
ALERT MQTT average temperature exceeded threshold
```

## Environment Variables

| Variable | Default |
| --- | --- |
| `MQTT_URL` | `mqtt://mosquitto:1883` |
| `MQTT_TOPIC` | `iot/readings` |
| `MQTT_QOS` | `0` |
| `ALERT_THRESHOLD` | `50` |
| `WINDOW_SECONDS` | `10` |

The service code defaults `MQTT_QOS` to `0` when run directly. Docker Compose sets the subscriber QoS to `2` by default for MQTT verification runs.

The service logs every window with start time, end time, message count, average temperature, and alert status.
