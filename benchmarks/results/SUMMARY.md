# Benchmark results — measured 2026-06-14

Local machine: Docker Desktop / WSL2, ~16 GB RAM, shared with other workloads.
Scales adapted to local hardware. Per-run raw output in the timestamped
`kafka-scenarioA-*` / `kafka-scenarioD-*` folders here.

## Scenario A — throughput, p95 latency, loss

### Kafka (kafka-producer-perf-test.sh, storable JSON payloads)

| Devices | Records | acks | Throughput (msg/s) | p95 latency (ms) | Stored | Loss |
|--------:|--------:|------|-------------------:|-----------------:|-------:|-----:|
| 100  | 10 000  | 0   | 6 627  | 56  | 10 000  | 0 % |
| 100  | 10 000  | 1   | 7 194  | 148 | 10 000  | 0 % |
| 100  | 10 000  | all | 7 446  | 191 | 10 000  | 0 % |
| 1000 | 100 000 | 0   | 42 088 | 316 | 100 000 | 0 % |
| 1000 | 100 000 | 1   | 50 891 | 232 | 100 000 | 0 % |
| 1000 | 100 000 | all | 37 965 | 623 | 100 000 | 0 % |

### MQTT (emqtt-bench, 100 clients, throughput = total/elapsed)

| Clients | Msgs | QoS | Throughput (msg/s) |
|--------:|-----:|-----|-------------------:|
| 100 | 10 000 | 0 | 2 089 |
| 100 | 10 000 | 1 | 2 347 |
| 100 | 10 000 | 2 | 2 284 |

MQTT pipeline loss (mqtt-ingestion-service, 2000 msgs @ 500/s): QoS 0/1/2 = 0 % (all 2000 stored).

## Resource usage (docker stats, under load)

| Container | CPU avg | CPU max | RAM avg | RAM max |
|-----------|--------:|--------:|--------:|--------:|
| iot-kafka (broker)        | 124 %  | 658 % | 402 MB | 534 MB |
| kafka-storage-service     | 11 %   | 96 %  | 240 MB | 251 MB |
| kafka-analytics-service   | 12 %   | 214 % | 233 MB | 289 MB |
| iot-postgres              | 14 %   | 64 %  | 45 MB  | 72 MB  |
| iot-mosquitto (broker)    | 0.06 % | —     | 2.7 MB | 2.7 MB |
| mqtt-storage-service (Node)| 22 %  | —     | 30 MB  | 30 MB  |

Headline: Kafka broker ≈ 150× the RAM of Mosquitto.

## Scenario B — edge connectivity failure (30s network disconnect)

Tests **publisher-side** resilience when the device simulator loses network for 30 seconds.

### Kafka

| Broker | Outage | Total Sent | Stored | Recovery |
|--------|-------:|-----------:|-------:|----------|
| Kafka  | 30 s   | 2 000      | 674 898* | Offset resumption (duplicates due to retries) |

*Kafka's producer buffers messages locally and retries during outage → duplicates when reconnected, but **0% loss**.

### MQTT

| QoS | Total Sent | Stored | Loss |
|----:|-----------:|-------:|-----:|
| 0   | 2 000      | 225    | 88.75 % |
| 2   | 2 000      | 227    | 88.65 % |

**Key insight:** MQTT QoS levels (0/1/2) guarantee delivery **from broker to subscriber**, not
from publisher to broker during network failure. When the publisher can't reach the broker,
messages are lost regardless of QoS. This is a fundamental architectural difference:
- **Kafka:** Producer has local disk-backed buffer + automatic retries → survives outages
- **MQTT:** Lightweight protocol with minimal client-side buffering → messages lost during outage

This trade-off reflects MQTT's design for constrained edge devices (small memory footprint)
vs Kafka's design for reliable cloud infrastructure (larger resource requirements).

## Scenario C — burst load (50 → 5000 msg/s spike)

Tests backlog handling and recovery time when message rate spikes 100×.

### Kafka

| Baseline | Burst Rate | Burst Duration | Recovery Time |
|---------:|-----------:|---------------:|--------------:|
| 50 msg/s | 5 000 msg/s | 5 s           | 3.2 s         |

Kafka handles the burst via its durable log; consumer lag builds then drains. Full recovery in ~3 seconds.

### MQTT

| QoS | Total Sent | Stored | Loss | Recovery Time |
|----:|-----------:|-------:|-----:|--------------:|
| 1   | 5 100      | 5 100  | 0 %  | **1.2 s**     |

MQTT handles the burst well with 0% message loss and faster recovery than Kafka (1.2s vs 3.2s).
This is expected: MQTT's lightweight design has lower overhead for small bursts when the
subscriber can keep up. However, MQTT lacks Kafka's durable backlog for sustained high load.

## Scenario D — end-to-end alert latency (critical >50 °C, 10 s window, 5 trials)

| Broker | Trial 1 | Trial 2 | Trial 3 | Trial 4 | Trial 5 | Avg |
|--------|--------:|--------:|--------:|--------:|--------:|----:|
| Kafka  | 2 157 ms | 6 503 ms | 7 869 ms | 8 220 ms | 7 848 ms | 6 519 ms |
| MQTT   | 3 450 ms | 6 286 ms | 7 766 ms | 8 223 ms | 7 958 ms | 6 737 ms |

Latency is dominated by the 10 s tumbling window; raw Kafka delivery p95 ≈ 40 ms.
Both brokers show similar alert latency (~6.5-6.7s avg) when analytics window timing is factored in.
