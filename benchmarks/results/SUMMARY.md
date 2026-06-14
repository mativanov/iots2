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

## Scenario D — end-to-end alert latency (critical >50 °C, 10 s window, 3 trials)

| Broker | Trial 1 | Trial 2 | Trial 3 | Avg |
|--------|--------:|--------:|--------:|----:|
| Kafka  | 6 936 ms | 8 016 ms | 7 927 ms | 7 626 ms |
| MQTT   | 8 282 ms | 8 901 ms | 9 357 ms | 8 847 ms |

Latency is dominated by the 10 s tumbling window; raw Kafka delivery p95 ≈ 40 ms.

## Not executed this pass
Scenarios B (edge connectivity failure) and C (burst load) — scripts provided
(`scenario-b-failure.ps1`, `scenario-c-burst.ps1` for both brokers), not run due
to time/hardware. Expected behavior documented in REPORT.md §5.3–5.4.
