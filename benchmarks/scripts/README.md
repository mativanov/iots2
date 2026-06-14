# Benchmark Scripts

PowerShell orchestration for the four spec scenarios (A–D), run against both
brokers. The scripts are thin wrappers around the **spec-mandated native tools**:

| Broker | Load / metric tool | How it runs |
|--------|--------------------|-------------|
| Kafka  | `kafka-producer-perf-test.sh` | inside `iot-kafka` via `docker exec` (ships with Kafka) |
| Kafka  | `kafka-consumer-groups.sh --describe` | consumer lag / partition offsets |
| MQTT   | `emqtt-bench` | one-shot `emqx/emqtt-bench` container joined to the compose network |
| both   | `docker stats` | per-container CPU / RAM sampler (`lib/common.ps1`) |

> These scripts were authored before the stack was first run on this machine
> (Docker was being installed). The orchestration logic is complete; the exact
> native-tool flags and output-parsing regexes should be confirmed against the
> installed tool versions on the first live run — see "First run checklist".

## Layout

```
scripts/
  lib/common.ps1            shared helpers (compose, psql, stats sampler, payloads, tool wrappers)
  kafka/scenario-a..d.ps1   Kafka scenarios
  mqtt/scenario-a..d.ps1    MQTT scenarios
  run-all.ps1               brings infra up + runs everything
results/                    timestamped output per scenario (CSV summaries, stats, logs)
```

## Prerequisites

```powershell
docker compose build           # build all service images
# then either run everything:
./run-all.ps1
# or a subset:
./run-all.ps1 -Only kafka -Scenarios A,D
```

Individual scenarios assume the relevant services are already up (each script
header lists its `docker compose up -d ...` prereq).

## The four scenarios

- **A — Massive Sensor Ingestion.** 100 / 1000 / 10000 devices. Kafka sweeps
  `acks=0/1/all`; MQTT sweeps `QoS 0/1/2`. Records **max throughput (msg/s)** (from
  the native tool) and **% lost** (produced vs rows stored in PostgreSQL).
- **B — Edge Connectivity Failure.** `docker network disconnect` severs the
  ingestion container for 30 s, then reconnects. Kafka demonstrates **offset
  resumption** (≈0 loss); MQTT contrasts **QoS 0 (fire-and-forget loss)** vs
  **QoS 2 (session resume)**.
- **C — Burst Event Load.** Baseline 50 → burst 5000 msg/s. Measures backlog and
  **recovery time** — Kafka via consumer lag draining to 0, MQTT via stored rows
  catching up to the burst size.
- **D — Real-Time Alerting.** End-to-end latency from a critical (>50 °C) value
  being produced to the Analytics Service logging `ALERT` (includes up to one 10 s
  tumbling window). Repeated over several trials.

## Metric definitions

- **Throughput** — `records/sec` from `kafka-producer-perf-test.sh`; max observed
  `rate` from `emqtt-bench`.
- **p95 latency** — `95th` percentile reported by `kafka-producer-perf-test.sh`.
  (MQTT p95 is taken from `emqtt-bench` latency output / Scenario D distribution.)
- **% lost** — `(produced − stored_in_postgres) / produced × 100`.
- **CPU / RAM** — time series in each run's `stats.csv` (from `docker stats`).
- **Consumer lag** — `kafka-consumer-groups.sh --describe` (Kafka only).

## First run checklist (confirm once Docker is up)

1. `emqtt-bench` image name/flags — verify `emqx/emqtt-bench` pull works and that
   `pub -c -I -q -L -s` match the installed version; adjust `Invoke-EmqttBenchPub`.
2. `kafka-producer-perf-test.sh` summary format — confirm the `records/sec` and
   `95th` regexes in `scenario-a` parse the real output.
3. Compose network name — `Get-ComposeNetwork` auto-detects it; sanity-check
   against `docker network ls`.
4. For Scenario D MQTT, analytics must run with `ALERT_THRESHOLD=0` for a
   deterministic alert (see script header).
