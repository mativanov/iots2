# Scenario A (Kafka) - Massive Sensor Ingestion.
#
# Spec: simulate 100 / 1000 / 10000 devices; record max throughput (msg/s) and
# percentage of lost messages, across acks = 0 / 1 / all.
#
# Method: push storable JSON via the native kafka-producer-perf-test.sh (the
# spec's mandated high-performance tool). Throughput + p95 latency come straight
# from the tool's report; loss = produced - stored(DB) while the storage service
# consumes into PostgreSQL. docker stats samples CPU/RAM throughout.
#
# Prereq: stack up with `docker compose up -d postgres kafka kafka-storage-service`.

param(
    [int[]]$DeviceScales      = @(100, 1000, 10000),
    [int]  $MessagesPerDevice = 100,
    [string[]]$AcksLevels     = @("0", "1", "all"),
    [int]  $DrainSeconds      = 20            # time to let storage flush before counting
)

. "$PSScriptRoot\..\lib\common.ps1"

$run     = Get-RunDir "kafka-scenarioA"
$summary = Join-Path $run "summary.csv"
$statsJob = Start-StatsSampler -OutCsv (Join-Path $run "stats.csv")

try {
    foreach ($scale in $DeviceScales) {
        $numRecords = $scale * $MessagesPerDevice
        $payload    = New-JsonPayloadFile -Path (Join-Path $run "payload-$scale.jsonl") -Count ([math]::Min($numRecords, 5000))
        Copy-FileToContainer -LocalPath $payload -Container (Get-Variable KafkaContainer -ValueOnly -Scope Script) -ContainerPath "/tmp/payload-$scale.jsonl"

        foreach ($acks in $AcksLevels) {
            Write-Host "`n=== Scenario A | devices=$scale acks=$acks records=$numRecords ==="
            Reset-Db -Broker kafka

            $before = Get-DbCount -Broker kafka
            $out = Invoke-KafkaPerfTest -NumRecords $numRecords -Acks $acks -Throughput -1 `
                       -PayloadFileInContainer "/tmp/payload-$scale.jsonl"

            # Parse the perf tool's summary line:
            #   "<n> records sent, <r> records/sec (<mb> MB/sec), <avg> ms avg latency, <max> ms max latency,
            #    <p50> ms 50th, <p95> ms 95th, <p99> ms 99th, <p999> ms 99.9th."
            $rate = if ($out -match '([\d.]+)\s+records/sec') { [double]$Matches[1] } else { $null }
            $p95  = if ($out -match '([\d.]+)\s+ms 95th')      { [double]$Matches[1] } else { $null }

            Write-Host "  draining $DrainSeconds s for storage to flush..."
            Start-Sleep -Seconds $DrainSeconds
            $stored = (Get-DbCount -Broker kafka) - $before
            $lossPct = if ($numRecords -gt 0) { [math]::Round(100.0 * ($numRecords - $stored) / $numRecords, 2) } else { 0 }

            (Get-KafkaConsumerLag) | Out-File (Join-Path $run "lag-$scale-acks$acks.txt") -Encoding utf8

            Save-ResultRow -Csv $summary -Row ([ordered]@{
                broker        = "kafka"
                scenario      = "A"
                devices       = $scale
                acks          = $acks
                records_sent  = $numRecords
                records_stored= $stored
                loss_pct      = $lossPct
                throughput_msgs = $rate
                p95_ms        = $p95
            })
            Write-Host "  -> throughput=$rate msg/s  p95=$p95 ms  stored=$stored  loss=$lossPct%"
        }
    }
}
finally {
    Stop-StatsSampler -Job $statsJob
    Write-Host "`nScenario A (Kafka) done. Results: $run"
}
