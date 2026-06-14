# Scenario C (Kafka) - Burst Event Load.
#
# Spec: jump from 50 -> 5000 msg/s for a few seconds; observe backlog formation,
# backpressure, and recovery time (time for the system to return to normal).
#
# Method: a steady baseline phase at 50 msg/s, then a short high-throughput burst
# pushed by kafka-producer-perf-test.sh at ~5000 msg/s. We sample consumer lag
# every second to watch the backlog build and then drain; recovery time = seconds
# from end-of-burst until lag returns to ~0.
#
# Prereq: `docker compose up -d postgres kafka kafka-storage-service`

param(
    [int]$BaselineRate   = 50,
    [int]$BurstRate      = 5000,
    [int]$BurstSeconds   = 5,
    [int]$BaselineSeconds= 10,
    [int]$MaxRecoverWait = 120
)

. "$PSScriptRoot\..\lib\common.ps1"

$run      = Get-RunDir "kafka-scenarioC"
$lagLog   = Join-Path $run "lag-timeline.csv"
$statsJob = Start-StatsSampler -OutCsv (Join-Path $run "stats.csv") -IntervalSec 1
"elapsed_s,phase,lag_raw" | Out-File $lagLog -Encoding utf8

# Background lag sampler (parses total LAG across partitions from kafka-consumer-groups).
$lagJob = Start-Job -ScriptBlock {
    param($container, $bin, $bootstrap, $csv, $t0)
    while ($true) {
        $out = & docker exec $container "$bin/kafka-consumer-groups.sh" --bootstrap-server $bootstrap --describe --group kafka-storage 2>$null | Out-String
        $lag = 0
        foreach ($line in ($out -split "`n")) {
            $cols = ($line.Trim() -split '\s+')
            if ($cols.Count -ge 6 -and $cols[5] -match '^\d+$') { $lag += [int]$cols[5] }
        }
        $elapsed = [math]::Round(((Get-Date) - [datetime]$t0).TotalSeconds, 1)
        "$elapsed,sample,$lag" | Add-Content -Path $csv -Encoding utf8
        Start-Sleep -Seconds 1
    }
} -ArgumentList $script:KafkaContainer, $script:KafkaBin, $script:KafkaBootstrap, $lagLog, (Get-Date).ToString("o")

try {
    Reset-Db -Broker kafka
    $payload = New-JsonPayloadFile -Path (Join-Path $run "burst.jsonl") -Count 5000
    Copy-FileToContainer -LocalPath $payload -Container $script:KafkaContainer -ContainerPath "/tmp/burst.jsonl"

    Write-Host "`n=== Scenario C | baseline ${BaselineRate}/s for ${BaselineSeconds}s ==="
    Invoke-KafkaPerfTest -NumRecords ($BaselineRate * $BaselineSeconds) -Acks "1" -Throughput $BaselineRate `
        -PayloadFileInContainer "/tmp/burst.jsonl" | Out-Null

    Write-Host "=== Scenario C | BURST ${BurstRate}/s for ${BurstSeconds}s ==="
    $burstEnd = $null
    Invoke-KafkaPerfTest -NumRecords ($BurstRate * $BurstSeconds) -Acks "1" -Throughput $BurstRate `
        -PayloadFileInContainer "/tmp/burst.jsonl" | Out-Null
    $burstEnd = Get-Date

    Write-Host "  burst sent; measuring recovery (lag -> 0)..."
    $recovered = $false
    for ($i = 0; $i -lt $MaxRecoverWait; $i++) {
        Start-Sleep -Seconds 1
        $lagOut = Get-KafkaConsumerLag
        $totalLag = 0
        foreach ($line in ($lagOut -split "`n")) {
            $cols = ($line.Trim() -split '\s+')
            if ($cols.Count -ge 6 -and $cols[5] -match '^\d+$') { $totalLag += [int]$cols[5] }
        }
        if ($totalLag -le 0) { $recovered = $true; break }
    }
    $recoverySec = if ($recovered) { [math]::Round(((Get-Date) - $burstEnd).TotalSeconds, 1) } else { ">$MaxRecoverWait" }

    Save-ResultRow -Csv (Join-Path $run "summary.csv") -Row ([ordered]@{
        broker        = "kafka"
        scenario      = "C"
        baseline_rate = $BaselineRate
        burst_rate    = $BurstRate
        burst_seconds = $BurstSeconds
        recovery_sec  = $recoverySec
    })
    Write-Host "  -> recovery time (lag drained to 0): $recoverySec s"
}
finally {
    Stop-Job $lagJob -EA SilentlyContinue; Remove-Job $lagJob -Force -EA SilentlyContinue
    Stop-StatsSampler -Job $statsJob
    Write-Host "`nScenario C (Kafka) done. Results: $run  (lag timeline: $lagLog)"
}
