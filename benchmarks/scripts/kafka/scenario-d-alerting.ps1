# Scenario D (Kafka) - Real-Time Alerting end-to-end latency.
#
# Spec: measure the end-to-end latency from the moment the simulator generates a
# critical value to the moment the Analytics Service logs the ALERT.
#
# Method: mark t0, publish a batch of critical (>50C) readings via the native
# producer, then watch the kafka-analytics-service logs until the ALERT line
# appears. Latency = alert-detected-time - t0. Because analytics uses a 10s
# tumbling window, this latency includes up to one window of aggregation delay,
# which is the realistic end-to-end figure the spec asks for. Repeated $Trials
# times for a distribution.
#
# Prereq: `docker compose up -d postgres kafka kafka-analytics-service`

param(
    [int]$Trials      = 5,
    [int]$CriticalMsgs= 50,
    [int]$TimeoutSec  = 30
)

. "$PSScriptRoot\..\lib\common.ps1"

$run     = Get-RunDir "kafka-scenarioD"
$summary = Join-Path $run "summary.csv"
$analytics = "kafka-analytics-service"

try {
    $payload = New-JsonPayloadFile -Path (Join-Path $run "critical.jsonl") -Count $CriticalMsgs -Critical
    Copy-FileToContainer -LocalPath $payload -Container $script:KafkaContainer -ContainerPath "/tmp/critical.jsonl"

    for ($t = 1; $t -le $Trials; $t++) {
        Write-Host "`n=== Scenario D | trial $t/$Trials ==="
        # Baseline marker in the log so we only match ALERTs produced after t0.
        $sinceMark = (Get-Date).ToUniversalTime()
        $t0 = Get-Date

        Invoke-KafkaPerfTest -NumRecords $CriticalMsgs -Acks "1" -Throughput -1 `
            -PayloadFileInContainer "/tmp/critical.jsonl" | Out-Null

        $latency = $null
        $deadline = (Get-Date).AddSeconds($TimeoutSec)
        while ((Get-Date) -lt $deadline) {
            $logs = & docker logs $analytics --since $sinceMark.ToString("yyyy-MM-ddTHH:mm:ssZ") 2>&1 | Out-String
            if ($logs -match "ALERT") {
                $latency = [math]::Round(((Get-Date) - $t0).TotalMilliseconds, 0)
                break
            }
            Start-Sleep -Milliseconds 250
        }

        Save-ResultRow -Csv $summary -Row ([ordered]@{
            broker      = "kafka"
            scenario    = "D"
            trial       = $t
            critical_msgs = $CriticalMsgs
            alert_latency_ms = if ($null -ne $latency) { $latency } else { "TIMEOUT" }
        })
        Write-Host "  -> alert end-to-end latency: $(if($null -ne $latency){"$latency ms"}else{'TIMEOUT'})"
        Start-Sleep -Seconds 12   # let the window roll over before the next trial
    }
}
finally {
    Write-Host "`nScenario D (Kafka) done. Results: $run"
}
