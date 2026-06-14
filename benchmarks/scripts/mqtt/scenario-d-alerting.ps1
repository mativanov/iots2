# Scenario D (MQTT) - Real-Time Alerting end-to-end latency.
#
# Spec: measure end-to-end latency from a critical value being generated to the
# Analytics Service logging the ALERT. We drive the mqtt-ingestion-service with a
# dataset slice forced above the alert threshold, mark t0, then watch the
# mqtt-analytics-service logs for the ALERT line. Latency includes up to one 10s
# tumbling window (the realistic figure the spec asks for). Repeated $Trials times.
#
# NOTE: the ingestion service replays CSV temperatures; to guarantee an alert we
# raise the threshold trick by lowering ALERT_THRESHOLD on the analytics service,
# OR ensure the dataset slice exceeds 50C. Here we lower the threshold to 0 for a
# deterministic alert and measure pure pipeline latency. Re-run with the real
# threshold for a functional (non-deterministic) check.
#
# Prereq: analytics started with ALERT_THRESHOLD=0 ->
#   docker compose up -d postgres mosquitto
#   docker compose run -d --name mqtt-analytics-service -e ALERT_THRESHOLD=0 mqtt-analytics-service

param(
    [int]$Trials = 5,
    [int]$MsgsPerTrial = 100,
    [int]$Qos = 1,
    [int]$TimeoutSec = 30
)

. "$PSScriptRoot\..\lib\common.ps1"

$run      = Get-RunDir "mqtt-scenarioD"
$summary  = Join-Path $run "summary.csv"
$analytics= "mqtt-analytics-service"

try {
    for ($t = 1; $t -le $Trials; $t++) {
        Write-Host "`n=== Scenario D (MQTT) | trial $t/$Trials ==="
        $sinceMark = (Get-Date).ToUniversalTime()
        $t0 = Get-Date

        Push-Location (Get-RepoRoot)
        try {
            & docker compose run --rm `
                -e MQTT_QOS=$Qos -e TOTAL_MESSAGES=$MsgsPerTrial -e MESSAGES_PER_SECOND=200 `
                mqtt-ingestion-service | Out-Null
        } finally { Pop-Location }

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
            broker   = "mqtt"
            scenario = "D"
            trial    = $t
            qos      = $Qos
            alert_latency_ms = if ($null -ne $latency) { $latency } else { "TIMEOUT" }
        })
        Write-Host "  -> alert end-to-end latency: $(if($null -ne $latency){"$latency ms"}else{'TIMEOUT'})"
        Start-Sleep -Seconds 12
    }
}
finally {
    Write-Host "`nScenario D (MQTT) done. Results: $run"
}
