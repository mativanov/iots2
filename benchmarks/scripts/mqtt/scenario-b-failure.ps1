# Scenario B (MQTT) - Edge Connectivity Failure.
#
# Spec: cut the device simulator's network for 30s with `docker network
# disconnect`, then observe recovery. For MQTT the relevant mechanism is the
# broker's handling of sessions/subscriptions: a QoS 0 publisher loses whatever
# it tried to send during the outage (fire-and-forget), whereas QoS 1/2 with a
# persistent session can resume delivery. This run contrasts QoS 0 vs QoS 2 loss.
#
# Prereq: `docker compose up -d postgres mosquitto mqtt-storage-service mqtt-analytics-service`

param(
    [int[]]$QosLevels      = @(0, 2),
    [int]  $TotalMessages  = 2000,
    [int]  $MessagesPerSecond = 50,
    [int]  $OutageSeconds  = 30,
    [int]  $DrainSeconds   = 20
)

. "$PSScriptRoot\..\lib\common.ps1"

$run      = Get-RunDir "mqtt-scenarioB"
$summary  = Join-Path $run "summary.csv"
$net      = Get-ComposeNetwork
$ingestion= "mqtt-ingestion-service"
$statsJob = Start-StatsSampler -OutCsv (Join-Path $run "stats.csv")

try {
    foreach ($qos in $QosLevels) {
        Write-Host "`n=== Scenario B (MQTT) | qos=$qos outage=${OutageSeconds}s ==="
        Reset-Db -Broker mqtt
        $before = Get-DbCount -Broker mqtt

        Push-Location (Get-RepoRoot)
        try {
            & docker compose run -d --name $ingestion `
                -e MQTT_QOS=$qos -e TOTAL_MESSAGES=$TotalMessages -e MESSAGES_PER_SECOND=$MessagesPerSecond `
                mqtt-ingestion-service | Out-Null
        } finally { Pop-Location }

        Start-Sleep -Seconds 5
        Write-Host "  cutting network ($OutageSeconds s)..."
        & docker network disconnect $net $ingestion
        Start-Sleep -Seconds $OutageSeconds
        Write-Host "  reconnecting..."
        & docker network connect $net $ingestion

        Start-Sleep -Seconds $DrainSeconds
        $stored  = (Get-DbCount -Broker mqtt) - $before
        $lossPct = [math]::Round(100.0 * ($TotalMessages - $stored) / $TotalMessages, 2)

        Save-ResultRow -Csv $summary -Row ([ordered]@{
            broker         = "mqtt"
            scenario       = "B"
            qos            = $qos
            total_messages = $TotalMessages
            outage_seconds = $OutageSeconds
            stored         = $stored
            loss_pct       = $lossPct
        })
        Write-Host "  -> qos=$qos stored=$stored/$TotalMessages loss=$lossPct%"
        & docker rm -f $ingestion 2>&1 | Out-Null
    }
}
finally {
    Stop-StatsSampler -Job $statsJob
    & docker rm -f $ingestion 2>&1 | Out-Null
    Write-Host "`nScenario B (MQTT) done. Results: $run"
}
