# Scenario C (MQTT) - Burst Event Load.
#
# Spec: jump 50 -> 5000 msg/s for a few seconds; observe backlog, backpressure
# and recovery time. MQTT has no durable log/backlog like Kafka: a burst beyond
# what subscribers drain is felt as broker queue growth and, at QoS 0, dropped
# messages. We use mqtt-ingestion-service (valid JSON payloads) and measure how
# long the storage subscriber takes to catch up (rows still arriving after burst).
#
# Prereq: `docker compose up -d postgres mosquitto mqtt-storage-service`

param(
    [int]$BaselineMessages = 100,
    [int]$BaselineRate = 50,
    [int]$BurstMessages = 5000,
    [int]$BurstRate = 5000,
    [int]$Qos = 1,
    [int]$MaxRecoverWait = 120
)

. "$PSScriptRoot\..\lib\common.ps1"

$run     = Get-RunDir "mqtt-scenarioC"
$summary = Join-Path $run "summary.csv"
$statsJob= Start-StatsSampler -OutCsv (Join-Path $run "stats.csv") -IntervalSec 1

try {
    Reset-Db -Broker mqtt
    $before = Get-DbCount -Broker mqtt

    Write-Host "`n=== Scenario C (MQTT) | baseline ${BaselineRate} msg/s, ${BaselineMessages} msgs ==="
    Push-Location (Get-RepoRoot)
    try {
        & docker compose run --rm `
            -e MQTT_QOS=$Qos -e TOTAL_MESSAGES=$BaselineMessages -e MESSAGES_PER_SECOND=$BaselineRate `
            mqtt-ingestion-service | Out-Null
    } finally { Pop-Location }

    Start-Sleep -Seconds 2  # Let baseline drain

    Write-Host "=== Scenario C (MQTT) | BURST ${BurstRate} msg/s, ${BurstMessages} msgs ==="
    $burstStart = Get-Date
    Push-Location (Get-RepoRoot)
    try {
        & docker compose run --rm `
            -e MQTT_QOS=$Qos -e TOTAL_MESSAGES=$BurstMessages -e MESSAGES_PER_SECOND=$BurstRate `
            mqtt-ingestion-service | Out-Null
    } finally { Pop-Location }
    $burstEnd = Get-Date

    $totalSent = $BaselineMessages + $BurstMessages
    Write-Host "  burst sent; measuring recovery (rows stabilize at $totalSent)..."
    $recovered = $false
    $prev = -1
    for ($i = 0; $i -lt $MaxRecoverWait; $i++) {
        Start-Sleep -Seconds 1
        $cur = (Get-DbCount -Broker mqtt) - $before
        Write-Host "    [$i s] stored: $cur / $totalSent"
        if ($cur -ge $totalSent) { $recovered = $true; break }
        if ($cur -eq $prev -and $cur -gt 0 -and $i -gt 5) { $recovered = $true; break }
        $prev = $cur
    }
    $recoverySec = if ($recovered) { [math]::Round(((Get-Date) - $burstEnd).TotalSeconds, 1) } else { ">$MaxRecoverWait" }
    $stored  = (Get-DbCount -Broker mqtt) - $before
    $lossPct = [math]::Round(100.0 * ($totalSent - $stored) / $totalSent, 2)

    Save-ResultRow -Csv $summary -Row ([ordered]@{
        broker        = "mqtt"
        scenario      = "C"
        qos           = $Qos
        baseline_msgs = $BaselineMessages
        burst_msgs    = $BurstMessages
        total_sent    = $totalSent
        stored        = $stored
        loss_pct      = $lossPct
        recovery_sec  = $recoverySec
    })
    Write-Host "  -> recovery=$recoverySec s  stored=$stored/$totalSent  loss=$lossPct%"
}
finally {
    Stop-StatsSampler -Job $statsJob
    Write-Host "`nScenario C (MQTT) done. Results: $run"
}
