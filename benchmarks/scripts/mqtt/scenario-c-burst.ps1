# Scenario C (MQTT) - Burst Event Load.
#
# Spec: jump 50 -> 5000 msg/s for a few seconds; observe backlog, backpressure
# and recovery time. MQTT has no durable log/backlog like Kafka: a burst beyond
# what subscribers drain is felt as broker queue growth and, at QoS 0, dropped
# messages. We push the burst with emqtt-bench and measure how long the storage
# subscriber takes to catch up (rows still arriving after the burst ends).
#
# Prereq: `docker compose up -d postgres mosquitto mqtt-storage-service`

param(
    [int]$BaselineRate = 50,
    [int]$BurstClients = 50,      # 50 clients * 100 msgs = 5000-msg burst
    [int]$BurstPerClient = 100,
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

    Write-Host "`n=== Scenario C (MQTT) | baseline ${BaselineRate}/s ==="
    Invoke-EmqttBenchPub -Clients 5 -Qos $Qos -LimitPerClient ($BaselineRate * 2) -IntervalMs 100 | Out-Null

    Write-Host "=== Scenario C (MQTT) | BURST ~$($BurstClients*$BurstPerClient) msgs (5000/s target) ==="
    $burstSent = $BurstClients * $BurstPerClient
    Invoke-EmqttBenchPub -Clients $BurstClients -Qos $Qos -LimitPerClient $BurstPerClient -IntervalMs 1 | Out-Null
    $burstEnd = Get-Date

    Write-Host "  measuring recovery (rows stop increasing & match sent)..."
    $recovered = $false
    $prev = -1
    for ($i = 0; $i -lt $MaxRecoverWait; $i++) {
        Start-Sleep -Seconds 1
        $cur = (Get-DbCount -Broker mqtt) - $before
        if ($cur -ge $burstSent -or ($cur -eq $prev -and $cur -gt 0)) { $recovered = $true; break }
        $prev = $cur
    }
    $recoverySec = if ($recovered) { [math]::Round(((Get-Date) - $burstEnd).TotalSeconds, 1) } else { ">$MaxRecoverWait" }
    $stored  = (Get-DbCount -Broker mqtt) - $before
    $lossPct = [math]::Round(100.0 * ($burstSent - $stored) / $burstSent, 2)

    Save-ResultRow -Csv $summary -Row ([ordered]@{
        broker       = "mqtt"
        scenario     = "C"
        qos          = $Qos
        burst_msgs   = $burstSent
        stored       = $stored
        loss_pct     = $lossPct
        recovery_sec = $recoverySec
    })
    Write-Host "  -> recovery=$recoverySec s  stored=$stored/$burstSent  loss=$lossPct%"
}
finally {
    Stop-StatsSampler -Job $statsJob
    Write-Host "`nScenario C (MQTT) done. Results: $run"
}
