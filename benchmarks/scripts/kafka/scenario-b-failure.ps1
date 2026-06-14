# Scenario B (Kafka) - Edge Connectivity Failure.
#
# Spec: use `docker network disconnect` to cut the device simulator's network for
# 30s, then observe the recovery mechanism. For Kafka the story is OFFSET
# RESUMPTION: the storage consumer keeps its committed offset, so after the
# producer reconnects no data is lost - the consumer simply continues from where
# it left off and catches up the backlog.
#
# This run drives the custom kafka-ingestion-service (valid JSON, steady rate) so
# the full pipeline is exercised, disconnects it mid-run, waits 30s, reconnects,
# and checks that produced == stored once the lag drains.
#
# Prereq: `docker compose up -d postgres kafka kafka-storage-service kafka-analytics-service`

param(
    [int]$TotalMessages    = 2000,
    [int]$MessagesPerSecond= 50,
    [int]$OutageSeconds    = 30,
    [int]$DrainSeconds     = 30
)

. "$PSScriptRoot\..\lib\common.ps1"

$run      = Get-RunDir "kafka-scenarioB"
$net      = Get-ComposeNetwork
$ingestion= "kafka-ingestion-service"
$statsJob = Start-StatsSampler -OutCsv (Join-Path $run "stats.csv")

try {
    Reset-Db -Broker kafka
    $before = Get-DbCount -Broker kafka

    Write-Host "`n=== Scenario B | starting ingestion ($TotalMessages msgs @ $MessagesPerSecond/s) ==="
    # Start ingestion detached so we can sever its network mid-flight.
    Push-Location (Get-RepoRoot)
    try {
        & docker compose run -d --name $ingestion `
            -e TOTAL_MESSAGES=$TotalMessages -e MESSAGES_PER_SECOND=$MessagesPerSecond `
            kafka-ingestion-service | Out-Null
    } finally { Pop-Location }

    Start-Sleep -Seconds 5
    Write-Host "  cutting network for $ingestion ($OutageSeconds s outage)..."
    $tDisc = Get-Date
    & docker network disconnect $net $ingestion
    Start-Sleep -Seconds $OutageSeconds

    Write-Host "  reconnecting $ingestion..."
    & docker network connect $net $ingestion
    $tRecon = Get-Date

    Write-Host "  draining $DrainSeconds s + capturing consumer lag..."
    Start-Sleep -Seconds $DrainSeconds
    (Get-KafkaConsumerLag) | Out-File (Join-Path $run "lag-after-recovery.txt") -Encoding utf8

    $stored = (Get-DbCount -Broker kafka) - $before
    $lossPct = [math]::Round(100.0 * ($TotalMessages - $stored) / $TotalMessages, 2)

    Save-ResultRow -Csv (Join-Path $run "summary.csv") -Row ([ordered]@{
        broker         = "kafka"
        scenario       = "B"
        total_messages = $TotalMessages
        outage_seconds = $OutageSeconds
        stored         = $stored
        loss_pct       = $lossPct
        recovery_note  = "offset-resumption"
    })
    Write-Host "  -> stored=$stored / $TotalMessages  loss=$lossPct%  (expect ~0% via offset resumption)"
}
finally {
    Stop-StatsSampler -Job $statsJob
    & docker rm -f $ingestion 2>$null | Out-Null
    Write-Host "`nScenario B (Kafka) done. Results: $run"
}
