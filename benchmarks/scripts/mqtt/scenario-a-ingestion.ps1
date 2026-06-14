# Scenario A (MQTT) - Massive Sensor Ingestion.
#
# Spec: simulate 100 / 1000 / 10000 devices; record max throughput (msg/s) and
# percentage of lost messages, across QoS = 0 / 1 / 2.
#
# Method (two complementary measurements, both spec-mandated tooling):
#   1) Throughput + latency: the official emqtt-bench publisher with -c = number
#      of devices and -q = QoS. emqtt-bench prints the achieved publish rate.
#   2) End-to-end loss: run the mqtt-ingestion-service (valid JSON, storable) at
#      each QoS for a fixed message count and compare to rows landed in Postgres.
#      (emqtt-bench's synthetic payloads aren't valid readings, so loss through
#      the storage pipeline is measured with the real producer.)
#
# Prereq: `docker compose up -d postgres mosquitto mqtt-storage-service`

param(
    [int[]]$DeviceScales = @(100, 1000, 10000),
    [int]  $MsgsPerClient= 100,
    [int[]]$QosLevels    = @(0, 1, 2),
    [int]  $LossMsgCount = 2000,
    [int]  $DrainSeconds = 15
)

. "$PSScriptRoot\..\lib\common.ps1"

$run     = Get-RunDir "mqtt-scenarioA"
$summary = Join-Path $run "summary.csv"
$statsJob= Start-StatsSampler -OutCsv (Join-Path $run "stats.csv")

try {
    foreach ($qos in $QosLevels) {
        foreach ($scale in $DeviceScales) {
            Write-Host "`n=== Scenario A (MQTT) | devices=$scale qos=$qos ==="

            # (1) throughput / latency via emqtt-bench
            $out = Invoke-EmqttBenchPub -Clients $scale -Qos $qos -LimitPerClient $MsgsPerClient -IntervalMs 10
            $out | Out-File (Join-Path $run "emqtt-bench-q$qos-c$scale.txt") -Encoding utf8
            # emqtt-bench prints periodic "pub total=... rate=<n>/sec" lines; take the max rate seen.
            $rates = [regex]::Matches($out, 'rate[:=]?\s*([\d.]+)') | ForEach-Object { [double]$_.Groups[1].Value }
            $throughput = if ($rates.Count) { ($rates | Measure-Object -Maximum).Maximum } else { $null }

            # (2) end-to-end loss via the real producer pipeline
            Reset-Db -Broker mqtt
            $before = Get-DbCount -Broker mqtt
            Push-Location (Get-RepoRoot)
            try {
                & docker compose run --rm `
                    -e MQTT_QOS=$qos -e TOTAL_MESSAGES=$LossMsgCount -e MESSAGES_PER_SECOND=200 `
                    -e DEVICE_COUNT=$scale `
                    mqtt-ingestion-service | Out-Null
            } finally { Pop-Location }
            Start-Sleep -Seconds $DrainSeconds
            $stored  = (Get-DbCount -Broker mqtt) - $before
            $lossPct = [math]::Round(100.0 * ($LossMsgCount - $stored) / $LossMsgCount, 2)

            Save-ResultRow -Csv $summary -Row ([ordered]@{
                broker          = "mqtt"
                scenario        = "A"
                devices         = $scale
                qos             = $qos
                throughput_msgs = $throughput
                loss_sample_n   = $LossMsgCount
                stored          = $stored
                loss_pct        = $lossPct
            })
            Write-Host "  -> throughput=$throughput msg/s  stored=$stored/$LossMsgCount  loss=$lossPct%"
        }
    }
}
finally {
    Stop-StatsSampler -Job $statsJob
    Write-Host "`nScenario A (MQTT) done. Results: $run"
}
