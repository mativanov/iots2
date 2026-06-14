# common.ps1 - shared helpers for the IoT MQTT-vs-Kafka benchmark scripts.
#
# Dot-source this from a scenario script:
#   . "$PSScriptRoot\..\lib\common.ps1"
#
# Everything here is thin orchestration around the spec-mandated native tools
# (kafka-producer-perf-test.sh, emqtt-bench) and docker. Nothing Docker-specific
# is hardcoded that we can discover at runtime (e.g. the compose network name).

$ErrorActionPreference = "Stop"

# --- Paths -----------------------------------------------------------------

# repo root = three levels up from benchmarks/scripts/lib
$script:RepoRoot   = (Resolve-Path "$PSScriptRoot\..\..\..").Path
$script:ResultsDir = Join-Path $RepoRoot "benchmarks\results"

# Container / topic constants (match docker-compose.yml + application.yml)
$script:KafkaContainer    = "iot-kafka"
$script:PostgresContainer = "iot-postgres"
$script:MosquittoContainer= "iot-mosquitto"
$script:KafkaBin          = "/opt/kafka/bin"
$script:KafkaTopic        = "iot.readings"
$script:MqttTopic         = "iot/readings"
$script:KafkaBootstrap    = "localhost:9092"   # advertised listener, valid *inside* iot-kafka
$script:EmqttBenchImage   = "emqx/emqtt-bench:latest"

function Get-RepoRoot { return $script:RepoRoot }

function Get-RunDir {
    param([Parameter(Mandatory)][string]$Scenario)
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $dir = Join-Path $ResultsDir "$Scenario-$stamp"
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
    return $dir
}

# --- Compose / docker wrappers --------------------------------------------

function Invoke-Compose {
    # Runs `docker compose` from the repo root so the default project name
    # (and thus network/volume names) is stable regardless of caller CWD.
    Push-Location $script:RepoRoot
    try { & docker compose @args; if ($LASTEXITCODE -ne 0) { throw "docker compose failed: $($args -join ' ')" } }
    finally { Pop-Location }
}

function Get-ComposeNetwork {
    # Discover the network the running containers actually sit on, rather than
    # assuming "<project>_default". Used by emqtt-bench (joins the same net) and
    # by Scenario B (network disconnect/connect).
    $net = & docker inspect $script:KafkaContainer --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}' 2>$null
    if (-not $net) {
        $net = & docker inspect $script:PostgresContainer --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}' 2>$null
    }
    if (-not $net) { throw "Could not determine the compose network - are the containers up?" }
    return ($net -split "`n")[0].Trim()
}

# --- PostgreSQL helpers ----------------------------------------------------

function Invoke-Psql {
    param([Parameter(Mandatory)][string]$Sql)
    $out = & docker exec $script:PostgresContainer psql -U iotuser -d iotdb -t -A -c $Sql 2>&1
    if ($LASTEXITCODE -ne 0) { throw "psql failed: $out" }
    return ($out | Out-String).Trim()
}

function Get-DbCount {
    param([ValidateSet("kafka","mqtt","all")][string]$Broker = "all")
    $where = if ($Broker -eq "all") { "" } else { "WHERE broker_type = '$Broker'" }
    return [int](Invoke-Psql "SELECT COUNT(*) FROM sensor_readings $where")
}

function Reset-Db {
    param([ValidateSet("kafka","mqtt","all")][string]$Broker = "all")
    if ($Broker -eq "all") {
        Invoke-Psql "TRUNCATE sensor_readings" | Out-Null
    } else {
        Invoke-Psql "DELETE FROM sensor_readings WHERE broker_type = '$Broker'" | Out-Null
    }
    Write-Host "  [db] cleared rows (broker=$Broker)"
}

# --- docker stats sampler (CPU / RAM per container) ------------------------

function Start-StatsSampler {
    # Samples `docker stats` every $IntervalSec into a CSV until Stop-StatsSampler.
    param(
        [Parameter(Mandatory)][string]$OutCsv,
        [int]$IntervalSec = 2
    )
    $job = Start-Job -ScriptBlock {
        param($csv, $interval)
        "timestamp,container,cpu_perc,mem_usage,mem_perc,net_io" | Out-File -FilePath $csv -Encoding utf8
        while ($true) {
            $ts = (Get-Date).ToString("o")
            $lines = & docker stats --no-stream --format "{{.Name}};{{.CPUPerc}};{{.MemUsage}};{{.MemPerc}};{{.NetIO}}" 2>$null
            foreach ($l in $lines) {
                if ($l) {
                    $p = $l -split ";"
                    "$ts,$($p[0]),$($p[1]),`"$($p[2])`",$($p[3]),`"$($p[4])`"" | Add-Content -Path $csv -Encoding utf8
                }
            }
            Start-Sleep -Seconds $interval
        }
    } -ArgumentList $OutCsv, $IntervalSec
    Write-Host "  [stats] sampler started -> $OutCsv (job $($job.Id))"
    return $job
}

function Stop-StatsSampler {
    param([Parameter(Mandatory)]$Job)
    if ($Job) {
        Stop-Job $Job -ErrorAction SilentlyContinue
        Remove-Job $Job -Force -ErrorAction SilentlyContinue
        Write-Host "  [stats] sampler stopped"
    }
}

# --- Payload generation ----------------------------------------------------

function New-JsonPayloadFile {
    # Produces a newline-delimited file of valid SensorReading JSON objects so the
    # native producers can push *storable* messages (storage/analytics parse them).
    # Used with kafka-producer-perf-test.sh --payload-file / --payload-delimiter.
    param(
        [Parameter(Mandatory)][string]$Path,
        [int]$Count = 1000,
        [double]$BaseTemp = 25.0,
        [double]$TempJitter = 10.0,
        [switch]$Critical   # force temps above the 50C alert threshold
    )
    $sb = [System.Text.StringBuilder]::new()
    $rand = [System.Random]::new(42)
    for ($i = 0; $i -lt $Count; $i++) {
        $temp = if ($Critical) { 60 + $rand.NextDouble() * 20 } else { $BaseTemp + ($rand.NextDouble() * 2 - 1) * $TempJitter }
        $obj = [ordered]@{
            messageId   = [guid]::NewGuid().ToString()
            deviceId    = "SENS{0:D4}" -f (($i % 100) + 1)
            temperature = [math]::Round($temp, 2)
            humidity    = [math]::Round(40 + $rand.NextDouble() * 40, 2)
            createdAt   = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
        }
        [void]$sb.AppendLine(($obj | ConvertTo-Json -Compress))
    }
    [System.IO.File]::WriteAllText($Path, $sb.ToString())
    Write-Host "  [payload] wrote $Count JSON records -> $Path"
    return $Path
}

# --- Kafka native tool wrappers -------------------------------------------

function Invoke-KafkaPerfTest {
    # Wraps kafka-producer-perf-test.sh. Returns the tool's stdout (which already
    # contains records/sec + latency percentiles - the spec's throughput & p95).
    param(
        [Parameter(Mandatory)][int]$NumRecords,
        [Parameter(Mandatory)][ValidateSet("0","1","all")][string]$Acks,
        [int]$Throughput = -1,            # -1 = unbounded (max throughput)
        [string]$PayloadFileInContainer   # optional: path to JSONL already copied into the container
    )
    $producerProps = @("bootstrap.servers=$script:KafkaBootstrap", "acks=$Acks")
    $cmd = @("$script:KafkaBin/kafka-producer-perf-test.sh",
             "--topic", $script:KafkaTopic,
             "--num-records", "$NumRecords",
             "--throughput", "$Throughput")
    if ($PayloadFileInContainer) {
        $cmd += @("--payload-file", $PayloadFileInContainer, "--payload-delimiter", "\n")
    } else {
        $cmd += @("--record-size", "256")
    }
    $cmd += @("--producer-props") + $producerProps
    Write-Host "  [kafka-perf] acks=$Acks num=$NumRecords throughput=$Throughput"
    $out = & docker exec $script:KafkaContainer @cmd 2>&1 | Out-String
    Write-Host $out
    return $out
}

function Copy-FileToContainer {
    param([Parameter(Mandatory)][string]$LocalPath,
          [Parameter(Mandatory)][string]$Container,
          [Parameter(Mandatory)][string]$ContainerPath)
    & docker cp $LocalPath "${Container}:${ContainerPath}"
    if ($LASTEXITCODE -ne 0) { throw "docker cp failed: $LocalPath -> ${Container}:${ContainerPath}" }
}

function Get-KafkaConsumerLag {
    param([string]$Group = "kafka-storage")
    $out = & docker exec $script:KafkaContainer "$script:KafkaBin/kafka-consumer-groups.sh" `
        --bootstrap-server $script:KafkaBootstrap --describe --group $Group 2>&1 | Out-String
    return $out
}

# --- MQTT native tool wrapper (emqtt-bench) -------------------------------

function Invoke-EmqttBenchPub {
    # Wraps the official emqtt-bench publisher, run as a one-shot container joined
    # to the compose network so it can reach the 'mosquitto' broker by name.
    param(
        [Parameter(Mandatory)][int]$Clients,        # = simulated devices (-c)
        [Parameter(Mandatory)][ValidateSet(0,1,2)][int]$Qos,
        [int]$IntervalMs = 10,                       # per-client publish interval (-I)
        [int]$LimitPerClient = 100,                  # messages per client (-L); total = Clients*Limit
        [int]$PayloadSize = 256                      # bytes (-s)
    )
    $net = Get-ComposeNetwork
    $cmd = @("run", "--rm", "--network", $net, $script:EmqttBenchImage,
             "pub", "-h", "mosquitto", "-p", "1883",
             "-t", $script:MqttTopic,
             "-c", "$Clients",
             "-I", "$IntervalMs",
             "-q", "$Qos",
             "-L", "$LimitPerClient",
             "-s", "$PayloadSize")
    Write-Host "  [emqtt-bench] clients=$Clients qos=$Qos limit/client=$LimitPerClient"
    $out = & docker @cmd 2>&1 | Out-String
    Write-Host $out
    return $out
}

# --- Results writer --------------------------------------------------------

function Save-ResultRow {
    param(
        [Parameter(Mandatory)][string]$Csv,
        [Parameter(Mandatory)][hashtable]$Row
    )
    $exists = Test-Path $Csv
    if (-not $exists) {
        ($Row.Keys -join ",") | Out-File -FilePath $Csv -Encoding utf8
    }
    (($Row.Keys | ForEach-Object { $Row[$_] }) -join ",") | Add-Content -Path $Csv -Encoding utf8
}

Write-Host "[common.ps1] loaded (repo root: $script:RepoRoot)"
