# run-all.ps1 - orchestrate the full benchmark campaign for both brokers.
#
# Brings up the required infrastructure, then runs scenarios A-D for Kafka and
# MQTT in turn. Each scenario writes its own timestamped folder under
# benchmarks/results/. Intended to be run AFTER `docker compose build`.
#
# Usage:
#   ./run-all.ps1                  # everything
#   ./run-all.ps1 -Only kafka      # one broker
#   ./run-all.ps1 -Scenarios A,D   # subset of scenarios

param(
    [ValidateSet("both","kafka","mqtt")][string]$Only = "both",
    [ValidateSet("A","B","C","D")][string[]]$Scenarios = @("A","B","C","D")
)

. "$PSScriptRoot\lib\common.ps1"

function Up([string[]]$svcs) {
    Write-Host "`n[compose] up: $($svcs -join ', ')"
    $composeArgs = @("up","-d") + $svcs
    Invoke-Compose @composeArgs
    Start-Sleep -Seconds 8
}

Write-Host "=== IoT broker benchmark campaign ==="
Write-Host "Make sure images are built:  docker compose build"

if ($Only -in @("both","kafka")) {
    Up @("postgres","kafka","kafka-storage-service","kafka-analytics-service")
    if ("A" -in $Scenarios) { & "$PSScriptRoot\kafka\scenario-a-ingestion.ps1" }
    if ("B" -in $Scenarios) { & "$PSScriptRoot\kafka\scenario-b-failure.ps1" }
    if ("C" -in $Scenarios) { & "$PSScriptRoot\kafka\scenario-c-burst.ps1" }
    if ("D" -in $Scenarios) { & "$PSScriptRoot\kafka\scenario-d-alerting.ps1" }
}

if ($Only -in @("both","mqtt")) {
    Up @("postgres","mosquitto","mqtt-storage-service","mqtt-analytics-service")
    if ("A" -in $Scenarios) { & "$PSScriptRoot\mqtt\scenario-a-ingestion.ps1" }
    if ("B" -in $Scenarios) { & "$PSScriptRoot\mqtt\scenario-b-failure.ps1" }
    if ("C" -in $Scenarios) { & "$PSScriptRoot\mqtt\scenario-c-burst.ps1" }
    if ("D" -in $Scenarios) { & "$PSScriptRoot\mqtt\scenario-d-alerting.ps1" }
}

Write-Host "`n=== Campaign complete. See benchmarks/results/ ==="
