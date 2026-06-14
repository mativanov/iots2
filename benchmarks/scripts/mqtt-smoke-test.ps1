param(
    [int]$MessagesPerQos = 100
)

$ErrorActionPreference = "Stop"

function Invoke-Step {
    param(
        [string]$Title,
        [scriptblock]$Command
    )

    Write-Host ""
    Write-Host "== $Title =="
    & $Command

    if ($LASTEXITCODE -ne 0) {
        throw "Step failed with exit code $LASTEXITCODE`: $Title"
    }
}

Invoke-Step "Build MQTT service images" {
    docker compose build mqtt-ingestion-service mqtt-storage-service mqtt-analytics-service
}

Invoke-Step "Start PostgreSQL, Mosquitto, MQTT storage, and MQTT analytics" {
    docker compose up -d postgres mosquitto mqtt-storage-service mqtt-analytics-service
}

Invoke-Step "Run MQTT ingestion with QoS 0" {
    docker compose run --rm -e MQTT_QOS=0 -e TOTAL_MESSAGES=$MessagesPerQos mqtt-ingestion-service
}

Invoke-Step "Run MQTT ingestion with QoS 1" {
    docker compose run --rm -e MQTT_QOS=1 -e TOTAL_MESSAGES=$MessagesPerQos mqtt-ingestion-service
}

Invoke-Step "Run MQTT ingestion with QoS 2" {
    docker compose run --rm -e MQTT_QOS=2 -e TOTAL_MESSAGES=$MessagesPerQos mqtt-ingestion-service
}

Invoke-Step "Wait briefly for storage flush" {
    Start-Sleep -Seconds 3
}

Invoke-Step "PostgreSQL message counts by delivery_mode" {
    docker compose exec postgres psql -U iotuser -d iotdb -c "SELECT broker_type, delivery_mode, COUNT(*) FROM sensor_readings GROUP BY broker_type, delivery_mode ORDER BY broker_type, delivery_mode;"
}

Invoke-Step "Recent MQTT storage logs" {
    docker compose logs mqtt-storage-service --tail=50
}

Invoke-Step "Recent MQTT analytics logs" {
    docker compose logs mqtt-analytics-service --tail=50
}
