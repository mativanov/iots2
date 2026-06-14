# Sample Data

CSV datasets will be added here later and transformed into sensor-reading events for MQTT and Kafka experiments.

The transformation process should stay dataset-agnostic. A future preparation step can map source CSV columns onto the shared event contract:

| Contract field | Meaning | Example source column names |
| --- | --- | --- |
| `messageId` | Unique event/message identifier | `message_id`, generated UUID |
| `deviceId` | Sensor or device identifier | `device_id`, `sensor_id`, `station_id` |
| `temperature` | Temperature reading | `temperature`, `temp`, `temperature_c` |
| `humidity` | Humidity reading | `humidity`, `relative_humidity` |
| `createdAt` | Event creation timestamp in ISO-8601 format | `created_at`, `timestamp`, `time` |

Future dataset preparation should:

- Read a CSV file from this folder or a configured external path.
- Normalize column names and data types.
- Convert timestamps to ISO-8601 format.
- Generate `messageId` values if the source dataset does not provide stable IDs.
- Emit records that match `shared/contracts/sensor-reading.schema.json`.
- Keep broker-specific behavior outside the dataset transformation step.

No ingestion logic is implemented in this foundation stage.
