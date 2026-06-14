# Cleaned Benchmark Results

Ovaj fajl je napravljen iz postojecih `summary.csv` i `stats.csv` fajlova bez ponovnog pokretanja testova. Originalni CSV fajlovi nisu menjani.

Napomena: deo CSV fajlova je generisan na lokalizovanom sistemu gde PowerShell decimalne brojeve pise sa zarezom, npr. `3,2`. Posto je CSV delimiter takodje zarez, neke linije imaju visak kolona. Brojke ispod su rekonstruisane po semantici svakog scenarija.

## Scenario A - Massive Sensor Ingestion

### Kafka, stabilan run za 100 i 1000 uredjaja

Izvor: `kafka-scenarioA-20260614-195354/summary.csv`

| Devices | Records sent | ACKS | Throughput msg/s | p95 ms | Stored | Loss |
| ------: | -----------: | :--- | ---------------: | -----: | -----: | ---: |
| 100 | 10 000 | 0 | 6 626.91 | 56 | 10 000 | 0% |
| 100 | 10 000 | 1 | 7 194.24 | 148 | 10 000 | 0% |
| 100 | 10 000 | all | 7 446.02 | 191 | 10 000 | 0% |
| 1000 | 100 000 | 0 | 42 087.54 | 316 | 100 000 | 0% |
| 1000 | 100 000 | 1 | 50 890.59 | 232 | 100 000 | 0% |
| 1000 | 100 000 | all | 37 965.07 | 623 | 100 000 | 0% |

### Kafka, kasniji run koji ukljucuje 10000 uredjaja

Izvor: `kafka-scenarioA-20260614-213925/summary.csv`

| Devices | Records sent | ACKS | Throughput msg/s | p95 ms | Stored | Loss |
| ------: | -----------: | :--- | ---------------: | -----: | -----: | ---: |
| 100 | 10 000 | 0 | 17 889.09 | 21 | 442 312 | -4323.12% |
| 100 | 10 000 | 1 | 17 301.04 | 33 | 10 000 | 0% |
| 100 | 10 000 | all | 13 717.42 | 189 | 10 000 | 0% |
| 1000 | 100 000 | 0 | 103 092.78 | 70 | 100 000 | 0% |
| 1000 | 100 000 | 1 | 71 581.96 | 306 | 100 000 | 0% |
| 1000 | 100 000 | all | 79 681.27 | 191 | 100 000 | 0% |
| 10000 | 1 000 000 | 0 | 145 078.80 | 1235 | 683 533 | 31.65% |
| 10000 | 1 000 000 | 1 | 211 237.85 | 1023 | 752 245 | 24.78% |
| 10000 | 1 000 000 | all | 239 635.75 | 829 | 632 000 | 36.8% |

Ovaj kasniji Kafka run nije dobar za glavni zakljucak o gubitku, jer prvi red ima vise sacuvanih poruka nego poslatih. To ukazuje na backlog/duplikate/stare poruke u topic-u ili neizolovan run. Za izvestaj je bezbednije koristiti stabilan run za 100 i 1000 uredjaja, a 10000 oznaciti kao lokalno nestabilan.

### MQTT

Izvor: `mqtt-scenarioA-20260614-214653/summary.csv`

| Devices | QoS | Loss sample | Stored | Loss | Throughput msg/s |
| ------: | --: | ----------: | -----: | ---: | :--------------- |
| 100 | 0 | 2 000 | 2 000 | 0% | n/a u CSV |
| 1000 | 0 | 2 000 | 2 000 | 0% | n/a u CSV |
| 10000 | 0 | 2 000 | 2 000 | 0% | n/a u CSV |
| 100 | 1 | 2 000 | 2 000 | 0% | n/a u CSV |
| 1000 | 1 | 2 000 | 2 000 | 0% | n/a u CSV |
| 10000 | 1 | 2 000 | 2 000 | 0% | n/a u CSV |
| 100 | 2 | 2 000 | 2 000 | 0% | n/a u CSV |
| 1000 | 2 | 2 000 | 2 000 | 0% | n/a u CSV |
| 10000 | 2 | 2 000 | 2 000 | 0% | n/a u CSV |

MQTT throughput nije upisan u ovaj `summary.csv`. U rucnom `SUMMARY.md` postoje vrednosti za 100 klijenata: QoS 0 = 2 089 msg/s, QoS 1 = 2 347 msg/s, QoS 2 = 2 284 msg/s.

## Scenario B - Edge Connectivity Failure

### Kafka

Izvor: `kafka-scenarioB-20260614-214331/summary.csv`

| Broker | Outage | Sent | Stored | Loss field | Recovery note |
| :----- | -----: | ---: | -----: | ---------: | :------------ |
| Kafka | 30 s | 2 000 | 674 898 | -33644.9% | offset-resumption |

Ova vrednost nije validna kao cist procenat gubitka jer je `stored` mnogo veci od `sent`. Moze se koristiti samo kao dokaz da Kafka nije izgubila poruke u ovom run-u, ali uz napomenu da postoje duplikati/backlog i da bi za tacan loss trebalo brojati jedinstveni `messageId`.

### MQTT

Izvor: `mqtt-scenarioB-20260614-222200/summary.csv`

| Broker | QoS | Outage | Sent | Stored | Loss |
| :----- | --: | -----: | ---: | -----: | ---: |
| MQTT | 0 | 30 s | 2 000 | 225 | 88.75% |
| MQTT | 2 | 30 s | 2 000 | 227 | 88.65% |

Raniji MQTT B run (`mqtt-scenarioB-20260614-215126`) je slican: QoS 0 = 240/2000 stored, QoS 2 = 238/2000 stored.

## Scenario C - Burst Event Load

### Kafka

Izvor: `kafka-scenarioC-20260614-214444/summary.csv`

| Broker | Baseline | Burst rate | Burst duration | Recovery time |
| :----- | -------: | ---------: | -------------: | ------------: |
| Kafka | 50 msg/s | 5 000 msg/s | 5 s | 3.2 s |

### MQTT

Izvor: `mqtt-scenarioC-20260614-222418/summary.csv`

| Broker | QoS | Baseline messages | Burst messages | Total sent | Stored | Loss | Recovery time |
| :----- | --: | ----------------: | -------------: | ---------: | -----: | ---: | ------------: |
| MQTT | 1 | 100 | 5 000 | 5 100 | 5 100 | 0% | 1.2 s |

Postoji i raniji neuspesan MQTT C run (`mqtt-scenarioC-20260614-220257`) sa `stored=0`, `loss=100%`, `recovery=>120`. Za zakljucak treba koristiti kasniji uspesan run.

## Scenario D - Real-Time Alerting

### Kafka

Izvor: `kafka-scenarioD-20260614-214510/summary.csv`

| Trial | Critical messages | Alert latency ms |
| ----: | ----------------: | ---------------: |
| 1 | 50 | 2 157 |
| 2 | 50 | 6 503 |
| 3 | 50 | 7 869 |
| 4 | 50 | 8 220 |
| 5 | 50 | 7 848 |

Kafka prosek: 6 519 ms. Minimum: 2 157 ms. Maksimum: 8 220 ms.

### MQTT

Izvor: `mqtt-scenarioD-20260614-220624/summary.csv`

| Trial | QoS | Alert latency ms |
| ----: | --: | ---------------: |
| 1 | 1 | 3 450 |
| 2 | 1 | 6 286 |
| 3 | 1 | 7 766 |
| 4 | 1 | 8 223 |
| 5 | 1 | 7 958 |

MQTT prosek: 6 737 ms. Minimum: 3 450 ms. Maksimum: 8 223 ms.

## Resource Monitoring - docker stats

### Kafka Scenario A, stabilan run

Izvor: `kafka-scenarioA-20260614-195354/stats.csv`

| Container | CPU avg | CPU max | RAM avg | RAM max |
| :-------- | ------: | ------: | ------: | ------: |
| iot-kafka | 123.65% | 658.12% | 401.8 MB | 533.6 MB |
| kafka-storage-service | 11.15% | 95.91% | 239.5 MB | 251.3 MB |
| kafka-analytics-service | 12.36% | 213.87% | 233.3 MB | 289.2 MB |
| iot-postgres | 13.59% | 64.09% | 44.5 MB | 72.2 MB |

### MQTT Scenario A

Izvor: `mqtt-scenarioA-20260614-214653/stats.csv`

| Container | CPU avg | CPU max | RAM avg | RAM max |
| :-------- | ------: | ------: | ------: | ------: |
| iot-mosquitto | 2.46% | 10.08% | 5.0 MB | 5.5 MB |
| mqtt-storage-service | 2.34% | 12.50% | 31.7 MB | 34.8 MB |
| mqtt-analytics-service | 1.99% | 8.88% | 23.0 MB | 25.1 MB |
| iot-postgres | 1.03% | 9.70% | 349.4 MB | 350.7 MB |

Napomena: u MQTT stats run-u su ostali upaljeni i Kafka kontejneri, ali za MQTT zakljucak treba gledati `iot-mosquitto`, `mqtt-storage-service` i `mqtt-analytics-service`.

## Brojke koje su najbezbednije za izvestaj

1. Kafka Scenario A: koristiti stabilan run za 100 i 1000 uredjaja (`kafka-scenarioA-20260614-195354`).
2. MQTT Scenario A: koristiti loss iz CSV-a, a throughput iz rucnog `SUMMARY.md`, jer throughput nije upisan u `mqtt-scenarioA` CSV.
3. Scenario B: MQTT loss koristiti direktno; Kafka opisati kao "bez gubitka, ali sa duplikatima/backlogom", ne kao cist loss procenat.
4. Scenario C: koristiti Kafka `3.2 s` i MQTT uspesan run `1.2 s`, `0% loss`.
5. Scenario D: koristiti pet trial-ova iz poslednjih Kafka/MQTT summary fajlova.
6. Resource monitoring: koristiti broker RAM/CPU iz `stats.csv`; zakljucak Kafka znatno teza od Mosquitto je validan.
