# Tehnički izveštaj — IoT mikroservisi zasnovani na događajima

## Uporedna evaluacija MQTT-a i Apache Kafke

**Predmet:** Internet stvari i servisa — Projekat 2
**Tema:** Performanse, skalabilnost i ograničenja message broker sistema (publish/subscribe) u IoT mikroservisnim arhitekturama

> Napomena o statusu: arhitektura i implementacija oba broker sistema su završene i kontejnerizovane. Brojčane vrednosti u uporednim tabelama (Poglavlje 5) popunjavaju se rezultatima merenja koja se pokreću skriptama iz `benchmarks/scripts/`. Polja označena sa `—` čekaju izvršavanje eksperimenata.

---

## 1. Uvod i cilj

Cilj projekta je da se ista event-driven IoT mikroservisna arhitektura implementira **dva puta** — jednom nad **MQTT (Mosquitto)** brokerom i jednom nad **Apache Kafkom (KRaft režim)** — i da se dva pristupa uporede po pitanju propusnosti (throughput), kašnjenja (latencija), pouzdanosti isporuke i potrošnje resursa. Fokus je na razumevanju *trade-off*-a između kašnjenja i pouzdanosti, kao i na pogodnosti svakog brokera za *edge* odnosno *cloud* okruženja.

Kompletan sistem je kontejnerizovan pomoću Docker Compose-a. Koriste se dve tehnologije, u skladu sa zahtevom za najmanje dve: **Node.js** za MQTT mikroservise i **Java 17 / Spring Boot 3.3** za Kafka mikroservise.

Koristi se isti IoT dataset kao u prvom projektu — `Smart_Farming_Crop_Yield_2024.csv` (senzori poljoprivrednih parcela: temperatura, vlažnost i dr.).

---

## 2. Mikroservisna arhitektura

Svaki broker ima identičan skup od tri mikroservisa, povezana isključivo preko brokera (asinhrono, bez direktnih poziva):

```
                 ┌──────────────────┐        ┌──────────────────┐
  CSV dataset →  │ Ingestion Service│ ─────▶ │   MESSAGE BROKER │ ─────▶ Storage Service ─▶ PostgreSQL
  (simulacija)   │ (IoT uređaji)    │  topic │ MQTT / Kafka     │   │
                 └──────────────────┘        └──────────────────┘   └──▶ Analytics Service (Tumbling Window)
```

- **Data Ingestion Service** — simulira IoT uređaje: učitava CSV dataset, generiše očitavanja senzora i objavljuje ih u realnom vremenu na odgovarajući topic (`iot/readings` za MQTT, `iot.readings` za Kafku). Format poruke je identičan za oba brokera: `messageId`, `deviceId`, `temperature`, `humidity`, `createdAt` (JSON).
- **Data Storage Service** — pretplaćen na broker, preuzima poruke i upisuje ih u PostgreSQL. Implementiran je **batching** (grupni upis na svakih do 500 poruka) radi optimizacije I/O tokom stres-testova (Scenariji A i C), kako baza ne bi postala usko grlo umesto brokera.
- **Analytics Service (Stream Processing)** — pretplaćen na isti tok, implementira **Tumbling Window** od 10 sekundi: na svakih 10 s računa prosečnu temperaturu i, ako prosek pređe prag (podrazumevano > 50 °C), ispisuje kritičan **ALERT** u log.

Svi servisi pišu u istu tabelu `sensor_readings` (deljena šema, `database/init.sql`); redovi se obeležavaju kolonom `broker_type` (`mqtt` / `kafka`), a nivo garancije isporuke kolonom `delivery_mode`.

### 2.1 MQTT implementacija (Node.js)

Tri Node.js servisa (`mqtt/`) koriste biblioteku `mqtt`. Ingestion publikuje sa konfigurabilnim **QoS 0/1/2**; storage i analytics se pretplaćuju (podrazumevano QoS 2). Broker je **Eclipse Mosquitto 2**.

### 2.2 Kafka implementacija (Java / Spring Boot)

Tri Spring Boot servisa (`kafka/`):
- **Ingestion** — `KafkaTemplate` producer; svaki zapis je keširan po `deviceId` (per-device redosled na istoj particiji). Nivo `acks` (0/1/all) se prosleđuje kao **header poruke**, pošto je `acks` isključivo producer-side podešavanje bez traga vidljivog potrošaču — storage ga čita iz headera i upisuje u `delivery_mode`.
- **Storage** — *batch listener* (`max.poll.records=500`) + JDBC batch insert; offseti se komituju tek nakon uspešnog upisa (`ack-mode: BATCH`) → **at-least-once** skladištenje.
- **Analytics** — zasebna consumer grupa (pub/sub fan-out u odnosu na storage) + `@Scheduled` flush 10-sekundnog prozora.

Kafka radi u **KRaft režimu** (bez ZooKeeper-a), radi uštede memorije na lokalnim mašinama (`apache/kafka:3.7.0`). Topic `iot.readings` se kreira sa **3 particije** radi demonstracije particionisanja i consumer lag-a.

---

## 3. Konfiguracija brokera i nivoi garancije isporuke

| Aspekt | MQTT (Mosquitto) | Kafka (KRaft) |
|---|---|---|
| Model garancije | QoS 0 / 1 / 2 | acks = 0 / 1 / all |
| Semantika | at-most-once / at-least-once / exactly-once* | bez potvrde / lider / svi ISR |
| Trajnost | sesije / retained (opciono) | trajni particionисani log + offseti |
| Skaliranje potrošača | deljeni topic | particije + consumer grupe |

\* MQTT „exactly-once" (QoS 2) odnosi se na isporuku poruke brokeru/pretplatniku kroz 4-fazni handshake; ne podrazumeva idempotentnost na nivou aplikacije.

Mapiranje na merenja:
- **MQTT:** QoS 0 → najbrži, moguć gubitak; QoS 1 → bez gubitka uz moguće duplikate; QoS 2 → bez gubitka i bez duplikata, najveće kašnjenje.
- **Kafka:** acks=0 → producer ne čeka potvrdu (najveći throughput, moguć gubitak); acks=1 → potvrda od lidera; acks=all → potvrda od svih in-sync replika (najpouzdanije, najveće kašnjenje). U ovom setapu replication factor = 1 (jedan broker), pa acks=1 i acks=all imaju sličnu trajnost ali različit overhead potvrde.

---

## 4. Eksperimentalna metodologija

Opterećenje i metrike se prikupljaju **namenskim alatima** koje zahteva specifikacija:

- **Kafka:** `kafka-producer-perf-test.sh` (nativna skripta visokih performansi koja dolazi uz Kafku), pokrenuta unutar `iot-kafka` kontejnera; `kafka-consumer-groups.sh --describe` za consumer lag.
- **MQTT:** zvanični **`emqtt-bench`** alat, pokrenut kao kontejner na istoj Docker mreži.
- **Resursi:** `docker stats` (CPU, RAM, mrežni saobraćaj) — uzorkuje se tokom svakog testa u `stats.csv`.

Sve skripte se nalaze u `benchmarks/scripts/` (vidi tamošnji README za detalje). Rezultati se snimaju u `benchmarks/results/<scenario>-<timestamp>/`.

### Scenariji

- **A — Massive Sensor Ingestion:** 100 / 1000 / 10000 uređaja; meri se maksimalni throughput (msg/s) i procenat izgubljenih poruka. Kafka prolazi acks=0/1/all; MQTT prolazi QoS 0/1/2.
- **B — Edge Connectivity Failures:** `docker network disconnect` prekida mrežu simulatora na 30 s, zatim `connect`. Posmatra se oporavak: kod Kafke **pomeranje offset-a** (potrošač nastavlja od poslednjeg komitovanog offseta → ~0% gubitka), kod MQTT-a razlika **QoS 0 (gubitak) vs QoS 2 (oporavak sesije)**.
- **C — Burst Event Load:** skok sa 50 na 5000 msg/s u trajanju od nekoliko sekundi; posmatra se formiranje backloga, backpressure i **recovery time** (vreme da se sistem vrati u normalu).
- **D — Real-Time Alerting:** end-to-end latencija od generisanja kritične vrednosti do ispisa ALERT-a (uključuje do jedan 10 s prozor); više ponavljanja radi raspodele.

---

## 5. Rezultati i uporedna tabela

> Vrednosti se popunjavaju iz `benchmarks/results/` nakon izvršavanja kampanje (`benchmarks/scripts/run-all.ps1`).

### 5.1 Glavna uporedna tabela (Poglavlje 6, pitanje 3)

| Metrika | MQTT (QoS 1) | Kafka (acks=1) |
|---|---|---|
| Max throughput (msg/s) | — | — |
| p95 latencija (ms) | — | — |
| CPU footprint (avg %) | — | — |
| RAM footprint (MB) | — | — |
| % izgubljenih poruka (Scenario A, 10k) | — | — |

### 5.2 Scenario A — throughput i gubitak

| Broker | Nivo | 100 uređaja (msg/s) | 1000 (msg/s) | 10000 (msg/s) | % gubitka (10k) |
|---|---|---|---|---|---|
| MQTT | QoS 0 | — | — | — | — |
| MQTT | QoS 1 | — | — | — | — |
| MQTT | QoS 2 | — | — | — | — |
| Kafka | acks=0 | — | — | — | — |
| Kafka | acks=1 | — | — | — | — |
| Kafka | acks=all | — | — | — | — |

### 5.3 Scenario B — oporavak nakon prekida (30 s)

| Broker | Nivo | Poslato | Sačuvano | % gubitka | Mehanizam |
|---|---|---|---|---|---|
| MQTT | QoS 0 | — | — | — | fire-and-forget |
| MQTT | QoS 2 | — | — | — | oporavak sesije |
| Kafka | acks=1 | — | — | — | pomeranje offset-a |

### 5.4 Scenario C — burst i recovery time

| Broker | Burst (msg) | % gubitka | Recovery time (s) |
|---|---|---|---|
| MQTT (QoS 1) | — | — | — |
| Kafka (acks=1) | — | — | — |

### 5.5 Scenario D — end-to-end alert latencija

| Broker | Prosek (ms) | Min (ms) | Max (ms) |
|---|---|---|---|
| MQTT | — | — | — |
| Kafka | — | — | — |

---

## 6. Analiza pouzdanosti i odgovori na inženjerska pitanja

### Pitanje 1 — Zašto je MQTT idealan na *edge* uređajima, a neadekvatan za istorijsku analitiku velikih podataka?

MQTT je projektovan za ograničene uređaje i nepouzdane mreže. **Prednosti na edge-u:**

- **Mali otisak.** Protokol je izuzetno lagan: minimalno zaglavlje (fiksno 2 bajta), jednostavan klijent koji staje u par desetina KB i radi na mikrokontrolerima sa malo RAM-a i baterijskim napajanjem.
- **Tolerancija na lošu mrežu.** Radi preko TCP-a uz mehanizme za prekide (keep-alive, Last Will & Testament, perzistentne sesije, QoS 1/2 retransmisije), što odgovara mobilnim/bežičnim vezama na terenu.
- **Fleksibilan QoS po poruci.** Uređaj bira kompromis kašnjenje↔pouzdanost za svaku poruku (npr. QoS 0 za česta, nekritična očitavanja; QoS 2 za alarme).
- **Push model sa malim kašnjenjem.** Broker odmah prosleđuje poruke pretplatnicima — pogodno za telemetriju u realnom vremenu i komande.

**Zašto postaje neadekvatan za istorijsku analitiku velikih podataka:**

- **Broker ne čuva istoriju.** Klasičan MQTT broker je *router* poruka, a ne skladište. Poruka koja nije isporučena (osim ograničenog retained/persistent slučaja) nestaje — nema *replay*-a istorijskog toka za naknadnu obradu.
- **Nema particionисања ni consumer grupa.** Ne postoji ugrađen model za horizontalno deljenje toka među više potrošača radi paralelne obrade ogromnih količina podataka; skaliranje analitike je teško.
- **Slaba kontrola redosleda i ponovne obrade.** Bez trajnog, uređenog loga sa offsetima, nije moguće „premotati" tok i ponovo izračunati agregate nad istorijom — ključno za batch/stream analitiku velikih podataka.
- **Bez ugrađene trajnosti i garancija end-to-end.** Pouzdano dugotrajno čuvanje zahteva poseban sloj (baza/Kafka) — MQTT sam po sebi nije *system of record*.

Ukratko: MQTT je odličan **transport** od senzora do ivice/clouda, ali nije platforma za skladištenje i ponovljivu obradu velikih istorijskih tokova.

### Pitanje 2 — Zašto Kafka dominira u *data-intensive* cloud sistemima, kolika je „cena" skalabilnosti i da li je realna na ograničenom edge hardveru?

**Zašto dominira u cloud-u:**

- **Trajni, uređeni, particionисani log.** Kafka čuva poruke na disku zadati period (retention) bez obzira na potrošnju. Potrošači mogu da čitaju, ponovo čitaju i „premotavaju" tok pomoću **offset**-a — temelj za istorijsku analitiku, *event sourcing* i ponovnu obradu.
- **Horizontalna skalabilnost preko particija.** Topic se deli na particije raspoređene po brokerima; **consumer grupe** paralelizuju obradu (jedna particija po potrošaču u grupi). Tako se postiže propusnost reda miliona poruka u sekundi.
- **Trajnost i replikacija.** `acks=all` + replikacija ISR čine ga *system of record*-om otpornim na otkaze brokera.
- **Decoupling i fan-out.** Više nezavisnih consumer grupa (npr. storage i analytics) čita isti tok bez međusobnog uticaja — idealno za mikroservise.

**Cena skalabilnosti (resursi):**

- **Memorija i CPU.** JVM-bazirani brokeri, page-cache za log, kompresija/replikacija i koordinacija (KRaft kontroler) troše znatno više RAM-a i CPU-a nego MQTT broker. Realno se računa sa stotinama MB do GB RAM-a po brokeru.
- **Disk I/O i prostor.** Trajni log po definiciji troši disk; visok throughput zahteva brz I/O podsistem.
- **Operativna složenost.** Particionисање, replikacija, retencija, monitoring lag-a — više konfiguracije i održavanja nego kod MQTT-a.

**Da li je realno na ograničenom edge hardveru?** Uglavnom **ne** kao puni broker. Iako KRaft režim uklanja ZooKeeper i smanjuje otisak, Kafka i dalje očekuje JVM, disk i RAM koje tipičan senzor/mikrokontroler nema. Na jačim edge *gateway*-ima (npr. industrijski PC) jeste izvodljiva, ali na pravom *constrained* hardveru se umesto toga koristi MQTT do ivice, a Kafka u cloud-u/regionalnom čvorištu (često uz MQTT→Kafka most). To je upravo arhitektonski *trade-off* koji ovaj projekat ilustruje.

### Pitanje 3 — Uporedna tabela performansi

Vidi **Poglavlje 5.1**. Popunjava se merenjima (Throughput, p95 latencija, CPU/RAM footprint).

---

## 7. Zaključak

MQTT i Kafka rešavaju komplementarne probleme. **MQTT** je optimalan kao lagani, push transport sa edge uređaja uz fleksibilan QoS i otpornost na loše mreže, ali nije skladište niti platforma za ponovljivu analitiku. **Kafka** je trajni, skalabilni log koji dominira u data-intensive cloud obradama, po ceni znatno veće potrošnje resursa i operativne složenosti — što je čini neprikladnom za rad direktno na hardverski ograničenim senzorima. Tipična produkcija ih kombinuje: MQTT do ivice, Kafka u cloud-u. Merenja iz Poglavlja 5 kvantifikuju ovaj kompromis kroz throughput, latenciju, gubitak poruka i potrošnju resursa.

---

## 8. Reprodukcija (isporučeni rezultati projekta)

1. **Git repo** — kompletan izvorni kod i konfiguracija.
2. **Docker Compose** — `docker-compose.yml` (PostgreSQL, Mosquitto, Kafka/KRaft, 6 mikroservisa).
3. **Konfiguracija brokera** — `mqtt/mosquitto.conf`; Kafka KRaft env u compose-u; `acks`/`QoS` parametrizovani.
4. **Benchmark skripte** — `benchmarks/scripts/` (scenariji A–D, oba brokera, nativni alati).
5. **Eksperimentalni podaci** — `benchmarks/results/` (CSV summary, stats, logovi).
6. **Tehnički izveštaj** — ovaj dokument (`REPORT.md` + `REPORT.docx`).

```powershell
docker compose build
docker compose up -d postgres kafka mosquitto
./benchmarks/scripts/run-all.ps1
```
