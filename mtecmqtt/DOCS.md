# MTECmqtt Bridge – Dokumentation

Die App **MTECmqtt Bridge** liest alle verifizierten Register von **M-TEC Energy Butler** Hybrid-Wechselrichtern (OEM: Wattsonic / Solinteg Gen3) lokal über **Modbus RTU-over-TCP** aus und veröffentlicht die Messwerte, Status- und Konfigurationsdaten über MQTT inklusive Home Assistant Auto-Discovery.

---

## 1. Überblick & Sicherheitsmodell

- **Strikt lesender Betrieb (`FC 0x03`):** Alle erweiterten Register werden ausschließlich lesend abgefragt (`writable: false`) und in Home Assistant unter `entity_category: diagnostic` standardmäßig aktiviert bereitgestellt (insgesamt **173 Entitäten** inkl. der **81** unveränderten Bestands-Entitäten).
- **Optimiertes Framing für den Espressif WLAN-Dongle:**
  - `50 ms` Inter-Cluster-Pacing und `250 ms` Inter-Group-Pacing verhindern Pufferüberläufe auf dem seriellen UART-Bus des Wechselrichter-Dongles.
  - Ephemeral Socket Mode (`MODBUS_CLOSE_AFTER_CYCLE: true`) schließt Port `502` nach jedem Zyklus sauber, sodass keine hängenden *Half-Open Sockets* entstehen.
  - Sofortiger Socket-Reset bei Lesefehlern verhindert *Timeout-Poisoning* (Out-of-Sync-Antworten).
- **Persistente MQTT-Zustände:** Alle State-, Attribut- und Discovery-Topics werden mit `retain=True` gesendet; beim Stoppen der App werden Home-Assistant-Entitäten **nicht** deregistriert.

---

## 2. Voraussetzungen

1. **M-TEC Energy Butler** (z. B. `Energy Butler 20kW-3P-3G40`, Gen3 / Wattsonic / Solinteg) im lokalen Netzwerk erreichbar:
   - Standard-Port: **`502`** (ältere Firmware-Versionen nutzten vereinzelt `5743`).
   - Standard-Slave-ID: **`252` (`0xFC`)**.
   - Protokoll: **Modbus RTU-over-TCP** (kein Modbus-TCP-MBAP-Header).
2. **MQTT-Broker** in Home Assistant (z. B. die offizielle *Mosquitto broker* App mit `core-mosquitto`) und aktivierte **MQTT-Integration** in Home Assistant.

---

## 3. Konfiguration

### `modbus` – Wechselrichter-Verbindung

| Option | Standard | Beschreibung |
| --- | --- | --- |
| `MODBUS_IP` | `"192.168.1.100"` | Lokale IP-Adresse oder Hostname des Wechselrichter-WLAN-/LAN-Dongles. |
| `MODBUS_PORT` | `502` | TCP-Port der Modbus-RTU-over-TCP-Bridge (bei neueren Firmwares `502`). |
| `MODBUS_SLAVE` | `252` | Modbus Slave-ID des Wechselrichters (`1–255`; bei M-TEC LAN/WLAN-Dongles typischerweise `252`). |
| `MODBUS_TIMEOUT` | `8` | Socket-Timeout je Anfrage in Sekunden (`3–30`). |
| `MODBUS_RETRIES` | `2` | Anzahl interner PyModbus-Wiederholungsversuche (`0–5`). |
| `MODBUS_CLOSE_AFTER_CYCLE` | `true` | **Empfohlen (`true`):** Schließt die TCP-Verbindung nach jedem Abfragezyklus sauber per `TCP FIN`, damit Port `502` frei bleibt und keine *Half-Open Sockets* blockieren. |

### `mqtt` – MQTT-Broker

| Option | Standard | Beschreibung |
| --- | --- | --- |
| `MQTT_SERVER` | `"core-mosquitto"` | Hostname oder IP des MQTT-Brokers (`core-mosquitto` bei Nutzung der Supervisor-App). |
| `MQTT_PORT` | `1883` | TCP-Port des MQTT-Brokers. |
| `MQTT_LOGIN` | `"mtec"` | Benutzername für den MQTT-Broker. |
| `MQTT_PASSWORD` | `"dein_passwort"` | Passwort für den MQTT-Broker (Sonderzeichen werden typsicher verarbeitet). |

### `refresh` – Abfrageintervalle

| Option | Standard | Beschreibung |
| --- | --- | --- |
| `REFRESH_NOW` | `30` | Zykluszeit in Sekunden (`10–300`) für die Basisgruppe `now-base`. |
| `REFRESH_ALL_NOW_GROUPS` | `true` | Wenn `true`, werden in jedem `REFRESH_NOW`-Takt neben `now-base` auch alle 5 erweiterten Echtzeitgruppen (`now-grid`, `now-inverter`, `now-backup`, `now-battery`, `now-pv`) ausgelesen. Bei `false` wird pro Takt eine Untergruppe im Round-Robin abgefragt. |
| `REFRESH_DAY` | `300` | Intervall in Sekunden (`60–3600`) für Tages-Energiezähler (`day`). |
| `REFRESH_TOTAL` | `600` | Intervall in Sekunden (`300–86400`) für Gesamt-Energiezähler (`total`). |
| `REFRESH_CONFIG` | `3600` | Intervall in Sekunden (`300–86400`) für Konfigurations-, Firmware-, Batterie-Metadaten- und Zeitplan-Register (`config`). |

### `advanced` – Diagnose

| Option | Standard | Beschreibung |
| --- | --- | --- |
| `DEBUG` | `false` | Aktiviert ausführliches Debug-Logging (inkl. Cluster-Abfragen und MQTT-Publishes) im App-Protokoll. |

---

## 4. MQTT-Topic-Struktur

Alle Nachrichten werden unterhalb der Seriennummer des Wechselrichters veröffentlicht:

- **Zustandswert (retained):** `MTEC/<seriennummer>/<gruppe>/<mqtt_key>`
- **JSON-Attribute (retained, sofern vorhanden):** `MTEC/<seriennummer>/<gruppe>/<mqtt_key>/attributes`
- **Home Assistant Discovery:** `homeassistant/<sensor|binary_sensor>/MTEC_<mqtt_key>/config`

Verfügbare Gruppen: `now-base`, `now-grid`, `now-inverter`, `now-backup`, `now-battery`, `now-pv`, `day`, `total`, `config`.

---

## 5. Wichtige technische Hinweise & Besonderheiten

- **Live-Fault-Bitmasken & `inverter_problem`:**
  M-TEC Gen3 Wechselrichter speichern **keine** historischen Fehlerprotokolle über Modbus – aktive Flags (`10112–10122`, `18000–18010`, `33016–33018`) löschen sich beim nächtlichen DC-Reset (`V_dc → 0 V`) von selbst. Der gruppenübergreifende `binary_sensor.mtec_energybutler_inverter_problem` (`device_class: problem`) schaltet bei jedem aktiven Fehler/Warnzustand auf `ON` (mit `active_faults`, `fault_count` und `last_fault_at` in den JSON-Attributen), sodass transiente Störungen dauerhaft im Home Assistant Recorder protokolliert werden. Die unbestätigten/informativen Register `10110` (`operation_flags`) und `10124` lösen bewusst **kein** `inverter_problem` aus.
- **Firmware-Version & stille OTA-Updates:**
  Register `10011–10014` wird zu `V<Master>-<DSP>` (z. B. `V1.0.0.1-27.53.5.0`) dekodiert und sowohl als Diagnose-Sensor als auch in `device.sw_version` eingetragen. Ändert sich die Firmware bei einem periodischen `config`-Poll, wird die Home Assistant Discovery automatisch aktualisiert.
- **Shadow Scan (`25020`):**
  Wird rein lesend als `Off`, `MPPT1`, `MPPT2` oder `On` ausgewiesen (inkl. Bit-Flags in den Attributen), um unbeabsichtigte Resets (z. B. nach Hersteller-OTA) sofort sichtbar zu machen.
- **Zeitplan-Zusammenfassung (`53006–53048`):**
  Alle 43 Zeitplan-Register werden in einem einzigen `config`-Frame gelesen und kompakt im Sensor `schedule_summary` (`Disabled` oder Zusammenfassung der aktiven Perioden) bereitgestellt; alle 6 Perioden (Modus, Quelle, Leistungs-Limit %, Start-/Endzeit) liegen vollständig strukturiert in den JSON-Attributen vor.
- **Sentinel-Filterung (`0xFFFF` / `0xFFFFFFFF`):**
  Nicht gemappte `U16`-/`U32`-Register oder Overflow-Glitches werden sauber gefiltert, ohne die Gruppe abzubrechen oder Statistiken zu verfälschen. Signierte `I16`-/`I32`-Werte (z. B. negative Netzleistung bei Einspeisung) bleiben davon unberührt.
- **PV-Strom-Grundrauschen (`0.3–0.5 A`):**
  Die Hardware-ADCs von `PV1` (`0.3 A`) und `PV2` (`0.4–0.5 A`) zeigen auch nachts/in der Dämmerung einen kleinen Nullpunkt-Offset. Alle Roh-Sensoren geben die Wechselrichter-Register unverfälscht wieder; für Plausibilitätsprüfungen ober-/unterhalb `~3.2 kW` steht zusätzlich `inverter_ac_power_phase_sum` (`30236 + 30242 + 30248`) als Diagnose-Sensor bereit.
