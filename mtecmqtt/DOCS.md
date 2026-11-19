# MTECmqtt Bridge – Dokumentation

Liest **M-TEC Energy Butler** Wechselrichter (Wattsonic / Solinteg Gen3) lokal per **Modbus RTU-over-TCP** rein lesend (`FC 0x03`) aus und stellt **173 Entitäten** (81 Bestands- + 92 Diagnose-Entitäten) per MQTT Auto-Discovery in Home Assistant bereit.

## Voraussetzungen
- **Wechselrichter im LAN/WLAN:** Modbus RTU-over-TCP auf Port `502` (ältere Firmware teils `5743`), Slave-ID `252`.
- **MQTT:** Laufender Broker (z. B. `core-mosquitto`) und aktive MQTT-Integration in Home Assistant.

## Konfiguration

| Gruppe / Option | Standard | Beschreibung |
| --- | --- | --- |
| `modbus.MODBUS_IP` | `"192.168.1.100"` | IP oder Hostname des Wechselrichter-Dongles. |
| `modbus.MODBUS_PORT` | `502` | TCP-Port der RTU-over-TCP-Bridge. |
| `modbus.MODBUS_SLAVE` | `252` | Modbus Slave-ID (`1–255`). |
| `modbus.MODBUS_TIMEOUT` | `8` | Socket-Timeout je Anfrage in Sekunden (`3–30`). |
| `modbus.MODBUS_RETRIES` | `2` | Interne Wiederholungsversuche (`0–5`). |
| `modbus.MODBUS_CLOSE_AFTER_CYCLE` | `true` | Schließt Port `502` nach jedem Zyklus (verhindert *Half-Open Sockets*). |
| `mqtt.MQTT_SERVER` | `"core-mosquitto"` | Hostname oder IP des MQTT-Brokers. |
| `mqtt.MQTT_PORT` | `1883` | TCP-Port des Brokers. |
| `mqtt.MQTT_LOGIN` | `"mtec"` | MQTT-Benutzername. |
| `mqtt.MQTT_PASSWORD` | `"dein_passwort"` | MQTT-Passwort (Sonderzeichen werden typsicher verarbeitet). |
| `refresh.REFRESH_NOW` | `30` | Zykluszeit in Sekunden für Echtzeitdaten (`10–300`). |
| `refresh.REFRESH_ALL_NOW_GROUPS` | `true` | `true`: alle `now-*`-Gruppen je Zyklus; `false`: Round-Robin. |
| `refresh.REFRESH_DAY` | `300` | Intervall in Sekunden für Tageswerte (`60–3600`). |
| `refresh.REFRESH_TOTAL` | `600` | Intervall in Sekunden für Gesamtwerte (`300–86400`). |
| `refresh.REFRESH_CONFIG` | `3600` | Intervall in Sekunden für Firmware-, Schutz- und Zeitplan-Register (`300–86400`). |
| `advanced.DEBUG` | `false` | Aktiviert ausführliches Debug-Logging im App-Protokoll. |

## MQTT-Topics
- **Werte (retained):** `MTEC/<seriennummer>/<gruppe>/<mqtt_key>`
- **Attribute (retained):** `MTEC/<seriennummer>/<gruppe>/<mqtt_key>/attributes`
- **Discovery:** `homeassistant/<sensor|binary_sensor>/MTEC_<mqtt_key>/config`
- **Gruppen:** `now-base`, `now-grid`, `now-inverter`, `now-backup`, `now-battery`, `now-pv`, `day`, `total`, `config`

## Technische Hinweise
- **Störungs-Tracking (`inverter_problem`):** Fasst aktive Inverter-, ARM- und BMS-Fehler (`10112–10122`, `18000–18010`, `33016–33018`) gruppenübergreifend zusammen (`active_faults`, `fault_count`, `last_fault_at`), da der Wechselrichter Fehler beim nächtlichen DC-Reset löscht. `10110` und `10124` lösen bewusst keinen Alarm aus.
- **Firmware & Shadow Scan:** `10011–10014` setzt `device.sw_version` inkl. automatischem Discovery-Update bei OTA-Änderungen; `25020` zeigt Shadow Scan (`Off`, `MPPT1`, `MPPT2`, `On`) rein lesend an.
- **Zeitplan (`53006–53048`):** Alle 6 Perioden werden in einem Frame gelesen, kompakt in `schedule_summary` zusammengefasst und strukturiert in den JSON-Attributen abgelegt.
- **Stabilität & Filterung:** `50 ms` / `250 ms` Pacing, Socket-Reset bei Fehlern, `retain=True` ohne `unregister` bei Stopp sowie `0xFFFF`/`0xFFFFFFFF`-Sentinel-Filterung nur für vorzeichenlose Register (`U16`/`U32`).
