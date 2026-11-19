# Changelog

## 3.1.2

- README, Dokumentation, Changelog und Architektur-Übersicht verschlankt.

## 3.1.1

- Redundante Standardwerte (`boot`, `startup`) aus `config.yaml` für den Home Assistant App-Linter entfernt.
- `APP_VERSION` und Metadaten auf `3.1.1` angeglichen.

## 3.1.0

### Neu
- Vollständige Read-Only Register-Abdeckung (`registers_extended.yaml`, insgesamt 173 HA-Entitäten; alle 81 Bestands-Entitäten unverändert).
- Live-Fault-/ARM-/BMS-Bitmasken inkl. JSON-Attributen und gruppenübergreifendem `binary_sensor.inverter_problem`.
- Firmware-Dekodierung (`device.sw_version` inkl. OTA-Update), Equipment-Info, Shadow Scan (`25020`), EMS-/Backup-/BMS-Limits und Zeitplan-Zusammenfassung (`schedule_summary`).
- Home Assistant App-Repository-Struktur mit Übersetzungen (`de`/`en`), CI-Linter und GHCR-Release-Workflow.

### Behoben
- Upstream-`STR`-Slicing-Bug (`serial_no` im Block `10000–10015`), numerische Cluster-Sortierung (max. 45 Register/Frame) und Lückenschutz.
- `0xFFFF`/`0xFFFFFFFF`-Sentinel-Filterung (nur `U16`/`U32`) und Entfernung leerer `unit_of_measurement`-Felder.

## 3.0.0

- Einführung von `mtec_runner.py` mit gepinnten Abhängigkeiten (`pymodbus`, `paho-mqtt`, `croedel/MTECmqtt`) und Build-Time-Selbsttest.
- Modbus Stability Guard (Pacing, TCP-Keepalive, Socket-Reset, `MODBUS_CLOSE_AFTER_CYCLE`, `REFRESH_ALL_NOW_GROUPS`, Backoff/Watchdog).
- MQTT & Discovery Guard (`retain=True`, Birth-Refresh, kein `unregister` bei Container-Stopp).
