# MTECmqtt Bridge – Home Assistant App Repository

[![Open your Home Assistant instance and show the add add-on repository dialog with a specific repository URL pre-filled.](https://my.home-assistant.io/badges/supervisor_add_addon_repository.svg)](https://my.home-assistant.io/redirect/supervisor_add_addon_repository/?repository_url=https%3A%2F%2Fgithub.com%2Ftobi-freu%2Fmtecmqtt)

Home Assistant App **MTECmqtt Bridge** (`mtecmqtt`, `v3.1.2`, `amd64` / `aarch64`) für **M-TEC Energy Butler** Wechselrichter (Wattsonic / Solinteg Gen3) auf Basis von [croedel/MTECmqtt](https://github.com/croedel/MTECmqtt).

## Funktionen
- **Read-Only Vollabdeckung (`FC 0x03`, 173 Entitäten):** Alle 81 Bestands-Entitäten plus 92 Diagnose-Entitäten (Fault-/ARM-/BMS-Bitmasken, `inverter_problem`, Firmware/OTA, Shadow Scan, BMS-Limits, Zeitplan-Zusammenfassung).
- **Modbus Stability Guard:** Inter-Cluster-/Inter-Group-Pacing (`50 ms` / `250 ms`), TCP-Keepalive, Socket-Reset bei Fehlern, `MODBUS_CLOSE_AFTER_CYCLE` und Self-Healing-Backoff.
- **MQTT & Discovery Guard:** Echtes `retain=True`, automatischer Refresh bei HA-Neustart und Schutz vor `unregister` beim App-Stopp.

## Installation in Home Assistant
1. Klicke oben auf **Add Repository** oder füge unter **Einstellungen → Apps → App-Store → Repositories** folgende URL hinzu:
   ```text
   https://github.com/tobi-freu/mtecmqtt
   ```
2. **MTECmqtt Bridge** installieren, unter **Konfiguration** (`MODBUS_IP`, `MQTT_*`) einrichten und starten.

## Entwicklung & Releases
- **CI ([.github/workflows/ci.yaml](.github/workflows/ci.yaml)):** App-Linter, YAML-/Translations-/Python-Checks und Multi-Arch-Build inkl. `--self-test`.
- **Release ([.github/workflows/release.yaml](.github/workflows/release.yaml)):** Veröffentlicht bei einem GitHub Release Multi-Arch-Images auf `ghcr.io/tobi-freu/mtecmqtt` (für vorgefertigte Images nach dem ersten öffentlichen Release `# image:` in [mtecmqtt/config.yaml](mtecmqtt/config.yaml) aktivieren).

## Dokumentation & Lizenz
- [App-Dokumentation](mtecmqtt/DOCS.md) · [Changelog](mtecmqtt/CHANGELOG.md) · [Architektur (v3)](docs/refactoring-v3.md) · [MIT-Lizenz](LICENSE)
