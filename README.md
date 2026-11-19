# MTECmqtt Bridge – Home Assistant App Repository

[![Open your Home Assistant instance and show the add add-on repository dialog with a specific repository URL pre-filled.](https://my.home-assistant.io/badges/supervisor_add_addon_repository.svg)](https://my.home-assistant.io/redirect/supervisor_add_addon_repository/?repository_url=https%3A%2F%2Fgithub.com%2Ftobi-freu%2Fmtecmqtt)

Dieses Repository stellt die Home Assistant App **MTECmqtt Bridge** (`mtecmqtt`) für **M-TEC Energy Butler** Hybrid-Wechselrichter (OEM: Wattsonic / Solinteg Gen3) bereit. Sie kapselt [croedel/MTECmqtt](https://github.com/croedel/MTECmqtt) in einem gehärteten Python-Wrapper und erweitert die Bridge um vollständige, strikt lesende (`FC 0x03`) Register-Abdeckung, Fehler-Persistenz und stabile Modbus-RTU-over-TCP-Kommunikation.

---

## Enthaltene App

- **[MTECmqtt Bridge (`mtecmqtt/`)](mtecmqtt/README.md)** – Aktuelle Version: `3.1.1` (`amd64` & `aarch64`)
  - **Vollständige Read-Only Register-Abdeckung (`173` HA-Entitäten):** Geräte-Identität, Firmware (`device.sw_version` inkl. OTA-Erkennung), aktive Fault-/ARM-/BMS-Bitmasken, aggregierter `binary_sensor.inverter_problem`, DC-Bus, Zähler-/Energiezähler, Shadow Scan (`25020`), Backup-/Peak-Shaving-/EMS-Parameter, BMS-Limits und Zeitplan-Zusammenfassung (`53006–53048`).
  - **Modbus Stability Guard für den M-TEC WLAN-Dongle:** Inter-Cluster- & Inter-Group-Pacing (`50 ms` / `250 ms`), TCP-Keepalive, sofortiger Socket-Reset bei Lesefehlern, `MODBUS_CLOSE_AFTER_CYCLE` (Ephemeral Socket Mode), Self-Healing-Backoff und Watchdog-Exit.
  - **MQTT & HA Discovery Guard:** Echtes `retain=True` für State- & Attribut-Topics, Sofort-Refresh bei Home-Assistant-Birth-Message und Schutz vor destruktivem `unregister` beim Container-Stopp.
  - **Sichere Konfiguration & Dependency-Pinning:** Typsichere YAML-Generierung aus `/data/options.json`, gepinnte Bibliotheken (`pymodbus==3.8.3`, `paho-mqtt==2.1.0`) und Build-Time-Selbsttest (`python3 /mtec_runner.py --self-test`).

---

## Installation in Home Assistant

1. Klicke oben auf den Button **Add Repository** oder öffne in Home Assistant:
   **Einstellungen → Apps → App-Store → Drei-Punkte-Menü (oben rechts) → Repositories**.
2. Füge folgende Repository-URL hinzu:
   ```text
   https://github.com/tobi-freu/mtecmqtt
   ```
3. Lade die App-Liste neu, wähle **MTECmqtt Bridge** aus und installiere die App.
4. Konfiguriere `MODBUS_IP`, `MQTT_SERVER`, `MQTT_LOGIN` und `MQTT_PASSWORD` im Reiter **Konfiguration** (Details siehe [mtecmqtt/DOCS.md](mtecmqtt/DOCS.md)) und starte die App.

---

## Entwicklung & Release-Workflow (GitHub CI/CD)

### Repository-Struktur
```text
.
├── .github/
│   ├── ISSUE_TEMPLATE/bug_report.md
│   ├── workflows/
│   │   ├── ci.yaml          # App-Linter, YAML/Python-Checks & Multi-Arch Build-Test
│   │   └── release.yaml     # Multi-Arch Build & Push nach ghcr.io bei GitHub Releases
│   └── dependabot.yml       # Automatische Updates für GitHub Actions & Dockerfile
├── docs/
│   └── refactoring-v3.md    # Architektur- & Refactoring-Dokumentation (v3.0.0)
├── mtecmqtt/                # Home Assistant App (Slug: mtecmqtt)
│   ├── translations/        # Supervisor UI-Übersetzungen (de.yaml, en.yaml)
│   ├── CHANGELOG.md
│   ├── config.yaml
│   ├── DOCS.md
│   ├── Dockerfile
│   ├── mtec_runner.py
│   ├── README.md
│   ├── registers_extended.yaml
│   └── run.sh
├── LICENSE
├── README.md
└── repository.yaml
```

### Automatisierte Prüfungen & Container-Releases
1. **Continuous Integration ([.github/workflows/ci.yaml](.github/workflows/ci.yaml)):**
   - Läuft bei jedem `push` auf `main` und bei Pull Requests.
   - Prüft die App-Konfiguration mit `frenck/action-app-linter@v2`, validiert alle YAML-Dateien, prüft die Konsistenz von `config.yaml` und `translations/{de,en}.yaml`, kompiliert `mtec_runner.py` und baut das Multi-Arch-Image (`linux/amd64`, `linux/aarch64`) inkl. `RUN python3 /mtec_runner.py --self-test` (ohne Push).
2. **Releases auf `ghcr.io` ([.github/workflows/release.yaml](.github/workflows/release.yaml)):**
   - Erhöhe `version:` in [mtecmqtt/config.yaml](mtecmqtt/config.yaml) synchron mit [mtecmqtt/CHANGELOG.md](mtecmqtt/CHANGELOG.md) und erstelle ein GitHub Release (Tag z. B. `v3.1.0` oder `3.1.0`).
   - Der Release-Workflow baut automatisch ein Multi-Arch-Manifest für `linux/amd64` und `linux/aarch64` und pusht es nach `ghcr.io/tobi-freu/mtecmqtt`.
   - **Wichtiger Hinweis vor Aktivierung von `image:`:** Stelle sicher, dass das Paket in GitHub Packages (`ghcr.io/tobi-freu/mtecmqtt`) auf **Public** steht, und entferne danach das `#` vor `# image: "ghcr.io/tobi-freu/mtecmqtt"` in [mtecmqtt/config.yaml](mtecmqtt/config.yaml), damit Home Assistant vorgefertigte Images statt eines lokalen Builds nutzt.

---

## Dokumentation & Lizenz

- App-Dokumentation: [mtecmqtt/DOCS.md](mtecmqtt/DOCS.md)
- Änderungsprotokoll: [mtecmqtt/CHANGELOG.md](mtecmqtt/CHANGELOG.md)
- Architektur & Hintergründe (v3.0.0): [docs/refactoring-v3.md](docs/refactoring-v3.md)
- Lizenz: [MIT](LICENSE) (Upstream-Basis: [croedel/MTECmqtt](https://github.com/croedel/MTECmqtt))
