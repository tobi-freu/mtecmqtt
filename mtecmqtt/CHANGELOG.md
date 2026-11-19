# Changelog

## 3.1.0

### Neu
- **Vollständige Read-Only Register-Abdeckung (`registers_extended.yaml`):** Erweiterung auf insgesamt **173** Home-Assistant-Entitäten (`entity_category: diagnostic`, alle standardmäßig aktiviert, 100 % Abwärtskompatibilität für alle 81 Bestands-Entitäten).
- **Live-Fault-Bitmasken & Problem-Persistenz:** Auslesen und Dekodieren von `10110` (Operation Flags), `10112–10124` (Inverter Fault Flags 1–3 & Slots), `18000–18010` (ARM Fault Flags) sowie `33016–33018` (BMS Error/Warning Code) inklusive JSON-Attributen (`raw_hex`, `raw_dec`, `active_bits`, `active_flags`).
- **Gruppenübergreifender `binary_sensor.inverter_problem`:** Aggregiert aktive Störungen über `now-base`, `now-inverter` und `now-battery` hinweg (inkl. `fault_count`, `active_faults`, `last_fault_at`), ohne `10110` oder `10124` fälschlich als Fehler zu werten.
- **Geräte-Identität, Firmware & OTA-Tracking:** Dekodierung von Equipment-Info (`10008`), Master/DSP-Firmware (`10011–10014` → `device.sw_version`), Safety-Version (`10015`), Grid Standard (`10104`), Output/Protocol-Registern (`10009–10010`) und ISO-RTC (`10100–10102`) inkl. automatischem Discovery-Update bei Firmware-Änderung.
- **Netz-, Bus-, Energie- & BMS-Diagnosen:** Meter-Summen (`11002`, `11004`), DC-Bus-Spannungen (`11036–11037`), AC-Phasensumme (`inverter_ac_power_phase_sum`), zusätzliche Tages-/Gesamt-Energiezähler (`11018`, `11020`, `31006`, `31008`, `31114`, `31118`), Batterie-Metadaten (`32000–32006`), BMS-Status (`33002`), Zell-IDs (`33008–33014`) sowie BMS-Spannungs-/Stromlimits (`33020–33023`).
- **Schutz-, Shadow-Scan-, EMS- & Zeitplan-Sichtbarkeit (`config`, rein lesend):** `25000–25020` (inkl. `shadow_scan`), `50001–50011` (`eps_backup_switch`, `off_grid_asymmetric_switch`, `peak_shaving_switch`, `max_grid_power_setting`), `50202–50210` (`ems_*`), `52500–52501`, `52600–52603` sowie kompakte Zeitplan-Zusammenfassung `schedule_summary` (`53006–53048`).
- **GitHub App-Repository & CI/CD:** Standardkonforme Repository-Struktur (`repository.yaml`, `mtecmqtt/`, `translations/{de,en}.yaml`, `DOCS.md`, `CHANGELOG.md`, GitHub Actions Linter/Multi-Arch Build & GHCR Release-Workflow).

### Behoben
- **Upstream `STR`-Slicing-Bug:** Korrigiert das Auslesen von String-Registern (`offset : offset + length`), damit `serial_no` (`10000`) im geclusterten `10000–10015`-Block exakt 16 Zeichen behält.
- **Cluster-Sortierung & Frame-Limit:** Numerische Sortierung statt String-Sortierung, Begrenzung auf max. `45` Register pro Frame und strikter Verzicht auf Lückenüberbrückung zu unmapped `0xFFFF`- oder Exception-Adressen.
- **Sentinel-Filterung:** Filtert `0xFFFF` / `0xFFFFFFFF` sauber für vorzeichenlose `U16`/`U32`-Register ohne Gruppenabbruch und ohne Verfälschung negativer `I16`/`I32`-Werte.
- **HA Discovery Hygiene:** Entfernt leere `unit_of_measurement: ""`-Einträge und ergänzt `entity_category`, `icon` sowie `json_attributes_topic`.

## 3.0.0

### Neu
- Einführung von `mtec_runner.py` als zentralem Stabilitäts- und Kompatibilitäts-Layer für `croedel/MTECmqtt` (Commit `71cdde40`).
- **Modbus Stability Guard:** Inter-Cluster- (`50 ms`) und Inter-Group-Pacing (`250 ms`), TCP-Keepalive, sofortiger Socket-Reset bei Lesefehlern, optionaler `MODBUS_CLOSE_AFTER_CYCLE`-Modus, `REFRESH_ALL_NOW_GROUPS` sowie Self-Healing-Backoff inkl. Watchdog-Exit.
- **MQTT & HA Discovery Guard:** Echtes `retain=True`, nicht-blockierender Birth-Message-Refresh und Schutz vor `send_unregister_info()` beim Container-Stopp.
- **Build-Time-Selbsttest:** Deterministisches Dependency-Pinning (`pymodbus==3.8.3`, `paho-mqtt==2.1.0`, `pyyaml==6.0.2`, `requests==2.32.3`) und Verifikation per `python3 /mtec_runner.py --self-test`.
