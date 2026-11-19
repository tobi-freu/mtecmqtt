#!/usr/bin/env python3
"""
MTECmqtt Bridge Runner & Stability Layer (v3.1.0)
-------------------------------------------------
Kapselt croedel/MTECmqtt für maximale Stabilität und vollständige Register-Abdeckung
im Home Assistant App/Add-on-Betrieb:
1. Typsichere Generierung von /data/config.yaml aus /data/options.json (ohne Shell/YAML-Injection).
2. Versionsagnostischer Adapter für pymodbus (3.5.x bis 3.9+) und paho-mqtt (v1.x & v2.x).
3. Modbus Stability Guard für den M-TEC Energy Butler Espressif WLAN-Dongle:
   - Inter-Cluster-Pacing (50 ms) & Inter-Group-Pacing (250 ms) gegen UART/Puffer-Überlauf
   - TCP Keepalive (SO_KEEPALIVE, TCP_KEEPIDLE=10, TCP_KEEPINTVL=5, TCP_KEEPCNT=3)
   - Sofortiger Socket-Reset & Buffer-Flush bei Lesefehlern (verhindert Timeout-Poisoning)
   - Ephemeral Socket Mode (MODBUS_CLOSE_AFTER_CYCLE): schließt Port 502 sauber nach jedem Zyklus
   - Self-Healing Backoff (60s Cooldown nach 3 Fehlzyklen, Exit-Code 1 nach 10 Fehlzyklen für HA Watchdog)
4. Vollständiger Read-Only Register-Layer (registers_extended.yaml):
   - Fix für Upstream STR-Slicing-Bug & numerische Cluster-Sortierung (max. 45 Register/Frame)
   - Sentinel-Filter (0xFFFF / 0xFFFFFFFF ausschließlich für vorzeichenlose U16/U32)
   - Dekoder für Firmware-Version, Equipment-Info, Fault-/ARM-/BMS-Bitmasken, Shadow Scan & Zeitplan
   - Gruppenübergreifender Fault-Aggregator (binary_sensor.inverter_problem + JSON-Attribute)
5. MQTT & Home Assistant Discovery Guard:
   - Persistente MQTT-Verbindung mit echtem retain=True (verhindert 'unknown'-Sensoren nach HA-Neustart)
   - Nicht-blockierender Birth-Message-Handler inkl. Sofort-Refresh für day/total/config
   - Erweiterte Discovery-Metadaten (device.sw_version, entity_category, json_attributes_topic, icon)
   - Unterbindet das destruktive Deregistrieren (send_unregister_info) bei Container-Stopp (SIGTERM)
"""

from __future__ import annotations

import inspect
import json
import logging
import os
import signal
import socket
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta
from typing import Any

import yaml

# ============================================================================
# Globale Laufzeit-Steuerung & Konstanten
# ============================================================================
APP_VERSION = "3.1.0"
FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(format=FORMAT, datefmt="%Y-%m-%d %H:%M:%S", level=logging.INFO)

_STOP_EVENT = threading.Event()
_FORCE_FULL_REFRESH = threading.Event()
_ACTIVE_MQTT_CLIENT: Any = None
_MQTT_CONNECTED = threading.Event()

# Timing-Parameter für den M-TEC Espressif WLAN-Dongle (RTU-over-TCP)
MODBUS_CLUSTER_DELAY_SEC = 0.05       # 50 ms Pause zwischen Register-Clustern innerhalb einer Gruppe
MODBUS_GROUP_DELAY_SEC = 0.25         # 250 ms Pause zwischen aufeinanderfolgenden Register-Gruppen
MODBUS_ERROR_RECOVERY_SEC = 2.0       # 2 s Pause vor Reconnect nach Lesefehler
MODBUS_COOLDOWN_AFTER_FAILS = 3       # Nach N Fehlzyklen in Folge -> Cooldown
MODBUS_COOLDOWN_SEC = 60              # 60 s Cooldown zum Abbau von Half-Open Sockets
MODBUS_MAX_CONSECUTIVE_FAILS = 10     # Nach N Fehlzyklen -> Exit(1) für HA Watchdog
MAX_CLUSTER_LENGTH = 45               # Sichere maximale Blockgröße pro Modbus RTU-over-TCP Frame

NOW_EXT_GROUPS = [
    "now-grid",
    "now-inverter",
    "now-backup",
    "now-battery",
    "now-pv",
]

# ============================================================================
# Nachschlage- & Bitmasken-Tabellen (M-TEC / Wattsonic / Solinteg Gen3)
# ============================================================================
EQUIPMENT_MODELS: dict[tuple[int, int], str] = {
    # Three Phase Hybrid 4-20K (hi = 30)
    (30, 0): "4.0K-25A-3P",
    (30, 1): "5.0K-25A-3P",
    (30, 2): "6.0K-25A-3P",
    (30, 3): "8.0K-25A-3P",
    (30, 4): "10K-25A-3P",
    (30, 5): "12K-25A-3P",
    (30, 6): "10K-40A-3P",
    (30, 7): "12K-40A-3P",
    (30, 8): "15K-40A-3P",
    (30, 9): "20K-40A-3P",
    # Single Phase Hybrid (hi = 31)
    (31, 0): "3.0K-30A-1P",
    (31, 1): "3.6K-30A-1P",
    (31, 2): "4.2K-30A-1P",
    (31, 3): "4.6K-30A-1P",
    (31, 4): "5.0K-30A-1P",
    (31, 5): "6.0K-30A-1P",
    (31, 6): "7.0K-30A-1P",
    (31, 7): "8.0K-30A-1P",
    (31, 8): "3.0K-30A-1P-S",
    (31, 9): "3.6K-30A-1P-S",
    # Three Phase Hybrid 25-50K (hi = 32)
    (32, 0): "25K-100A-3P",
    (32, 1): "30K-100A-3P",
    (32, 2): "36K-100A-3P",
    (32, 3): "40K-100A-3P",
    (32, 4): "50K-100A-3P",
    # M2HT Series (hi = 40..43)
    (40, 0): "M2HT-3K-30",
    (40, 1): "M2HT-3.6K-30",
    (40, 2): "M2HT-4.2K-30",
    (40, 3): "M2HT-4.6K-30",
    (40, 4): "M2HT-5K-30",
    (40, 5): "M2HT-6K-30",
    (41, 0): "M2HT-25K-150",
    (41, 1): "M2HT-29.9K-150",
    (41, 2): "M2HT-30K-150",
    (41, 3): "M2HT-40K-150",
    (41, 4): "M2HT-50K-150",
    (42, 3): "M2HT-10K-30",
    (43, 0): "M2HT-75K-300",
    (43, 1): "M2HT-80K-300",
    (43, 2): "M2HT-99K-300",
    (43, 3): "M2HT-100K-300",
    (43, 4): "M2HT-110K-300",
    (43, 5): "M2HT-125K-300",
}

GRID_STANDARDS: dict[int, str] = {
    12: "Germany (VDE-AR-N 4105)",
}

BATTERY_TYPES: dict[int, str] = {
    2: "EMS",
    10: "Wattsonic Li-HV",
    14: "Reserved",
}

SHADOW_SCAN_STATES: dict[int, str] = {
    0: "Off",
    1: "MPPT1",
    2: "MPPT2",
    3: "On",
}

BITMASK_TABLES: dict[str, dict[int, str]] = {
    "operation_flags": {
        0: "WorkMode Abn.",
        1: "Emergency Stop",
        2: "DC Abn.",
        3: "Mains Abn.",
        4: "OffGrid Dis.",
        5: "Batt. Abn.",
        6: "Cmd Stop",
        7: "SocLow&NoPV",
        8: "ComErr Slave",
        9: "Meter Abn.",
        10: "Bypass Wait",
        11: "NPD Standby",
        12: "Generator Abn.",
        13: "S14 Undefined",
        14: "OffGrid",
        15: "NPD Clearing",
        16: "Cmd PLim",
        17: "OFreq PLim",
        18: "OTemp PLim",
        19: "OCurr PLim",
        20: "Reactive PLim",
        21: "Exp PLim",
        22: "Slow Loading",
        23: "OVolt PLim",
        24: "System PLim",
        25: "EMS CmdLim",
        26: "S27 Undefined",
        27: "S28 Undefined",
        28: "S29 Undefined",
        29: "S30 Undefined",
        30: "S31 Undefined",
        31: "PV PLim",
    },
    "fault_flag_1": {
        0: "Mains Lost",
        1: "Grid Voltage Fault",
        2: "Grid Frequency Fault",
        3: "DCI Fault",
        4: "ISO Over Limitation",
        5: "GFCI Fault",
        6: "PV Over Voltage",
        7: "Bus Voltage Fault",
        8: "Inverter Over Temperature",
    },
    "fault_flag_2": {
        1: "SPI Fault",
        2: "E2 Fault",
        3: "GFCI Device Fault",
        4: "AC Transducer Fault",
        5: "Relay Check Fail",
        6: "Internal Fan Fault",
        7: "External Fan Fault",
    },
    "fault_flag_3": {
        0: "Bus Hardware Fault",
        1: "PV Power Low",
        2: "Battery Voltage Fault",
        3: "Backup Voltage Fault",
        4: "Bus Voltage Low",
        5: "System Hardware Fault",
        6: "Backup Over Power",
        7: "Inverter Over Voltage",
        8: "Inverter Over Frequency",
        9: "Inverter Over Current",
        10: "Phase Order Error",
    },
    "arm_fault_flag_1": {
        0: "SCI Fault",
        1: "FLASH Fault",
        2: "Meter Comm Fault",
        3: "BMS Comm Fault",
    },
    "arm_fault_flag_2": {
        0: "BMS Comm Fault",
    },
    "bms_error_code": {
        0: "Internal COM Fault",
        1: "Voltage Sensor Fault",
        2: "Temperature Sensor Fault",
        3: "Relay Fault",
        4: "Cells Damage Fault",
    },
    "bms_warning_code": {
        0: "Cells Low Voltage Warning",
        1: "Cells High Voltage Warning",
        2: "Battery Module Discharge Low Voltage Warning",
        3: "Battery Module Charge Over Voltage Warning",
        4: "Charge Low Temperature Warning",
        5: "Charge Over Temperature Warning",
        6: "Discharge Low Temperature Warning",
        7: "Discharge Over Temperature Warning",
        8: "Battery Module Charge Over-current Warning",
        9: "Battery Module Discharge Over Current Warning",
        10: "Battery Module Low Voltage Warning",
        11: "Battery Module Over Voltage Warning",
        12: "Power Terminal Over Temperature Warning",
        13: "Ambient Low Temperature Warning",
        14: "Ambient Over Temperature Warning",
    },
    "raw_u32": {},
}


# ============================================================================
# 1. Sichere Konfigurations-Erzeugung (/data/options.json -> /data/config.yaml)
# ============================================================================
def load_addon_options(options_path: str = "/data/options.json") -> dict[str, Any]:
    """Liest /data/options.json fehlertolerant ein und ergänzt sichere Standardwerte."""
    raw_opts: dict[str, Any] = {}
    if os.path.isfile(options_path):
        try:
            with open(options_path, "r", encoding="utf-8") as fh:
                loaded = json.load(fh)
                if isinstance(loaded, dict):
                    raw_opts = loaded
        except Exception as exc:
            logging.warning("Konnte %s nicht lesen (%s) - nutze Standardwerte.", options_path, exc)

    modbus = raw_opts.get("modbus") if isinstance(raw_opts.get("modbus"), dict) else {}
    mqtt = raw_opts.get("mqtt") if isinstance(raw_opts.get("mqtt"), dict) else {}
    refresh = raw_opts.get("refresh") if isinstance(raw_opts.get("refresh"), dict) else {}
    advanced = raw_opts.get("advanced") if isinstance(raw_opts.get("advanced"), dict) else {}

    modbus_port = int(modbus.get("MODBUS_PORT", 502))
    config_data: dict[str, Any] = {
        # Modbus-Einstellungen
        "MODBUS_IP": str(modbus.get("MODBUS_IP", "192.168.1.100")).strip(),
        "MODBUS_PORT": modbus_port,
        "MODBUS_PORT2": modbus_port,
        "MODBUS_SLAVE": int(modbus.get("MODBUS_SLAVE", 252)),
        "MODBUS_TIMEOUT": int(modbus.get("MODBUS_TIMEOUT", 8)),
        "MODBUS_RETRIES": int(modbus.get("MODBUS_RETRIES", 2)),
        "MODBUS_FRAMER": str(modbus.get("MODBUS_FRAMER", "rtu")).strip().lower(),
        "MODBUS_CLOSE_AFTER_CYCLE": bool(modbus.get("MODBUS_CLOSE_AFTER_CYCLE", True)),
        # MQTT-Einstellungen
        "MQTT_DISABLE": False,
        "MQTT_SERVER": str(mqtt.get("MQTT_SERVER", "core-mosquitto")).strip(),
        "MQTT_PORT": int(mqtt.get("MQTT_PORT", 1883)),
        "MQTT_LOGIN": str(mqtt.get("MQTT_LOGIN", "mtec")),
        "MQTT_PASSWORD": str(mqtt.get("MQTT_PASSWORD", "")),
        "MQTT_TOPIC": "MTEC",
        "MQTT_FLOAT_FORMAT": "{:.2f}",
        "MQTT_RETAIN": True,
        # Refresh-Intervalle (Sekunden)
        "REFRESH_NOW": max(5, int(refresh.get("REFRESH_NOW", 30))),
        "REFRESH_ALL_NOW_GROUPS": bool(refresh.get("REFRESH_ALL_NOW_GROUPS", True)),
        "REFRESH_DAY": max(30, int(refresh.get("REFRESH_DAY", 300))),
        "REFRESH_TOTAL": max(60, int(refresh.get("REFRESH_TOTAL", 600))),
        "REFRESH_CONFIG": max(60, int(refresh.get("REFRESH_CONFIG", 3600))),
        # Home Assistant Auto-Discovery
        "HASS_ENABLE": True,
        "HASS_BASE_TOPIC": "homeassistant",
        "HASS_BIRTH_GRACETIME": 5,
        # Diagnose
        "DEBUG": bool(advanced.get("DEBUG", False)),
    }
    return config_data


def write_config_yaml(config_data: dict[str, Any], config_path: str = "/data/config.yaml") -> None:
    """Schreibt die Konfiguration typsicher als YAML-Datei (immun gegen Sonderzeichen)."""
    os.makedirs(os.path.dirname(os.path.abspath(config_path)), exist_ok=True)
    with open(config_path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(config_data, fh, default_flow_style=False, allow_unicode=True, sort_keys=False)


# ============================================================================
# 2. Versionsagnostische Adapter für pymodbus (3.5.x bis 3.9+)
# ============================================================================
def ensure_pymodbus_compatibility() -> None:
    """Stellt sicher, dass historische Imports in MTECmodbusAPI.py in jeder pymodbus-Version funktionieren."""
    import pymodbus.constants as pm_const

    if not hasattr(pm_const, "Endian"):
        class _EndianShim:
            AUTO = "@"
            BIG = ">"
            LITTLE = "<"

        setattr(pm_const, "Endian", _EndianShim)


def resolve_modbus_framer(framer_name: Any) -> Any:
    """
    Wandelt einen Framer-Namen ('rtu', 'socket', 'ascii', 'tls') in das von der
    installierten pymodbus-Version erwartete Objekt (FramerType-Enum, Framer-Klasse oder String) um.
    """
    if not isinstance(framer_name, str):
        return framer_name

    key = framer_name.strip().lower()

    # 1. Moderne pymodbus-Versionen (>= 3.7 / 3.8+): FramerType Enum
    try:
        from pymodbus.framer import FramerType  # type: ignore

        mapping = {
            "rtu": getattr(FramerType, "RTU", None),
            "socket": getattr(FramerType, "SOCKET", None),
            "ascii": getattr(FramerType, "ASCII", None),
            "tls": getattr(FramerType, "TLS", None),
        }
        if mapping.get(key) is not None:
            return mapping[key]
    except ImportError:
        pass

    # 2. Klassische pymodbus-Versionen (3.5.x - 3.6.x): Framer-Klassen aus pymodbus.transaction
    try:
        from pymodbus import transaction as pm_tx  # type: ignore

        tx_mapping = {
            "rtu": getattr(pm_tx, "ModbusRtuFramer", None),
            "socket": getattr(pm_tx, "ModbusSocketFramer", None),
            "ascii": getattr(pm_tx, "ModbusAsciiFramer", None),
            "binary": getattr(pm_tx, "ModbusBinaryFramer", None),
            "tls": getattr(pm_tx, "ModbusTlsFramer", None),
        }
        if tx_mapping.get(key) is not None:
            return tx_mapping[key]
    except ImportError:
        pass

    return key


def _call_with_slave_kwarg(func: Any, slave_id: int, **kwargs: Any) -> Any:
    """
    Ruft eine pymodbus-Methode (read_holding_registers / write_register) versionssicher auf,
    egal ob der Parameter 'slave', 'device_id' oder 'unit' heißt.
    """
    try:
        sig = inspect.signature(func)
        params = sig.parameters
        if "slave" in params:
            kwargs["slave"] = slave_id
        elif "device_id" in params:
            kwargs["device_id"] = slave_id
        elif "unit" in params:
            kwargs["unit"] = slave_id
        else:
            kwargs["slave"] = slave_id
    except (ValueError, TypeError):
        kwargs["slave"] = slave_id
    return func(**kwargs)


def configure_socket_keepalive(sock: Any) -> None:
    """Aktiviert TCP Keepalive auf dem unterliegenden Socket gegen stille Half-Open-Verbindungen."""
    if sock is None:
        return
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if hasattr(socket, "IPPROTO_TCP"):
            if hasattr(socket, "TCP_KEEPIDLE"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 10)
            if hasattr(socket, "TCP_KEEPINTVL"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 5)
            if hasattr(socket, "TCP_KEEPCNT"):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
            tcp_user_timeout = getattr(socket, "TCP_USER_TIMEOUT", 18)
            if sys.platform.startswith("linux"):
                sock.setsockopt(socket.IPPROTO_TCP, tcp_user_timeout, 15000)
    except Exception as exc:
        logging.debug("TCP-Keepalive konnte nicht vollständig gesetzt werden: %s", exc)


# ============================================================================
# 3. Erweiterte Register-Registry, Decoder & Fault-Aggregator
# ============================================================================
def _cap_ha_state(text: str, limit: int = 250) -> str:
    """Begrenzt einen HA-Sensorzustand sicher unter das 255-Zeichen-Limit."""
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def find_extended_registers_path() -> str | None:
    """Sucht registers_extended.yaml im Container-Root oder neben mtec_runner.py."""
    candidates = [
        os.environ.get("MTECMQTT_EXTENDED_REGISTERS"),
        "/registers_extended.yaml",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "registers_extended.yaml"),
    ]
    for candidate in candidates:
        if candidate and os.path.isfile(candidate):
            return candidate
    return None


def _normalize_register_item(raw_item: dict[str, Any]) -> dict[str, Any]:
    """Ergänzt dieselben Standardfelder wie croedel/MTECmqtt.config.init_register_map."""
    item = raw_item.copy()
    item.pop("merge", None)
    defaults: dict[str, Any] = {
        "length": None,
        "type": None,
        "unit": "",
        "scale": 1,
        "writable": False,
        "mqtt": None,
        "group": None,
    }
    for key, default_val in defaults.items():
        if key not in item or item[key] is None:
            item[key] = default_val
    return item


def load_extended_registers(
    register_map: dict[str, Any],
    register_groups: list[str],
    ext_path: str | None = None,
) -> int:
    """
    Lädt registers_extended.yaml und führt neue/erweiterte Register idempotent in
    register_map und register_groups zusammen.
    """
    path = ext_path or find_extended_registers_path()
    if not path:
        raise FileNotFoundError("registers_extended.yaml konnte nicht gefunden werden.")

    with open(path, "r", encoding="utf-8") as fh:
        ext_data = yaml.safe_load(fh) or {}

    if not isinstance(ext_data, dict):
        raise ValueError(f"Ungültiges Format in {path}: Dict erwartet.")

    added_or_merged = 0
    for raw_key, raw_val in ext_data.items():
        if not isinstance(raw_val, dict):
            continue
        key_str = str(raw_key)
        is_merge = bool(raw_val.get("merge"))

        if key_str in register_map and is_merge:
            target = register_map[key_str]
            for field_key, field_val in raw_val.items():
                if field_key != "merge":
                    target[field_key] = field_val
            if target.get("group") and target["group"] not in register_groups:
                register_groups.append(target["group"])
            added_or_merged += 1
        elif key_str not in register_map:
            if not raw_val.get("name"):
                logging.warning("Überspringe ungültigen Register-Eintrag %s (fehlendes name-Feld).", key_str)
                continue
            item = _normalize_register_item(raw_val)
            register_map[key_str] = item
            if item.get("group") and item["group"] not in register_groups:
                register_groups.append(item["group"])
            added_or_merged += 1

    logging.info("Loaded %s extended register entries from %s (total mapped: %s)", added_or_merged, path, len(register_map))
    return added_or_merged


def decode_bitmask_u32(
    raw_val: int,
    table_name: str,
    empty_label: str = "OK",
) -> tuple[str, dict[str, Any]]:
    """Dekodiert eine 32-Bit-Bitmaske in einen lesbaren Zustand und strukturierte Attribute."""
    u32 = int(raw_val) & 0xFFFFFFFF
    table = BITMASK_TABLES.get(table_name, {})
    active_bits: list[int] = []
    active_flags: list[str] = []

    if u32 != 0:
        for bit in range(32):
            if u32 & (1 << bit):
                active_bits.append(bit)
                label = table.get(bit) or f"Bit {bit} (0x{1 << bit:X})"
                active_flags.append(label)

    state_str = empty_label if not active_flags else _cap_ha_state(", ".join(active_flags))
    attrs = {
        "raw_hex": f"0x{u32:08X}",
        "raw_dec": u32,
        "active_bits": active_bits,
        "active_flags": active_flags,
    }
    return state_str, attrs


def decode_schedule_summary(regs: list[int]) -> dict[str, Any]:
    """Dekodiert Register 53006..53048 (Bitmaske + 6 Lade-/Entladeperioden) kompakt für HA."""
    mask = int(regs[0]) & 0xFFFF
    mode_map = {0: "None", 1: "Charge", 2: "Discharge"}
    source_map = {0: "PV", 1: "PV+Grid"}

    periods: list[dict[str, Any]] = []
    active_summaries: list[str] = []

    for idx in range(6):
        base = 1 + idx * 7
        enabled = bool(mask & (1 << idx))
        mode_raw = int(regs[base]) & 0xFFFF
        charge_by_raw = int(regs[base + 1]) & 0xFFFF
        power_limit_raw = int(regs[base + 3]) & 0xFFFF
        start_reg = int(regs[base + 5]) & 0xFFFF
        stop_reg = int(regs[base + 6]) & 0xFFFF

        mode_str = mode_map.get(mode_raw, f"Mode {mode_raw}")
        charge_by_str = source_map.get(charge_by_raw, f"Source {charge_by_raw}")
        power_limit_pct = round(power_limit_raw / 10.0, 1)
        start_time = f"{start_reg >> 8:02d}:{start_reg & 0xFF:02d}"
        stop_time = f"{stop_reg >> 8:02d}:{stop_reg & 0xFF:02d}"

        periods.append(
            {
                "period": idx + 1,
                "enabled": enabled,
                "mode": mode_str,
                "charge_by": charge_by_str,
                "power_limit_pct": power_limit_pct,
                "start_time": start_time,
                "stop_time": stop_time,
            }
        )
        if enabled:
            active_summaries.append(
                f"P{idx + 1} {mode_str}({charge_by_str}) {power_limit_pct:.1f}% {start_time}-{stop_time}"
            )

    state_str = "Disabled" if not active_summaries else _cap_ha_state("; ".join(active_summaries))
    attrs = {
        "enabled_mask_hex": f"0x{mask:04X}",
        "enabled_mask_dec": mask,
        "enabled_periods": [p["period"] for p in periods if p["enabled"]],
        "periods": periods,
    }
    return {"value": state_str, "attributes": attrs}


def decode_custom_register(decoder: str, regs: list[int], item: dict[str, Any]) -> dict[str, Any]:
    """Dekodiert herstellerspezifische Registerformate (Firmware, Modell, RTC, Enums, Bitmasken)."""
    unit = item.get("unit", "")
    name = item["name"]

    if decoder == "fw_master_dsp":
        if len(regs) < 4:
            raise ValueError("fw_master_dsp requires 4 registers")
        ba = [b for r in regs[:4] for b in ((r >> 8) & 0xFF, r & 0xFF)]
        master = f"V{ba[0]}.{ba[1]}.{ba[2]}.{ba[3]}"
        dsp = f"{ba[4]}.{ba[5]}.{ba[6]}.{ba[7]}"
        attrs = {
            "master_version": master,
            "dsp_version": dsp,
            "raw_hex": " ".join(f"{r:04X}" for r in regs[:4]),
        }
        return {"name": name, "value": f"{master}-{dsp}", "unit": unit, "attributes": attrs}

    if decoder == "u16_ver":
        r = regs[0]
        attrs = {"raw_dec": r, "raw_hex": f"0x{r:04X}"}
        return {"name": name, "value": f"{r >> 8}.{r & 0xFF}", "unit": unit, "attributes": attrs}

    if decoder == "rtc_iso":
        if len(regs) < 3:
            raise ValueError("rtc_iso requires 3 registers")
        r1, r2, r3 = regs[0], regs[1], regs[2]
        val = f"{2000 + (r1 >> 8):04d}-{r1 & 0xFF:02d}-{r2 >> 8:02d} {r2 & 0xFF:02d}:{r3 >> 8:02d}:{r3 & 0xFF:02d}"
        return {"name": name, "value": val, "unit": unit}

    if decoder == "equipment_model":
        r = regs[0]
        hi, lo = (r >> 8) & 0xFF, r & 0xFF
        val = EQUIPMENT_MODELS.get((hi, lo), f"Type {hi} Model {lo} (0x{r:04X})")
        attrs = {"raw_dec": r, "raw_hex": f"0x{r:04X}", "type_code": hi, "model_code": lo}
        return {"name": name, "value": val, "unit": unit, "attributes": attrs}

    if decoder == "grid_standard":
        r = regs[0]
        val = GRID_STANDARDS.get(r, f"Code {r}")
        attrs = {"raw_dec": r, "raw_hex": f"0x{r:04X}"}
        return {"name": name, "value": val, "unit": unit, "attributes": attrs}

    if decoder == "battery_type":
        r = regs[0]
        val = BATTERY_TYPES.get(r, f"Type {r}")
        attrs = {"raw_dec": r, "raw_hex": f"0x{r:04X}"}
        return {"name": name, "value": val, "unit": unit, "attributes": attrs}

    if decoder == "shadow_scan":
        r = regs[0]
        val = SHADOW_SCAN_STATES.get(r, f"Bitmask 0x{r:04X}")
        attrs = {
            "raw_dec": r,
            "raw_hex": f"0x{r:04X}",
            "mppt1_enabled": bool(r & 1),
            "mppt2_enabled": bool(r & 2),
        }
        return {"name": name, "value": val, "unit": unit, "attributes": attrs}

    if decoder == "hex_u16":
        r = regs[0]
        hex_str = f"0x{r:04X}"
        return {"name": name, "value": hex_str, "unit": unit, "attributes": {"raw_dec": r, "raw_hex": hex_str}}

    if decoder == "enum_u16":
        r = regs[0]
        enum_map = item.get("enum_map") or {}
        mapped = enum_map.get(r)
        if mapped is None:
            mapped = enum_map.get(str(r))
        val = mapped if mapped is not None else f"Unknown ({r})"
        attrs = {"raw_dec": r, "raw_hex": f"0x{r:04X}"}
        return {"name": name, "value": val, "unit": unit, "attributes": attrs, "raw_value": r}

    if decoder == "bitmask_u32":
        if len(regs) < 2:
            raise ValueError("bitmask_u32 requires 2 registers")
        raw_val = (regs[0] << 16) | regs[1]
        state_str, attrs = decode_bitmask_u32(
            raw_val,
            str(item.get("bitmask", "raw_u32")),
            empty_label=str(item.get("empty_label", "OK")),
        )
        return {"name": name, "value": state_str, "unit": unit, "attributes": attrs, "raw_value": raw_val}

    if decoder == "schedule_summary":
        if len(regs) < 43:
            raise ValueError("schedule_summary requires 43 registers")
        decoded = decode_schedule_summary(regs[:43])
        return {"name": name, "value": decoded["value"], "unit": unit, "attributes": decoded["attributes"]}

    raise ValueError(f"Unknown decoder: {decoder}")


def decode_register_value(rawdata: Any, offset: int, item: dict[str, Any]) -> dict[str, Any] | None:
    """
    Dekodiert ein einzelnes Register bzw. einen Registerblock aus rawdata.registers:
    - Filtert 0xFFFF / 0xFFFFFFFF ausschließlich für vorzeichenlose U16/U32 als Sentinel.
    - Korrigiert den Upstream-STR-Slicing-Bug (exakte Länge statt length*2+1).
    - Unterstützt alle Standardtypen und Custom-Dekoder.
    """
    try:
        length = int(item.get("length") or 1)
        all_regs = getattr(rawdata, "registers", None)
        if all_regs is None or len(all_regs) < offset + length:
            return None

        regs = [int(r) & 0xFFFF for r in all_regs[offset : offset + length]]
        reg_type = item.get("type")
        unit = item.get("unit", "")
        scale = item.get("scale", 1) or 1

        # Sentinel-Check strikt NUR für vorzeichenlose U16 / U32 (niemals für I16 / I32!)
        if reg_type == "U16" and regs[0] == 0xFFFF:
            return {"name": item["name"], "value": None, "unit": unit, "sentinel": True}
        if reg_type == "U32" and length >= 2 and ((regs[0] << 16) | regs[1]) == 0xFFFFFFFF:
            return {"name": item["name"], "value": None, "unit": unit, "sentinel": True}

        decoder = item.get("decoder")
        if decoder:
            return decode_custom_register(str(decoder), regs, item)

        val: Any = None
        if reg_type == "U16":
            val = regs[0]
        elif reg_type == "I16":
            v = regs[0]
            val = v - 0x10000 if v >= 0x8000 else v
        elif reg_type == "U32":
            val = (regs[0] << 16) | regs[1]
        elif reg_type == "I32":
            v = (regs[0] << 16) | regs[1]
            val = v - 0x100000000 if v >= 0x80000000 else v
        elif reg_type == "BYTE":
            if length == 1:
                val = f"{regs[0] >> 8:02d} {regs[0] & 0xFF:02d}"
            elif length == 2:
                val = f"{regs[0] >> 8:02d} {regs[0] & 0xFF:02d}  {regs[1] >> 8:02d} {regs[1] & 0xFF:02d}"
            elif length == 4:
                val = (
                    f"{regs[0] >> 8:02d} {regs[0] & 0xFF:02d} {regs[1] >> 8:02d} {regs[1] & 0xFF:02d}  "
                    f"{regs[2] >> 8:02d} {regs[2] & 0xFF:02d} {regs[3] >> 8:02d} {regs[3] & 0xFF:02d}"
                )
            else:
                val = " ".join(f"{r >> 8:02d} {r & 0xFF:02d}" for r in regs)
        elif reg_type == "BIT":
            val = " ".join(f"{r:08b}" for r in regs)
        elif reg_type == "DAT":
            r1, r2, r3 = regs[0], regs[1], regs[2]
            val = f"{r1 >> 8:02d}-{r1 & 0xFF:02d}-{r2 >> 8:02d} {r2 & 0xFF:02d}:{r3 >> 8:02d}:{r3 & 0xFF:02d}"
        elif reg_type == "STR":
            raw_bytes = b"".join(r.to_bytes(2, "big") for r in regs)
            val = raw_bytes.decode("ascii", errors="ignore").rstrip("\x00 ")
        else:
            logging.error("Unknown type %s to decode", reg_type)
            return None

        if isinstance(val, (int, float)) and not isinstance(val, bool) and val != 0 and scale > 1:
            val = val / scale

        return {"name": item["name"], "value": val, "unit": unit}
    except Exception as exc:
        logging.error("Exception while decoding data: %s", exc)
        return None


class FaultStateTracker:
    """
    Aggregiert aktive Störungs- und Warnregister über now-base, now-inverter und now-battery
    zu einem übergreifenden HA problem binary_sensor ('inverter_problem').
    """

    def __init__(self) -> None:
        self.active_faults: dict[str, str] = {}
        self.last_fault_at: str | None = None

    def reset(self) -> None:
        self.active_faults.clear()
        self.last_fault_at = None

    def snapshot(self) -> tuple[str, tuple[tuple[str, str], ...]]:
        items = tuple(sorted(self.active_faults.items()))
        return ("ON" if items else "OFF"), items

    def update_from_data(self, data: dict[str, Any], register_map: dict[str, Any]) -> bool:
        before = self.snapshot()

        for reg_key, entry in data.items():
            meta = register_map.get(reg_key)
            if not meta or not isinstance(entry, dict) or entry.get("sentinel"):
                continue

            if meta.get("problem_flag") is True:
                raw_val = entry.get("raw_value")
                if raw_val is None and isinstance(entry.get("value"), (int, float)):
                    raw_val = int(entry["value"])
                if raw_val:
                    self.active_faults[reg_key] = f"{meta['name']} ({reg_key}): {entry.get('value')}"
                else:
                    self.active_faults.pop(reg_key, None)

            elif reg_key == "10105":
                if entry.get("value") == 3:
                    self.active_faults[reg_key] = f"{meta['name']} ({reg_key}): fault (3)"
                else:
                    self.active_faults.pop(reg_key, None)

            elif meta.get("problem_enum_values"):
                raw_val = entry.get("raw_value")
                if raw_val in meta["problem_enum_values"]:
                    self.active_faults[reg_key] = f"{meta['name']} ({reg_key}): {entry.get('value')} ({raw_val})"
                else:
                    self.active_faults.pop(reg_key, None)

        if self.active_faults:
            self.last_fault_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        return self.snapshot() != before

    def build_problem_entry(self) -> dict[str, Any]:
        active_list = [self.active_faults[k] for k in sorted(self.active_faults, key=lambda x: int(x) if x.isnumeric() else 0)]
        return {
            "name": "Inverter problem",
            "value": "ON" if active_list else "OFF",
            "unit": "",
            "attributes": {
                "fault_count": len(active_list),
                "active_faults": active_list,
                "last_fault_at": self.last_fault_at,
            },
        }


# ============================================================================
# 4. Runtime-Patching für MTECmodbusAPI, mqtt & HassIntegration
# ============================================================================
def apply_runtime_patches() -> None:
    """Klinkt die Stabilitäts-, Register- und Kompatibilitäts-Wrapper in die geladenen mtecmqtt-Module ein."""
    ensure_pymodbus_compatibility()

    import mtecmqtt.config as mtec_cfg_mod
    import mtecmqtt.hass_int as mtec_hass_mod
    import mtecmqtt.MTECmodbusAPI as mtec_api_mod
    import mtecmqtt.mtec_mqtt as mtec_main_mod
    import mtecmqtt.mqtt as mtec_mqtt_mod
    from pymodbus.client import ModbusTcpClient

    cfg = mtec_cfg_mod.cfg
    register_map = mtec_cfg_mod.register_map
    register_groups = mtec_cfg_mod.register_groups
    load_extended_registers(register_map, register_groups)

    API = mtec_api_mod.MTECmodbusAPI
    fault_tracker = FaultStateTracker()
    setattr(mtec_main_mod, "_fault_tracker", fault_tracker)
    setattr(mtec_main_mod, "_fault_state_changed", False)

    # ------------------------------------------------------------------------
    # A. Modbus Stability Guard, Cluster-Fix & Decoder (MTECmodbusAPI)
    # ------------------------------------------------------------------------
    orig_init = API.__init__

    def guarded_init(self: Any) -> None:
        orig_init(self)
        self._last_request_ts: float = 0.0
        self._cluster_read_aborted: bool = False
        self._has_connected_once: bool = False
        self._cluster_cache = {}

    def guarded_create_register_clusters(self: Any, registers: list[str]) -> list[dict[str, Any]]:
        """Sortiert Register numerisch, begrenzt Frame-Längen auf <= 45 und überbrückt keine Lücken."""
        numeric_regs = sorted(
            [str(reg) for reg in registers if str(reg).isnumeric()],
            key=lambda r: int(r),
        )
        cluster_list: list[dict[str, Any]] = []
        cluster: dict[str, Any] = {"start": 0, "length": 0, "items": []}

        for register in numeric_regs:
            item = register_map.get(register)
            if not item:
                logging.warning("Unknown register: %s - skipped.", register)
                continue
            item_len = int(item.get("length") or 0)
            if item_len <= 0:
                logging.warning("Register %s has invalid length %s - skipped.", register, item_len)
                continue

            reg_addr = int(register)
            if cluster["start"] > 0 and (
                reg_addr != cluster["start"] + cluster["length"]
                or cluster["length"] + item_len > MAX_CLUSTER_LENGTH
            ):
                cluster_list.append(cluster)
                cluster = {"start": 0, "length": 0, "items": []}

            if cluster["start"] == 0:
                cluster = {"start": reg_addr, "length": 0, "items": []}

            cluster["length"] += item_len
            cluster["items"].append(item)

        if cluster["start"] > 0:
            cluster_list.append(cluster)

        return cluster_list

    def guarded_decode_rawdata(self: Any, rawdata: Any, offset: int, item: dict[str, Any]) -> dict[str, Any] | None:
        return decode_register_value(rawdata, offset, item)

    def guarded_connect(
        self: Any,
        ip_addr: str,
        port: int,
        framer: Any,
        timeout: int,
        retries: int,
    ) -> bool:
        resolved_framer = resolve_modbus_framer(framer)
        port_int = int(port)
        logging.debug(
            "Verbinde mit Modbus-Server %s:%s (framer=%r, timeout=%ss, retries=%s)",
            ip_addr,
            port_int,
            resolved_framer,
            timeout,
            retries,
        )
        try:
            if getattr(self, "modbus_client", None) is not None:
                try:
                    self.modbus_client.close()
                except Exception:
                    pass
                self.modbus_client = None

            self.modbus_client = ModbusTcpClient(
                ip_addr,
                port=port_int,
                framer=resolved_framer,
                timeout=timeout,
                retries=retries,
            )
            if self.modbus_client.connect():
                configure_socket_keepalive(getattr(self.modbus_client, "socket", None))
                if not getattr(self, "_has_connected_once", False):
                    logging.info("Successfully connected to server %s:%s", ip_addr, port_int)
                    self._has_connected_once = True
                else:
                    logging.debug("Connected to Modbus server %s:%s", ip_addr, port_int)
                return True
        except Exception as exc:
            logging.error("Exception connecting to Modbus server %s:%s: %s", ip_addr, port_int, exc)

        logging.error("Couldn't connect to server %s:%s", ip_addr, port_int)
        self.modbus_client = None
        return False

    def guarded_disconnect(self: Any) -> None:
        client = getattr(self, "modbus_client", None)
        if client is not None:
            logging.debug("Disconnecting from Modbus server")
            try:
                client.close()
            except Exception as exc:
                logging.debug("Exception while disconnecting: %s", exc)
            self.modbus_client = None

    def guarded_reconnect(self: Any) -> bool:
        """Sofortiger Socket-Reset mit kurzer Erholungspause (ohne 30s-Sperre des Originals)."""
        logging.info("Resetting Modbus TCP connection and flushing receive buffer...")
        self.last_reconnect = datetime.now()
        self.disconnect()
        if _STOP_EVENT.wait(timeout=MODBUS_ERROR_RECOVERY_SEC):
            return False
        if self.connect():
            logging.info("Successfully re-connected to Modbus server")
            return True
        logging.error("Couldn't re-connect to Modbus server")
        return False

    def guarded_read_registers(self: Any, register: int | str, length: int) -> Any:
        """
        Liest ein Register-Cluster mit Inter-Request-Pacing, prüft auf Fehler/Kurz-Antworten
        und führt bei einem Fehler sofort einen sauberen Socket-Reset + 1 Retry durch.
        """
        if getattr(self, "_cluster_read_aborted", False):
            return None

        for attempt in (1, 2):
            if self.modbus_client is None or not self.modbus_client.is_socket_open():
                if not self.connect():
                    self._cluster_read_aborted = True
                    return None

            # Inter-Cluster-Pacing (mindestens 50 ms Abstand zum vorherigen Request)
            last_ts = getattr(self, "_last_request_ts", 0.0)
            if last_ts > 0.0:
                elapsed = time.monotonic() - last_ts
                if elapsed < MODBUS_CLUSTER_DELAY_SEC:
                    time.sleep(MODBUS_CLUSTER_DELAY_SEC - elapsed)

            result = None
            err_msg = None
            try:
                result = _call_with_slave_kwarg(
                    self.modbus_client.read_holding_registers,
                    self.slave,
                    address=int(register),
                    count=int(length),
                )
            except Exception as exc:
                err_msg = f"Exception: {exc}"
            finally:
                self._last_request_ts = time.monotonic()

            if err_msg is None:
                if result is None:
                    err_msg = "Empty response (None)"
                elif hasattr(result, "isError") and result.isError():
                    err_msg = f"Modbus error response: {result}"
                elif not hasattr(result, "registers") or len(result.registers) != int(length):
                    got = len(getattr(result, "registers", []))
                    err_msg = f"Incomplete register count (requested {length}, received {got})"
                else:
                    return result

            if attempt == 1:
                logging.warning(
                    "Read error at register %s (len=%s): %s -> resetting socket & retrying once",
                    register,
                    length,
                    err_msg,
                )
                if not self.reconnect():
                    self._cluster_read_aborted = True
                    return None
            else:
                logging.error(
                    "Error while reading register %s, length %s from pymodbus after retry: %s",
                    register,
                    length,
                    err_msg,
                )
                self.disconnect()
                self._cluster_read_aborted = True
                return None

        return None

    def guarded_read_modbus_data(self: Any, registers: list[str] | None = None) -> dict[str, Any]:
        """
        Erzwingt mindestens 250 ms Pause zwischen aufeinanderfolgenden Gruppen-Abfragen
        und bricht bei einem nicht behebbaren Cluster-Fehler die Gruppe sofort sauber ab.
        """
        self._cluster_read_aborted = False
        data: dict[str, Any] = {}

        last_ts = getattr(self, "_last_request_ts", 0.0)
        if last_ts > 0.0:
            elapsed_group = time.monotonic() - last_ts
            if elapsed_group < MODBUS_GROUP_DELAY_SEC:
                if _STOP_EVENT.wait(timeout=MODBUS_GROUP_DELAY_SEC - elapsed_group):
                    return data

        logging.debug("Retrieving data...")
        if registers is None:
            registers = [reg for reg in register_map if reg.isnumeric()]

        cluster_list = self._get_register_clusters(registers)
        for reg_cluster in cluster_list:
            if self._cluster_read_aborted or _STOP_EVENT.is_set():
                break
            offset = 0
            logging.debug(
                "Fetching data for cluster start %s, length %s, items %s",
                reg_cluster["start"],
                reg_cluster["length"],
                len(reg_cluster["items"]),
            )
            rawdata = self._read_registers(reg_cluster["start"], reg_cluster["length"])
            if rawdata:
                for item in reg_cluster["items"]:
                    if item.get("type"):
                        data_decoded = self._decode_rawdata(rawdata, offset, item)
                        if data_decoded:
                            reg_key = str(reg_cluster["start"] + offset)
                            data[reg_key] = data_decoded
                        else:
                            logging.error(
                                "Decoding error while decoding register %s",
                                reg_cluster["start"] + offset,
                            )
                    offset += int(item["length"])
            else:
                break

        logging.debug("Data retrieval completed")
        return data

    def guarded_write_register(self: Any, register: int | str, value: Any) -> bool:
        item = register_map.get(str(register), None)
        if not item:
            logging.error("Can't write unknown register: %s", register)
            return False
        if not item.get("writable", False):
            logging.error("Can't write register which is marked read-only: %s", register)
            return False

        try:
            if isinstance(value, str):
                value = float(value) if "." in value else int(value)
        except Exception:
            logging.error("Invalid numeric value: %s", value)
            return False

        if item["scale"] > 1:
            value *= item["scale"]

        if self.modbus_client is None or not self.modbus_client.is_socket_open():
            if not self.connect():
                return False

        try:
            result = _call_with_slave_kwarg(
                self.modbus_client.write_register,
                self.slave,
                address=int(register),
                value=int(value),
            )
        except Exception as exc:
            logging.error("Exception while writing register %s to pymodbus: %s", register, exc)
            self.disconnect()
            return False

        if result is None or (hasattr(result, "isError") and result.isError()):
            logging.error("Error while writing register %s to pymodbus", register)
            self.disconnect()
            return False
        return True

    API.__init__ = guarded_init
    API._create_register_clusters = guarded_create_register_clusters
    API._decode_rawdata = guarded_decode_rawdata
    API._connect = guarded_connect
    API.disconnect = guarded_disconnect
    API.reconnect = guarded_reconnect
    API._read_registers = guarded_read_registers
    API.read_modbus_data = guarded_read_modbus_data
    API.write_register = guarded_write_register

    # ------------------------------------------------------------------------
    # B. Versionsagnostischer paho-mqtt Adapter & Retained Publisher
    # ------------------------------------------------------------------------
    def extract_rc(args: tuple[Any, ...], kwargs: dict[str, Any], default: Any = 0) -> Any:
        if "rc" in kwargs:
            return kwargs["rc"]
        if "reason_code" in kwargs:
            return kwargs["reason_code"]
        if len(args) >= 4:
            # VERSION2: (client, userdata, flags, reason_code, properties)
            return args[3]
        if len(args) == 3:
            # VERSION1: (client, userdata, rc)
            return args[2]
        return default

    def _is_rc_success(rc: Any) -> bool:
        if rc == 0:
            return True
        if hasattr(rc, "is_failure"):
            return not rc.is_failure
        if hasattr(rc, "value"):
            return rc.value == 0
        return str(rc).lower() in ("0", "success", "normal disconnection")

    def safe_on_mqtt_connect(*args: Any, **kwargs: Any) -> None:
        client = args[0] if len(args) > 0 else kwargs.get("client")
        userdata = args[1] if len(args) > 1 else kwargs.get("userdata")
        rc = extract_rc(args, kwargs, default=0)
        if _is_rc_success(rc):
            _MQTT_CONNECTED.set()
            logging.info("Connected to MQTT broker")
            if client is not None and userdata is not None:
                try:
                    status_topic = f"{cfg.get('HASS_BASE_TOPIC', 'homeassistant')}/status"
                    client.subscribe(status_topic, qos=0)
                except Exception as exc:
                    logging.warning("Konnte HASS-Status-Topic nicht abonnieren: %s", exc)
        else:
            logging.error("Error while connecting to MQTT broker: rc=%s", rc)

    def safe_on_mqtt_disconnect(*args: Any, **kwargs: Any) -> None:
        _MQTT_CONNECTED.clear()
        rc = extract_rc(args, kwargs, default=0)
        if _STOP_EVENT.is_set() or _is_rc_success(rc):
            logging.info("MQTT broker disconnected cleanly (rc=%s)", rc)
        else:
            logging.warning("MQTT broker disconnected unexpectedly (rc=%s) - auto-reconnect active", rc)

    def safe_on_mqtt_subscribe(*args: Any, **kwargs: Any) -> None:
        mid = args[2] if len(args) > 2 else kwargs.get("mid", "?")
        logging.info("MQTT broker subscribed to mid %s", mid)

    def safe_on_mqtt_message(*args: Any, **kwargs: Any) -> None:
        userdata = args[1] if len(args) > 1 else kwargs.get("userdata")
        message = args[2] if len(args) > 2 else kwargs.get("message")
        if message is None:
            return
        try:
            msg = message.payload.decode("utf-8").strip()
            if msg == "online" and userdata:
                gracetime = int(cfg.get("HASS_BIRTH_GRACETIME", 5))
                logging.info(
                    "Received HASS online message. Scheduling discovery & full refresh in %s sec",
                    gracetime,
                )

                def _delayed_birth_sync() -> None:
                    if _STOP_EVENT.wait(timeout=gracetime):
                        return
                    try:
                        userdata.send_discovery_info()
                        _FORCE_FULL_REFRESH.set()
                    except Exception as exc:
                        logging.warning("Error sending discovery info after HASS birth: %s", exc)

                threading.Thread(target=_delayed_birth_sync, daemon=True).start()
        except Exception as exc:
            logging.warning("Error while handling MQTT message: %s", exc)

    def safe_mqtt_start(hass: Any = None) -> Any:
        global _ACTIVE_MQTT_CLIENT
        mqttcl = getattr(mtec_mqtt_mod, "mqttcl", None)
        if mqttcl is None:
            logging.error("paho-mqtt ist nicht verfügbar!")
            return None
        try:
            if hasattr(mqttcl, "CallbackAPIVersion"):
                client = mqttcl.Client(mqttcl.CallbackAPIVersion.VERSION2)
            else:
                client = mqttcl.Client()

            client.user_data_set(hass)
            if cfg.get("MQTT_LOGIN"):
                client.username_pw_set(cfg["MQTT_LOGIN"], cfg.get("MQTT_PASSWORD", ""))

            client.on_connect = safe_on_mqtt_connect
            client.on_disconnect = safe_on_mqtt_disconnect
            client.on_message = safe_on_mqtt_message
            client.on_subscribe = safe_on_mqtt_subscribe

            if hasattr(client, "reconnect_delay_set"):
                client.reconnect_delay_set(min_delay=1, max_delay=30)

            client.connect(cfg["MQTT_SERVER"], int(cfg["MQTT_PORT"]), keepalive=60)
            client.loop_start()
            _ACTIVE_MQTT_CLIENT = client
            logging.info("MQTT client started")
            return client
        except Exception as exc:
            logging.warning("Couldn't start MQTT: %s", exc)
            return None

    def safe_mqtt_stop(client: Any) -> None:
        global _ACTIVE_MQTT_CLIENT
        _MQTT_CONNECTED.clear()
        _ACTIVE_MQTT_CLIENT = None
        if client is not None:
            try:
                client.loop_stop()
                client.disconnect()
                logging.info("MQTT client stopped")
            except Exception as exc:
                logging.warning("Couldn't stop MQTT: %s", exc)

    def safe_mqtt_publish(topic: str, payload: Any, retain: bool | None = None) -> None:
        """
        Publiziert über den persistenten MQTT-Client und setzt retain=True für State-,
        Attribute- & Discovery-Topics, damit nach einem HA-Neustart keine 'unknown'-Entitäten entstehen.
        """
        if cfg.get("MQTT_DISABLE"):
            logging.info("- %s: %s", topic, payload)
            return

        if retain is None:
            retain = bool(cfg.get("MQTT_RETAIN", True)) and not topic.endswith("/command")

        logging.debug("- %s: %s (retain=%s)", topic, payload, retain)

        client = _ACTIVE_MQTT_CLIENT
        if client is not None and _MQTT_CONNECTED.is_set():
            try:
                info = client.publish(topic, payload=payload, qos=0, retain=retain)
                if getattr(info, "rc", 0) == 0:
                    return
            except Exception as exc:
                logging.debug("Persistent MQTT publish failed (%s), falling back to single", exc)

        publish_mod = getattr(mtec_mqtt_mod, "publish", None)
        if publish_mod is not None:
            auth = None
            if cfg.get("MQTT_LOGIN"):
                auth = {"username": cfg["MQTT_LOGIN"], "password": cfg.get("MQTT_PASSWORD", "")}
            try:
                publish_mod.single(
                    topic,
                    payload=payload,
                    hostname=cfg["MQTT_SERVER"],
                    port=int(cfg["MQTT_PORT"]),
                    auth=auth,
                    retain=retain,
                )
            except Exception as exc:
                logging.error("Couldn't send MQTT command to %s: %s", topic, exc)

    mtec_mqtt_mod.on_mqtt_connect = safe_on_mqtt_connect
    mtec_mqtt_mod.on_mqtt_disconnect = safe_on_mqtt_disconnect
    mtec_mqtt_mod.on_mqtt_subscribe = safe_on_mqtt_subscribe
    mtec_mqtt_mod.on_mqtt_message = safe_on_mqtt_message
    mtec_mqtt_mod.mqtt_start = safe_mqtt_start
    mtec_mqtt_mod.mqtt_stop = safe_mqtt_stop
    mtec_mqtt_mod.mqtt_publish = safe_mqtt_publish

    mtec_hass_mod.mqtt_publish = safe_mqtt_publish
    mtec_main_mod.mqtt_publish = safe_mqtt_publish
    mtec_main_mod.mqtt_start = safe_mqtt_start
    mtec_main_mod.mqtt_stop = safe_mqtt_stop

    # ------------------------------------------------------------------------
    # C. Erweiterte Home Assistant Discovery & Clean Read/Write-Layer
    # ------------------------------------------------------------------------
    def patched_hass_initialize(self: Any, serial_no: str, sw_version: str | None = None) -> None:
        self.serial_no = serial_no
        self.device_info = {
            "identifiers": [self.serial_no],
            "name": "MTEC Energybutler",
            "manufacturer": "MTEC",
            "model": "Energybutler",
            "via_device": "MTECmqtt",
        }
        if sw_version:
            self.device_info["sw_version"] = str(sw_version)
        self.devices_array.clear()
        self._build_devices_array()
        self._build_automation_array()
        self.send_discovery_info()
        self.is_initialized = True

    def patched_hass_update_sw_version(self: Any, sw_version: str | None) -> bool:
        if not sw_version or not self.is_initialized:
            return False
        fw_str = str(sw_version)
        if self.device_info.get("sw_version") == fw_str:
            return False
        self.device_info["sw_version"] = fw_str
        self.devices_array.clear()
        self._build_devices_array()
        self._build_automation_array()
        self.send_discovery_info()
        return True

    def _build_common_discovery(self: Any, item: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        state_topic = f"MTEC/{self.serial_no}/{item['group']}/{item['mqtt']}"
        data_item: dict[str, Any] = {
            "name": item["name"],
            "unique_id": f"MTEC_{item['mqtt']}",
            "state_topic": state_topic,
            "device": self.device_info,
        }
        if item.get("hass_device_class"):
            data_item["device_class"] = item["hass_device_class"]
        if item.get("hass_entity_category"):
            data_item["entity_category"] = item["hass_entity_category"]
        if item.get("hass_icon"):
            data_item["icon"] = item["hass_icon"]
        if "hass_enabled_by_default" in item and item["hass_enabled_by_default"] is not None:
            data_item["enabled_by_default"] = bool(item["hass_enabled_by_default"])
        if item.get("hass_json_attributes"):
            data_item["json_attributes_topic"] = f"{state_topic}/attributes"
        return state_topic, data_item

    def patched_append_sensor(self: Any, item: dict[str, Any]) -> None:
        _state_topic, data_item = _build_common_discovery(self, item)
        if item.get("unit"):
            data_item["unit_of_measurement"] = item["unit"]
        if item.get("hass_value_template"):
            data_item["value_template"] = item["hass_value_template"]
        if item.get("hass_state_class"):
            data_item["state_class"] = item["hass_state_class"]

        topic = f"{cfg['HASS_BASE_TOPIC']}/sensor/MTEC_{item['mqtt']}/config"
        self.devices_array.append([topic, json.dumps(data_item)])

    def patched_append_binary_sensor(self: Any, item: dict[str, Any]) -> None:
        _state_topic, data_item = _build_common_discovery(self, item)
        if item.get("hass_payload_on") is not None:
            data_item["payload_on"] = item["hass_payload_on"]
        if item.get("hass_payload_off") is not None:
            data_item["payload_off"] = item["hass_payload_off"]

        topic = f"{cfg['HASS_BASE_TOPIC']}/binary_sensor/MTEC_{item['mqtt']}/config"
        self.devices_array.append([topic, json.dumps(data_item)])

    def safe_send_unregister_info(self: Any) -> None:
        logging.info(
            "Skipping Home Assistant unregister on shutdown (preserving discovery entities & retained states)"
        )

    mtec_hass_mod.HassIntegration.initialize = patched_hass_initialize
    mtec_hass_mod.HassIntegration.update_sw_version = patched_hass_update_sw_version
    mtec_hass_mod.HassIntegration._append_sensor = patched_append_sensor
    mtec_hass_mod.HassIntegration._append_binary_sensor = patched_append_binary_sensor
    mtec_hass_mod.HassIntegration.send_unregister_info = safe_send_unregister_info

    def _num_val(data_dict: dict[str, Any], key: str) -> float | int | None:
        entry = data_dict.get(key)
        if not isinstance(entry, dict) or entry.get("sentinel"):
            return None
        val = entry.get("value")
        return val if isinstance(val, (int, float)) and not isinstance(val, bool) else None

    def patched_read_mtec_data(api: Any, group: str) -> dict[str, Any] | None:
        logging.info("Reading registers for group: %s", group)
        registers = api.get_register_list(group)
        if not registers:
            return None
        now = datetime.now()
        data = api.read_modbus_data(registers=registers)
        if not data or getattr(api, "_cluster_read_aborted", False):
            return None

        fault_changed = fault_tracker.update_from_data(data, register_map)
        setattr(mtec_main_mod, "_fault_state_changed", fault_changed)

        pvdata: dict[str, Any] = {}
        for register in registers:
            item = register_map[register]
            mqtt_key = item.get("mqtt")
            if not mqtt_key:
                continue

            if register.isnumeric():
                entry = data.get(register)
                if entry is None:
                    logging.debug("Register %s missing from decoded output", register)
                    continue
                if entry.get("sentinel"):
                    logging.debug("Skipping sentinel register %s (%s)", register, mqtt_key)
                    continue
                pvdata[mqtt_key] = entry
                continue

            # Berechnete Pseudo-Register
            calc_val: Any = None
            clamp_negative = True
            if register == "consumption":
                v_inv, v_grid = _num_val(data, "11016"), _num_val(data, "11000")
                if v_inv is not None and v_grid is not None:
                    calc_val = v_inv - v_grid
            elif register == "consumption-day":
                vals = [_num_val(data, k) for k in ("31005", "31001", "31004", "31000", "31003")]
                if all(v is not None for v in vals):
                    calc_val = vals[0] + vals[1] + vals[2] - vals[3] - vals[4]  # type: ignore[operator]
            elif register == "autarky-day":
                v_grid_p = _num_val(data, "31001")
                v_cons = pvdata.get("consumption_day")
                if v_grid_p is not None and isinstance(v_cons, (int, float)):
                    calc_val = 100 * (1 - (v_grid_p / v_cons)) if v_cons > 0 else 0
            elif register == "ownconsumption-day":
                v_feed, v_pv = _num_val(data, "31000"), _num_val(data, "31005")
                if v_feed is not None and v_pv is not None:
                    calc_val = 100 * (1 - v_feed / v_pv) if v_pv > 0 else 0
            elif register == "consumption-total":
                vals = [_num_val(data, k) for k in ("31112", "31104", "31110", "31102", "31108")]
                if all(v is not None for v in vals):
                    calc_val = vals[0] + vals[1] + vals[2] - vals[3] - vals[4]  # type: ignore[operator]
            elif register == "autarky-total":
                v_grid_p = _num_val(data, "31104")
                v_cons = pvdata.get("consumption_total")
                if v_grid_p is not None and isinstance(v_cons, (int, float)):
                    calc_val = 100 * (1 - (v_grid_p / v_cons)) if v_cons > 0 else 0
            elif register == "ownconsumption-total":
                v_feed, v_pv = _num_val(data, "31102"), _num_val(data, "31112")
                if v_feed is not None and v_pv is not None:
                    calc_val = 100 * (1 - v_feed / v_pv) if v_pv > 0 else 0
            elif register == "api-date":
                calc_val = now.strftime("%Y-%m-%d %H:%M:%S")
                clamp_negative = False
            elif register == "inverter-ac-power-phase-sum":
                vals = [_num_val(data, k) for k in ("30236", "30242", "30248")]
                if all(v is not None for v in vals):
                    calc_val = vals[0] + vals[1] + vals[2]  # type: ignore[operator]
                clamp_negative = False
            elif register == "inverter-problem":
                calc_val = fault_tracker.build_problem_entry()
                clamp_negative = False
            else:
                logging.warning("Unknown calculated pseudo-register: %s", register)
                continue

            if calc_val is None:
                logging.warning("Skipped pseudo-register %s due to missing/sentinel operand", register)
                continue
            if clamp_negative and isinstance(calc_val, float) and calc_val < 0:
                calc_val = 0
            pvdata[mqtt_key] = calc_val

        return pvdata

    def patched_write_to_mqtt(pvdata: dict[str, Any], base_topic: str) -> None:
        float_fmt = cfg.get("MQTT_FLOAT_FORMAT", "{:.2f}")
        for param, data in pvdata.items():
            topic = base_topic + param
            attrs: dict[str, Any] | None = None
            raw_val: Any = data

            if isinstance(data, dict):
                if data.get("sentinel") or data.get("value") is None:
                    continue
                raw_val = data["value"]
                if isinstance(data.get("attributes"), dict):
                    attrs = data["attributes"]
            elif data is None:
                continue

            if isinstance(raw_val, bool):
                payload: Any = f"{raw_val:d}"
            elif isinstance(raw_val, float):
                payload = float_fmt.format(raw_val)
            else:
                payload = raw_val

            mtec_main_mod.mqtt_publish(topic, payload)
            if attrs is not None:
                mtec_main_mod.mqtt_publish(f"{topic}/attributes", json.dumps(attrs, ensure_ascii=False))

    mtec_main_mod.read_MTEC_data = patched_read_mtec_data
    mtec_main_mod.write_to_MQTT = patched_write_to_mqtt


# ============================================================================
# 5. Gehärtete Hauptschleife (Ephemeral Socket, Pacing, Backoff, Watchdog)
# ============================================================================
def run_daemon() -> int:
    """Führt den gehärteten MTECmqtt Polling-Loop aus."""
    import mtecmqtt.config as mtec_cfg_mod
    import mtecmqtt.hass_int as mtec_hass_mod
    import mtecmqtt.MTECmodbusAPI as mtec_api_mod
    import mtecmqtt.mtec_mqtt as mtec_main_mod

    cfg = mtec_cfg_mod.cfg
    read_MTEC_data = mtec_main_mod.read_MTEC_data
    write_to_MQTT = mtec_main_mod.write_to_MQTT

    def _on_signal(sig_num: int, _frame: Any) -> None:
        logging.warning("Received signal %s. Initiating immediate graceful shutdown...", sig_num)
        _STOP_EVENT.set()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    if cfg.get("DEBUG"):
        logging.getLogger().setLevel(logging.DEBUG)

    close_after_cycle = bool(cfg.get("MODBUS_CLOSE_AFTER_CYCLE", True))
    refresh_all_now = bool(cfg.get("REFRESH_ALL_NOW_GROUPS", True))
    logging.info(
        "Starting MTECmqtt Bridge v%s (MODBUS=%s:%s, close_after_cycle=%s, refresh_all_now_groups=%s)",
        APP_VERSION,
        cfg["MODBUS_IP"],
        cfg["MODBUS_PORT"],
        close_after_cycle,
        refresh_all_now,
    )

    hass = mtec_hass_mod.HassIntegration() if cfg.get("HASS_ENABLE") else None
    mqttclient = mtec_main_mod.mqtt_start(hass)

    api = mtec_api_mod.MTECmodbusAPI()

    # 1. Initialen Connect + Auslesen der "config"-Gruppe (mit Retries für belegten Dongle-Port)
    pv_config = None
    startup_attempts = 0
    while not _STOP_EVENT.is_set() and not pv_config:
        startup_attempts += 1
        if api.modbus_client is None or not api.modbus_client.is_socket_open():
            if not api.connect():
                if startup_attempts >= 5:
                    logging.critical(
                        "Can't connect to MODBUS server %s:%s after %s attempts - exiting with code 1 for Watchdog",
                        cfg["MODBUS_IP"],
                        cfg["MODBUS_PORT"],
                        startup_attempts,
                    )
                    mtec_main_mod.mqtt_stop(mqttclient)
                    return 1
                logging.warning(
                    "Initial Modbus connection failed (attempt %s/5) - retrying in 15s...",
                    startup_attempts,
                )
                _STOP_EVENT.wait(timeout=15)
                continue

        pv_config = read_MTEC_data(api, "config")
        if not pv_config:
            api.disconnect()
            if startup_attempts >= 5:
                logging.critical(
                    "Couldn't retrieve initial inverter config after %s attempts - exiting with code 1",
                    startup_attempts,
                )
                mtec_main_mod.mqtt_stop(mqttclient)
                return 1
            logging.warning("Couldn't retrieve initial config (attempt %s/5) - retrying in 10s...", startup_attempts)
            _STOP_EVENT.wait(timeout=10)

    if _STOP_EVENT.is_set() or not pv_config:
        api.disconnect()
        mtec_main_mod.mqtt_stop(mqttclient)
        return 0

    serial_no = pv_config["serial_no"]["value"]
    fw_version = pv_config.get("firmware_version", {}).get("value") if isinstance(pv_config.get("firmware_version"), dict) else None
    topic_base = f"{cfg['MQTT_TOPIC']}/{serial_no}/"
    if hass and not hass.is_initialized:
        hass.initialize(serial_no, sw_version=fw_version)

    # Initiale Config-Daten sofort retained publizieren
    write_to_MQTT(pv_config, topic_base + "config/")

    now = datetime.now()
    next_read_config = now + timedelta(seconds=int(cfg["REFRESH_CONFIG"]))
    next_read_day = now      # Sofort im 1. Zyklus lesen
    next_read_total = now    # Sofort im 1. Zyklus lesen
    now_ext_idx = 0
    first_cycle = True
    consecutive_failed_cycles = 0

    def _poll_group(group_name: str) -> bool:
        """Liest eine Registergruppe (inkl. automatischem 250ms Inter-Group-Pacing) und publiziert sie per MQTT."""
        if _STOP_EVENT.is_set():
            return False
        pvdata = read_MTEC_data(api, group_name)
        if pvdata:
            write_to_MQTT(pvdata, f"{topic_base}{group_name}/")
            if group_name == "config" and hass:
                new_fw = (
                    pvdata.get("firmware_version", {}).get("value")
                    if isinstance(pvdata.get("firmware_version"), dict)
                    else None
                )
                if hass.update_sw_version(new_fw):
                    logging.info("Detected inverter firmware change -> updated HA sw_version to %s", new_fw)
            elif group_name in ("now-inverter", "now-battery") and getattr(mtec_main_mod, "_fault_state_changed", False):
                tracker = getattr(mtec_main_mod, "_fault_tracker", None)
                if tracker is not None:
                    write_to_MQTT({"inverter_problem": tracker.build_problem_entry()}, f"{topic_base}now-base/")
            return True
        return False

    # 2. Hauptschleife
    while not _STOP_EVENT.is_set():
        cycle_start = datetime.now()

        # Falls Home Assistant neu gestartet wurde (Birth Message), alle Timer zurücksetzen
        if _FORCE_FULL_REFRESH.is_set():
            _FORCE_FULL_REFRESH.clear()
            logging.info("Triggering immediate refresh of all groups (day, total, config) after HASS Birth")
            next_read_day = cycle_start
            next_read_total = cycle_start
            next_read_config = cycle_start
            first_cycle = True

        # Basis-Gruppe ("now-base") lesen
        base_data = read_MTEC_data(api, "now-base")
        if base_data:
            write_to_MQTT(base_data, topic_base + "now-base/")
            consecutive_failed_cycles = 0
            cycle_ok = True
        else:
            consecutive_failed_cycles += 1
            cycle_ok = False
            logging.warning(
                "Cycle failed to read 'now-base' (consecutive failed cycles: %s/%s)",
                consecutive_failed_cycles,
                MODBUS_MAX_CONSECUTIVE_FAILS,
            )

        # Weitere Gruppen nur abfragen, wenn der Dongle in diesem Zyklus sauber antwortet
        if cycle_ok and not _STOP_EVENT.is_set():
            if refresh_all_now or first_cycle:
                for ext_group in NOW_EXT_GROUPS:
                    if not _poll_group(ext_group):
                        cycle_ok = False
                        break
                if cycle_ok:
                    first_cycle = False
            else:
                ext_group = NOW_EXT_GROUPS[now_ext_idx]
                if not _poll_group(ext_group):
                    cycle_ok = False
                now_ext_idx = 0 if now_ext_idx >= len(NOW_EXT_GROUPS) - 1 else now_ext_idx + 1

        if cycle_ok and not _STOP_EVENT.is_set():
            if next_read_day <= datetime.now():
                if _poll_group("day"):
                    next_read_day = datetime.now() + timedelta(seconds=int(cfg["REFRESH_DAY"]))

            if next_read_total <= datetime.now() and not _STOP_EVENT.is_set():
                if _poll_group("total"):
                    next_read_total = datetime.now() + timedelta(seconds=int(cfg["REFRESH_TOTAL"]))

            if next_read_config <= datetime.now() and not _STOP_EVENT.is_set():
                if _poll_group("config"):
                    next_read_config = datetime.now() + timedelta(seconds=int(cfg["REFRESH_CONFIG"]))

        # Option A: Verbindung nach jedem Abfragezyklus sauber schließen (Ephemeral Socket Mode)
        if close_after_cycle or not cycle_ok:
            api.disconnect()

        # Self-Healing Backoff & Watchdog-Prüfung bei wiederholten Fehlzyklen
        if consecutive_failed_cycles >= MODBUS_MAX_CONSECUTIVE_FAILS:
            logging.critical(
                "Reached %s consecutive failed Modbus cycles - exiting with code 1 to trigger HA Supervisor Watchdog",
                consecutive_failed_cycles,
            )
            api.disconnect()
            mtec_main_mod.mqtt_stop(mqttclient)
            return 1

        if consecutive_failed_cycles > 0 and consecutive_failed_cycles % MODBUS_COOLDOWN_AFTER_FAILS == 0:
            logging.warning(
                "Entering %ss self-healing cooldown (socket closed) to let inverter Wi-Fi dongle clear stuck TCP sessions...",
                MODBUS_COOLDOWN_SEC,
            )
            api.disconnect()
            _STOP_EVENT.wait(timeout=MODBUS_COOLDOWN_SEC)
            continue

        elapsed_cycle = (datetime.now() - cycle_start).total_seconds()
        sleep_sec = max(1.0, float(cfg["REFRESH_NOW"]) - elapsed_cycle)
        logging.debug("Cycle finished in %.2fs - sleeping %.2fs", elapsed_cycle, sleep_sec)
        _STOP_EVENT.wait(timeout=sleep_sec)

    # Sauberer Shutdown bei SIGTERM / SIGINT (ohne HA-Entitäten zu löschen)
    api.disconnect()
    mtec_main_mod.mqtt_stop(mqttclient)
    logging.info("MTECmqtt Bridge stopped cleanly")
    return 0


# ============================================================================
# 6. Integrierter Build-Time & Laufzeit-Selbsttest (--self-test)
# ============================================================================
class _FakeRawRegisters:
    def __init__(self, registers: list[int]) -> None:
        self.registers = registers


def run_self_test() -> int:
    """
    Verifiziert zur Docker-Build-Zeit (und bei lokalen Tests):
    1. Sichere YAML-Konfigurationsgenerierung (inkl. Sonderzeichen im Passwort).
    2. Laden & Zusammenführen von registers_extended.yaml + Rückwärtskompatibilität aller 81 Alt-Entitäten.
    3. STR-Slicing-Fix, Cluster-Grenzen, Sentinel-Filter (vs. negative I16/I32), Bitmasken, Schedule-Decoder.
    4. Gruppenübergreifender Fault-Aggregator (inkl. Ausschluss von 10110 & 10124).
    5. HA Discovery (sw_version, entity_category, json_attributes_topic, kein leeres unit_of_measurement).
    """
    logging.info("Running MTECmqtt Bridge v%s self-test...", APP_VERSION)
    orig_cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            os.chdir(tmpdir)
            opts_file = os.path.join(tmpdir, "options.json")
            cfg_file = os.path.join(tmpdir, "config.yaml")

            # 1. Test: Sonderzeichen-Sicherheit beim YAML-Schreiben
            test_password = 'p@ss"w0rd#with$special:chars\\and spaces'
            sample_options = {
                "modbus": {
                    "MODBUS_IP": "192.168.1.100",
                    "MODBUS_PORT": 502,
                    "MODBUS_SLAVE": 252,
                    "MODBUS_TIMEOUT": 8,
                    "MODBUS_RETRIES": 2,
                    "MODBUS_CLOSE_AFTER_CYCLE": True,
                },
                "mqtt": {
                    "MQTT_SERVER": "core-mosquitto",
                    "MQTT_PORT": 1883,
                    "MQTT_LOGIN": "mtec",
                    "MQTT_PASSWORD": test_password,
                },
                "refresh": {
                    "REFRESH_NOW": 30,
                    "REFRESH_ALL_NOW_GROUPS": True,
                    "REFRESH_DAY": 300,
                    "REFRESH_TOTAL": 600,
                    "REFRESH_CONFIG": 3600,
                },
                "advanced": {"DEBUG": False},
            }
            with open(opts_file, "w", encoding="utf-8") as fh:
                json.dump(sample_options, fh)

            generated = load_addon_options(opts_file)
            write_config_yaml(generated, cfg_file)
            with open(cfg_file, "r", encoding="utf-8") as fh:
                reloaded = yaml.safe_load(fh)
            assert reloaded["MQTT_PASSWORD"] == test_password, "Password roundtrip mismatch!"
            assert reloaded["MODBUS_CLOSE_AFTER_CYCLE"] is True
            assert reloaded["REFRESH_ALL_NOW_GROUPS"] is True

            # Vor Runtime-Patching: Bestands-Entitäten von Upstream-HASS als Regression-Baseline erfassen
            ensure_pymodbus_compatibility()
            import mtecmqtt.config as mtec_cfg_mod
            import mtecmqtt.hass_int as mtec_hass_mod
            import mtecmqtt.MTECmodbusAPI as mtec_api_mod
            import mtecmqtt.mtec_mqtt as mtec_main_mod
            import mtecmqtt.mqtt as mtec_mqtt_mod

            baseline_entities: dict[str, dict[str, Any]] = {}
            for _reg, _item in mtec_cfg_mod.register_map.items():
                if _item.get("group") and any("hass_" in k for k in _item.keys()):
                    baseline_entities[f"MTEC_{_item['mqtt']}"] = {
                        "name": _item["name"],
                        "group": _item["group"],
                        "mqtt": _item["mqtt"],
                        "unit": _item.get("unit", ""),
                        "component": _item.get("hass_component_type", "sensor"),
                    }
            assert len(baseline_entities) == 81, f"Expected 81 baseline HA entities, got {len(baseline_entities)}"

            apply_runtime_patches()

            # 2. Test: Register-Map, Eindeutigkeit der MQTT-Keys & Gruppen
            assert len(mtec_cfg_mod.register_map) >= 130, "register_map unexpectedly small after extend"
            for grp in ["config", "now-base", "day", "total"] + NOW_EXT_GROUPS:
                assert grp in mtec_cfg_mod.register_groups, f"Missing register group {grp}"

            seen_mqtt_keys: set[str] = set()
            for reg_k, reg_v in mtec_cfg_mod.register_map.items():
                if reg_v.get("mqtt"):
                    mk = str(reg_v["mqtt"])
                    assert mk not in seen_mqtt_keys, f"Duplicate mqtt key {mk} on register {reg_k}"
                    seen_mqtt_keys.add(mk)
                # Alle erweiterten Register bleiben strikt read-only
                if reg_k not in ("50000", "25100", "25103", "52502", "52503", "52504", "52505"):
                    assert not reg_v.get("writable", False), f"Register {reg_k} must stay read-only!"

            # 3. Test: Framer-Auflösung & ModbusTcpClient-Instanziierung
            rtu_framer = resolve_modbus_framer("rtu")
            sock_framer = resolve_modbus_framer("socket")
            assert rtu_framer is not None and sock_framer is not None
            from pymodbus.client import ModbusTcpClient

            test_client = ModbusTcpClient("127.0.0.1", port=502, framer=rtu_framer, timeout=5, retries=1)
            assert hasattr(test_client, "read_holding_registers")
            assert hasattr(test_client, "convert_from_registers")

            # 4. Test: paho-mqtt Callbacks mit v1 (3 Args) und v2 (5 Args) Signaturen
            mtec_mqtt_mod.on_mqtt_disconnect(None, None, 0)
            mtec_mqtt_mod.on_mqtt_disconnect(None, None, {}, 0, None)
            mtec_mqtt_mod.on_mqtt_connect(None, None, {}, 0, None)

            # 5. Test: HassIntegration Discovery, Bestands-Regression & sw_version
            published_discovery: list[tuple[str, Any]] = []
            orig_hass_pub = mtec_hass_mod.mqtt_publish
            mtec_hass_mod.mqtt_publish = lambda topic, payload, **_kw: published_discovery.append((topic, payload))
            try:
                hass = mtec_hass_mod.HassIntegration()
                hass.initialize("Z000000012345678", sw_version="V1.0.0.1-27.53.5.0")
                assert hass.is_initialized is True
                assert len(hass.devices_array) >= 140, f"Expected >=140 HA entities, got {len(hass.devices_array)}"
                assert len(published_discovery) == len(hass.devices_array)

                discovered_by_uid: dict[str, dict[str, Any]] = {}
                for d_topic, d_payload in published_discovery:
                    parsed = json.loads(d_payload)
                    assert parsed.get("unit_of_measurement") != "", f"Empty unit_of_measurement in {d_topic}"
                    assert parsed.get("enabled_by_default") is not False, f"Disabled entity in {d_topic}"
                    assert parsed["device"]["sw_version"] == "V1.0.0.1-27.53.5.0"
                    assert parsed["device"]["identifiers"] == ["Z000000012345678"]
                    discovered_by_uid[parsed["unique_id"]] = parsed

                for uid, base in baseline_entities.items():
                    assert uid in discovered_by_uid, f"Baseline entity {uid} missing!"
                    cur = discovered_by_uid[uid]
                    assert cur["name"] == base["name"], f"Name mismatch on {uid}"
                    assert cur["state_topic"] == f"MTEC/Z000000012345678/{base['group']}/{base['mqtt']}"
                    if base["unit"]:
                        assert cur["unit_of_measurement"] == base["unit"]

                prob_disc = discovered_by_uid["MTEC_inverter_problem"]
                assert prob_disc["device_class"] == "problem"
                assert prob_disc["entity_category"] == "diagnostic"
                assert prob_disc["json_attributes_topic"] == "MTEC/Z000000012345678/now-base/inverter_problem/attributes"

                published_discovery.clear()
                hass.send_unregister_info()
                assert len(published_discovery) == 0, "send_unregister_info must not publish empty payloads!"
            finally:
                mtec_hass_mod.mqtt_publish = orig_hass_pub

            # 6. Test: Cluster-Sortierung, Gap-Verbot & MAX_CLUSTER_LENGTH = 45
            api = mtec_api_mod.MTECmodbusAPI()
            assert hasattr(api, "_last_request_ts")
            cfg_clusters = api._create_register_clusters(api.get_register_list("config"))
            assert all(c["length"] <= MAX_CLUSTER_LENGTH for c in cfg_clusters)
            starts = [c["start"] for c in cfg_clusters]
            assert starts == sorted(starts), "Clusters must be numerically sorted!"
            # Verhindert Lücken-Überbrückung zu 0xFFFF / Exception-Bereichen
            for c in cfg_clusters:
                covered = set(range(c["start"], c["start"] + c["length"]))
                for forbidden in (21001, 25004, 25009, 25017, 25021, 32007, 50012, 50211, 52506, 52604):
                    assert forbidden not in covered, f"Cluster {c['start']}.. bridged forbidden register {forbidden}!"

            # 7. Test: STR-Slicing-Regressionstest auf geclustertem Block 10000..10015
            live_id_regs = [
                0x5A30, 0x3030, 0x3030, 0x3030, 0x3132, 0x3334, 0x3536, 0x3738,  # 10000..10007 = Z000000012345678
                0x1E09, 0x0000, 0x0000, 0x0100, 0x0001, 0x1B35, 0x0500, 0x0104,  # 10008..10015
            ]
            raw_id = _FakeRawRegisters(live_id_regs)
            dec_sn = api._decode_rawdata(raw_id, 0, mtec_cfg_mod.register_map["10000"])
            dec_eq = api._decode_rawdata(raw_id, 8, mtec_cfg_mod.register_map["10008"])
            dec_fw = api._decode_rawdata(raw_id, 11, mtec_cfg_mod.register_map["10011"])
            dec_sub = api._decode_rawdata(raw_id, 15, mtec_cfg_mod.register_map["10015"])
            assert dec_sn and dec_sn["value"] == "Z000000012345678", f"Bad serial slice: {dec_sn}"
            assert dec_eq and dec_eq["value"] == "20K-40A-3P", f"Bad equipment info: {dec_eq}"
            assert dec_fw and dec_fw["value"] == "V1.0.0.1-27.53.5.0", f"Bad firmware: {dec_fw}"
            assert dec_sub and dec_sub["value"] == "1.4", f"Bad safety version: {dec_sub}"

            # 8. Test: Sentinel-Filter vs. vorzeichenbehaftetes I16 / I32
            sent_u16 = api._decode_rawdata(_FakeRawRegisters([0xFFFF]), 0, mtec_cfg_mod.register_map["25020"])
            sent_u32 = api._decode_rawdata(_FakeRawRegisters([0xFFFF, 0xFFFF]), 0, mtec_cfg_mod.register_map["11002"])
            neg_i16 = api._decode_rawdata(_FakeRawRegisters([0xFFFF]), 0, mtec_cfg_mod.register_map["50203"])
            neg_i32 = api._decode_rawdata(_FakeRawRegisters([0xFFFF, 0xFE65]), 0, mtec_cfg_mod.register_map["11000"])
            assert sent_u16 and sent_u16.get("sentinel") is True
            assert sent_u32 and sent_u32.get("sentinel") is True
            assert neg_i16 and not neg_i16.get("sentinel") and neg_i16["value"] == -0.01
            assert neg_i32 and not neg_i32.get("sentinel") and neg_i32["value"] == -411

            # 9. Test: Bitmasken, Schedule-Decoder & Cross-Group Fault-Aggregator
            dec_op = api._decode_rawdata(_FakeRawRegisters([0x0040, 0x0000]), 0, mtec_cfg_mod.register_map["10110"])
            assert dec_op and dec_op["value"] == "Slow Loading"

            dec_f1 = api._decode_rawdata(_FakeRawRegisters([0x0000, 0x0010]), 0, mtec_cfg_mod.register_map["10112"])
            assert dec_f1 and dec_f1["value"] == "ISO Over Limitation"

            sched_zero = _FakeRawRegisters([0] * 43)
            dec_s0 = api._decode_rawdata(sched_zero, 0, mtec_cfg_mod.register_map["53006"])
            assert dec_s0 and dec_s0["value"] == "Disabled"

            sched_p1 = [0] * 43
            sched_p1[0] = 0x0001        # Period 1 enabled
            sched_p1[1] = 1             # Charge
            sched_p1[2] = 1             # PV+Grid
            sched_p1[4] = 500           # 50.0%
            sched_p1[6] = (1 << 8) | 0  # 01:00
            sched_p1[7] = (5 << 8) | 0  # 05:00
            dec_s1 = api._decode_rawdata(_FakeRawRegisters(sched_p1), 0, mtec_cfg_mod.register_map["53006"])
            assert dec_s1 and "P1 Charge(PV+Grid) 50.0% 01:00-05:00" in dec_s1["value"]
            assert dec_s1["attributes"]["enabled_periods"] == [1]

            tracker = mtec_main_mod._fault_tracker
            tracker.reset()
            # 10110=Slow Loading & 10124=1 dürfen inverter_problem NICHT auf ON setzen
            tracker.update_from_data(
                {
                    "10105": {"value": 2},
                    "10110": dec_op,
                    "10112": {"value": "OK", "raw_value": 0},
                    "10124": {"value": 1},
                },
                mtec_cfg_mod.register_map,
            )
            assert tracker.build_problem_entry()["value"] == "OFF"

            # Fault in now-inverter (18000=Meter Comm Fault) hält inverter_problem ON über now-base-Reads hinweg
            dec_arm = api._decode_rawdata(_FakeRawRegisters([0x0000, 0x0004]), 0, mtec_cfg_mod.register_map["18000"])
            tracker.update_from_data({"18000": dec_arm}, mtec_cfg_mod.register_map)
            prob_on = tracker.build_problem_entry()
            assert prob_on["value"] == "ON"
            assert prob_on["attributes"]["fault_count"] == 1
            assert "Meter Comm Fault" in prob_on["attributes"]["active_faults"][0]
            assert prob_on["attributes"]["last_fault_at"] is not None

            # now-base ohne aktive Fehler lässt 18000-Fault bestehen
            tracker.update_from_data({"10112": {"value": "OK", "raw_value": 0}}, mtec_cfg_mod.register_map)
            assert tracker.build_problem_entry()["value"] == "ON"

            # Erst klares 18000=0 schaltet inverter_problem wieder auf OFF (last_fault_at bleibt erhalten)
            tracker.update_from_data({"18000": {"value": "OK", "raw_value": 0}}, mtec_cfg_mod.register_map)
            prob_off = tracker.build_problem_entry()
            assert prob_off["value"] == "OFF"
            assert prob_off["attributes"]["last_fault_at"] is not None

            # 10. Test: write_to_MQTT überspringt Sentinels & publiziert Attribute
            sent_msgs: list[tuple[str, Any]] = []
            orig_main_pub = mtec_main_mod.mqtt_publish
            mtec_main_mod.mqtt_publish = lambda topic, payload, **_kw: sent_msgs.append((topic, payload))
            try:
                mtec_main_mod.write_to_MQTT(
                    {
                        "shadow_scan": sent_u16,
                        "fault_flag_1": dec_f1,
                    },
                    "MTEC/Z000000012345678/config/",
                )
                topics_only = [t for t, _ in sent_msgs]
                assert "MTEC/Z000000012345678/config/shadow_scan" not in topics_only
                assert "MTEC/Z000000012345678/config/fault_flag_1" in topics_only
                assert "MTEC/Z000000012345678/config/fault_flag_1/attributes" in topics_only
            finally:
                mtec_main_mod.mqtt_publish = orig_main_pub

            assert mtec_main_mod.mqtt_publish is mtec_mqtt_mod.mqtt_publish

        finally:
            os.chdir(orig_cwd)

    logging.info("Self-test passed successfully!")
    return 0


def main() -> int:
    if "--self-test" in sys.argv:
        return run_self_test()

    options_path = os.environ.get("MTECMQTT_OPTIONS_PATH", "/data/options.json")
    config_path = os.path.join(os.getcwd(), "config.yaml")

    config_data = load_addon_options(options_path)
    write_config_yaml(config_data, config_path)

    apply_runtime_patches()

    import mtecmqtt.config as mtec_cfg_mod

    mtec_cfg_mod.cfg.update(config_data)
    return run_daemon()


if __name__ == "__main__":
    sys.exit(main())
