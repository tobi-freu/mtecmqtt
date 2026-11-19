#!/usr/bin/env python3
"""
MTECmqtt Bridge Runner & Stability Layer (v3.0.0)
-------------------------------------------------
Kapselt croedel/MTECmqtt für maximale Stabilität im Home Assistant Add-on-Betrieb:
1. Typsichere Generierung von /data/config.yaml aus /data/options.json (ohne Shell/YAML-Injection).
2. Versionsagnostischer Adapter für pymodbus (3.5.x bis 3.9+) und paho-mqtt (v1.x & v2.x).
3. Modbus Stability Guard für den M-TEC Energy Butler Espressif WLAN-Dongle:
   - Inter-Cluster-Pacing (50 ms) & Inter-Group-Pacing (250 ms) gegen UART/Puffer-Überlauf
   - TCP Keepalive (SO_KEEPALIVE, TCP_KEEPIDLE=10, TCP_KEEPINTVL=5, TCP_KEEPCNT=3)
   - Sofortiger Socket-Reset & Buffer-Flush bei Lesefehlern (verhindert Timeout-Poisoning)
   - Ephemeral Socket Mode (MODBUS_CLOSE_AFTER_CYCLE): schließt Port 502 sauber nach jedem Zyklus
   - Self-Healing Backoff (60s Cooldown nach 3 Fehlzyklen, Exit-Code 1 nach 10 Fehlzyklen für HA Watchdog)
4. MQTT & Home Assistant Discovery Guard:
   - Persistente MQTT-Verbindung mit echtem retain=True (verhindert 'unknown'-Sensoren nach HA-Neustart)
   - Nicht-blockierender Birth-Message-Handler inkl. Sofort-Refresh für day/total/config
   - Unterbindet das destruktive Deregistrieren (send_unregister_info) bei Container-Stopp (SIGTERM)
5. Option REFRESH_ALL_NOW_GROUPS zum Auslesen aller Echtzeit-Untergruppen in jedem Zyklus.
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

NOW_EXT_GROUPS = [
    "now-grid",
    "now-inverter",
    "now-backup",
    "now-battery",
    "now-pv",
]


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
        "MODBUS_IP": str(modbus.get("MODBUS_IP", "192.168.178.47")).strip(),
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
            TCP_USER_TIMEOUT = getattr(socket, "TCP_USER_TIMEOUT", 18)
            if sys.platform.startswith("linux"):
                sock.setsockopt(socket.IPPROTO_TCP, TCP_USER_TIMEOUT, 15000)
    except Exception as exc:
        logging.debug("TCP-Keepalive konnte nicht vollständig gesetzt werden: %s", exc)


# ============================================================================
# 3. Runtime-Patching für MTECmodbusAPI, mqtt & HassIntegration
# ============================================================================
def apply_runtime_patches() -> None:
    """Klinkt die Stabilitäts- und Kompatibilitäts-Wrapper in die geladenen mtecmqtt-Module ein."""
    ensure_pymodbus_compatibility()

    import mtecmqtt.config as mtec_cfg_mod
    import mtecmqtt.hass_int as mtec_hass_mod
    import mtecmqtt.MTECmodbusAPI as mtec_api_mod
    import mtecmqtt.mtec_mqtt as mtec_main_mod
    import mtecmqtt.mqtt as mtec_mqtt_mod
    from pymodbus.client import ModbusTcpClient

    cfg = mtec_cfg_mod.cfg
    register_map = mtec_cfg_mod.register_map
    API = mtec_api_mod.MTECmodbusAPI

    # ------------------------------------------------------------------------
    # A. Modbus Stability Guard (MTECmodbusAPI)
    # ------------------------------------------------------------------------
    orig_init = API.__init__

    def guarded_init(self: Any) -> None:
        orig_init(self)
        self._last_request_ts: float = 0.0
        self._cluster_read_aborted: bool = False
        self._has_connected_once: bool = False

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
                    offset += item["length"]
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
        Publiziert über den persistenten MQTT-Client und setzt retain=True für State- &
        Discovery-Topics, damit nach einem HA-Neustart keine 'unknown'-Entitäten entstehen.
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
    # C. Home Assistant Discovery Guard & Clean Read Wrapper
    # ------------------------------------------------------------------------
    def safe_send_unregister_info(self: Any) -> None:
        logging.info(
            "Skipping Home Assistant unregister on shutdown (preserving discovery entities & retained states)"
        )

    mtec_hass_mod.HassIntegration.send_unregister_info = safe_send_unregister_info

    orig_read_mtec_data = mtec_main_mod.read_MTEC_data

    def safe_read_mtec_data(api: Any, group: str) -> dict[str, Any] | None:
        pvdata = orig_read_mtec_data(api, group)
        if not pvdata or getattr(api, "_cluster_read_aborted", False):
            return None
        return pvdata

    mtec_main_mod.read_MTEC_data = safe_read_mtec_data


# ============================================================================
# 4. Gehärtete Hauptschleife (Ephemeral Socket, Pacing, Backoff, Watchdog)
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
        "Starting MTECmqtt Bridge v3.0.0 (MODBUS=%s:%s, close_after_cycle=%s, refresh_all_now_groups=%s)",
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
    topic_base = f"{cfg['MQTT_TOPIC']}/{serial_no}/"
    if hass and not hass.is_initialized:
        hass.initialize(serial_no)

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
# 5. Integrierter Build-Time & Laufzeit-Selbsttest (--self-test)
# ============================================================================
def run_self_test() -> int:
    """
    Verifiziert zur Docker-Build-Zeit (und bei lokalen Tests):
    1. Sichere YAML-Konfigurationsgenerierung (inkl. Sonderzeichen im Passwort).
    2. Kompatibilität mit der installierten pymodbus- und paho-mqtt-Version.
    3. Korrektes Einhängen aller Wrapper-Hooks (Modbus Stability Guard, Retain, Unregister-Schutz).
    """
    logging.info("Running MTECmqtt Bridge v3.0.0 self-test...")
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
                    "MODBUS_IP": "192.168.178.47",
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

            apply_runtime_patches()

            import mtecmqtt.config as mtec_cfg_mod
            import mtecmqtt.hass_int as mtec_hass_mod
            import mtecmqtt.MTECmodbusAPI as mtec_api_mod
            import mtecmqtt.mtec_mqtt as mtec_main_mod
            import mtecmqtt.mqtt as mtec_mqtt_mod

            # 2. Test: Register-Map & Gruppen vollständig geladen
            assert len(mtec_cfg_mod.register_map) > 50, "register_map unexpectedly small"
            for grp in ["config", "now-base", "day", "total"] + NOW_EXT_GROUPS:
                assert grp in mtec_cfg_mod.register_groups, f"Missing register group {grp}"

            # 3. Test: Framer-Auflösung & ModbusTcpClient-Instanziierung (ohne Netzwerkzugriff)
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

            # 5. Test: HassIntegration Unregister-Schutz & Discovery-Generierung
            published_discovery: list[tuple[str, Any]] = []
            orig_pub = mtec_hass_mod.mqtt_publish
            mtec_hass_mod.mqtt_publish = lambda topic, payload, **_kw: published_discovery.append((topic, payload))
            try:
                hass = mtec_hass_mod.HassIntegration()
                hass.initialize("TEST_SERIAL_12345")
                assert hass.is_initialized is True
                assert len(hass.devices_array) >= 75, f"Expected >=75 HA entities, got {len(hass.devices_array)}"
                assert len(published_discovery) == len(hass.devices_array)

                published_discovery.clear()
                hass.send_unregister_info()  # Darf keine leeren Payloads senden
                assert len(published_discovery) == 0, "send_unregister_info must not publish empty payloads!"
            finally:
                mtec_hass_mod.mqtt_publish = orig_pub

            # 6. Test: MTECmodbusAPI Guard-Methoden vorhanden
            api = mtec_api_mod.MTECmodbusAPI()
            assert hasattr(api, "_last_request_ts")
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
