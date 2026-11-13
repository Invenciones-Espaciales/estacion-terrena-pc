"""Rocket Telemetry Ground Station Core Application.

Asynchronous FastAPI service for real-time sounding rocket telemetry acquisition,
validation, persistence, and WebSocket broadcasting.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import datetime
import json
import logging
import os
import shutil
import sqlite3
import struct
import sys
import time
import urllib.request
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

try:
    import serial_asyncio
    import uvicorn
    import yaml
    from fastapi import FastAPI, Request, Response, WebSocket, WebSocketDisconnect
    from fastapi.responses import FileResponse, JSONResponse
    from fastapi.staticfiles import StaticFiles
except ModuleNotFoundError as exc:
    missing_mod = exc.name or "required dependencies"
    sys.stderr.write(
        "\n====================================================================\n"
        f"[ERROR] Missing required dependency: '{missing_mod}'\n"
        "The Ground Station requires packages installed in the virtual environment.\n\n"
        "To resolve this issue:\n"
        "  1. Activate the project virtual environment:\n"
        "       source .venv/bin/activate       # On Linux / macOS\n"
        "       .venv\\Scripts\\activate          # On Windows\n\n"
        "  2. If the virtual environment is not initialized yet:\n"
        "       python3 -m venv .venv\n"
        "       source .venv/bin/activate\n"
        "       pip install -r requirements.txt\n\n"
        "  3. Re-run:\n"
        "       python main.py\n"
        "====================================================================\n\n"
    )
    sys.exit(1)

# ============================================================
# WINDOWS / ASYNCIO COMPATIBILITY
# ============================================================

# pyserial-asyncio operates more reliably with WindowsSelectorEventLoopPolicy on Windows
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ============================================================
# PATHS & RUNTIME ENVIRONMENT
# ============================================================

IS_FROZEN = getattr(sys, "frozen", False)

if IS_FROZEN:
    # Executable bundle environment (PyInstaller)
    APP_DIR = Path(sys.executable).resolve().parent
    BUNDLE_DIR = Path(getattr(sys, "_MEIPASS", APP_DIR))
else:
    # Standard Python source execution
    APP_DIR = Path(__file__).resolve().parent
    BUNDLE_DIR = APP_DIR

BASE_DIR = APP_DIR
DATA_DIR = APP_DIR / "data"

# Config files: prioritize external config next to .exe, fallback to bundled defaults
CONFIG_DIR = APP_DIR / "config" if (APP_DIR / "config").exists() else (BUNDLE_DIR / "config")
# Frontend assets: prioritize external override next to .exe, fallback to bundled assets
FRONTEND_DIR = APP_DIR / "frontend" if (APP_DIR / "frontend").exists() else (BUNDLE_DIR / "frontend")

APP_CONFIG_FILE = CONFIG_DIR / "app.yaml"
FRAME_CONFIG_FILE = CONFIG_DIR / "frame.yaml"

DATA_DIR.mkdir(parents=True, exist_ok=True)
TILES_DIR = DATA_DIR / "tiles"
TILES_DIR.mkdir(parents=True, exist_ok=True)
OFFLINE_FALLBACK_FILE = FRONTEND_DIR / "assets" / "tile_offline.png"

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("telemetry")

# ============================================================
# CONFIGURATION DATA STRUCTURES
# ============================================================

@dataclass
class FieldConfig:
    name: str
    type: str
    scale: float = 1.0
    value_offset: float = 0.0
    byte_offset: Optional[int] = None
    byte_length: Optional[int] = None
    values: dict[int, str] = field(default_factory=dict)
    expected_value: Optional[int] = None


@dataclass
class FrameConfig:
    endian: str
    fields: list[FieldConfig]
    sequence_field: Optional[str] = None
    sync: bytes = b""
    crc_type: Optional[str] = None
    crc_size: int = 0
    frame_length: Optional[int] = None
    crc_offset: Optional[int] = None
    crc_endian: Optional[str] = None
    crc_calculate_from: Optional[int] = None
    crc_calculate_to: Optional[int] = None
    crc_polynomial: int = 0x1021
    crc_initial_value: int = 0xFFFF
    crc_final_xor: int = 0

    @property
    def payload_size(self) -> int:
        return sum(field_size(f) for f in self.fields)

    @property
    def frame_size(self) -> int:
        if self.frame_length is not None:
            return self.frame_length

        if all(f.byte_offset is not None for f in self.fields):
            field_end = max(f.byte_offset + field_size(f) for f in self.fields)
            crc_end = (self.crc_offset + self.crc_size) if self.crc_offset is not None else 0
            return max(field_end, crc_end, len(self.sync))

        return len(self.sync) + self.payload_size + self.crc_size


@dataclass
class SerialConfig:
    port: str
    baudrate: int
    reconnect_delay: float


@dataclass
class WebSocketConfig:
    host: str
    port: int


@dataclass
class StorageConfig:
    database: str
    csv: str
    database_enabled: bool = True
    csv_enabled: bool = True
    disable_db_in_demo: bool = True
    disable_csv_in_demo: bool = True


@dataclass
class DemoConfig:
    enabled: bool = False
    save_to_db: bool = False
    save_to_csv: bool = False


@dataclass
class GoNoGoWeatherConfig:
    max_wind_speed_ms: float = 10.0
    max_cloud_cover_pct: float = 75.0
    min_visibility_km: float = 5.0


@dataclass
class GoNoGoConfig:
    required_hold_s: float = 30.0
    altitude_tol_m: float = 5.0
    accel_xy_tol_g: float = 0.1
    accel_z_nominal_g: float = 1.0
    accel_z_tol_g: float = 0.15
    gps_radius_m: float = 5.0
    weather_limits: GoNoGoWeatherConfig = field(default_factory=GoNoGoWeatherConfig)
    manual_checklist: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class AltitudeChartConfig:
    pad_max_m: float = 50.0
    expected_apogee_m: float = 1500.0
    step_m: float = 500.0


@dataclass
class AccelChartConfig:
    min_g: float = -2.0
    max_g: float = 12.0


@dataclass
class VelocityChartConfig:
    min_vz_ms: float = -40.0
    max_vz_ms: float = 160.0
    step_ms: float = 50.0


@dataclass
class ChartsConfig:
    scaling_mode: str = "ratchet"
    altitude: AltitudeChartConfig = field(default_factory=AltitudeChartConfig)
    acceleration: AccelChartConfig = field(default_factory=AccelChartConfig)
    velocity: VelocityChartConfig = field(default_factory=VelocityChartConfig)


@dataclass
class AppConfig:
    serial: SerialConfig
    websocket: WebSocketConfig
    storage: StorageConfig
    gonogo: GoNoGoConfig = field(default_factory=GoNoGoConfig)
    charts: ChartsConfig = field(default_factory=ChartsConfig)
    demo: DemoConfig = field(default_factory=DemoConfig)


# ============================================================
# BINARY TYPES
# ============================================================

# type -> (struct format, size)
TYPE_FORMATS: dict[str, tuple[str, int]] = {
    "uint8": ("B", 1),
    "int8": ("b", 1),
    "uint16": ("H", 2),
    "int16": ("h", 2),
    "uint24": ("", 3),
    "int24": ("", 3),
    "uint32": ("I", 4),
    "int32": ("i", 4),
    "uint64": ("Q", 8),
    "int64": ("q", 8),
    "float32": ("f", 4),
    "float64": ("d", 8),
    "bool": ("?", 1),
    "bytes": ("", 0),
}


# ============================================================
# CONFIG LOADING & VALIDATION
# ============================================================

def load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with path.open("r", encoding="utf-8") as file:
        data = yaml.safe_load(file)

    if not isinstance(data, dict):
        raise ValueError(f"Invalid configuration format: {path}")

    return data


def parse_hex_bytes(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, list):
        return bytes(value)
    if isinstance(value, str):
        value = value.strip().replace(" ", "").replace("0x", "")
        return bytes.fromhex(value) if value else b""
    raise ValueError("The sync field must be a list of bytes or a hexadecimal string.")


def load_app_config() -> AppConfig:
    data = load_yaml(APP_CONFIG_FILE)

    serial_data = data.get("serial", {})
    websocket_data = data.get("websocket", {})
    storage_data = data.get("storage", {})

    serial_config = SerialConfig(
        port=str(serial_data.get("port", "COM6")),
        baudrate=int(serial_data.get("baudrate", 115200)),
        reconnect_delay=float(serial_data.get("reconnect_delay", 3)),
    )

    websocket_config = WebSocketConfig(
        host=str(websocket_data.get("host", "0.0.0.0")),
        port=int(websocket_data.get("port", 8000)),
    )

    storage_config = StorageConfig(
        database=str(storage_data.get("database", str(DATA_DIR / "telemetry.db"))),
        csv=str(storage_data.get("csv", str(DATA_DIR / "telemetry.csv"))),
        database_enabled=bool(storage_data.get("database_enabled", True)),
        csv_enabled=bool(storage_data.get("csv_enabled", True)),
        disable_db_in_demo=bool(storage_data.get("disable_db_in_demo", True)),
        disable_csv_in_demo=bool(storage_data.get("disable_csv_in_demo", True)),
    )

    demo_data = data.get("demo", {})
    demo_config = DemoConfig(
        enabled=bool(demo_data.get("enabled", False)),
        save_to_db=bool(demo_data.get("save_to_db", False)),
        save_to_csv=bool(demo_data.get("save_to_csv", False)),
    )

    gonogo_data = data.get("gonogo", {})
    weather_data = gonogo_data.get("weather_limits", {})

    weather_config = GoNoGoWeatherConfig(
        max_wind_speed_ms=float(weather_data.get("max_wind_speed_ms", 10.0)),
        max_cloud_cover_pct=float(weather_data.get("max_cloud_cover_pct", 75.0)),
        min_visibility_km=float(weather_data.get("min_visibility_km", 5.0)),
    )

    manual_checklist = list(gonogo_data.get("manual_checklist", [
        {"id": "ant_gs", "item": "Antena de GS instalada", "responsible": "Comunicaciones"},
        {"id": "ant_rocket", "item": "Antena del cohete", "responsible": "Mecánica"},
        {"id": "rocket_secured", "item": "Cohete asegurado", "responsible": "Mecánica"},
        {"id": "area_clear", "item": "Área despejada", "responsible": "Operaciones"},
        {"id": "rec_ready", "item": "Recuperación lista", "responsible": "Recuperación"},
        {"id": "weather_check", "item": "Meteorología (<15 km/h)", "responsible": "Control de vuelo"},
        {"id": "wiring_check", "item": "Cableado e ignitores", "responsible": "Mecánica"},
    ]))

    gonogo_config = GoNoGoConfig(
        required_hold_s=float(gonogo_data.get("required_hold_s", 30.0)),
        altitude_tol_m=float(gonogo_data.get("altitude_tol_m", 5.0)),
        accel_xy_tol_g=float(gonogo_data.get("accel_xy_tol_g", 0.1)),
        accel_z_nominal_g=float(gonogo_data.get("accel_z_nominal_g", 1.0)),
        accel_z_tol_g=float(gonogo_data.get("accel_z_tol_g", 0.15)),
        gps_radius_m=float(gonogo_data.get("gps_radius_m", 5.0)),
        weather_limits=weather_config,
        manual_checklist=manual_checklist,
    )

    charts_data = data.get("charts", {})
    alt_data = charts_data.get("altitude", {})
    acc_data = charts_data.get("acceleration", {})
    vel_data = charts_data.get("velocity", {})

    charts_config = ChartsConfig(
        scaling_mode=str(charts_data.get("scaling_mode", "ratchet")),
        altitude=AltitudeChartConfig(
            pad_max_m=float(alt_data.get("pad_max_m", 50.0)),
            expected_apogee_m=float(alt_data.get("expected_apogee_m", 1500.0)),
            step_m=float(alt_data.get("step_m", 500.0)),
        ),
        acceleration=AccelChartConfig(
            min_g=float(acc_data.get("min_g", -2.0)),
            max_g=float(acc_data.get("max_g", 12.0)),
        ),
        velocity=VelocityChartConfig(
            min_vz_ms=float(vel_data.get("min_vz_ms", -40.0)),
            max_vz_ms=float(vel_data.get("max_vz_ms", 160.0)),
            step_ms=float(vel_data.get("step_ms", 50.0)),
        ),
    )

    return AppConfig(
        serial=serial_config,
        websocket=websocket_config,
        storage=storage_config,
        gonogo=gonogo_config,
        charts=charts_config,
        demo=demo_config,
    )


def load_frame_config() -> FrameConfig:
    data = load_yaml(FRAME_CONFIG_FILE)
    protocol_data = data.get("protocol", {})
    frame_data = data.get("frame", {})

    endian_value = str(frame_data.get("byte_order", protocol_data.get("endian", "little"))).lower()
    endian = {"little_endian": "little", "big_endian": "big"}.get(endian_value, endian_value)
    if endian not in ("little", "big"):
        raise ValueError("Byte order must be 'little' or 'big' (little_endian/big_endian accepted).")

    fields_data = data.get("fields", frame_data.get("fields", []))
    if not fields_data:
        raise ValueError("No fields defined in fields configuration.")

    fields: list[FieldConfig] = []
    for item in fields_data:
        if not isinstance(item, dict):
            raise ValueError("Each field in frame.fields must be an object/dict.")

        name = item.get("name")
        field_type = item.get("type")

        if not name:
            raise ValueError("Field missing required 'name' attribute.")

        if field_type not in TYPE_FORMATS:
            raise ValueError(f"Unknown data type '{field_type}' for field '{name}'.")

        declared_length = item.get("length")
        if field_type == "bytes" and declared_length is None:
            raise ValueError(f"Field '{name}' of type bytes requires length declaration.")
        if field_type != "bytes" and (declared_length is not None and int(declared_length) != TYPE_FORMATS[field_type][1]):
            raise ValueError(f"Field '{name}' declares {declared_length} bytes, but type '{field_type}' requires {TYPE_FORMATS[field_type][1]}.")
        if field_type == "bytes" and int(declared_length) <= 0:
            raise ValueError(f"Field '{name}' length must be greater than zero.")

        byte_offset = int(item["offset"]) if item.get("offset") is not None else None
        value_offset = item.get("offset_value", item.get("offset", 0.0) if byte_offset is None else 0.0)

        values = item.get("values", {})
        if not isinstance(values, dict):
            raise ValueError(f"Enumerated values for field '{name}' must be an object/dict.")

        fields.append(
            FieldConfig(
                name=str(name),
                type=str(field_type),
                scale=float(item.get("scale", 1.0)),
                value_offset=float(value_offset),
                byte_offset=byte_offset,
                byte_length=int(declared_length) if declared_length is not None else None,
                values={int(v): str(lbl) for v, lbl in values.items()},
                expected_value=int(item["expected_value"]) if "expected_value" in item else None,
            )
        )

    sequence_field = frame_data.get("sequence_field")
    if sequence_field is None:
        sequence_field = next(
            (fc.name for fc in fields if fc.name in ("frame_number", "sequence")),
            None,
        )

    sync = parse_hex_bytes(frame_data.get("sync"))
    crc_data = frame_data.get("crc", {})
    if not isinstance(crc_data, dict):
        raise ValueError("frame.crc must be an object/dict.")

    crc_enabled = crc_data.get("enabled", True)
    crc_type = crc_data.get("algorithm", crc_data.get("type")) if crc_enabled else None
    crc_size = int(crc_data.get("length", crc_data.get("size", 0))) if crc_enabled else 0
    crc_offset = int(crc_data["offset"]) if crc_data.get("offset") is not None else None

    crc_endian_value = str(crc_data.get("byte_order", endian)).lower()
    crc_endian = {"little_endian": "little", "big_endian": "big"}.get(crc_endian_value, crc_endian_value)
    if crc_endian not in ("little", "big"):
        raise ValueError("CRC byte order must be 'little' or 'big'.")

    crc_algorithm = (
        str(crc_type).upper().replace("-", "").replace("_", "").replace("/", "")
        if crc_type else None
    )
    if crc_algorithm not in (None, "CRC16CCITT", "CRC16CCITTFALSE"):
        raise ValueError(f"CRC algorithm not implemented: {crc_type}")
    if crc_enabled and (crc_data.get("reflect_input", False) or crc_data.get("reflect_output", False)):
        raise ValueError("Reflected bit CRC is not implemented.")

    frame_length_value = frame_data.get("length")
    frame_length = int(frame_length_value) if frame_length_value is not None else None

    config = FrameConfig(
        endian=endian,
        fields=fields,
        sequence_field=sequence_field,
        sync=sync,
        crc_type=crc_type,
        crc_size=crc_size,
        frame_length=frame_length,
        crc_offset=crc_offset,
        crc_endian=crc_endian,
        crc_calculate_from=int(crc_data["calculate_from"]) if "calculate_from" in crc_data else None,
        crc_calculate_to=int(crc_data["calculate_to"]) if "calculate_to" in crc_data else None,
        crc_polynomial=int(crc_data.get("polynomial", 0x1021)),
        crc_initial_value=int(crc_data.get("initial_value", 0xFFFF)),
        crc_final_xor=int(crc_data.get("final_xor", 0)),
    )

    if config.crc_type and config.crc_size <= 0:
        raise ValueError("CRC is enabled but missing length/size.")
    if config.frame_length is not None and config.frame_length <= 0:
        raise ValueError("frame.length must be greater than zero.")

    occupied: list[tuple[int, int]] = []
    for fc in fields:
        if fc.byte_offset is None:
            continue
        end = fc.byte_offset + field_size(fc)
        if fc.byte_offset < 0 or (config.frame_length is not None and end > config.frame_length):
            raise ValueError(f"Field '{fc.name}' exceeds frame boundaries.")
        if any(fc.byte_offset < prev_end and prev_start < end for prev_start, prev_end in occupied):
            raise ValueError(f"Field '{fc.name}' overlaps with another field.")
        occupied.append((fc.byte_offset, end))

    if config.crc_offset is not None:
        crc_end = config.crc_offset + config.crc_size
        if config.crc_offset < 0 or (config.frame_length is not None and crc_end > config.frame_length):
            raise ValueError("CRC exceeds frame boundaries.")
        if any(config.crc_offset < field_end and field_start < crc_end for field_start, field_end in occupied):
            raise ValueError("CRC overlaps with another field.")

    if config.frame_length is not None and not all(fc.byte_offset is not None for fc in fields):
        raise ValueError("All fields must specify an explicit offset when frame.length is defined.")

    logger.info("Frame configured | payload=%d bytes | frame=%d bytes", config.payload_size, config.frame_size)
    return config


def field_size(field_config: FieldConfig) -> int:
    if field_config.type == "bytes":
        return field_config.byte_length or 0
    return TYPE_FORMATS[field_config.type][1]


# ============================================================
# CRC-16 ALGORITHMS & INTEGRITY VERIFICATION
# ============================================================

def crc16_ccitt(
    data: bytes,
    polynomial: int = 0x1021,
    initial_value: int = 0xFFFF,
    final_xor: int = 0,
) -> int:
    crc = initial_value
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ polynomial) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc ^ final_xor


def validate_crc(
    data: bytes,
    crc_bytes: bytes,
    crc_type: Optional[str],
    endian: str,
    polynomial: int = 0x1021,
    initial_value: int = 0xFFFF,
    final_xor: int = 0,
) -> bool:
    if not crc_type:
        return True

    crc_algorithm = (
        crc_type.upper().replace("-", "").replace("_", "").replace("/", "")
    )
    if crc_algorithm in ("CRC16CCITT", "CRC16CCITTFALSE"):
        if len(crc_bytes) != 2:
            return False
        calculated = crc16_ccitt(data, polynomial, initial_value, final_xor)
        received = int.from_bytes(crc_bytes, byteorder=endian)
        return calculated == received

    raise ValueError(f"CRC algorithm not implemented: {crc_type}")


# ============================================================
# FRAME PARSER
# ============================================================

class FrameParser:
    def __init__(self, config: FrameConfig):
        self.config = config
        self.struct_prefix = "<" if config.endian == "little" else ">"

    def parse(self, frame: bytes) -> Optional[dict[str, Any]]:
        expected_size = self.config.frame_size
        if len(frame) != expected_size:
            logger.warning("Invalid frame size: %d, expected %d", len(frame), expected_size)
            return None

        offset = 0

        # Validate SYNC header
        if self.config.sync:
            sync_size = len(self.config.sync)
            received_sync = frame[:sync_size]
            if received_sync != self.config.sync:
                logger.warning("Invalid SYNC byte: %s", received_sync.hex())
                return None
            offset += sync_size

        # Validate CRC Checksum
        if self.config.crc_type and self.config.crc_size:
            crc_offset = (
                self.config.crc_offset
                if self.config.crc_offset is not None
                else len(frame) - self.config.crc_size
            )
            crc_bytes = frame[crc_offset : crc_offset + self.config.crc_size]
            calculate_from = (
                self.config.crc_calculate_from
                if self.config.crc_calculate_from is not None
                else 0
            )
            calculate_to = (
                self.config.crc_calculate_to
                if self.config.crc_calculate_to is not None
                else crc_offset - 1
            )
            if calculate_from < 0 or calculate_to < calculate_from or calculate_to >= len(frame):
                logger.warning("Invalid CRC calculation range")
                return None

            crc_data = frame[calculate_from : calculate_to + 1]
            if not validate_crc(
                crc_data,
                crc_bytes,
                self.config.crc_type,
                self.config.crc_endian or self.config.endian,
                self.config.crc_polynomial,
                self.config.crc_initial_value,
                self.config.crc_final_xor,
            ):
                logger.warning("Invalid CRC checksum")
                return None

        result: dict[str, Any] = {}
        payload_offset = offset

        for field_config in self.config.fields:
            struct_format, _ = TYPE_FORMATS[field_config.type]
            size = field_size(field_config)
            field_offset = (
                field_config.byte_offset
                if field_config.byte_offset is not None
                else payload_offset
            )
            if field_offset < 0 or field_offset + size > len(frame):
                logger.warning("Field exceeds frame boundaries: %s", field_config.name)
                return None

            if field_config.type in ("uint24", "int24"):
                raw_value = int.from_bytes(
                    frame[field_offset : field_offset + size],
                    byteorder=self.config.endian,
                    signed=field_config.type == "int24",
                )
            elif field_config.type == "bytes":
                raw_value = frame[field_offset : field_offset + size].hex()
            else:
                raw_value = struct.unpack_from(
                    self.struct_prefix + struct_format,
                    frame,
                    field_offset,
                )[0]

            if field_config.expected_value is not None and raw_value != field_config.expected_value:
                logger.warning(
                    "Unexpected value for %s: %s, expected %s",
                    field_config.name,
                    raw_value,
                    field_config.expected_value,
                )
                return None

            if field_config.type == "bytes":
                value = raw_value
            else:
                value = raw_value * field_config.scale + field_config.value_offset

            # Preserve integer type when no scaling or offset is applied
            if (
                field_config.scale == 1.0
                and field_config.value_offset == 0.0
                and field_config.type not in ("float32", "float64")
            ):
                value = raw_value

            result[field_config.name] = value
            if field_config.values:
                result[f"{field_config.name}_label"] = field_config.values.get(raw_value)

            if field_config.byte_offset is None:
                payload_offset += size

        return result


# ============================================================
# FRAME BUFFER & RESYNCHRONIZATION
# ============================================================

class FrameBuffer:
    def __init__(self, parser: FrameParser):
        self.parser = parser
        self.buffer = bytearray()
        self.invalid_frames = 0
        self.last_raw_frames: list[bytes] = []
        self.resyncing = False

    def feed(self, data: bytes) -> list[dict[str, Any]]:
        self.buffer.extend(data)
        frames: list[dict[str, Any]] = []
        self.last_raw_frames = []

        frame_size = self.parser.config.frame_size
        sync = self.parser.config.sync
        min_frame_size = frame_size

        while True:
            if len(self.buffer) < min_frame_size:
                break

            # Search SYNC header if configured
            if sync:
                sync_index = self.buffer.find(sync)
                if sync_index == -1:
                    # Retain minimal bytes to detect SYNC split across UART chunks
                    keep = max(0, len(sync) - 1)
                    if keep:
                        self.buffer = self.buffer[-keep:]
                    else:
                        self.buffer.clear()
                    break

                if sync_index > 0:
                    del self.buffer[:sync_index]
                    if len(self.buffer) < min_frame_size:
                        break

            # Extract frame candidate
            candidate_parsed = None
            candidate_size = 0
            raw_frame = None

            if len(self.buffer) >= frame_size:
                raw_cand = bytes(self.buffer[:frame_size])
                cand_parsed = self.parser.parse(raw_cand)
                if cand_parsed is not None:
                    candidate_parsed = cand_parsed
                    candidate_size = frame_size
                    raw_frame = raw_cand

            if candidate_parsed is not None:
                del self.buffer[:candidate_size]
                frames.append(candidate_parsed)
                self.last_raw_frames.append(raw_frame)
                self.resyncing = False
            else:
                if not self.resyncing:
                    self.invalid_frames += 1

                if sync:
                    # Discard first sync byte to advance sliding window and find genuine sync
                    del self.buffer[:len(sync)]
                    self.resyncing = True
                elif self.parser.config.crc_type:
                    # Slide 1 byte forward until recovered via CRC
                    del self.buffer[0]
                    self.resyncing = True
                else:
                    del self.buffer[:min_frame_size]
                    self.resyncing = False

        return frames


# ============================================================
# PACKET STATISTICS
# ============================================================

class PacketStatistics:
    def __init__(self, sequence_field: Optional[str]):
        self.sequence_field = sequence_field
        self.received = 0
        self.lost = 0
        self.invalid = 0
        self.last_sequence: Optional[int] = None

    def register(self, frame: dict[str, Any]):
        self.received += 1
        if not self.sequence_field or self.sequence_field not in frame:
            return

        sequence = frame[self.sequence_field]
        if self.last_sequence is None:
            self.last_sequence = sequence
            return

        expected = self.last_sequence + 1
        if sequence != expected and sequence > expected:
            self.lost += sequence - expected

        self.last_sequence = sequence

    def snapshot(self) -> dict[str, Any]:
        total = self.received + self.lost
        loss_percent = (self.lost / total) * 100 if total else 0.0
        return {
            "received": self.received,
            "frames_received": self.received,
            "lost": self.lost,
            "invalid": self.invalid,
            "loss_percent": loss_percent,
        }

    def reset(self):
        self.received = 0
        self.lost = 0
        self.invalid = 0
        self.last_sequence = None


# ============================================================
# DATABASE (SQLITE)
# ============================================================

class Database:
    TELEMETRY_COLUMNS: tuple[str, ...] = (
        "sync",
        "frame_number",
        "timestamp",
        "status",
        "status_label",
        "pressure",
        "altura_actual",
        "accel_x",
        "accel_y",
        "accel_z",
        "gyro_x",
        "gyro_y",
        "gyro_z",
        "temp_imu",
        "latitude",
        "longitude",
        "altitud_gps",
        "satellites",
        "ground_speed",
        "pin_frenado",
        "pin_frenado_label",
        "pin_principal",
        "pin_principal_label",
    )

    def __init__(self, path: str):
        self.path = Path(path)
        if not self.path.is_absolute():
            self.path = DATA_DIR / self.path if len(self.path.parts) == 1 else BASE_DIR / self.path

        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path, check_same_thread=False)
        self.connection.execute("PRAGMA journal_mode = WAL;")
        self.connection.execute("PRAGMA synchronous = NORMAL;")
        self.connection.execute("PRAGMA cache_size = -64000;")
        self.connection.execute("PRAGMA temp_store = MEMORY;")

        existing_cols = {
            row[1]
            for row in self.connection.execute("PRAGMA table_info(telemetry);").fetchall()
        }
        if existing_cols and ("payload" in existing_cols or "received_at" not in existing_cols):
            self.connection.execute("DROP INDEX IF EXISTS idx_telemetry_timestamp;")
            self.connection.execute("DROP TABLE IF EXISTS telemetry;")

        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                received_at REAL NOT NULL,
                sync INTEGER,
                frame_number INTEGER,
                timestamp INTEGER,
                status INTEGER,
                status_label TEXT,
                pressure REAL,
                altura_actual REAL,
                accel_x REAL,
                accel_y REAL,
                accel_z REAL,
                gyro_x REAL,
                gyro_y REAL,
                gyro_z REAL,
                temp_imu REAL,
                latitude REAL,
                longitude REAL,
                altitud_gps REAL,
                satellites INTEGER,
                ground_speed REAL,
                pin_frenado INTEGER,
                pin_frenado_label TEXT,
                pin_principal INTEGER,
                pin_principal_label TEXT
            )
            """
        )
        self.connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_telemetry_received_at
            ON telemetry(received_at)
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS gonogo_audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                iso_time TEXT NOT NULL,
                action TEXT NOT NULL,
                item_id TEXT,
                item_label TEXT,
                state TEXT NOT NULL,
                operator TEXT,
                details TEXT
            )
            """
        )
        self.connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_gonogo_audit_ts
            ON gonogo_audit_log(timestamp)
            """
        )
        self.connection.commit()

    def insert(self, data: dict[str, Any], sequence_field: Optional[str] = None):
        cols_sql = ", ".join(("received_at", *self.TELEMETRY_COLUMNS))
        placeholders = ", ".join(["?"] * (1 + len(self.TELEMETRY_COLUMNS)))
        values = (time.time(), *(data.get(col) for col in self.TELEMETRY_COLUMNS))
        self.connection.execute(
            f"INSERT INTO telemetry ({cols_sql}) VALUES ({placeholders})",
            values,
        )
        self.connection.commit()

    def insert_gonogo_audit(
        self,
        action: str,
        item_id: Optional[str],
        item_label: Optional[str],
        state: str,
        operator: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        ts = time.time()
        iso = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).isoformat()
        det_str = json.dumps(details, ensure_ascii=False) if details else None
        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT INTO gonogo_audit_log (timestamp, iso_time, action, item_id, item_label, state, operator, details)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (ts, iso, action, item_id, item_label, state, operator, det_str),
        )
        self.connection.commit()
        return {
            "id": cursor.lastrowid,
            "timestamp": ts,
            "iso_time": iso,
            "action": action,
            "item_id": item_id,
            "item_label": item_label,
            "state": state,
            "operator": operator,
            "details": details,
        }

    def get_gonogo_audit_logs(self, limit: int = 100) -> list[dict[str, Any]]:
        try:
            cursor = self.connection.cursor()
            cursor.execute(
                """
                SELECT id, timestamp, iso_time, action, item_id, item_label, state, operator, details
                FROM gonogo_audit_log
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = cursor.fetchall()
            logs = []
            for r in rows:
                det = None
                if r[8]:
                    try:
                        det = json.loads(r[8])
                    except Exception:
                        det = r[8]
                logs.append({
                    "id": r[0],
                    "timestamp": r[1],
                    "iso_time": r[2],
                    "action": r[3],
                    "item_id": r[4],
                    "item_label": r[5],
                    "state": r[6],
                    "operator": r[7],
                    "details": det,
                })
            return logs
        except Exception:
            logger.exception("Error querying Go/No-Go audit logs from SQLite")
            return []

    def get_recent(self, seconds: float = 60.0, limit: int = 1200) -> list[dict[str, Any]]:
        try:
            cursor = self.connection.cursor()
            min_ts = time.time() - max(1.0, seconds)
            cols_sql = ", ".join(("received_at", *self.TELEMETRY_COLUMNS))
            cursor.execute(
                f"""
                SELECT {cols_sql}
                FROM telemetry
                WHERE received_at >= ?
                ORDER BY received_at ASC
                LIMIT ?
                """,
                (min_ts, limit),
            )
            rows = cursor.fetchall()
            if not rows:
                cursor.execute(
                    f"""
                    SELECT {cols_sql}
                    FROM telemetry
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    (min(limit, 300),),
                )
                rows = list(reversed(cursor.fetchall()))

            results = []
            for row in rows:
                payload_data = {
                    col: val
                    for col, val in zip(self.TELEMETRY_COLUMNS, row[1:])
                    if val is not None or not col.endswith("_label")
                }
                results.append({
                    "timestamp": row[0],
                    "sequence": payload_data.get("frame_number"),
                    "data": payload_data,
                    "raw_hex": None,
                    "crc_value": None,
                })
            return results
        except Exception:
            logger.exception("Error querying history from SQLite")
            return []

    def close(self):
        self.connection.close()


# ============================================================
# CSV LOGGER
# ============================================================

class CSVLogger:
    def __init__(self, path: str, fields: list[str]):
        self.path = Path(path)
        if not self.path.is_absolute():
            self.path = DATA_DIR / self.path if len(self.path.parts) == 1 else BASE_DIR / self.path

        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fields = fields
        self.file = self.path.open("a", newline="", encoding="utf-8")
        self.writer = csv.DictWriter(self.file, fieldnames=["received_at", *self.fields])
        if self.path.stat().st_size == 0:
            self.writer.writeheader()
            self.file.flush()

    def write(self, data: dict[str, Any]):
        row = {"received_at": time.time(), **{f: data.get(f) for f in self.fields}}
        self.writer.writerow(row)
        self.file.flush()

    def close(self):
        self.file.close()


class GoNoGoCSVLogger:
    def __init__(self, path: str):
        self.path = Path(path)
        if not self.path.is_absolute():
            self.path = DATA_DIR / self.path if len(self.path.parts) == 1 else BASE_DIR / self.path

        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fields = [
            "timestamp",
            "iso_time",
            "action",
            "item_id",
            "item_label",
            "state",
            "operator",
            "details",
        ]
        self.file = self.path.open("a", newline="", encoding="utf-8")
        self.writer = csv.DictWriter(self.file, fieldnames=self.fields)
        if self.path.stat().st_size == 0:
            self.writer.writeheader()
            self.file.flush()

    def write(self, entry: dict[str, Any]):
        row = {
            "timestamp": entry.get("timestamp", time.time()),
            "iso_time": entry.get("iso_time", datetime.datetime.fromtimestamp(time.time(), tz=datetime.timezone.utc).isoformat()),
            "action": entry.get("action", ""),
            "item_id": entry.get("item_id", ""),
            "item_label": entry.get("item_label", ""),
            "state": entry.get("state", ""),
            "operator": entry.get("operator", ""),
            "details": json.dumps(entry.get("details"), ensure_ascii=False) if entry.get("details") else "",
        }
        self.writer.writerow(row)
        self.file.flush()

    def close(self):
        self.file.close()


# ============================================================
# WEBSOCKET MANAGER
# ============================================================

class WebSocketManager:
    def __init__(self):
        self.clients: set[WebSocket] = set()
        self.lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        async with self.lock:
            self.clients.add(websocket)
        logger.info("WebSocket connected | clients=%d", len(self.clients))

    async def disconnect(self, websocket: WebSocket):
        async with self.lock:
            self.clients.discard(websocket)
        logger.info("WebSocket disconnected | clients=%d", len(self.clients))

    async def broadcast(self, message: dict[str, Any]):
        if not self.clients:
            return
        dead_clients = []
        async with self.lock:
            clients = list(self.clients)

        for websocket in clients:
            try:
                await websocket.send_json(message)
            except Exception:
                dead_clients.append(websocket)

        if dead_clients:
            async with self.lock:
                for websocket in dead_clients:
                    self.clients.discard(websocket)


# ============================================================
# RUNTIME STATE
# ============================================================

@dataclass
class RuntimeState:
    connected: bool = False
    last_frame: Optional[dict[str, Any]] = None
    last_timestamp: Optional[float] = None
    frames_received: int = 0
    last_raw_frame: Optional[str] = None
    last_crc_value: Optional[str] = None
    history: deque = field(default_factory=lambda: deque(maxlen=3600))
    audit_logs: deque[dict[str, Any]] = field(default_factory=lambda: deque(maxlen=500))
    demo_mode: bool = False
    demo_time: float = 0.0
    demo_reset_requested: bool = False
    save_db_override: Optional[bool] = None
    save_csv_override: Optional[bool] = None
    active_serial_port: Optional[str] = None


# ============================================================
# PERSISTENCE & HARDWARE WEAR PROTECTION
# ============================================================

def is_database_saving_enabled() -> bool:
    """Evaluate whether saving to SQLite is active.

    Protects the Raspberry Pi SD card flash memory from wear:
    1. Overridden if explicitly set by CLI flag or API toggle.
    2. Disabled if storage.database_enabled is False.
    3. Disabled if demo_mode is active and demo.save_to_db is False (or disable_db_in_demo is True).
    """
    if runtime.save_db_override is False:
        return False
    if runtime.save_db_override is True:
        return True
    if app_config is None or not app_config.storage.database_enabled:
        return False
    if runtime.demo_mode:
        if app_config.storage.disable_db_in_demo or not app_config.demo.save_to_db:
            return False
    return True


def is_csv_saving_enabled() -> bool:
    """Evaluate whether saving to CSV log files is active.

    Suppresses unnecessary SD card write operations during demo or testing runs.
    """
    if runtime.save_csv_override is False:
        return False
    if runtime.save_csv_override is True:
        return True
    if app_config is None or not app_config.storage.csv_enabled:
        return False
    if runtime.demo_mode:
        if app_config.storage.disable_csv_in_demo or not app_config.demo.save_to_csv:
            return False
    return True


# ============================================================
# GLOBAL INSTANCES
# ============================================================

app_config: Optional[AppConfig] = None
frame_config: Optional[FrameConfig] = None
parser: Optional[FrameParser] = None
frame_buffer: Optional[FrameBuffer] = None
statistics: Optional[PacketStatistics] = None
database: Optional[Database] = None
csv_logger: Optional[CSVLogger] = None
gonogo_csv_logger: Optional[GoNoGoCSVLogger] = None
websocket_manager = WebSocketManager()
runtime = RuntimeState()

background_tasks: set[asyncio.Task] = set()


def create_background_task(coroutine) -> asyncio.Task:
    task = asyncio.create_task(coroutine)
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)

    def log_task_exception(completed_task):
        try:
            completed_task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Error in background task")

    task.add_done_callback(log_task_exception)
    return task


# ============================================================
# FRAME PROCESSING PIPELINE
# ============================================================

async def process_frame(frame: dict[str, Any], raw_frame: Optional[bytes] = None):
    if statistics is None:
        return

    statistics.register(frame)

    # Persist to SQLite only when database saving is active (avoids RPi SD card wear in demo)
    if database is not None and is_database_saving_enabled():
        try:
            database.insert(frame, frame_config.sequence_field if frame_config else None)
        except Exception:
            logger.exception("Error saving telemetry frame to SQLite database")

    # Persist to CSV only when CSV logging is active
    if csv_logger is not None and is_csv_saving_enabled():
        try:
            csv_logger.write(frame)
        except Exception:
            logger.exception("Error writing telemetry frame to CSV log")

    runtime.last_frame = frame
    runtime.last_timestamp = time.time()
    runtime.frames_received += 1

    if raw_frame is not None:
        runtime.last_raw_frame = raw_frame.hex(" ").upper()
        if frame_config is not None and frame_config.crc_size:
            crc_offset = (
                frame_config.crc_offset
                if frame_config.crc_offset is not None
                else len(raw_frame) - frame_config.crc_size
            )
            crc_bytes = raw_frame[crc_offset : crc_offset + frame_config.crc_size]
            runtime.last_crc_value = crc_bytes.hex().upper()

    snapshot = statistics.snapshot()
    snapshot["frames_received"] = runtime.frames_received

    runtime.history.append({
        "timestamp": runtime.last_timestamp,
        "sequence": frame.get(frame_config.sequence_field if frame_config else "frame_number"),
        "data": frame,
        "raw_hex": runtime.last_raw_frame,
        "crc_value": runtime.last_crc_value,
        "frames_received": runtime.frames_received,
    })

    await websocket_manager.broadcast({
        "type": "telemetry",
        "timestamp": runtime.last_timestamp,
        "frame_length": frame_config.frame_size if frame_config is not None else None,
        "crc_valid": True,
        "crc_value": runtime.last_crc_value,
        "raw_hex": runtime.last_raw_frame,
        "data": frame,
        "statistics": snapshot,
    })


# ============================================================
# SERIAL ACQUISITION PROTOCOL
# ============================================================

class SerialProtocol(asyncio.Protocol):
    def __init__(self, loop):
        self.loop = loop
        self.transport = None
        self.disconnected = asyncio.Event()

    def connection_made(self, transport):
        self.transport = transport
        runtime.connected = True
        logger.info("UART interface connected")
        create_background_task(websocket_manager.broadcast({"type": "connection", "connected": True}))

    def data_received(self, data: bytes):
        if frame_buffer is None or statistics is None or not data:
            return

        logger.debug("UART RX: %s", data.hex(" "))
        try:
            frames = frame_buffer.feed(data)
            previous_invalid = statistics.invalid
            statistics.invalid = frame_buffer.invalid_frames

            for frame, raw_frame in zip(frames, frame_buffer.last_raw_frames):
                create_background_task(process_frame(frame, raw_frame))

            if statistics.invalid > previous_invalid:
                create_background_task(
                    websocket_manager.broadcast({
                        "type": "statistics",
                        "statistics": statistics.snapshot(),
                    })
                )
        except Exception:
            logger.exception("Error processing incoming UART data")

    def connection_lost(self, exc):
        runtime.connected = False
        if exc:
            logger.error("UART disconnected | exception=%r (%s)", exc, type(exc).__name__)
        else:
            logger.warning("UART disconnected | normal closure")

        self.disconnected.set()
        create_background_task(websocket_manager.broadcast({"type": "connection", "connected": False}))


def resolve_serial_port(configured_port: str) -> str:
    """Resolve serial port, falling back to auto-discovered candidate if configured port does not exist."""
    try:
        path = Path(configured_port)
        if path.exists():
            return configured_port
    except Exception:
        pass

    try:
        import serial.tools.list_ports
        available = list(serial.tools.list_ports.comports())
        if not available:
            return configured_port

        norm_cfg = str(configured_port).strip().upper()
        for port_info in available:
            if str(port_info.device).strip().upper() == norm_cfg:
                return configured_port

        candidates = [
            p.device for p in available
            if "ACM" in p.device or "USB" in p.device or p.device.upper().startswith("COM")
        ]
        if candidates:
            logger.warning(
                "Configured UART port '%s' not found. Auto-selecting candidate port '%s' (available: %s)",
                configured_port,
                candidates[0],
                [p.device for p in available],
            )
            return candidates[0]

        return available[0].device
    except Exception as exc:
        logger.debug("Serial auto-discovery error: %s", exc)
        return configured_port


async def demo_telemetry_loop():
    """Generates synthetic 290-second flight kinematics at 10 Hz in RAM without serial hardware or disk I/O."""
    runtime.connected = True
    runtime.active_serial_port = "SIMULADOR_DEMO (10 Hz)"
    logger.info("Demo telemetry loop started @ 10 Hz from t=%.1fs (pure RAM generation, SD card writes disabled)", runtime.demo_time)
    await websocket_manager.broadcast({"type": "connection", "connected": True})

    CYCLE_DURATION_S = 290.0
    BASE_LAT = -34.6037
    BASE_LON = -58.3816
    BASE_PRESS = 1013.25
    t = runtime.demo_time
    seq = 1
    boot_time = time.time() - t

    while True:
        try:
            if runtime.demo_reset_requested:
                runtime.demo_reset_requested = False
                t = runtime.demo_time
                seq = 1
                boot_time = time.time() - t
                logger.info("Demo simulation reset: restarted from t=%.1fs (sequence 1)", t)

            now_ms = int((time.time() - boot_time) * 1000) & 0xFFFFFFFF
            t_mod = t % CYCLE_DURATION_S

            if t_mod < 150.0:
                status = 0
            elif t_mod < 154.0:
                status = 1
            elif t_mod < 176.0:
                status = 2
            elif t_mod < 178.0:
                status = 4
            elif t_mod < 180.0:
                status = 8
            elif t_mod < 206.0:
                status = 16
            elif t_mod < 208.0:
                status = 32
            elif t_mod < 226.0:
                status = 64
            else:
                status = 128

            if t_mod < 150.0:
                alt = 0.0
                vz = 0.0
                az = 1.0
                ax = 0.01
                ay = -0.01
                gx = 0.1
                gy = -0.1
                gz = 0.05
                speed = 0.0
                pin_frenado = 0
                pin_principal = 0
            elif t_mod < 154.0:
                tau = t_mod - 150.0
                az = 4.5
                vz = 45.0 * tau
                alt = 0.5 * 45.0 * (tau ** 2)
                ax = 0.1
                ay = -0.05
                gx = 0.5
                gy = -0.3
                gz = 15.0
                speed = vz * 0.1
                pin_frenado = 0
                pin_principal = 0
            elif t_mod < 176.0:
                tau = t_mod - 154.0
                az = -1.0
                vz = max(0.0, 180.0 - 8.18 * tau)
                alt = min(1500.0, 360.0 + 180.0 * tau - 0.5 * 8.18 * (tau ** 2))
                ax = 0.02
                ay = -0.02
                gx = 0.2
                gy = -0.1
                gz = 5.0
                speed = 12.0
                pin_frenado = 0
                pin_principal = 0
            elif t_mod < 206.0:
                tau = t_mod - 176.0
                az = 1.0
                vz = -25.0
                alt = max(400.0, 1500.0 - 36.6 * tau)
                ax = 0.05
                ay = 0.02
                gx = 0.4
                gy = -0.2
                gz = 2.0
                speed = 8.5
                pin_frenado = 1
                pin_principal = 0
            elif t_mod < 226.0:
                tau = t_mod - 206.0
                az = 1.0
                vz = -5.0
                alt = max(0.0, 400.0 - 20.0 * tau)
                ax = 0.01
                ay = 0.01
                gx = 0.1
                gy = -0.1
                gz = 1.0
                speed = 3.5
                pin_frenado = 1
                pin_principal = 1
            else:
                alt = 0.0
                vz = 0.0
                az = 1.0
                ax = 0.0
                ay = 0.0
                gx = 0.0
                gy = 0.0
                gz = 0.0
                speed = 0.0
                pin_frenado = 1
                pin_principal = 1

            press = round(BASE_PRESS - (alt / 8.3), 2)
            lat = BASE_LAT + (alt * 0.0000008)
            lon = BASE_LON + (alt * 0.0000012)

            frame = {
                "sync": 0xAA,
                "frame_number": seq,
                "timestamp": now_ms,
                "status": status,
                "pressure": press,
                "altura_actual": round(alt, 2),
                "accel_x": round(ax, 2),
                "accel_y": round(ay, 2),
                "accel_z": round(az, 2),
                "gyro_x": round(gx, 2),
                "gyro_y": round(gy, 2),
                "gyro_z": round(gz, 2),
                "temp_imu": 28.5,
                "latitude": round(lat, 6),
                "longitude": round(lon, 6),
                "altitud_gps": round(alt + 45.0, 1),
                "satellites": 12,
                "ground_speed": round(speed, 1),
                "pin_frenado": pin_frenado,
                "pin_principal": pin_principal,
            }

            if frame_config is not None:
                for fc in frame_config.fields:
                    if fc.values and fc.name in frame:
                        val = frame[fc.name]
                        frame[f"{fc.name}_label"] = fc.values.get(val, str(val))

            raw_payload = struct.pack(
                "<B H I B f f f f f f f f f f f f B f B B",
                0xAA,
                seq,
                now_ms,
                status,
                press,
                alt,
                ax, ay, az,
                gx, gy, gz,
                28.5,
                lat, lon, alt + 45.0,
                12,
                speed,
                pin_frenado,
                pin_principal,
            )
            crc_val = crc16_ccitt(raw_payload)
            raw_pkt = raw_payload + crc_val.to_bytes(2, byteorder="little")

            await process_frame(frame, raw_pkt)

            seq = (seq + 1) & 0xFFFF
            t += 0.1
            await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            logger.info("Demo telemetry loop cancelled")
            break
        except Exception:
            logger.exception("Error in demo telemetry loop")
            await asyncio.sleep(0.1)


async def serial_loop():
    while True:
        if app_config is None:
            await asyncio.sleep(1)
            continue

        transport = None
        target_port = resolve_serial_port(app_config.serial.port)
        runtime.active_serial_port = target_port
        try:
            logger.info(
                "Connecting UART to %s @ %d baud (configured: %s)",
                target_port,
                app_config.serial.baudrate,
                app_config.serial.port,
            )
            loop = asyncio.get_running_loop()
            transport, protocol = await serial_asyncio.create_serial_connection(
                loop,
                lambda: SerialProtocol(loop),
                target_port,
                baudrate=app_config.serial.baudrate,
            )
            logger.info("UART port '%s' opened successfully", target_port)
            await protocol.disconnected.wait()
        except asyncio.CancelledError:
            logger.info("Serial loop cancelled")
            if transport is not None:
                transport.close()
            raise
        except Exception:
            runtime.connected = False
            logger.exception("UART communication error on port '%s'", target_port)
        finally:
            runtime.connected = False
            if transport is not None:
                try:
                    transport.close()
                except Exception:
                    pass

        logger.info("Retrying UART connection in %.1f seconds", app_config.serial.reconnect_delay)
        await asyncio.sleep(app_config.serial.reconnect_delay)


# ============================================================
# FASTAPI APPLICATION & ENDPOINTS
# ============================================================

app = FastAPI(
    title="Rocket Telemetry Ground Station",
    description="Real-time telemetry acquisition, processing, and monitoring server",
    version="1.0.0",
)

app.mount("/frontend", StaticFiles(directory=FRONTEND_DIR), name="frontend")
if (FRONTEND_DIR / "css").exists():
    app.mount("/css", StaticFiles(directory=FRONTEND_DIR / "css"), name="css")
if (FRONTEND_DIR / "js").exists():
    app.mount("/js", StaticFiles(directory=FRONTEND_DIR / "js"), name="js")
if (FRONTEND_DIR / "assets").exists():
    app.mount("/assets", StaticFiles(directory=FRONTEND_DIR / "assets"), name="assets")


@app.get("/")
@app.get("/index.html")
async def root():
    index_file = FRONTEND_DIR / "index.html"
    if not index_file.exists():
        logger.error("Frontend dashboard not found: %s", index_file)
        return {"error": "Frontend not found", "path": str(index_file)}
    return FileResponse(index_file)


@app.get("/gonogo")
@app.get("/gonogo.html")
async def gonogo():
    gonogo_file = FRONTEND_DIR / "gonogo.html"
    if not gonogo_file.exists():
        logger.error("Go/No-Go view not found: %s, falling back to index.html", gonogo_file)
        return FileResponse(FRONTEND_DIR / "index.html")
    return FileResponse(gonogo_file)


@app.get("/recovery")
@app.get("/recovery.html")
async def recovery():
    recovery_file = FRONTEND_DIR / "recovery.html"
    if not recovery_file.exists():
        logger.error("Recovery view not found: %s, falling back to index.html", recovery_file)
        return FileResponse(FRONTEND_DIR / "index.html")
    return FileResponse(recovery_file)


# ============================================================
# OFFLINE MAP TILE ENGINE
# ============================================================

@app.get("/tiles/{z}/{x}/{y}.png")
async def get_tile(z: int, x: int, y: int):
    """Serve offline map tiles from local storage cache, fetching from OSM if uncached and online."""
    if z < 0 or z > 19:
        return Response(status_code=400, content="Invalid zoom level")
    max_coord = 1 << z
    if x < 0 or x >= max_coord or y < 0 or y >= max_coord:
        return Response(status_code=400, content="Tile coordinates out of bounds")

    tile_path = TILES_DIR / str(z) / str(x) / f"{y}.png"
    if tile_path.exists() and tile_path.stat().st_size > 0:
        return FileResponse(
            tile_path,
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=86400, immutable"},
        )

    # Attempt to fetch from OSM in a background thread if online
    def fetch_osm_tile() -> Optional[bytes]:
        candidate_urls = [
            # Esri has worldwide high-resolution street maps from zoom 0 to 19 (fast global Akamai CDN)
            f"https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}",
        ]
        # OpenTopoMap supports zoom levels <= 17
        if z <= 17:
            candidate_urls.append(f"https://tile.opentopomap.org/{z}/{x}/{y}.png")
        # Esri satellite imagery fallback
        candidate_urls.append(f"https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}")

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) RocketTelemetryGroundStation/1.0",
            "Accept": "image/png,image/jpeg,image/*;q=0.8,*/*;q=0.5",
        }
        for url in candidate_urls:
            try:
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=3.5) as resp:
                    if resp.status == 200:
                        data = resp.read()
                        if (
                            len(data) < 500
                            or len(data) in (6987, 6933, 2049, 4343)
                            or b"Blocked" in data
                            or b"API KEY REQUIRED" in data
                            or b"max zoom" in data
                        ):
                            continue
                        return data
            except Exception:
                continue
        return None

    tile_data = await asyncio.to_thread(fetch_osm_tile)
    if tile_data and len(tile_data) > 0:
        try:
            tile_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = tile_path.with_suffix(".tmp")
            temp_path.write_bytes(tile_data)
            temp_path.replace(tile_path)
            return FileResponse(
                tile_path,
                media_type="image/png",
                headers={"Cache-Control": "public, max-age=86400, immutable"},
            )
        except Exception as exc:
            logger.warning("Failed to save downloaded tile %d/%d/%d: %s", z, x, y, exc)
            return Response(content=tile_data, media_type="image/png")

    if OFFLINE_FALLBACK_FILE.exists():
        return FileResponse(
            OFFLINE_FALLBACK_FILE,
            media_type="image/png",
            headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
        )
    return Response(status_code=404, content="Tile not found")


@app.get("/api/tiles/stats")
async def get_tile_stats():
    """Return local offline map tile cache statistics."""
    def compute_stats():
        if not TILES_DIR.exists():
            return {"tile_count": 0, "total_size_mb": 0.0, "zooms": []}
        count = 0
        total_bytes = 0
        zooms = set()
        for p in TILES_DIR.glob("*/*/*.png"):
            try:
                stat = p.stat()
                total_bytes += stat.st_size
                count += 1
                z_name = p.parent.parent.name
                if z_name.isdigit():
                    zooms.add(int(z_name))
            except OSError:
                pass
        return {
            "tile_count": count,
            "total_size_mb": round(total_bytes / (1024 * 1024), 2),
            "zooms": sorted(list(zooms)),
            "cache_dir": str(TILES_DIR),
        }
    return await asyncio.to_thread(compute_stats)



@app.get("/api/history")
async def history(seconds: float = 60.0, limit: int = 1200):
    """Return historical telemetry records for the past N seconds (default: 60s)."""
    now = time.time()
    min_ts = now - max(1.0, seconds)

    # 1. Attempt ultra-fast in-memory cache lookup
    in_memory = [
        frame for frame in runtime.history
        if frame.get("timestamp") and frame["timestamp"] >= min_ts
    ]

    if len(in_memory) >= 5:
        if len(in_memory) > limit:
            in_memory = in_memory[-limit:]
        return {
            "count": len(in_memory),
            "seconds": seconds,
            "frames_received": runtime.frames_received,
            "connected": runtime.connected,
            "last_timestamp": runtime.last_timestamp,
            "frames": in_memory,
        }

    # 2. Fall back to SQLite database if in-memory cache is sparse
    if database is not None:
        db_frames = database.get_recent(seconds=seconds, limit=limit)
        return {
            "count": len(db_frames),
            "seconds": seconds,
            "frames_received": runtime.frames_received,
            "connected": runtime.connected,
            "last_timestamp": runtime.last_timestamp,
            "frames": db_frames,
        }

    return {
        "count": len(in_memory),
        "seconds": seconds,
        "frames_received": runtime.frames_received,
        "connected": runtime.connected,
        "last_timestamp": runtime.last_timestamp,
        "frames": in_memory,
    }


@app.get("/api/status")
async def status():
    db_active = database is not None and is_database_saving_enabled()
    csv_active = csv_logger is not None and is_csv_saving_enabled()
    port_str = (
        "SIMULADOR_DEMO (10 Hz)"
        if runtime.demo_mode
        else (runtime.active_serial_port or (app_config.serial.port if app_config is not None else None))
    )
    return {
        "connected": runtime.connected,
        "demo_mode": runtime.demo_mode,
        "frames_received": runtime.frames_received,
        "last_timestamp": runtime.last_timestamp,
        "serial_port": port_str,
        "baudrate": app_config.serial.baudrate if app_config is not None else None,
        "statistics": statistics.snapshot() if statistics else {},
        "station": (
            lambda du: {
                "serial_port": port_str,
                "baudrate": app_config.serial.baudrate if app_config is not None else None,
                "connected": runtime.connected,
                "demo_mode": runtime.demo_mode,
                "database_active": db_active,
                "csv_active": csv_active,
                "database_saving_enabled": is_database_saving_enabled(),
                "csv_saving_enabled": is_csv_saving_enabled(),
                "sd_protection_active": not is_database_saving_enabled(),
                "disk_free_pct": round(du.free / du.total * 100, 1),
                "disk_free_mb": round(du.free / (1024 * 1024), 1),
                "min_required_mb_1h": 50.0,
                "disk_go_1h": (du.free / (1024 * 1024)) >= 50.0,
                "storage_db": app_config.storage.database if app_config is not None else None,
                "storage_csv": app_config.storage.csv if app_config is not None else None,
                "server_time": datetime.datetime.now().strftime("%H:%M:%S"),
            }
        )(shutil.disk_usage(".")),
        "gonogo": (
            {
                "required_hold_s": app_config.gonogo.required_hold_s,
                "altitude_tol_m": app_config.gonogo.altitude_tol_m,
                "accel_xy_tol_g": app_config.gonogo.accel_xy_tol_g,
                "accel_z_nominal_g": app_config.gonogo.accel_z_nominal_g,
                "accel_z_tol_g": app_config.gonogo.accel_z_tol_g,
                "gps_radius_m": app_config.gonogo.gps_radius_m,
                "weather_limits": {
                    "max_wind_speed_ms": app_config.gonogo.weather_limits.max_wind_speed_ms,
                    "max_cloud_cover_pct": app_config.gonogo.weather_limits.max_cloud_cover_pct,
                    "min_visibility_km": app_config.gonogo.weather_limits.min_visibility_km,
                },
                "manual_checklist": app_config.gonogo.manual_checklist,
            }
            if app_config is not None
            else {}
        ),
        "charts": (
            {
                "scaling_mode": app_config.charts.scaling_mode,
                "altitude": {
                    "pad_max_m": app_config.charts.altitude.pad_max_m,
                    "expected_apogee_m": app_config.charts.altitude.expected_apogee_m,
                    "step_m": app_config.charts.altitude.step_m,
                },
                "acceleration": {
                    "min_g": app_config.charts.acceleration.min_g,
                    "max_g": app_config.charts.acceleration.max_g,
                },
                "velocity": {
                    "min_vz_ms": app_config.charts.velocity.min_vz_ms,
                    "max_vz_ms": app_config.charts.velocity.max_vz_ms,
                    "step_ms": app_config.charts.velocity.step_ms,
                },
            }
            if app_config is not None
            else {}
        ),
    }


@app.get("/api/config")
async def get_config():
    if app_config is None:
        return {}
    return {
        "charts": {
            "scaling_mode": app_config.charts.scaling_mode,
            "altitude": {
                "pad_max_m": app_config.charts.altitude.pad_max_m,
                "expected_apogee_m": app_config.charts.altitude.expected_apogee_m,
                "step_m": app_config.charts.altitude.step_m,
            },
            "acceleration": {
                "min_g": app_config.charts.acceleration.min_g,
                "max_g": app_config.charts.acceleration.max_g,
            },
            "velocity": {
                "min_vz_ms": app_config.charts.velocity.min_vz_ms,
                "max_vz_ms": app_config.charts.velocity.max_vz_ms,
                "step_ms": app_config.charts.velocity.step_ms,
            },
        },
        "gonogo": {
            "required_hold_s": app_config.gonogo.required_hold_s,
            "altitude_tol_m": app_config.gonogo.altitude_tol_m,
            "accel_xy_tol_g": app_config.gonogo.accel_xy_tol_g,
            "accel_z_nominal_g": app_config.gonogo.accel_z_nominal_g,
            "accel_z_tol_g": app_config.gonogo.accel_z_tol_g,
            "gps_radius_m": app_config.gonogo.gps_radius_m,
            "weather_limits": {
                "max_wind_speed_ms": app_config.gonogo.weather_limits.max_wind_speed_ms,
                "max_cloud_cover_pct": app_config.gonogo.weather_limits.max_cloud_cover_pct,
                "min_visibility_km": app_config.gonogo.weather_limits.min_visibility_km,
            },
            "manual_checklist": app_config.gonogo.manual_checklist,
        },
    }


@app.get("/api/telemetry")
async def telemetry():
    return {
        "connected": runtime.connected,
        "timestamp": runtime.last_timestamp,
        "data": runtime.last_frame,
        "frame_length": frame_config.frame_size if frame_config is not None else None,
        "crc_valid": runtime.last_frame is not None,
        "crc_value": runtime.last_crc_value,
        "raw_hex": runtime.last_raw_frame,
        "statistics": statistics.snapshot() if statistics else {},
    }


@app.websocket("/ws/telemetry")
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket_manager.connect(websocket)
    try:
        # Immediately send current connection status
        await websocket.send_json({"type": "connection", "connected": runtime.connected})

        if runtime.last_frame is not None:
            await websocket.send_json({
                "type": "telemetry",
                "timestamp": runtime.last_timestamp,
                "frame_length": frame_config.frame_size if frame_config is not None else None,
                "crc_valid": True,
                "crc_value": runtime.last_crc_value,
                "raw_hex": runtime.last_raw_frame,
                "data": runtime.last_frame,
                "statistics": statistics.snapshot() if statistics else {},
            })

        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        logger.exception("WebSocket communication error")
    finally:
        await websocket_manager.disconnect(websocket)


# ============================================================
# GO / NO-GO AUDIT LOG ENDPOINTS
# ============================================================

@app.get("/api/gonogo/logs")
async def get_gonogo_logs(limit: int = 100):
    logs = []
    if database is not None and is_database_saving_enabled():
        try:
            logs = database.get_gonogo_audit_logs(limit=min(limit, 500))
        except Exception:
            logger.exception("Error reading Go/No-Go audit logs from database")

    # If database persistence is disabled or empty, return from in-memory ring-buffer
    if not logs and runtime.audit_logs:
        logs = list(runtime.audit_logs)[-limit:]
        logs.reverse()

    return {"logs": logs}


@app.post("/api/gonogo/log")
async def log_gonogo_event(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"status": "error", "message": "Invalid JSON body"}, status_code=400)

    action = body.get("action", "UPDATE")
    item_id = body.get("item_id")
    item_label = body.get("item_label")
    state_val = body.get("state", "UNKNOWN")
    operator = body.get("operator", "OPERATOR")
    details = body.get("details")

    entry = None
    if database is not None and is_database_saving_enabled():
        try:
            entry = database.insert_gonogo_audit(
                action=action,
                item_id=item_id,
                item_label=item_label,
                state=state_val,
                operator=operator,
                details=details,
            )
        except Exception:
            logger.exception("Error writing Go/No-Go audit log to database")

    # If database is disabled (e.g. in demo mode for SD card wear prevention), handle in memory
    if entry is None:
        ts = time.time()
        iso = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).isoformat()
        entry = {
            "id": len(runtime.audit_logs) + 1,
            "timestamp": ts,
            "iso_time": iso,
            "action": action,
            "item_id": item_id,
            "item_label": item_label,
            "state": state_val,
            "operator": operator,
            "details": details,
        }

    runtime.audit_logs.append(entry)

    if gonogo_csv_logger is not None and is_csv_saving_enabled():
        try:
            gonogo_csv_logger.write(entry)
        except Exception:
            logger.exception("Error writing to Go/No-Go CSV log")

    # Broadcast audit event to all connected UI clients
    await websocket_manager.broadcast({
        "type": "gonogo_audit",
        "entry": entry,
    })

    return {"status": "ok", "entry": entry}


@app.post("/api/storage/toggle")
async def toggle_storage(request: Request):
    """Dynamically toggle database and CSV persistence to protect RPi SD card from wear."""
    try:
        body = await request.json()
    except Exception:
        body = {}

    if "database_enabled" in body:
        runtime.save_db_override = bool(body["database_enabled"])
    if "csv_enabled" in body:
        runtime.save_csv_override = bool(body["csv_enabled"])
    if "demo_mode" in body:
        runtime.demo_mode = bool(body["demo_mode"])

    return {
        "status": "ok",
        "demo_mode": runtime.demo_mode,
        "database_saving_enabled": is_database_saving_enabled(),
        "csv_saving_enabled": is_csv_saving_enabled(),
        "sd_protection_active": not is_database_saving_enabled(),
    }


@app.post("/api/demo/reset")
async def demo_reset(t: float = 0.0):
    """Reset the backend demo flight simulation to t=0 (the beginning) or any target second."""
    runtime.demo_time = max(0.0, float(t))
    runtime.demo_reset_requested = True
    runtime.frames_received = 0
    runtime.history.clear()
    if statistics:
        statistics.reset()
    logger.info("Demo flight simulation reset to t=%.1fs (from the beginning)", runtime.demo_time)
    return {"status": "ok", "message": f"Demo reset to t={runtime.demo_time:.1f}s", "time": runtime.demo_time}


# ============================================================
# STARTUP & SHUTDOWN LIFECYCLE
# ============================================================

@app.on_event("startup")
async def startup():
    global app_config, frame_config, parser, frame_buffer, statistics, database, csv_logger, gonogo_csv_logger

    logger.info("Initializing Ground Station...")

    # Load configuration
    app_config = load_app_config()
    frame_config = load_frame_config()
    parser = FrameParser(frame_config)
    frame_buffer = FrameBuffer(parser)
    statistics = PacketStatistics(frame_config.sequence_field)

    # Detect demo mode from config or environment variables if not already set by CLI
    if app_config.demo.enabled or os.environ.get("GS_DEMO_MODE") in ("1", "true", "TRUE"):
        runtime.demo_mode = True
    if os.environ.get("GS_NO_DB") in ("1", "true", "TRUE"):
        runtime.save_db_override = False
    if os.environ.get("GS_NO_CSV") in ("1", "true", "TRUE"):
        runtime.save_csv_override = False

    # Initialize SQLite database only when saving is enabled
    if is_database_saving_enabled():
        database = Database(app_config.storage.database)
        logger.info("Database: %s (active)", database.path)
        # Pre-hydrate recent history from SQLite only in live telemetry mode
        if not runtime.demo_mode:
            try:
                recent_frames = database.get_recent(seconds=120.0, limit=1200)
                for rf in recent_frames:
                    runtime.history.append(rf)
                if recent_frames:
                    logger.info("Pre-loaded history from SQLite: %d frames", len(recent_frames))
            except Exception:
                logger.exception("Error pre-loading history from SQLite")
    else:
        database = None
        logger.info(
            "SQLite database persistence INACTIVE (RPi SD card protection: demo=%s, db_enabled=%s)",
            runtime.demo_mode,
            app_config.storage.database_enabled,
        )

    # Initialize CSV logger only when CSV saving is enabled
    if is_csv_saving_enabled():
        csv_logger = CSVLogger(
            app_config.storage.csv,
            [
                out_name
                for ff in frame_config.fields
                for out_name in ([ff.name, f"{ff.name}_label"] if ff.values else [ff.name])
            ],
        )
        gonogo_csv_path = Path(app_config.storage.csv).parent / "gonogo_audit.csv"
        gonogo_csv_logger = GoNoGoCSVLogger(str(gonogo_csv_path))
        logger.info("CSV Log: %s (active)", csv_logger.path)
        logger.info("Go/No-Go Audit CSV Log: %s (active)", gonogo_csv_logger.path)
    else:
        csv_logger = None
        gonogo_csv_logger = None
        logger.info("CSV logging INACTIVE (RPi SD card protection: zero disk write mode)")

    # Start telemetry source: synthetic feeder in demo mode, UART serial loop in hardware mode
    if runtime.demo_mode:
        logger.info("Starting background demo telemetry feeder @ 10 Hz (pure RAM, zero SD writes)")
        create_background_task(demo_telemetry_loop())
    else:
        create_background_task(serial_loop())

    logger.info("Ground Station initialized successfully")


@app.on_event("shutdown")
async def shutdown():
    runtime.connected = False
    logger.info("Shutting down Ground Station...")

    # Cancel background tasks
    current_task = asyncio.current_task()
    tasks_to_cancel = [t for t in background_tasks if t is not current_task]
    for t in tasks_to_cancel:
        t.cancel()

    if tasks_to_cancel:
        await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

    if database is not None:
        try:
            database.close()
        except Exception:
            logger.exception("Error closing SQLite connection")

    if csv_logger is not None:
        try:
            csv_logger.close()
        except Exception:
            logger.exception("Error closing CSV file")

    if gonogo_csv_logger is not None:
        try:
            gonogo_csv_logger.close()
        except Exception:
            logger.exception("Error closing Go/No-Go CSV file")

    logger.info("Ground Station shutdown complete")


# ============================================================
# APPLICATION ENTRYPOINT & CLI PARSER
# ============================================================

def parse_cli_args(args: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rocket Telemetry Ground Station Core Service",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--demo", "-d",
        action="store_true",
        help="Run in demo/simulation mode with synthetic 10 Hz telemetry (SD card protection: DB saving off)",
    )
    parser.add_argument(
        "--start-time", "-t",
        type=float,
        default=0.0,
        help="Initial flight simulation time in seconds for demo mode (default: 0.0, from the beginning)",
    )
    parser.add_argument(
        "--no-db",
        action="store_true",
        help="Disable SQLite database persistence (protects Raspberry Pi SD card flash memory)",
    )
    parser.add_argument(
        "--no-csv",
        action="store_true",
        help="Disable CSV file logging (protects Raspberry Pi SD card flash memory)",
    )
    parser.add_argument(
        "--no-storage",
        action="store_true",
        help="Disable all disk persistence (pure RAM-only operation for maximum SD card longevity)",
    )
    parser.add_argument(
        "--save-db-in-demo",
        action="store_true",
        help="Force saving telemetry to SQLite database even in demo mode (default: disabled)",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=None,
        help="Custom host interface to bind (overrides app.yaml)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Custom port to bind (overrides app.yaml)",
    )
    return parser.parse_args(args)


if __name__ == "__main__":
    cli = parse_cli_args()
    if cli.demo:
        runtime.demo_mode = True
        runtime.demo_time = max(0.0, float(cli.start_time))
    if cli.no_db or cli.no_storage:
        runtime.save_db_override = False
    elif cli.save_db_in_demo:
        runtime.save_db_override = True
    if cli.no_csv or cli.no_storage:
        runtime.save_csv_override = False

    startup_config = load_app_config()
    bind_host = cli.host or startup_config.websocket.host
    bind_port = cli.port or startup_config.websocket.port

    uvicorn.run(
        app,
        host=bind_host,
        port=bind_port,
        log_level="info",
    )
