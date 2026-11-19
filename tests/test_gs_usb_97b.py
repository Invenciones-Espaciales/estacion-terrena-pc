"""Automated verification for the 97-byte Ground Station USB frame and H <-> B router."""

import asyncio
import os
import sqlite3
import struct
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "tests"))

import main
from main import (
    Database,
    FrameBuffer,
    FrameParser,
    GS_HEADER_SIZE,
    GS_HEADER_STRUCT,
    PacketStatistics,
    SerialProtocol,
    crc16_ccitt,
    load_frame_config,
    parse_gs_header,
)
from test_telemetry_65b import build_test_packet


def build_usb_97b_packet(
    rssi: int = -68,
    snr: float = 9.5,
    gs_uptime_ms: int = 12345,
    rx_packets: int = 42,
    rx_lost: int = 1,
    rx_crc_errors: int = 0,
    rx_size_errors: int = 0,
    tx_reboots: int = 0,
    rocket_crc_ok: bool = True,
    link_active: bool = True,
    tx_reboot_event: bool = False,
    corrupt_gs_crc: bool = False,
    **rocket_kwargs,
) -> bytes:
    """Build a deterministic 97-byte UsbTelemetryPacket (32B GS Header + 65B Rocket)."""
    rocket_bytes = build_test_packet(
        corrupt_crc=(not rocket_crc_ok),
        **rocket_kwargs,
    )

    flags = 0x10  # proto v1
    if rocket_crc_ok:
        flags |= 0x01
    if link_active:
        flags |= 0x02
    if tx_reboot_event:
        flags |= 0x04

    gs_body = struct.pack(
        "<B B h f I I I I I H",
        0xAB,
        flags,
        rssi,
        snr,
        gs_uptime_ms,
        rx_packets,
        rx_lost,
        rx_crc_errors,
        rx_size_errors,
        tx_reboots,
    )
    assert len(gs_body) == 30
    gs_crc = crc16_ccitt(gs_body)
    if corrupt_gs_crc:
        gs_crc ^= 0x5555

    gs_header = gs_body + gs_crc.to_bytes(2, byteorder="little")
    assert len(gs_header) == GS_HEADER_SIZE == 32
    full = gs_header + rocket_bytes
    assert len(full) == 97
    return full


class MockTransport:
    def __init__(self):
        self.writes: list[bytes] = []
        self.closed = False

    def is_closing(self) -> bool:
        return self.closed

    def write(self, data: bytes):
        self.writes.append(data)

    def close(self):
        self.closed = True


class TestGroundStation97BSuite(unittest.TestCase):
    def setUp(self):
        self.cfg = load_frame_config()
        self.parser = FrameParser(self.cfg)
        self.buf = FrameBuffer(self.parser)

    def test_01_valid_97b_parse_and_extra_fields(self):
        pkt = build_usb_97b_packet(
            frame_number=12,
            altura_actual=345.5,
            rssi=-71,
            snr=8.75,
            gs_uptime_ms=98765,
            rx_packets=12,
            rx_lost=2,
            rx_crc_errors=1,
            rx_size_errors=3,
            tx_reboots=1,
            tx_reboot_event=True,
        )
        assert GS_HEADER_STRUCT.size == 32
        gs = parse_gs_header(pkt[:32])
        assert gs is not None
        assert gs["rssi"] == -71
        assert abs(gs["snr"] - 8.75) < 1e-2

        frames = self.buf.feed(pkt)
        assert len(frames) == 1
        assert self.buf.invalid_frames == 0
        f = frames[0]
        assert f["frame_number"] == 12
        assert abs(f["altura_actual"] - 345.5) < 1e-2
        assert f["rssi"] == -71
        assert abs(f["snr"] - 8.75) < 1e-2
        assert f["gs_uptime_ms"] == 98765
        assert f["rx_packets"] == 12
        assert f["rx_lost"] == 2
        assert f["rx_crc_errors"] == 1
        assert f["rx_size_errors"] == 3
        assert f["tx_reboots"] == 1
        assert f["gs_tx_reboot"] is True

    def test_02_fragmented_97b_stream_and_ascii_noise(self):
        ascii_banner = b"---------- PACKET RECEIVED ----------\r\nRadio RF: -60 dBm\r\n"
        pkt = build_usb_97b_packet(frame_number=55, rssi=-62, snr=10.5)
        stream = ascii_banner + pkt

        collected = []
        for chunk_size in (11, 17, 19, 23):
            pos = 0
            temp_buf = FrameBuffer(self.parser)
            collected.clear()
            while pos < len(stream):
                collected.extend(temp_buf.feed(stream[pos : pos + chunk_size]))
                pos += chunk_size
            assert len(collected) == 1
            assert collected[0]["frame_number"] == 55
            assert collected[0]["rssi"] == -62

    def test_03_corrupted_rocket_crc_preserves_gs_diagnostics(self):
        pkt = build_usb_97b_packet(
            frame_number=77,
            rssi=-89,
            snr=-2.5,
            rx_crc_errors=4,
            rocket_crc_ok=False,
        )
        frames = self.buf.feed(pkt)
        assert len(frames) == 0
        assert self.buf.invalid_frames == 1
        assert len(self.buf.last_invalid_gs_events) == 1
        ev = self.buf.last_invalid_gs_events[0]
        assert ev["rssi"] == -89
        assert ev["rx_crc_errors"] == 4
        assert ev["gs_rocket_crc_ok"] is False

    def test_04_corrupted_gs_crc_recovers_inner_65b_rocket_frame(self):
        pkt = build_usb_97b_packet(
            frame_number=88,
            corrupt_gs_crc=True,
        )
        frames = self.buf.feed(pkt)
        assert len(frames) == 1
        assert frames[0]["frame_number"] == 88
        assert "rssi" not in frames[0]

    def test_05_serial_protocol_handshake_and_ascii_retry(self):
        async def scenario():
            old_buf = main.frame_buffer
            old_stats = main.statistics
            try:
                main.frame_buffer = FrameBuffer(self.parser)
                main.statistics = PacketStatistics("frame_number")

                loop = asyncio.get_running_loop()
                proto = SerialProtocol(loop)
                transport = MockTransport()

                proto.connection_made(transport)
                assert transport.writes[0] == b"B\n"
                assert proto.binary_locked is False

                # Simulate ESP32 still in 'H' mode after boot reset; force debounce window elapsed
                proto._last_cmd_mono -= 1.0
                proto.data_received(b"[MODE] Booting in HUMAN-READABLE\r\n")
                assert transport.writes[-1] == b"B\n"

                # Simulate ESP32 switching to 97B Binary mode
                pkt = build_usb_97b_packet(frame_number=1, rssi=-58, snr=11.0, rx_packets=1)
                proto.data_received(pkt)
                await asyncio.sleep(0.05)

                assert proto.binary_locked is True
                snap = main.statistics.snapshot()
                assert snap["gs"] is not None
                assert snap["gs"]["rssi"] == -58

                # Simulate clean exit command
                proto.send_mode_command(b"H\n")
                assert transport.writes[-1] == b"H\n"
                proto.connection_lost(None)
                await asyncio.sleep(0.01)
            finally:
                main.frame_buffer = old_buf
                main.statistics = old_stats

        asyncio.run(scenario())

    def test_06_sqlite_migration_for_existing_db(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "legacy.db")
            conn = sqlite3.connect(db_path)
            conn.execute(
                """
                CREATE TABLE telemetry (
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
            conn.commit()
            conn.close()

            db = Database(db_path)
            pkt = build_usb_97b_packet(frame_number=101, rssi=-67, snr=8.5)
            parsed = self.buf.feed(pkt)[0]
            db.insert(parsed, "frame_number")
            recent = db.get_recent(seconds=60.0, limit=10)
            db.close()

            assert len(recent) == 1
            assert recent[0]["data"]["rssi"] == -67
            assert abs(recent[0]["data"]["snr"] - 8.5) < 1e-2


if __name__ == "__main__":
    unittest.main(verbosity=2)
