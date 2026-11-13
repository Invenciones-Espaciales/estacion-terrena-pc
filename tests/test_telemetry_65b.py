import sys
import os
import struct
import tempfile
import asyncio
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import main
from main import (
    load_frame_config,
    load_app_config,
    FrameParser,
    FrameBuffer,
    PacketStatistics,
    Database,
    CSVLogger,
    crc16_ccitt,
)

def build_test_packet(
    frame_number=1,
    timestamp_ms=1000,
    status=1,
    pressure=1013.25,
    altura_actual=25.0,
    accel_x=0.05,
    accel_y=-0.02,
    accel_z=1.02,
    gyro_x=0.5,
    gyro_y=-0.3,
    gyro_z=12.4,
    temp_imu=29.2,
    latitude=-34.603722,
    longitude=-58.381592,
    altitud_gps=50.0,
    satellites=12,
    ground_speed=4.5,
    pin_frenado=0,
    pin_principal=0,
    corrupt_crc=False
):
    # Pack 63 bytes (bytes 0..62)
    # Format: <B H I B f f f f f f f f f f f f B f B B
    payload = struct.pack(
        '<B H I B f f f f f f f f f f f f B f B B',
        0xAA,
        frame_number,
        timestamp_ms,
        status,
        pressure,
        altura_actual,
        accel_x,
        accel_y,
        accel_z,
        gyro_x,
        gyro_y,
        gyro_z,
        temp_imu,
        latitude,
        longitude,
        altitud_gps,
        satellites,
        ground_speed,
        pin_frenado,
        pin_principal
    )
    assert len(payload) == 63, f"Expected 63 bytes payload, got {len(payload)}"

    crc = crc16_ccitt(payload)
    if corrupt_crc:
        crc ^= 0xAAAA
    
    packet = payload + crc.to_bytes(2, byteorder='little')
    assert len(packet) == 65, f"Expected 65 bytes packet, got {len(packet)}"
    return packet


def run_test_frame_config():
    print("[TEST 1] Testing Frame Configuration Loading...")
    cfg = load_frame_config()
    assert cfg.frame_size == 65, f"Expected frame_size 65, got {cfg.frame_size}"
    assert cfg.crc_offset == 63, f"Expected crc_offset 63, got {cfg.crc_offset}"
    assert cfg.crc_calculate_from == 0
    assert cfg.crc_calculate_to == 62
    assert cfg.sync == b"\xAA", f"Expected sync 0xAA, got {cfg.sync}"
    assert cfg.endian == "little"

    field_names = [f.name for f in cfg.fields]
    expected_fields = [
        "sync", "frame_number", "timestamp", "status",
        "pressure", "altura_actual", "accel_x", "accel_y", "accel_z",
        "gyro_x", "gyro_y", "gyro_z", "temp_imu",
        "latitude", "longitude", "altitud_gps", "satellites",
        "ground_speed", "pin_frenado", "pin_principal"
    ]
    for ef in expected_fields:
        assert ef in field_names, f"Missing field in frame config: {ef}"
    print("  -> PASSED: 65-byte frame configuration loaded and validated.")


def run_test_parser():
    print("[TEST 2] Testing FrameParser on Definitive 65-Byte Packet...")
    cfg = load_frame_config()
    parser = FrameParser(cfg)

    pkt = build_test_packet(
        frame_number=105,
        timestamp_ms=155400,
        status=1,
        pressure=995.4,
        altura_actual=152.8,
        accel_x=0.15,
        accel_y=-0.12,
        accel_z=4.85,
        gyro_x=1.25,
        gyro_y=-0.85,
        gyro_z=45.0,
        temp_imu=31.5,
        latitude=-34.6038,
        longitude=-58.3817,
        altitud_gps=177.8,
        satellites=14,
        ground_speed=18.6,
        pin_frenado=1,
        pin_principal=0
    )

    parsed = parser.parse(pkt)
    assert parsed is not None, "Parser failed to parse valid 65-byte frame"

    assert parsed["sync"] == 0xAA
    assert parsed["frame_number"] == 105
    assert parsed["timestamp"] == 155400
    assert parsed["status"] == 1
    assert parsed["status_label"] == "POWERED"
    assert abs(parsed["pressure"] - 995.4) < 1e-3
    assert abs(parsed["altura_actual"] - 152.8) < 1e-3
    assert abs(parsed["accel_x"] - 0.15) < 1e-3
    assert abs(parsed["accel_y"] - (-0.12)) < 1e-3
    assert abs(parsed["accel_z"] - 4.85) < 1e-3
    assert abs(parsed["gyro_x"] - 1.25) < 1e-3
    assert abs(parsed["gyro_y"] - (-0.85)) < 1e-3
    assert abs(parsed["gyro_z"] - 45.0) < 1e-3
    assert abs(parsed["temp_imu"] - 31.5) < 1e-3
    assert abs(parsed["latitude"] - (-34.6038)) < 1e-4
    assert abs(parsed["longitude"] - (-58.3817)) < 1e-4
    assert abs(parsed["altitud_gps"] - 177.8) < 1e-3
    assert parsed["satellites"] == 14
    assert abs(parsed["ground_speed"] - 18.6) < 1e-3
    assert parsed["pin_frenado"] == 1
    assert parsed["pin_frenado_label"] == "ACTIVADO"
    assert parsed["pin_principal"] == 0
    assert parsed["pin_principal_label"] == "DESACTIVADO"
    print("  -> PASSED: All 20 fields decoded accurately with high floating-point precision.")


def run_test_corrupted_crc():
    print("[TEST 3] Testing CRC Integrity Rejection...")
    cfg = load_frame_config()
    parser = FrameParser(cfg)

    corrupt_pkt = build_test_packet(corrupt_crc=True)
    res = parser.parse(corrupt_pkt)
    assert res is None, "Parser should reject packet with invalid CRC"
    print("  -> PASSED: Corrupted CRC packet successfully rejected.")


def run_test_buffer_sliding_window_with_noise():
    print("[TEST 4] Testing FrameBuffer Resynchronization & Noise Immunity...")
    cfg = load_frame_config()
    parser = FrameParser(cfg)
    buf = FrameBuffer(parser)

    # 1. Feed random garbage without sync
    garbage = b"\x01\x02\x03\x04\x05"
    frames = buf.feed(garbage)
    assert len(frames) == 0

    # 2. Feed false sync byte followed by noise (e.g. \xAA\x11\x22\x33)
    false_sync_garbage = b"\xAA\x11\x22\x33\x44"
    frames = buf.feed(false_sync_garbage)
    assert len(frames) == 0

    # 3. Feed legitimate packet right after
    pkt1 = build_test_packet(frame_number=1, timestamp_ms=100)
    frames = buf.feed(pkt1)
    assert len(frames) == 1, f"Expected 1 frame, got {len(frames)}"
    assert frames[0]["frame_number"] == 1

    # 4. Feed two consecutive packets in one UART chunk
    pkt2 = build_test_packet(frame_number=2, timestamp_ms=200)
    pkt3 = build_test_packet(frame_number=3, timestamp_ms=300)
    frames = buf.feed(pkt2 + pkt3)
    assert len(frames) == 2
    assert frames[0]["frame_number"] == 2
    assert frames[1]["frame_number"] == 3

    print("  -> PASSED: FrameBuffer sliding window cleanly handles noise and false syncs.")


def run_test_storage():
    print("[TEST 5] Testing SQLite & CSV Logging with 65-Byte Telemetry Fields...")
    cfg = load_frame_config()
    parser = FrameParser(cfg)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_telemetry.db")
        csv_path = os.path.join(tmpdir, "test_telemetry.csv")

        db = Database(db_path)
        csv_fields = [
            output_name
            for frame_field in cfg.fields
            for output_name in (
                [frame_field.name, f"{frame_field.name}_label"]
                if frame_field.values
                else [frame_field.name]
            )
        ]
        csv_log = CSVLogger(csv_path, csv_fields)

        pkt = build_test_packet(frame_number=50, timestamp_ms=5000)
        parsed = parser.parse(pkt)

        db.insert(parsed, sequence_field="frame_number")
        csv_log.write(parsed)

        # Verify db.get_recent() reconstructs packet dictionary from flat columns
        recent = db.get_recent(seconds=60.0, limit=10)
        assert len(recent) == 1
        assert recent[0]["sequence"] == 50
        assert recent[0]["data"]["frame_number"] == 50
        assert recent[0]["data"]["pressure"] == 1013.25
        assert recent[0]["data"]["pin_frenado_label"] == "DESACTIVADO"

        db.close()
        csv_log.close()

        # Verify DB flat relational schema & content
        import sqlite3
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT received_at, sync, frame_number, timestamp, status, status_label,
                   pressure, altura_actual, accel_x, accel_y, accel_z,
                   gyro_x, gyro_y, gyro_z, temp_imu,
                   latitude, longitude, altitud_gps, satellites, ground_speed,
                   pin_frenado, pin_frenado_label, pin_principal, pin_principal_label
            FROM telemetry
            """
        )
        row = cur.fetchone()
        assert row is not None
        assert row[0] > 0  # received_at
        assert row[1] == 0xAA  # sync
        assert row[2] == 50  # frame_number
        assert row[3] == 5000  # timestamp
        assert row[6] == 1013.25  # pressure
        assert row[20] == 0  # pin_frenado
        assert row[21] == "DESACTIVADO"  # pin_frenado_label
        assert row[22] == 0  # pin_principal
        assert row[23] == "DESACTIVADO"  # pin_principal_label
        conn.close()

        # Verify CSV content
        with open(csv_path, "r", encoding="utf-8") as f:
            header = f.readline().strip().split(",")
            assert "altura_actual" in header
            assert "gyro_z" in header
            assert "pin_principal" in header
            data_line = f.readline().strip()
            assert len(data_line) > 0

    print("  -> PASSED: SQLite and CSV successfully store new 65-byte telemetry payloads.")


def run_test_fragmented_streaming():
    print("[TEST 6] Testing Streaming Fragment Reassembly (UART Byte Fragmentation)...")
    cfg = load_frame_config()
    parser = FrameParser(cfg)
    buf = FrameBuffer(parser)

    pkt = build_test_packet(frame_number=88, timestamp_ms=8800)
    assert len(pkt) == 65

    # Feed packet in uneven chunks: 5 bytes at a time
    accumulated_frames = []
    chunk_size = 5
    for i in range(0, len(pkt), chunk_size):
        chunk = pkt[i:i + chunk_size]
        new_frames = buf.feed(chunk)
        accumulated_frames.extend(new_frames)
        if i + chunk_size < len(pkt):
            assert len(new_frames) == 0, f"Unexpected premature frame at offset {i}"

    assert len(accumulated_frames) == 1, f"Expected 1 reassembled frame, got {len(accumulated_frames)}"
    assert accumulated_frames[0]["frame_number"] == 88
    print("  -> PASSED: Fragmented chunks correctly reassembled without frame loss.")


def run_test_consecutive_false_syncs():
    print("[TEST 7] Testing Consecutive False Sync Bytes (0xAA 0xAA 0xAA Noise)...")
    cfg = load_frame_config()
    parser = FrameParser(cfg)
    buf = FrameBuffer(parser)

    # Sequence of multiple false sync bytes with invalid lengths/CRC
    noise = b"\xAA\xAA\xAA\x00\x01\x02\xAA\x55"
    frames = buf.feed(noise)
    assert len(frames) == 0

    # Genuine frame right after
    pkt = build_test_packet(frame_number=99, timestamp_ms=9900)
    frames = buf.feed(pkt)
    assert len(frames) == 1
    assert frames[0]["frame_number"] == 99
    print("  -> PASSED: Consecutive false sync sequences safely discarded and true frame recovered.")


def run_test_packet_statistics_and_loss():
    print("[TEST 8] Testing Packet Statistics & Sequence Loss Tracking...")
    stats = PacketStatistics("frame_number")

    # Frames 1, 2, 3 (nominal)
    stats.register({"frame_number": 1})
    stats.register({"frame_number": 2})
    stats.register({"frame_number": 3})
    snap = stats.snapshot()
    assert snap["received"] == 3
    assert snap["lost"] == 0
    assert snap["loss_percent"] == 0.0

    # Jump from 3 to 7 (4, 5, 6 lost = 3 lost)
    stats.register({"frame_number": 7})
    snap = stats.snapshot()
    assert snap["received"] == 4
    assert snap["lost"] == 3
    # Total packets = 4 received + 3 lost = 7. loss % = 3/7 * 100 ~ 42.85%
    assert abs(snap["loss_percent"] - (3 / 7 * 100)) < 0.1
    print("  -> PASSED: Packet loss tracking accurately calculates missing sequences.")


class TestTelemetry65BSuite(unittest.TestCase):
    """Unittest test case runner ensuring standard python -m unittest discovery works."""

    def test_01_frame_config(self):
        run_test_frame_config()

    def test_02_parser(self):
        run_test_parser()

    def test_03_corrupted_crc(self):
        run_test_corrupted_crc()

    def test_04_buffer_sliding_window_with_noise(self):
        run_test_buffer_sliding_window_with_noise()

    def test_05_storage(self):
        run_test_storage()

    def test_06_fragmented_streaming(self):
        run_test_fragmented_streaming()

    def test_07_consecutive_false_syncs(self):
        run_test_consecutive_false_syncs()

    def test_08_packet_statistics_and_loss(self):
        run_test_packet_statistics_and_loss()


if __name__ == "__main__":
    print("=" * 60)
    print("RUNNING AUTOMATED 65-BYTE LORA TELEMETRY FRAME VERIFICATION")
    print("=" * 60)
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestTelemetry65BSuite)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    print("=" * 60)
    if result.wasSuccessful():
        print(f"ALL {result.testsRun} TESTS PASSED SUCCESSFULLY (100% PASS RATE)")
        print("=" * 60)
        sys.exit(0)
    else:
        print("TEST SUITE FAILED")
        print("=" * 60)
        sys.exit(1)
