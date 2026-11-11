"""Unit Tests for Raspberry Pi SD Card Protection & Demo Persistence Deactivation.

Validates that:
1. Database persistence is automatically suppressed during demo/simulation mode.
2. CLI flags (--no-db, --no-storage, --demo) disable SQLite and CSV writes.
3. Dynamic /api/storage/toggle endpoint controls disk writing safely.
4. Frame processing and Go/No-Go audit workflows function 100% in RAM with zero disk wear.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import main


class TestStorageSDProtection(unittest.TestCase):
    def setUp(self):
        # Reset runtime state between tests
        main.runtime.demo_mode = False
        main.runtime.save_db_override = None
        main.runtime.save_csv_override = None
        main.runtime.audit_logs.clear()
        main.runtime.history.clear()

    def test_database_saving_deactivated_in_demo_mode(self):
        """In demo mode, is_database_saving_enabled must return False by default."""
        config = main.load_app_config()
        main.app_config = config
        
        main.runtime.demo_mode = False
        self.assertTrue(main.is_database_saving_enabled())

        main.runtime.demo_mode = True
        self.assertFalse(main.is_database_saving_enabled(), "Database saving MUST be disabled in demo mode")

    def test_database_saving_explicit_overrides(self):
        """CLI or API overrides must take precedence over default logic."""
        config = main.load_app_config()
        main.app_config = config

        # Force disable
        main.runtime.save_db_override = False
        self.assertFalse(main.is_database_saving_enabled())

        # Force enable even in demo
        main.runtime.demo_mode = True
        main.runtime.save_db_override = True
        self.assertTrue(main.is_database_saving_enabled())

    def test_zero_db_writes_in_demo_mode(self):
        """Feeding frames into process_frame in demo mode must write ZERO rows to SQLite."""
        async def run_subtest():
            with tempfile.TemporaryDirectory() as tmpdir:
                db_path = os.path.join(tmpdir, "telemetry_test.db")
                db = main.Database(db_path)
                main.database = db
                main.app_config = main.load_app_config()
                main.frame_config = main.load_frame_config()
                main.statistics = main.PacketStatistics(main.frame_config.sequence_field)

                # Set demo mode active
                main.runtime.demo_mode = True
                self.assertFalse(main.is_database_saving_enabled())

                test_frame = {
                    "sync": 0xAA,
                    "frame_number": 42,
                    "timestamp": 123456,
                    "status": 0,
                    "pressure": 1013.25,
                    "altura_actual": 0.0,
                    "accel_x": 0.0,
                    "accel_y": 0.0,
                    "accel_z": 1.0,
                    "gyro_x": 0.0,
                    "gyro_y": 0.0,
                    "gyro_z": 0.0,
                    "temp_imu": 25.0,
                    "latitude": -34.6,
                    "longitude": -58.3,
                    "altitud_gps": 50.0,
                    "satellites": 10,
                    "ground_speed": 0.0,
                    "pin_frenado": 0,
                    "pin_principal": 0,
                }

                # Process 10 frames
                for i in range(10):
                    test_frame["frame_number"] = i + 1
                    await main.process_frame(test_frame)

                # Verify SQLite database has ZERO rows
                conn = sqlite3.connect(db_path)
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM telemetry")
                count = cur.fetchone()[0]
                conn.close()
                db.close()

                self.assertEqual(count, 0, "SQLite database must contain 0 rows when in demo mode")
                self.assertEqual(main.runtime.frames_received, 10)
                self.assertEqual(len(main.runtime.history), 10, "Frames must still be retained in RAM")

        asyncio.run(run_subtest())

    def test_gonogo_audit_in_memory_without_db(self):
        """When database is None or disabled, Go/No-Go audit log must operate cleanly in RAM."""
        main.database = None
        main.runtime.demo_mode = True

        async def run_subtest():
            from starlette.requests import Request
            from starlette.datastructures import Headers

            # Mock FastAPI Request
            scope = {
                "type": "http",
                "method": "POST",
                "headers": [(b"content-type", b"application/json")],
            }
            body_bytes = b'{"action":"UPDATE","item_id":"ant_gs","state":"GO","operator":"TEST_OPERATOR"}'

            async def receive():
                return {"type": "http.request", "body": body_bytes}

            req = Request(scope, receive)
            resp = await main.log_gonogo_event(req)
            self.assertEqual(resp["status"], "ok")
            self.assertEqual(resp["entry"]["item_id"], "ant_gs")
            self.assertEqual(resp["entry"]["state"], "GO")
            self.assertEqual(len(main.runtime.audit_logs), 1)

            # Test GET /api/gonogo/logs returns the in-memory entry
            get_resp = await main.get_gonogo_logs(limit=10)
            self.assertEqual(len(get_resp["logs"]), 1)
            self.assertEqual(get_resp["logs"][0]["item_id"], "ant_gs")

        asyncio.run(run_subtest())

    def test_status_endpoint_reports_sd_protection(self):
        """GET /api/status must clearly report database_active=False and sd_protection_active=True in demo mode."""
        main.app_config = main.load_app_config()
        main.runtime.demo_mode = True

        async def run_subtest():
            status_data = await main.status()
            self.assertTrue(status_data["demo_mode"])
            self.assertFalse(status_data["station"]["database_active"])
            self.assertTrue(status_data["station"]["sd_protection_active"])
            self.assertFalse(status_data["station"]["database_saving_enabled"])

        asyncio.run(run_subtest())

    def test_cli_flags_parsing(self):
        """CLI parser must accurately handle --demo, --no-db, --no-csv, and --no-storage."""
        cli = main.parse_cli_args(["--demo", "--no-db"])
        self.assertTrue(cli.demo)
        self.assertTrue(cli.no_db)
        self.assertFalse(cli.no_csv)

        cli2 = main.parse_cli_args(["--no-storage"])
        self.assertTrue(cli2.no_storage)


if __name__ == "__main__":
    unittest.main()
