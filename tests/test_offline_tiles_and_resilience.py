"""Unit tests for offline map tile engine, SQLite WAL configuration, and serial resilience."""

from __future__ import annotations

import asyncio
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from fastapi.responses import FileResponse, Response

import main


class TestOfflineTilesAndResilience(unittest.TestCase):
    def test_tile_coordinates_validation(self):
        """Invalid zoom or coordinate indices should return HTTP 400 Response."""
        # Negative zoom
        res = asyncio.run(main.get_tile(-1, 0, 0))
        self.assertIsInstance(res, Response)
        self.assertEqual(res.status_code, 400)

        # Zoom exceeding 19
        res = asyncio.run(main.get_tile(20, 0, 0))
        self.assertIsInstance(res, Response)
        self.assertEqual(res.status_code, 400)

        # Coordinate out of bounds (at zoom 2, max index is 3)
        res = asyncio.run(main.get_tile(2, 4, 1))
        self.assertIsInstance(res, Response)
        self.assertEqual(res.status_code, 400)

        res = asyncio.run(main.get_tile(2, 1, 4))
        self.assertIsInstance(res, Response)
        self.assertEqual(res.status_code, 400)

    def test_tile_stats_endpoint(self):
        """Stats endpoint should report cached tile count and zoom levels."""
        data = asyncio.run(main.get_tile_stats())
        self.assertIsInstance(data, dict)
        self.assertIn("tile_count", data)
        self.assertIn("total_size_mb", data)
        self.assertIn("zooms", data)
        self.assertIsInstance(data["zooms"], list)
        self.assertGreaterEqual(data["tile_count"], 30)
        self.assertIn(11, data["zooms"])

    def test_cached_tile_serving(self):
        """Locally cached tile should be served with PNG media type."""
        cached_tiles = list(main.TILES_DIR.glob("*/*/*.png"))
        if not cached_tiles:
            self.skipTest("No predownloaded tiles found in cache")

        sample = cached_tiles[0]
        y = int(sample.stem)
        x = int(sample.parent.name)
        z = int(sample.parent.parent.name)

        res = asyncio.run(main.get_tile(z, x, y))
        self.assertIsInstance(res, FileResponse)
        self.assertEqual(res.media_type, "image/png")
        self.assertEqual(res.headers.get("cache-control"), "public, max-age=86400, immutable")

    def test_offline_fallback_on_network_failure(self):
        """When OSM is unreachable and tile is uncached, offline fallback tile is served."""
        with patch("urllib.request.urlopen", side_effect=OSError("Network unreachable")):
            res = asyncio.run(main.get_tile(19, 524000, 524000))
            self.assertIsInstance(res, (FileResponse, Response))
            self.assertEqual(res.media_type, "image/png")

    def test_serial_port_auto_discovery(self):
        """Should discover alternative USB/ACM serial ports if configured port is missing."""
        existing_port = "/dev/null"
        self.assertEqual(main.resolve_serial_port(existing_port), existing_port)

        mock_port_1 = MagicMock()
        mock_port_1.device = "/dev/ttyACM1"
        mock_port_2 = MagicMock()
        mock_port_2.device = "/dev/ttyUSB0"

        with patch("serial.tools.list_ports.comports", return_value=[mock_port_1, mock_port_2]):
            selected = main.resolve_serial_port("/dev/ttyACM0")
            self.assertEqual(selected, "/dev/ttyACM1")


if __name__ == "__main__":
    unittest.main()
