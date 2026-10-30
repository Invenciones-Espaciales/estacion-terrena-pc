#!/usr/bin/env python3
"""Offline Map Tile Pre-downloader.

Downloads OpenStreetMap map tiles for a specified geographic area and zoom
levels into local storage (data/tiles/{z}/{x}/{y}.png). This ensures 100%
offline map functionality during rocket launches in remote field locations
with zero cellular coverage.
"""

from __future__ import annotations

import argparse
import math
import sys
import time
import urllib.request
from pathlib import Path


def deg2num(lat_deg: float, lon_deg: float, zoom: int) -> tuple[int, int]:
    """Convert latitude, longitude in degrees to Slippy map tile X, Y coordinates."""
    lat_rad = math.radians(lat_deg)
    n = 1 << zoom
    xtile = int((lon_deg + 180.0) / 360.0 * n)
    # Clamp tangent argument to avoid domain errors near poles
    clamped_rad = max(-1.4844, min(1.4844, lat_rad))
    ytile = int((1.0 - math.asinh(math.tan(clamped_rad)) / math.pi) / 2.0 * n)
    return max(0, min(n - 1, xtile)), max(0, min(n - 1, ytile))


def predownload_tiles(
    lat: float,
    lon: float,
    radius_km: float,
    min_zoom: int,
    max_zoom: int,
    output_dir: Path,
    delay: float = 0.08,
    force: bool = False,
) -> bool:
    """Download tiles within radius around (lat, lon) for specified zoom range."""
    output_dir.mkdir(parents=True, exist_ok=True)

    delta_lat = radius_km / 111.32
    rad_lat = math.radians(lat)
    cos_lat = max(0.01, math.cos(rad_lat))
    delta_lon = radius_km / (111.32 * cos_lat)

    lat_min, lat_max = lat - delta_lat, lat + delta_lat
    lon_min, lon_max = lon - delta_lon, lon + delta_lon

    tasks: list[tuple[int, int, int]] = []
    for z in range(min_zoom, max_zoom + 1):
        x1, y1 = deg2num(lat_max, lon_min, z)
        x2, y2 = deg2num(lat_min, lon_max, z)
        xmin, xmax = min(x1, x2), max(x1, x2)
        ymin, ymax = min(y1, y2), max(y1, y2)

        for x in range(xmin, xmax + 1):
            for y in range(ymin, ymax + 1):
                tasks.append((z, x, y))

    total_tiles = len(tasks)
    print(f"============================================================")
    print(f"Offline Map Tile Downloader")
    print(f"Center:        {lat:.5f}, {lon:.5f}")
    print(f"Radius:        {radius_km:.2f} km")
    print(f"Zoom range:    {min_zoom} -> {max_zoom}")
    print(f"Total tiles:   {total_tiles}")
    print(f"Output folder: {output_dir.resolve()}")
    print(f"============================================================")

    downloaded = 0
    cached = 0
    failed = 0
    total_bytes = 0
    start_time = time.time()

    headers = {
        "User-Agent": "RocketTelemetryGroundStation/1.0 (+https://github.com/invenciones-espaciales/estacion-terrena-pc)",
        "Accept": "image/png,image/*;q=0.8,*/*;q=0.5",
    }

    for idx, (z, x, y) in enumerate(tasks, 1):
        tile_file = output_dir / str(z) / str(x) / f"{y}.png"
        if not force and tile_file.exists() and tile_file.stat().st_size > 0:
            cached += 1
            total_bytes += tile_file.stat().st_size
            continue

        url = f"https://tile.openstreetmap.org/{z}/{x}/{y}.png"
        req = urllib.request.Request(url, headers=headers)
        success = False

        for attempt in range(1, 4):
            try:
                with urllib.request.urlopen(req, timeout=5.0) as resp:
                    if resp.status == 200:
                        data = resp.read()
                        tile_file.parent.mkdir(parents=True, exist_ok=True)
                        tmp_file = tile_file.with_suffix(".tmp")
                        tmp_file.write_bytes(data)
                        tmp_file.replace(tile_file)
                        downloaded += 1
                        total_bytes += len(data)
                        success = True
                        break
            except Exception as exc:
                if attempt == 3:
                    print(f"  [WARN] Failed tile {z}/{x}/{y}: {exc}", file=sys.stderr)
                time.sleep(0.5 * attempt)

        if not success:
            failed += 1

        if delay > 0:
            time.sleep(delay)

        if idx % 10 == 0 or idx == total_tiles:
            elapsed = time.time() - start_time
            rate = (downloaded + cached) / max(0.1, elapsed)
            print(
                f"\rProgress: {idx}/{total_tiles} ({idx/total_tiles*100:.1f}%) | "
                f"DL: {downloaded} | Cache: {cached} | Fail: {failed} | "
                f"{rate:.1f} tiles/s",
                end="",
                flush=True,
            )

    elapsed_total = time.time() - start_time
    print()
    print(f"------------------------------------------------------------")
    print(f"Completed in {elapsed_total:.2f}s:")
    print(f"  Downloaded:    {downloaded}")
    print(f"  Already cached:{cached}")
    print(f"  Failed:        {failed}")
    print(f"  Total storage: {total_bytes / (1024 * 1024):.2f} MB")
    print(f"============================================================")
    return failed == 0


def main():
    parser = argparse.ArgumentParser(
        description="Pre-download OpenStreetMap tiles for offline rocket ground station operations."
    )
    parser.add_argument(
        "--lat",
        type=float,
        default=-34.6037,
        help="Center latitude of the launch / ground station field (default: -34.6037)",
    )
    parser.add_argument(
        "--lon",
        type=float,
        default=-58.3816,
        help="Center longitude of the launch / ground station field (default: -58.3816)",
    )
    parser.add_argument(
        "--radius-km",
        type=float,
        default=5.0,
        help="Coverage radius in kilometers around center (default: 5.0 km)",
    )
    parser.add_argument(
        "--min-zoom",
        type=int,
        default=10,
        help="Minimum zoom level (default: 10)",
    )
    parser.add_argument(
        "--max-zoom",
        type=int,
        default=15,
        help="Maximum zoom level (default: 15)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/tiles",
        help="Output directory path for cached tiles (default: data/tiles)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.08,
        help="Delay in seconds between tile requests to respect OSM rate limits (default: 0.08)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-download even if tile already exists on disk",
    )

    args = parser.parse_args()
    ok = predownload_tiles(
        lat=args.lat,
        lon=args.lon,
        radius_km=args.radius_km,
        min_zoom=args.min_zoom,
        max_zoom=args.max_zoom,
        output_dir=Path(args.output_dir),
        delay=args.delay,
        force=args.force,
    )
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
