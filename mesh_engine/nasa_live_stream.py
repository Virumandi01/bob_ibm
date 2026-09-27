"""
mesh_engine/nasa_live_stream.py
================================
Live keyless telemetry fetcher for SpaceMesh.

Sources
-------
- ISS Real-Time Position : https://api.wheretheiss.at/v1/satellites/25544
- NASA JPL CAD Close-Approach : https://ssd-api.jpl.nasa.gov/cad.api?dist-max=0.05&limit=5

No API keys required for either endpoint.

Normalised output dict fields
------------------------------
satellite_id          str   — NORAD catalog ID string
timestamp             float — Unix epoch (UTC)
latitude              float — degrees
longitude             float — degrees
altitude_km           float
velocity_kms          float
footprint_km          float
solar_lat             float
solar_lon             float
visibility            str   — "daylight" | "eclipsed"
nasa_neo_count        int   — live count from JPL CAD response
raw_unfiltered_api_dump dict — full upstream responses, unmodified
"""

from __future__ import annotations

import asyncio
import json
import math
import random
import time
import urllib.error
import urllib.request
from typing import Any

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_ISS_URL = "https://api.wheretheiss.at/v1/satellites/25544"
_JPL_CAD_URL = "https://ssd-api.jpl.nasa.gov/cad.api?dist-max=0.05&limit=5"
_HTTP_TIMEOUT = 10  # seconds


# ---------------------------------------------------------------------------
# Low-level HTTP helpers (stdlib only — no third-party deps required)
# ---------------------------------------------------------------------------

def _fetch_json(url: str, timeout: int = _HTTP_TIMEOUT) -> dict[str, Any]:
    """Synchronous JSON fetch via urllib (no external deps)."""
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "SpaceMesh/1.0 (telemetry-fetcher)"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return json.loads(raw)


async def _async_fetch_json(url: str, timeout: int = _HTTP_TIMEOUT) -> dict[str, Any]:
    """Run the blocking urllib fetch in a thread-pool so it doesn't block the event loop."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _fetch_json, url, timeout)


# ---------------------------------------------------------------------------
# Jitter fallback (used only when the machine is offline)
# ---------------------------------------------------------------------------

def _jitter_fallback(satellite_id: str = "25544") -> dict[str, Any]:
    """
    Generate a plausible-looking ISS telemetry snapshot with light random jitter.
    Used exclusively when both live endpoints are unreachable.
    """
    now = time.time()
    # Approximate ISS orbital parameters
    base_lat = math.sin(now / 5580) * 51.6          # ~51.6° inclination
    base_lon = ((now / 92.68) % 360) - 180           # ~92-min orbit → lon drift
    return {
        "satellite_id": satellite_id,
        "timestamp": now,
        "latitude": round(base_lat + random.uniform(-0.01, 0.01), 6),
        "longitude": round(base_lon + random.uniform(-0.01, 0.01), 6),
        "altitude_km": round(408.0 + random.uniform(-2.0, 2.0), 3),
        "velocity_kms": round(7.66 + random.uniform(-0.01, 0.01), 4),
        "footprint_km": round(4500.0 + random.uniform(-50, 50), 1),
        "solar_lat": round(random.uniform(-23.5, 23.5), 4),
        "solar_lon": round(random.uniform(-180.0, 180.0), 4),
        "visibility": random.choice(["daylight", "eclipsed"]),
        "nasa_neo_count": 0,
        "raw_unfiltered_api_dump": {"_source": "jitter_fallback", "online": False},
    }


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

def _normalise_iss(raw: dict[str, Any]) -> dict[str, Any]:
    """Map the wheretheiss.at response onto the canonical telemetry schema."""
    return {
        "satellite_id": str(raw.get("id", "25544")),
        "timestamp": float(raw.get("timestamp", time.time())),
        "latitude": float(raw.get("latitude", 0.0)),
        "longitude": float(raw.get("longitude", 0.0)),
        "altitude_km": float(raw.get("altitude", 0.0)),
        "velocity_kms": float(raw.get("velocity", 0.0)),
        "footprint_km": float(raw.get("footprint", 0.0)),
        "solar_lat": float(raw.get("solar_lat", 0.0)),
        "solar_lon": float(raw.get("solar_lon", 0.0)),
        "visibility": str(raw.get("visibility", "unknown")),
    }


def _extract_neo_count(jpl_raw: dict[str, Any]) -> int:
    """Return the number of close-approach objects from the JPL CAD response."""
    try:
        data = jpl_raw.get("data", [])
        return int(len(data))
    except (TypeError, ValueError):
        return 0


# ---------------------------------------------------------------------------
# Public class
# ---------------------------------------------------------------------------

class NasaTelemetryStream:
    """
    Live keyless telemetry fetcher for SpaceMesh.

    Usage (async)
    -------------
    stream = NasaTelemetryStream()
    telemetry = await stream.fetch()

    Usage (sync convenience)
    ------------------------
    telemetry = NasaTelemetryStream().fetch_sync()
    """

    def __init__(
        self,
        iss_url: str = _ISS_URL,
        jpl_url: str = _JPL_CAD_URL,
        http_timeout: int = _HTTP_TIMEOUT,
    ) -> None:
        self.iss_url = iss_url
        self.jpl_url = jpl_url
        self.http_timeout = http_timeout

    # ------------------------------------------------------------------
    # Core async fetch
    # ------------------------------------------------------------------

    async def fetch(self) -> dict[str, Any]:
        """
        Pull live data from both upstream APIs, normalise, and merge.

        Falls back to `_jitter_fallback()` if the machine is offline or
        either endpoint is unreachable.
        """
        iss_raw: dict[str, Any] = {}
        jpl_raw: dict[str, Any] = {}
        online = True

        # --- ISS telemetry ---
        try:
            iss_raw = await _async_fetch_json(self.iss_url, self.http_timeout)
        except (urllib.error.URLError, OSError, TimeoutError, json.JSONDecodeError):
            online = False

        # --- JPL CAD (best-effort; doesn't affect ISS fields) ---
        if online:
            try:
                jpl_raw = await _async_fetch_json(self.jpl_url, self.http_timeout)
            except (urllib.error.URLError, OSError, TimeoutError, json.JSONDecodeError):
                jpl_raw = {}

        if not online:
            return _jitter_fallback()

        # --- Normalise & merge ---
        telemetry = _normalise_iss(iss_raw)
        telemetry["nasa_neo_count"] = _extract_neo_count(jpl_raw)
        telemetry["raw_unfiltered_api_dump"] = {
            "iss": iss_raw,
            "jpl_cad": jpl_raw,
        }
        return telemetry

    # ------------------------------------------------------------------
    # Sync convenience wrapper
    # ------------------------------------------------------------------

    def fetch_sync(self) -> dict[str, Any]:
        """Blocking wrapper around :meth:`fetch` for non-async callers."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Inside an already-running loop (e.g. Jupyter / Streamlit)
                import concurrent.futures

                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(asyncio.run, self.fetch())
                    return future.result()
            return loop.run_until_complete(self.fetch())
        except RuntimeError:
            return asyncio.run(self.fetch())

    # ------------------------------------------------------------------
    # Continuous streaming helper
    # ------------------------------------------------------------------

    async def stream(self, interval_seconds: float = 5.0):
        """
        Async generator that yields a fresh telemetry dict every
        `interval_seconds` seconds indefinitely.

        Example
        -------
        async for snapshot in NasaTelemetryStream().stream(interval_seconds=10):
            print(snapshot["latitude"], snapshot["longitude"])
        """
        while True:
            yield await self.fetch()
            await asyncio.sleep(interval_seconds)
