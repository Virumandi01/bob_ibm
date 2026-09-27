"""
Solar Radiation Analyzer — SpaceMeshNode

Produced outputs:
    radiation_exposure_index (float, 0.0–1.0):
        Normalised measure of solar radiation exposure at the given orbital
        position.  Zero when the spacecraft is in eclipse; otherwise derived
        from the cosine of the solar latitude/longitude angles, scaled by an
        altitude factor that accounts for the reduced atmospheric shielding at
        higher orbits.

    thermal_stress_level (str: "LOW" | "MODERATE" | "HIGH" | "CRITICAL"):
        Categorical heat load driven by radiation_exposure_index.
        LOW      < 0.25
        MODERATE < 0.55
        HIGH     < 0.80
        CRITICAL ≥ 0.80

    shielding_mode (str: "PASSIVE" | "ACTIVE" | "EMERGENCY"):
        Recommended shielding posture derived from thermal_stress_level.
        PASSIVE   — LOW or MODERATE stress
        ACTIVE    — HIGH stress
        EMERGENCY — CRITICAL stress
"""

import math

from mesh_engine.core import SpaceMeshNode


class SolarRadiationAnalyzer(SpaceMeshNode):
    name = "solar_radiation_analyzer"
    required_inputs = ["solar_lat", "solar_lon", "altitude_km", "visibility"]
    produced_outputs = ["radiation_exposure_index", "thermal_stress_level", "shielding_mode"]

    async def process(self, payload: dict) -> dict:
        solar_lat = payload["solar_lat"]
        solar_lon = payload["solar_lon"]
        altitude_km = payload["altitude_km"]
        visibility = payload["visibility"]

        # ── 1. radiation_exposure_index ───────────────────────────────────────
        if visibility == "eclipsed":
            radiation_exposure_index = 0.0
        else:  # "daylight"
            base = math.cos(math.radians(solar_lat)) * math.cos(math.radians(solar_lon))
            base = max(0.0, min(base, 1.0))                      # clamp to [0, 1]
            altitude_factor = min(altitude_km / 500.0, 1.0)
            radiation_exposure_index = round(
                min(base * (0.8 + 0.2 * altitude_factor), 1.0), 4
            )

        # ── 2. thermal_stress_level ───────────────────────────────────────────
        if radiation_exposure_index < 0.25:
            thermal_stress_level = "LOW"
        elif radiation_exposure_index < 0.55:
            thermal_stress_level = "MODERATE"
        elif radiation_exposure_index < 0.80:
            thermal_stress_level = "HIGH"
        else:
            thermal_stress_level = "CRITICAL"

        # ── 3. shielding_mode ─────────────────────────────────────────────────
        if thermal_stress_level in ("LOW", "MODERATE"):
            shielding_mode = "PASSIVE"
        elif thermal_stress_level == "HIGH":
            shielding_mode = "ACTIVE"
        else:  # CRITICAL
            shielding_mode = "EMERGENCY"

        return {
            "radiation_exposure_index": radiation_exposure_index,
            "thermal_stress_level": thermal_stress_level,
            "shielding_mode": shielding_mode,
        }
