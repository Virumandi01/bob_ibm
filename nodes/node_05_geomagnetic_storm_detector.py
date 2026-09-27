"""
Geomagnetic Storm Detector — Hot-added SpaceMeshNode demo.

Produces
--------
geomagnetic_storm_index : float, 0.0–1.0
    Composite geomagnetic activity index derived from solar radiation exposure,
    orbital altitude, and solar angular position.  Higher values indicate
    greater risk of satellite attitude / electronics disruption.

geomagnetic_alert_level : str
    Categorical alert: "QUIET" | "UNSETTLED" | "STORM" | "SEVERE"
"""

import math

from mesh_engine.core import SpaceMeshNode


class GeomagneticStormDetector(SpaceMeshNode):
    name = "geomagnetic_storm_detector"
    required_inputs = ["radiation_exposure_index", "altitude_km", "solar_lat"]
    produced_outputs = ["geomagnetic_storm_index", "geomagnetic_alert_level"]

    async def process(self, payload: dict) -> dict:
        rei = float(payload.get("radiation_exposure_index", 0.0))
        alt = float(payload.get("altitude_km", 400.0))
        slat = float(payload.get("solar_lat", 0.0))

        # Higher radiation, lower altitude, and equatorial solar latitude raise index
        alt_factor = max(0.0, 1.0 - (alt - 200.0) / 800.0)
        lat_factor = 1.0 - abs(slat) / 90.0
        index = round(min(0.4 * rei + 0.35 * alt_factor + 0.25 * lat_factor, 1.0), 4)

        if index < 0.25:
            level = "QUIET"
        elif index < 0.50:
            level = "UNSETTLED"
        elif index < 0.75:
            level = "STORM"
        else:
            level = "SEVERE"

        return {
            "geomagnetic_storm_index": index,
            "geomagnetic_alert_level": level,
        }
