"""
Orbital kinematics node for the SpaceMesh engine.

Produced outputs
----------------
orbital_period_min
    The time (in minutes) for one complete orbit, derived from the
    vis-viva / Kepler third-law relationship:
        T = 2π * sqrt(a³ / μ)
    where a is the semi-major axis (Earth radius + altitude) and
    μ = 398 600.4418 km³/s² is Earth's standard gravitational parameter.

specific_orbital_energy
    The specific mechanical energy of the orbit (km²/s²):
        ε = v² / 2 − μ / a
    Negative values indicate a closed (elliptical / circular) orbit;
    zero or positive values indicate parabolic / hyperbolic trajectories.

ground_track_quadrant
    The hemisphere quadrant directly below the satellite, determined by
    the sign of the geodetic latitude and longitude:
        "NE"  latitude ≥ 0 and longitude ≥ 0
        "NW"  latitude ≥ 0 and longitude <  0
        "SE"  latitude <  0 and longitude ≥ 0
        "SW"  latitude <  0 and longitude <  0
"""

import math

from mesh_engine.core import SpaceMeshNode


class OrbitalKinematicsNode(SpaceMeshNode):
    name = "orbital_kinematics"
    required_inputs = ["altitude_km", "velocity_kms", "latitude", "longitude"]
    produced_outputs = ["orbital_period_min", "specific_orbital_energy", "ground_track_quadrant"]

    # WGS-84 / standard gravitational constants
    _R_E = 6371.0          # Earth mean radius, km
    _MU  = 398600.4418     # Earth gravitational parameter, km³/s²

    async def process(self, payload: dict) -> dict:
        altitude_km  = payload["altitude_km"]
        velocity_kms = payload["velocity_kms"]
        latitude     = payload["latitude"]
        longitude    = payload["longitude"]

        # Semi-major axis
        a = self._R_E + altitude_km

        # 1. Orbital period (seconds → minutes)
        period_s = 2.0 * math.pi * math.sqrt(a ** 3 / self._MU)
        orbital_period_min = round(period_s / 60.0, 4)

        # 2. Specific orbital energy (km²/s²)
        specific_orbital_energy = round(
            (velocity_kms ** 2) / 2.0 - self._MU / a, 4
        )

        # 3. Ground-track quadrant
        if latitude >= 0 and longitude >= 0:
            ground_track_quadrant = "NE"
        elif latitude >= 0 and longitude < 0:
            ground_track_quadrant = "NW"
        elif latitude < 0 and longitude >= 0:
            ground_track_quadrant = "SE"
        else:
            ground_track_quadrant = "SW"

        return {
            "orbital_period_min":      orbital_period_min,
            "specific_orbital_energy": specific_orbital_energy,
            "ground_track_quadrant":   ground_track_quadrant,
        }
