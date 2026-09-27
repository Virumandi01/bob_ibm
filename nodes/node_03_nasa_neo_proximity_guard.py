"""
NASA NEO Proximity Guard — SpaceMeshNode
========================================

Produced outputs
----------------
collision_risk_score : float, 0.0–1.0
    A weighted composite risk score derived from three normalised factors:

    * neo_factor      (weight 0.5) — normalised Near-Earth Object count.
      10 or more tracked NEOs in the vicinity drives this factor to its
      maximum value of 1.0.

    * altitude_factor (weight 0.3) — inverse of orbital altitude.
      Lower orbits have a higher probability of encountering debris or
      an NEO fragment; the factor is clamped to [0, 1] and reaches 0 at
      or above 2 000 km.

    * period_factor   (weight 0.2) — normalised orbital period.
      A longer period means the vehicle spends more time traversing
      high-risk regions; the factor saturates at 120 minutes.

    Final score = (0.5 × neo_factor) + (0.3 × altitude_factor)
                + (0.2 × period_factor), capped at 1.0 and rounded to
                four decimal places.

maneuver_recommended : bool
    True when collision_risk_score >= 0.55, indicating that an avoidance
    manoeuvre should be considered by the guidance system.
"""

from mesh_engine.core import SpaceMeshNode


class NasaNeoProximityGuard(SpaceMeshNode):
    name = "nasa_neo_proximity_guard"
    required_inputs = ["altitude_km", "nasa_neo_count", "orbital_period_min"]
    produced_outputs = ["collision_risk_score", "maneuver_recommended"]

    async def process(self, payload: dict) -> dict:
        altitude_km = payload["altitude_km"]
        nasa_neo_count = payload["nasa_neo_count"]
        orbital_period_min = payload["orbital_period_min"]

        neo_factor = min(nasa_neo_count / 10.0, 1.0)
        altitude_factor = max(0.0, 1.0 - altitude_km / 2000.0)
        period_factor = min(orbital_period_min / 120.0, 1.0)

        score = (0.5 * neo_factor) + (0.3 * altitude_factor) + (0.2 * period_factor)
        collision_risk_score = round(min(score, 1.0), 4)

        maneuver_recommended = collision_risk_score >= 0.55

        return {
            "collision_risk_score": collision_risk_score,
            "maneuver_recommended": maneuver_recommended,
        }
