"""
Downlink synthesizer — terminal synthesis node for the SpaceMesh engine.

This node aggregates the outputs of the upstream analysis nodes (orbital
kinematics, solar radiation, and proximity guard) into a single consolidated
mission health assessment.  It is the last node in the processing chain and
is responsible for producing the two artefacts that are transmitted in the
priority downlink window:

mission_health_summary
    A human-readable status string ("NOMINAL", "CAUTION", "WARNING", or
    "CRITICAL") derived from a weighted composite health score.  If an
    autonomous maneuver has been flagged by a preceding node the label is
    extended with " | MANEUVER ADVISED".

priority_downlink_packet
    A structured dictionary containing the numeric health score, the status
    label, the maneuver flag, and a decomposed risk vector so that ground
    operators can audit the individual contributions to the overall score.
"""

from mesh_engine.core import SpaceMeshNode


class DownlinkSynthesizerNode(SpaceMeshNode):
    name = "downlink_synthesizer"
    required_inputs = [
        "orbital_period_min",
        "radiation_exposure_index",
        "collision_risk_score",
        "maneuver_recommended",
    ]
    produced_outputs = ["mission_health_summary", "priority_downlink_packet"]

    async def process(self, payload: dict) -> dict:
        orbital_period_min      = payload["orbital_period_min"]
        radiation_exposure_index = payload["radiation_exposure_index"]
        collision_risk_score    = payload["collision_risk_score"]
        maneuver_recommended    = payload["maneuver_recommended"]

        # 1. Composite health score (0.0 – 1.0)
        orbital_exposure = min(orbital_period_min / 120.0, 1.0)
        health = (
            1.0
            - 0.4 * collision_risk_score
            - 0.4 * radiation_exposure_index
            - 0.2 * orbital_exposure
        )
        health = round(max(0.0, min(health, 1.0)), 4)

        # 2. Mission health summary string
        if health >= 0.80:
            mission_health_summary = "NOMINAL"
        elif health >= 0.55:
            mission_health_summary = "CAUTION"
        elif health >= 0.30:
            mission_health_summary = "WARNING"
        else:
            mission_health_summary = "CRITICAL"

        if maneuver_recommended:
            mission_health_summary += " | MANEUVER ADVISED"

        # 3. Priority downlink packet
        priority_downlink_packet = {
            "health_score":    health,
            "mission_status":  mission_health_summary,
            "maneuver_flag":   maneuver_recommended,
            "risk_vector": {
                "collision":        collision_risk_score,
                "radiation":        radiation_exposure_index,
                "orbital_exposure": round(orbital_exposure, 4),
            },
        }

        return {
            "mission_health_summary":  mission_health_summary,
            "priority_downlink_packet": priority_downlink_packet,
        }
