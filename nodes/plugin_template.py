"""
nodes/plugin_template.py
========================
Drop-in plugin template for live-demo / rapid prototyping.

HOW TO USE
----------
1. Copy this file to ``nodes/my_new_node.py``.
2. Rename the class and set a unique ``name``.
3. Fill in ``required_inputs``, ``produced_outputs``, and the ``process`` body.
4. Drop the file into ``nodes/`` — MeshOrchestrator auto-discovers and wires it
   on the **next telemetry cycle** with no restart required.

CONTRACTS (read these before editing)
--------------------------------------
- ``name``            — unique snake_case string; duplicate names crash startup.
- ``required_inputs`` — keys this node reads from the incoming merged payload.
- ``produced_outputs``— keys this node adds to the outgoing payload.
- ``process()``       — async; treat ``payload`` as read-only; return a new dict
                        containing **at minimum** the keys in ``produced_outputs``.
- Stateless          — nodes may be replicated 1–8× by the auto-scaler; do not
                        store mutable state on ``self``.

SELF-HEALING GUARANTEE
-----------------------
The orchestrator wraps every ``process()`` call in a circuit-breaker.
If your node raises after 1 retry, it receives a zeroed-out fallback payload —
downstream nodes are never starved.

LIVE API PATTERN (keyless)
---------------------------
Use ``urllib.request`` (stdlib, no key required) to hit any open endpoint:

    import urllib.request, json
    with urllib.request.urlopen(URL, timeout=5) as r:
        data = json.loads(r.read())

Example keyless endpoints already used by the mesh:
  - ISS position : https://api.wheretheiss.at/v1/satellites/25544
  - JPL NEO CAD  : https://ssd-api.jpl.nasa.gov/cad.api?dist-max=0.05&limit=5
"""

from __future__ import annotations

import asyncio
import json
import urllib.request
from typing import Any

from mesh_engine.core import SpaceMeshNode


class PluginTemplateNode(SpaceMeshNode):
    """
    Rename this class (e.g. ``MyLiveApiNode``) and update ``name``.

    Template demonstrates how to hit a live keyless API, parse the result,
    and return a clean output dict — all within the SpaceMesh I/O contract.
    """

    # ------------------------------------------------------------------ #
    # I/O contract — EDIT THESE                                           #
    # ------------------------------------------------------------------ #

    name = "plugin_template"  # ← change to a unique snake_case identifier

    required_inputs: list[str] = [
        # List the payload keys your node reads.
        # Example: "altitude_km", "latitude"
    ]

    produced_outputs: list[str] = [
        # List the keys your node adds to the payload.
        # Example: "my_derived_metric"
    ]

    # ------------------------------------------------------------------ #
    # Optional: live API endpoint (set to None if unused)                 #
    # ------------------------------------------------------------------ #

    _API_URL: str | None = None  # e.g. "https://api.example.com/data"
    _API_TIMEOUT_S: int = 5

    # ------------------------------------------------------------------ #
    # Core logic                                                          #
    # ------------------------------------------------------------------ #

    async def process(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Replace the body below with your real computation.

        ``payload`` is read-only — build and return a brand-new dict.
        """
        # --- optional live API fetch (runs in a thread to keep async clean) ---
        api_data: dict[str, Any] = {}
        if self._API_URL:
            try:
                api_data = await asyncio.get_event_loop().run_in_executor(
                    None, self._fetch_json, self._API_URL
                )
            except Exception:
                pass  # fallback: api_data stays empty; circuit-breaker handles retries

        # --- your computation here ---
        # example_metric = payload.get("altitude_km", 0.0) * 1.0

        # Return a dict with AT MINIMUM the keys in produced_outputs.
        return {
            # "my_derived_metric": example_metric,
        }

    # ------------------------------------------------------------------ #
    # Helper — blocking HTTP fetch (called via executor above)            #
    # ------------------------------------------------------------------ #

    def _fetch_json(self, url: str) -> dict[str, Any]:
        with urllib.request.urlopen(url, timeout=self._API_TIMEOUT_S) as resp:
            return json.loads(resp.read().decode())
