"""
mesh_engine/core.py
====================
Base class for every SpaceMesh processing node.

Every drop-in node inside ``nodes/`` must subclass :class:`SpaceMeshNode`
and declare its I/O contract via ``required_inputs`` / ``produced_outputs``.
The orchestrator uses those declarations to auto-wire the execution DAG.
"""

from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from typing import Any


class SpaceMeshNode(ABC):
    """
    Abstract base class for all SpaceMesh nodes.

    Subclass contract
    -----------------
    - Set class attributes ``name``, ``required_inputs``, ``produced_outputs``.
    - Implement the ``async process(self, payload: dict) -> dict`` method.
    - Treat ``payload`` as **read-only**; return a new dict with your outputs.

    Example
    -------
    .. code-block:: python

        class AltitudeAlertNode(SpaceMeshNode):
            name = "altitude_alert"
            required_inputs  = ["altitude_km"]
            produced_outputs = ["altitude_alert_flag"]

            async def process(self, payload: dict) -> dict:
                flag = payload["altitude_km"] < 370.0
                return {"altitude_alert_flag": flag}
    """

    # ------------------------------------------------------------------
    # Class-level I/O contract (must be overridden)
    # ------------------------------------------------------------------

    #: Unique snake_case identifier for this node.
    name: str = ""

    #: Keys this node reads from the incoming payload.
    required_inputs: list[str] = []

    #: Keys this node adds to / overwrites in the outgoing payload.
    produced_outputs: list[str] = []

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    async def process(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Execute this node's logic.

        Parameters
        ----------
        payload:
            Merged telemetry/data dict assembled by the orchestrator from
            all upstream nodes' outputs.

        Returns
        -------
        dict
            Must contain **at minimum** the keys declared in
            ``produced_outputs``.  Extra keys are forwarded as-is.
        """

    # ------------------------------------------------------------------
    # I/O analysis helper
    # ------------------------------------------------------------------

    def analyze_and_filter_io(
        self,
        payload: dict[str, Any],
        needed_downstream_keys: set[str],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """
        Inspect the outgoing payload, strip keys not needed downstream,
        and return both the filtered dict and a metrics summary.

        Parameters
        ----------
        payload:
            The full outgoing dict (i.e. the merged result after this node
            has run, still containing ``raw_unfiltered_api_dump`` and any
            other large intermediate keys).
        needed_downstream_keys:
            The set of keys that downstream nodes have declared in their
            ``required_inputs``.  Any key in ``payload`` that is **not** in
            this set is stripped.

        Returns
        -------
        (filtered_payload, io_metrics)

        ``io_metrics`` contains:

        .. code-block:: python

            {
                "original_key_count":  int,
                "filtered_key_count":  int,
                "stripped_keys":       list[str],
                "original_bytes":      int,   # rough sys.getsizeof sum
                "filtered_bytes":      int,
                "bytes_stripped":      int,
                "efficiency_pct":      float, # bytes_stripped / original * 100
            }

        Notes
        -----
        ``sys.getsizeof`` measures the shallow memory footprint of each
        value — it is used as a *fast proxy* for byte cost, not an exact
        serialised-wire-size.  It is accurate enough to drive efficiency
        reporting without pulling in a JSON serialiser.
        """
        original_keys = list(payload.keys())

        # Measure original byte footprint (shallow per value)
        original_bytes: int = sum(
            sys.getsizeof(v) for v in payload.values()
        )

        # Determine which keys survive
        kept_keys = [k for k in original_keys if k in needed_downstream_keys]
        stripped_keys = [k for k in original_keys if k not in needed_downstream_keys]

        filtered_payload = {k: payload[k] for k in kept_keys}

        # Measure filtered byte footprint
        filtered_bytes: int = sum(
            sys.getsizeof(v) for v in filtered_payload.values()
        )

        bytes_stripped = original_bytes - filtered_bytes
        efficiency_pct = (
            round(bytes_stripped / original_bytes * 100, 2)
            if original_bytes > 0
            else 0.0
        )

        io_metrics: dict[str, Any] = {
            "original_key_count": len(original_keys),
            "filtered_key_count": len(kept_keys),
            "stripped_keys": stripped_keys,
            "original_bytes": original_bytes,
            "filtered_bytes": filtered_bytes,
            "bytes_stripped": bytes_stripped,
            "efficiency_pct": efficiency_pct,
        }

        return filtered_payload, io_metrics

    # ------------------------------------------------------------------
    # Repr
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__} name={self.name!r} "
            f"in={self.required_inputs} out={self.produced_outputs}>"
        )
