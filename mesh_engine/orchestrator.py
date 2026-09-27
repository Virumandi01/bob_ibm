"""
mesh_engine/orchestrator.py
============================
MeshOrchestrator — autonomous, self-healing, auto-scaling execution engine.

Responsibilities
----------------
1. **Auto-discovery**: scans every ``*.py`` file inside ``nodes/`` and
   imports all :class:`~mesh_engine.core.SpaceMeshNode` subclasses.
2. **DAG wiring**: matches ``produced_outputs`` → ``required_inputs`` to
   build a directed acyclic graph; detects dependency order automatically.
3. **Auto-scaling**: :meth:`auto_scale_node` dynamically adjusts async
   worker replica counts (1–8) per node based on queue depth and latency.
4. **Self-healing**: every node execution is wrapped in a circuit-breaker
   with 1 automatic retry and safe fallback output injection so no single
   broken node can crash the mesh.
"""

from __future__ import annotations

import asyncio
import importlib.util
import inspect
import logging
import sys
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from mesh_engine.core import SpaceMeshNode

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Circuit-breaker state machine
# ---------------------------------------------------------------------------

class CircuitState(Enum):
    CLOSED = "CLOSED"        # normal operation
    OPEN = "OPEN"            # failing — short-circuit to fallback
    HALF_OPEN = "HALF_OPEN"  # testing recovery


@dataclass
class CircuitBreaker:
    """Per-node circuit breaker with single-retry logic."""

    node_name: str
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    last_failure_time: float = 0.0

    # How long (seconds) to stay OPEN before moving to HALF_OPEN
    recovery_timeout: float = 30.0
    # Number of failures before opening the circuit
    failure_threshold: int = 1
    # Retry back-off in seconds
    retry_backoff: float = 0.05

    def _should_attempt(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            if time.monotonic() - self.last_failure_time >= self.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                logger.info("[circuit] %s → HALF_OPEN", self.node_name)
                return True
            return False
        # HALF_OPEN: allow one probe
        return True

    def record_success(self) -> None:
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        logger.debug("[circuit] %s → CLOSED (success)", self.node_name)

    def record_failure(self) -> None:
        self.failure_count += 1
        self.last_failure_time = time.monotonic()
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN
            logger.warning("[circuit] %s → OPEN (failure_count=%d)", self.node_name, self.failure_count)

    async def call(
        self,
        coro_fn,
        payload: dict[str, Any],
        fallback: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Execute ``coro_fn(payload)`` with 1 retry and circuit-breaker logic.

        If the circuit is OPEN the fallback is returned immediately.
        On failure after the retry the fallback is returned and the circuit
        is opened.
        """
        if not self._should_attempt():
            logger.warning("[circuit] %s is OPEN — injecting fallback", self.node_name)
            return fallback

        for attempt in range(2):  # attempt 0 = first try, attempt 1 = retry
            try:
                result: dict[str, Any] = await coro_fn(payload)
                self.record_success()
                return result
            except Exception as exc:  # noqa: BLE001
                if attempt == 0:
                    logger.warning(
                        "[circuit] %s attempt 1 failed (%s) — retrying in %.0fms",
                        self.node_name,
                        exc,
                        self.retry_backoff * 1000,
                    )
                    await asyncio.sleep(self.retry_backoff)
                else:
                    logger.error(
                        "[circuit] %s retry also failed (%s) — injecting fallback",
                        self.node_name,
                        exc,
                    )
                    self.record_failure()
                    return fallback

        # Should never reach here
        return fallback  # pragma: no cover


# ---------------------------------------------------------------------------
# Node replica / scaling bookkeeping
# ---------------------------------------------------------------------------

@dataclass
class NodeMeta:
    node: SpaceMeshNode
    circuit: CircuitBreaker
    replicas: int = 1
    semaphore: asyncio.Semaphore = field(default_factory=lambda: asyncio.Semaphore(1))

    def set_replicas(self, n: int) -> None:
        if n != self.replicas:
            logger.info("[scale] %s replicas %d → %d", self.node.name, self.replicas, n)
            self.replicas = n
            self.semaphore = asyncio.Semaphore(n)


# ---------------------------------------------------------------------------
# DAG helpers
# ---------------------------------------------------------------------------

def _topological_sort(
    nodes: list[SpaceMeshNode],
) -> list[SpaceMeshNode]:
    """
    Return nodes in a valid topological execution order based on
    ``required_inputs`` / ``produced_outputs`` declarations.

    Raises ``ValueError`` if a cycle is detected.
    """
    # Build adjacency: node A must run before node B if A produces something B requires
    name_to_node = {n.name: n for n in nodes}
    produced_by: dict[str, str] = {}  # field → node.name
    for node in nodes:
        for key in node.produced_outputs:
            if key in produced_by:
                raise ValueError(
                    f"Duplicate produced_output '{key}' from "
                    f"'{produced_by[key]}' and '{node.name}'"
                )
            produced_by[key] = node.name

    # Dependency edges: name → set of names that must run first
    deps: dict[str, set[str]] = {n.name: set() for n in nodes}
    for node in nodes:
        for req in node.required_inputs:
            if req in produced_by:
                deps[node.name].add(produced_by[req])

    # Kahn's algorithm
    in_degree: dict[str, int] = {name: len(d) for name, d in deps.items()}
    queue = [name for name, deg in in_degree.items() if deg == 0]
    order: list[SpaceMeshNode] = []

    while queue:
        name = queue.pop(0)
        order.append(name_to_node[name])
        for other_name, other_deps in deps.items():
            if name in other_deps:
                in_degree[other_name] -= 1
                if in_degree[other_name] == 0:
                    queue.append(other_name)

    if len(order) != len(nodes):
        remaining = set(n.name for n in nodes) - set(n.name for n in order)
        raise ValueError(f"Cycle detected in node DAG involving: {remaining}")

    return order


def _build_fallback(node: SpaceMeshNode) -> dict[str, Any]:
    """Return a zeroed-out fallback dict for ``node.produced_outputs``."""
    return {key: None for key in node.produced_outputs}


# ---------------------------------------------------------------------------
# Node auto-discovery
# ---------------------------------------------------------------------------

def discover_nodes(nodes_dir: str | Path = "nodes") -> list[SpaceMeshNode]:
    """
    Scan ``nodes_dir`` for Python files, import them, and return one
    instantiated object per :class:`SpaceMeshNode` subclass found.

    Raises ``ValueError`` on duplicate ``name`` attributes.
    """
    nodes_path = Path(nodes_dir)
    instances: list[SpaceMeshNode] = []
    seen_names: dict[str, str] = {}  # name → source file

    if not nodes_path.is_dir():
        logger.info("[discovery] nodes directory '%s' not found — no nodes loaded", nodes_path)
        return instances

    for py_file in sorted(nodes_path.glob("*.py")):
        if py_file.name.startswith("_"):
            continue  # skip __init__.py etc.

        module_name = f"nodes.{py_file.stem}"
        spec = importlib.util.spec_from_file_location(module_name, py_file)
        if spec is None or spec.loader is None:
            logger.warning("[discovery] could not create spec for %s — skipping", py_file)
            continue

        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)  # type: ignore[union-attr]
        except Exception as exc:  # noqa: BLE001
            logger.error("[discovery] failed to import %s: %s", py_file, exc)
            continue

        for _attr_name, obj in inspect.getmembers(module, inspect.isclass):
            if (
                issubclass(obj, SpaceMeshNode)
                and obj is not SpaceMeshNode
                and not inspect.isabstract(obj)
            ):
                node_name: str = obj.name  # type: ignore[assignment]
                if not node_name:
                    logger.warning("[discovery] %s has no name — skipping", obj)
                    continue
                if node_name in seen_names:
                    raise ValueError(
                        f"Duplicate node name '{node_name}' in "
                        f"'{py_file}' and '{seen_names[node_name]}'"
                    )
                seen_names[node_name] = str(py_file)
                instances.append(obj())
                logger.info("[discovery] loaded node '%s' from %s", node_name, py_file.name)

    return instances


# ---------------------------------------------------------------------------
# MeshOrchestrator
# ---------------------------------------------------------------------------

class MeshOrchestrator:
    """
    Autonomous orchestrator for the SpaceMesh node graph.

    Parameters
    ----------
    nodes_dir:
        Path to the directory containing drop-in node modules.
    """

    def __init__(self, nodes_dir: str | Path = "nodes") -> None:
        self._nodes_dir = Path(nodes_dir)
        self._meta: dict[str, NodeMeta] = {}      # name → NodeMeta
        self._dag_order: list[SpaceMeshNode] = []  # topologically sorted

        raw_nodes = discover_nodes(self._nodes_dir)
        if raw_nodes:
            self._dag_order = _topological_sort(raw_nodes)
            for node in self._dag_order:
                self._meta[node.name] = NodeMeta(
                    node=node,
                    circuit=CircuitBreaker(node_name=node.name),
                )
            logger.info(
                "[orchestrator] DAG wired: %s",
                " → ".join(n.name for n in self._dag_order),
            )
        else:
            logger.info("[orchestrator] no nodes discovered; mesh is empty")

    # ------------------------------------------------------------------
    # Auto-scaling
    # ------------------------------------------------------------------

    def auto_scale_node(
        self,
        node_name: str,
        queue_depth: int,
        latency_ms: float,
    ) -> int:
        """
        Adjust the number of async worker replicas for ``node_name``.

        Scaling table
        -------------
        queue_depth ≤ 2                         → 1 replica
        queue_depth 3–5, latency < 200 ms       → 2 replicas
        queue_depth 3–5, latency ≥ 200 ms       → 4 replicas
        queue_depth > 5                         → min(8, queue_depth)

        Returns the new replica count.
        """
        if queue_depth <= 2:
            replicas = 1
        elif queue_depth <= 5:
            replicas = 2 if latency_ms < 200 else 4
        else:
            replicas = min(8, queue_depth)

        if node_name in self._meta:
            self._meta[node_name].set_replicas(replicas)
        else:
            logger.warning("[scale] unknown node '%s'", node_name)

        return replicas

    # ------------------------------------------------------------------
    # Single-run execution
    # ------------------------------------------------------------------

    async def run(self, initial_payload: dict[str, Any]) -> dict[str, Any]:
        """
        Execute every node in DAG order, threading outputs into the
        cumulative payload for downstream consumers.

        Parameters
        ----------
        initial_payload:
            Seed data (e.g. raw telemetry from :class:`NasaTelemetryStream`).

        Returns
        -------
        The fully merged output payload after all nodes have run.
        """
        payload: dict[str, Any] = dict(initial_payload)

        # Pre-compute the set of all downstream required keys so each node
        # can call analyze_and_filter_io correctly.
        all_downstream_keys: set[str] = set()
        for node in self._dag_order:
            all_downstream_keys.update(node.required_inputs)

        for node in self._dag_order:
            meta = self._meta[node.name]
            fallback = _build_fallback(node)

            async with meta.semaphore:
                result = await meta.circuit.call(
                    coro_fn=node.process,
                    payload=payload,
                    fallback=fallback,
                )

            # Merge node outputs into cumulative payload
            payload.update(result)

            # Run I/O analysis and log efficiency metrics
            _, io_metrics = node.analyze_and_filter_io(payload, all_downstream_keys)
            logger.debug(
                "[io] %s  efficiency=%.1f%%  stripped=%s",
                node.name,
                io_metrics["efficiency_pct"],
                io_metrics["stripped_keys"],
            )

        return payload

    # ------------------------------------------------------------------
    # Continuous mesh loop
    # ------------------------------------------------------------------

    async def run_mesh_loop(
        self,
        telemetry_stream,
        interval_seconds: float = 5.0,
    ) -> None:
        """
        Pull live telemetry and drive the entire node DAG on each tick.

        Parameters
        ----------
        telemetry_stream:
            Any async-iterable that yields telemetry dicts, e.g.
            ``NasaTelemetryStream().stream(interval_seconds=5)``.
        interval_seconds:
            Passed to the stream generator (tick interval).
        """
        logger.info("[mesh] starting continuous loop (interval=%.1fs)", interval_seconds)
        async for snapshot in telemetry_stream:
            start = time.monotonic()
            result = await self.run(snapshot)
            elapsed_ms = (time.monotonic() - start) * 1000
            logger.info(
                "[mesh] tick complete in %.1fms  lat=%.1f  lon=%.1f  neo=%s",
                elapsed_ms,
                result.get("latitude", 0.0),
                result.get("longitude", 0.0),
                result.get("nasa_neo_count", "?"),
            )

    # ------------------------------------------------------------------
    # Repr
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        node_names = [n.name for n in self._dag_order]
        return f"<MeshOrchestrator nodes={node_names}>"


# ---------------------------------------------------------------------------
# CLI entry-point  (python -m mesh_engine.orchestrator)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import os

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )

    from mesh_engine.nasa_live_stream import NasaTelemetryStream

    interval = float(os.environ.get("SPACEMESH_INTERVAL", "5"))
    stream = NasaTelemetryStream()
    orchestrator = MeshOrchestrator(nodes_dir="nodes")

    print(f"SpaceMesh starting — {orchestrator}")
    print(f"Tick interval: {interval}s   (set SPACEMESH_INTERVAL to override)")

    asyncio.run(
        orchestrator.run_mesh_loop(
            telemetry_stream=stream.stream(interval_seconds=interval),
            interval_seconds=interval,
        )
    )
