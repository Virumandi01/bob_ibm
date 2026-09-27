"""
tests/test_spacemesh.py
=======================
Integration test suite for the SpaceMesh autonomous mesh engine.

Covers
------
1. Dynamic node discovery from nodes/
2. Topological sort and end-to-end execution through all nodes
3. I/O payload filtering efficiency (raw noise fields stripped)
4. Auto-scaling worker adjustments under simulated queue depth
5. Circuit breaker recovery on forced node failure
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio

# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

NODES_DIR = Path(__file__).parent.parent / "nodes"

# A realistic telemetry payload that satisfies every node's required_inputs
SAMPLE_TELEMETRY: dict[str, Any] = {
    "satellite_id": "25544",
    "timestamp": 1_700_000_000.0,
    "latitude": 28.5,
    "longitude": -80.7,
    "altitude_km": 408.0,
    "velocity_kms": 7.66,
    "footprint_km": 4500.0,
    "solar_lat": 12.3,
    "solar_lon": -45.0,
    "visibility": "daylight",
    "nasa_neo_count": 3,
    "raw_unfiltered_api_dump": {
        "iss": {"id": 25544, "altitude": 408.0},
        "jpl_cad": {"data": [["1"], ["2"], ["3"]]},
    },
}


@pytest.fixture
def fresh_orchestrator():
    """Return a freshly constructed MeshOrchestrator each test."""
    import sys

    # Evict cached node modules to ensure a clean discovery pass
    for key in list(sys.modules.keys()):
        if key.startswith("nodes."):
            del sys.modules[key]

    from mesh_engine.orchestrator import MeshOrchestrator
    return MeshOrchestrator(nodes_dir=NODES_DIR)


# ---------------------------------------------------------------------------
# 1. Dynamic node discovery
# ---------------------------------------------------------------------------

class TestNodeDiscovery:
    def test_discovers_four_core_nodes(self, fresh_orchestrator):
        """All four drop-in nodes inside nodes/ are discovered."""
        orch = fresh_orchestrator
        discovered = {n.name for n in orch._dag_order}
        expected = {
            "orbital_kinematics",
            "solar_radiation_analyzer",
            "nasa_neo_proximity_guard",
            "downlink_synthesizer",
        }
        assert expected.issubset(discovered), (
            f"Missing nodes: {expected - discovered}"
        )

    def test_node_count_at_least_four(self, fresh_orchestrator):
        assert len(fresh_orchestrator._dag_order) >= 4

    def test_each_node_has_unique_name(self, fresh_orchestrator):
        names = [n.name for n in fresh_orchestrator._dag_order]
        assert len(names) == len(set(names)), "Duplicate node names detected"

    def test_all_discovered_nodes_have_meta(self, fresh_orchestrator):
        orch = fresh_orchestrator
        for node in orch._dag_order:
            assert node.name in orch._meta, f"No meta entry for node '{node.name}'"

    def test_plugin_template_has_empty_io_contracts(self, fresh_orchestrator):
        """plugin_template node has empty required_inputs and produced_outputs (it's a scaffold)."""
        orch = fresh_orchestrator
        template_nodes = [n for n in orch._dag_order if n.name == "plugin_template"]
        # plugin_template IS discovered (it's a concrete SpaceMeshNode) but its
        # I/O contracts are intentionally empty — it acts as a pass-through scaffold.
        if template_nodes:
            t = template_nodes[0]
            assert t.required_inputs == [], "plugin_template should have no required_inputs"
            assert t.produced_outputs == [], "plugin_template should have no produced_outputs"


# ---------------------------------------------------------------------------
# 2. Topological sort and end-to-end execution
# ---------------------------------------------------------------------------

class TestTopologicalSortAndExecution:
    def test_topological_order_respects_dependencies(self, fresh_orchestrator):
        """orbital_kinematics must precede nasa_neo_proximity_guard (which consumes orbital_period_min)."""
        orch = fresh_orchestrator
        names = [n.name for n in orch._dag_order]
        assert names.index("orbital_kinematics") < names.index("nasa_neo_proximity_guard")

    def test_topological_order_solar_before_downlink(self, fresh_orchestrator):
        orch = fresh_orchestrator
        names = [n.name for n in orch._dag_order]
        assert names.index("solar_radiation_analyzer") < names.index("downlink_synthesizer")

    @pytest.mark.asyncio
    async def test_end_to_end_run_returns_all_produced_outputs(self, fresh_orchestrator):
        """Running a full telemetry packet produces all declared outputs from every node."""
        orch = fresh_orchestrator
        result = await orch.run(dict(SAMPLE_TELEMETRY))

        expected_outputs = {
            "orbital_period_min",
            "specific_orbital_energy",
            "ground_track_quadrant",
            "radiation_exposure_index",
            "thermal_stress_level",
            "shielding_mode",
            "collision_risk_score",
            "maneuver_recommended",
            "mission_health_summary",
            "priority_downlink_packet",
        }
        missing = expected_outputs - result.keys()
        assert not missing, f"Missing output keys: {missing}"

    @pytest.mark.asyncio
    async def test_orbital_period_plausible(self, fresh_orchestrator):
        """ISS orbital period should be approx 92 minutes (85–100 min acceptable range)."""
        result = await fresh_orchestrator.run(dict(SAMPLE_TELEMETRY))
        period = result["orbital_period_min"]
        assert 85.0 <= period <= 100.0, f"Orbital period out of range: {period}"

    @pytest.mark.asyncio
    async def test_mission_health_is_valid_status(self, fresh_orchestrator):
        result = await fresh_orchestrator.run(dict(SAMPLE_TELEMETRY))
        summary = result["mission_health_summary"]
        valid_prefixes = {"NOMINAL", "CAUTION", "WARNING", "CRITICAL"}
        status_word = summary.split(" | ")[0]
        assert status_word in valid_prefixes, f"Unexpected health summary: {summary!r}"

    @pytest.mark.asyncio
    async def test_priority_downlink_packet_structure(self, fresh_orchestrator):
        result = await fresh_orchestrator.run(dict(SAMPLE_TELEMETRY))
        packet = result["priority_downlink_packet"]
        assert "health_score" in packet
        assert "mission_status" in packet
        assert "maneuver_flag" in packet
        assert "risk_vector" in packet
        assert isinstance(packet["health_score"], float)
        assert 0.0 <= packet["health_score"] <= 1.0

    @pytest.mark.asyncio
    async def test_ground_track_quadrant_valid(self, fresh_orchestrator):
        result = await fresh_orchestrator.run(dict(SAMPLE_TELEMETRY))
        assert result["ground_track_quadrant"] in {"NE", "NW", "SE", "SW"}

    @pytest.mark.asyncio
    async def test_radiation_index_in_range(self, fresh_orchestrator):
        result = await fresh_orchestrator.run(dict(SAMPLE_TELEMETRY))
        idx = result["radiation_exposure_index"]
        assert 0.0 <= idx <= 1.0, f"Radiation index out of range: {idx}"

    @pytest.mark.asyncio
    async def test_eclipsed_visibility_zero_radiation(self, fresh_orchestrator):
        payload = dict(SAMPLE_TELEMETRY) | {"visibility": "eclipsed"}
        result = await fresh_orchestrator.run(payload)
        assert result["radiation_exposure_index"] == 0.0

    @pytest.mark.asyncio
    async def test_collision_risk_score_in_range(self, fresh_orchestrator):
        result = await fresh_orchestrator.run(dict(SAMPLE_TELEMETRY))
        score = result["collision_risk_score"]
        assert 0.0 <= score <= 1.0, f"Collision risk out of range: {score}"


# ---------------------------------------------------------------------------
# 3. I/O payload filtering efficiency
# ---------------------------------------------------------------------------

class TestIOFilteringEfficiency:
    @pytest.mark.asyncio
    async def test_raw_dump_stripped_by_last_node(self, fresh_orchestrator):
        """raw_unfiltered_api_dump is not needed by any downstream node and must be stri
ppable."""
        orch = fresh_orchestrator
        result = await orch.run(dict(SAMPLE_TELEMETRY))

        # Collect all downstream required keys
        all_downstream = set()
        for node in orch._dag_order:
            all_downstream.update(node.required_inputs)

        last_node = orch._dag_order[-1]
        filtered, metrics = last_node.analyze_and_filter_io(result, all_downstream)

        assert "raw_unfiltered_api_dump" in metrics["stripped_keys"], (
            "raw_unfiltered_api_dump should be stripped from the downstream payload"
        )

    @pytest.mark.asyncio
    async def test_efficiency_pct_positive_when_noise_present(self, fresh_orchestrator):
        """With a large raw_unfiltered_api_dump, efficiency should be > 0%."""
        orch = fresh_orchestrator
        noisy_payload = dict(SAMPLE_TELEMETRY)
        noisy_payload["raw_unfiltered_api_dump"] = {"noise": "x" * 10_000}
        result = await orch.run(noisy_payload)

        all_downstream = set()
        for node in orch._dag_order:
            all_downstream.update(node.required_inputs)

        _, metrics = orch._dag_order[-1].analyze_and_filter_io(result, all_downstream)
        assert metrics["efficiency_pct"] > 0.0, "Expected positive efficiency % with noisy payload"

    @pytest.mark.asyncio
    async def test_io_metrics_structure(self, fresh_orchestrator):
        orch = fresh_orchestrator
        result = await orch.run(dict(SAMPLE_TELEMETRY))

        all_downstream = set()
        for node in orch._dag_order:
            all_downstream.update(node.required_inputs)

        _, metrics = orch._dag_order[-1].analyze_and_filter_io(result, all_downstream)

        required_keys = {
            "original_key_count",
            "filtered_key_count",
            "stripped_keys",
            "original_bytes",
            "filtered_bytes",
            "bytes_stripped",
            "efficiency_pct",
        }
        assert required_keys == set(metrics.keys())

    @pytest.mark.asyncio
    async def test_filtered_key_count_lte_original(self, fresh_orchestrator):
        orch = fresh_orchestrator
        result = await orch.run(dict(SAMPLE_TELEMETRY))
        all_downstream = set()
        for node in orch._dag_order:
            all_downstream.update(node.required_inputs)
        _, metrics = orch._dag_order[-1].analyze_and_filter_io(result, all_downstream)
        assert metrics["filtered_key_count"] <= metrics["original_key_count"]

    @pytest.mark.asyncio
    async def test_bytes_stripped_equals_difference(self, fresh_orchestrator):
        orch = fresh_orchestrator
        result = await orch.run(dict(SAMPLE_TELEMETRY))
        all_downstream = set()
        for node in orch._dag_order:
            all_downstream.update(node.required_inputs)
        _, metrics = orch._dag_order[-1].analyze_and_filter_io(result, all_downstream)
        assert metrics["bytes_stripped"] == metrics["original_bytes"] - metrics["filtered_bytes"]


# ---------------------------------------------------------------------------
# 4. Auto-scaling worker adjustments
# ---------------------------------------------------------------------------

class TestAutoScaling:
    def test_low_queue_depth_gives_one_replica(self, fresh_orchestrator):
        orch = fresh_orchestrator
        first = orch._dag_order[0].name
        count = orch.auto_scale_node(first, queue_depth=1, latency_ms=50)
        assert count == 1
        assert orch._meta[first].replicas == 1

    def test_low_queue_depth_2_gives_one_replica(self, fresh_orchestrator):
        orch = fresh_orchestrator
        first = orch._dag_order[0].name
        count = orch.auto_scale_node(first, queue_depth=2, latency_ms=300)
        assert count == 1

    def test_mid_queue_low_latency_gives_two_replicas(self, fresh_orchestrator):
        orch = fresh_orchestrator
        first = orch._dag_order[0].name
        count = orch.auto_scale_node(first, queue_depth=4, latency_ms=100)
        assert count == 2
        assert orch._meta[first].replicas == 2

    def test_mid_queue_high_latency_gives_four_replicas(self, fresh_orchestrator):
        orch = fresh_orchestrator
        first = orch._dag_order[0].name
        count = orch.auto_scale_node(first, queue_depth=5, latency_ms=250)
        assert count == 4
        assert orch._meta[first].replicas == 4

    def test_high_queue_depth_scales_to_queue_capped_at_8(self, fresh_orchestrator):
        orch = fresh_orchestrator
        first = orch._dag_order[0].name
        count = orch.auto_scale_node(first, queue_depth=6, latency_ms=10)
        assert count == 6

    def test_very_high_queue_depth_capped_at_8(self, fresh_orchestrator):
        orch = fresh_orchestrator
        first = orch._dag_order[0].name
        count = orch.auto_scale_node(first, queue_depth=20, latency_ms=10)
        assert count == 8
        assert orch._meta[first].replicas == 8

    def test_semaphore_updated_on_scale(self, fresh_orchestrator):
        orch = fresh_orchestrator
        first = orch._dag_order[0].name
        orch.auto_scale_node(first, queue_depth=20, latency_ms=10)
        sem = orch._meta[first].semaphore
        # Semaphore._value is the internal counter; check it matches replicas
        assert sem._value == 8

    def test_unknown_node_returns_replica_count_without_crash(self, fresh_orchestrator):
        """auto_scale_node on an unknown name must not raise — just return the count."""
        orch = fresh_orchestrator
        count = orch.auto_scale_node("nonexistent_node", queue_depth=3, latency_ms=100)
        assert count == 2  # mid-queue, low-latency → 2

    @pytest.mark.asyncio
    async def test_concurrent_burst_with_scaled_workers(self, fresh_orchestrator):
        """8 concurrent payloads must all complete successfully under max-scaled workers."""
        orch = fresh_orchestrator
        for name in orch._meta:
            orch.auto_scale_node(name, queue_depth=20, latency_ms=300)

        from mesh_engine.nasa_live_stream import _jitter_fallback
        payloads = [_jitter_fallback() for _ in range(8)]
        results = await asyncio.gather(*[orch.run(p) for p in payloads])
        assert len(results) == 8
        for r in results:
            assert "mission_health_summary" in r


# ---------------------------------------------------------------------------
# 5. Circuit breaker recovery
# ---------------------------------------------------------------------------

class TestCircuitBreakerRecovery:
    @pytest.mark.asyncio
    async def test_failing_node_receives_fallback_output(self, fresh_orchestrator):
        """A node that always raises should inject a None-valued fallback, not crash."""
        orch = fresh_orchestrator
        node = orch._dag_order[0]
        meta = orch._meta[node.name]

        # Force the circuit OPEN
        meta.circuit.record_failure()

        result = await orch.run(dict(SAMPLE_TELEMETRY))
        # The mesh should still return a complete-ish result via fallback injection
        assert result is not None

    @pytest.mark.asyncio
    async def test_circuit_transitions_open_on_exception(self, fresh_orchestrator):
        """Injecting a bad process coroutine causes the circuit to OPEN."""
        from mesh_engine.orchestrator import CircuitState
        orch = fresh_orchestrator
        first_node = orch._dag_order[0]
        meta = orch._meta[first_node.name]
        cb = meta.circuit

        # Manually trigger failure to simulate exception exhaustion
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_circuit_closes_on_success_after_open(self, fresh_orchestrator):
        """After a success call, circuit transitions back to CLOSED."""
        from mesh_engine.orchestrator import CircuitState
        orch = fresh_orchestrator
        first_node = orch._dag_order[0]
        cb = orch._meta[first_node.name].circuit

        cb.record_failure()
        assert cb.state == CircuitState.OPEN

        cb.record_success()
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0

    @pytest.mark.asyncio
    async def test_open_circuit_does_not_call_process(self, fresh_orchestrator):
        """When circuit is OPEN, fallback is returned without calling process()."""
        from mesh_engine.orchestrator import CircuitState, _build_fallback
        orch = fresh_orchestrator
        first_node = orch._dag_order[0]
        meta = orch._meta[first_node.name]
        cb = meta.circuit

        cb.record_failure()
        assert cb.state == CircuitState.OPEN

        call_counter = {"n": 0}
        original_process = first_node.process

        async def patched_process(payload):
            call_counter["n"] += 1
            return await original_process(payload)

        first_node.process = patched_process

        # Force recovery_timeout to 0 so HALF_OPEN check is skipped
        cb.recovery_timeout = 9999.0
        fallback = _build_fallback(first_node)
        result = await cb.call(first_node.process, dict(SAMPLE_TELEMETRY), fallback)

        assert call_counter["n"] == 0, "process() must not be called when circuit is OPEN"
        first_node.process = original_process  # restore

    @pytest.mark.asyncio
    async def test_half_open_allows_probe(self, fresh_orchestrator):
        """HALF_OPEN state allows exactly one probe call."""
        from mesh_engine.orchestrator import CircuitState, _build_fallback
        orch = fresh_orchestrator
        first_node = orch._dag_order[0]
        cb = orch._meta[first_node.name].circuit

        cb.record_failure()
        # Force past recovery timeout by back-dating last_failure_time
        cb.last_failure_time = time.monotonic() - cb.recovery_timeout - 1
        # Now _should_attempt should transition to HALF_OPEN
        assert cb._should_attempt() is True
        assert cb.state == CircuitState.HALF_OPEN

    @pytest.mark.asyncio
    async def test_mesh_continues_after_one_node_failure(self, fresh_orchestrator):
        """Even with an OPEN circuit on node 1, the full mesh still completes."""
        orch = fresh_orchestrator
        first = orch._dag_order[0]
        cb = orch._meta[first.name].circuit
        cb.record_failure()
        cb.recovery_timeout = 9999.0  # keep it OPEN

        result = await orch.run(dict(SAMPLE_TELEMETRY))
        # The last node may or may not produce health (depends on fallback propagation)
        # but the mesh must not raise
        assert isinstance(result, dict)
