"""
app.py — SpaceMesh Live Mission Dashboard
==========================================
Production-ready Streamlit dashboard for the SpaceMesh autonomous
self-healing developer workflow mesh.

Features
--------
- Live system status badge with active worker count and feed connectivity
- Stream Telemetry button (live NASA/ISS fetch or offline jitter fallback)
- Live DAG visualizer with node wiring, status, and replica counts
- I/O Pruning & Efficiency Inspector (bandwidth / token efficiency metrics)
- Downlink Mission Telemetry card (health summary, maneuver alerts, packet)
- Demo Action 1 — Hot-Add Live Solar Flare Node
- Demo Action 2 — Simulate High-Load Spikes & Circuit Breaker
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import sys
import time
from pathlib import Path
from typing import Any

import streamlit as st

# ── page config must be the first Streamlit call ─────────────────────────────
st.set_page_config(
    page_title="SpaceMesh Mission Dashboard",
    page_icon="🛰️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── inject custom CSS ─────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    .badge-nominal   { background:#16a34a; color:#fff; padding:3px 10px; border-radius:12px; font-size:0.8em; font-weight:700; }
    .badge-caution   { background:#d97706; color:#fff; padding:3px 10px; border-radius:12px; font-size:0.8em; font-weight:700; }
    .badge-warning   { background:#dc2626; color:#fff; padding:3px 10px; border-radius:12px; font-size:0.8em; font-weight:700; }
    .badge-critical  { background:#7f1d1d; color:#fff; padding:3px 10px; border-radius:12px; font-size:0.8em; font-weight:700; }
    .badge-open      { background:#dc2626; color:#fff; padding:3px 10px; border-radius:12px; font-size:0.8em; font-weight:700; }
    .badge-scaled    { background:#7c3aed; color:#fff; padding:3px 10px; border-radius:12px; font-size:0.8em; font-weight:700; }
    .badge-closed    { background:#16a34a; color:#fff; padding:3px 10px; border-radius:12px; font-size:0.8em; font-weight:700; }
    .maneuver-alert  { background:#7f1d1d; color:#fef2f2; padding:10px 16px; border-radius:8px; border-left:4px solid #dc2626; margin:8px 0; }
    .dag-node-box    { background:#f0f9ff; border:1px solid #bae6fd; border-radius:8px; padding:12px; margin:6px 0; color:#0f172a; }
    .dag-node-box b  { color:#1e293b; }
    .dag-node-box small { color:#0f172a; }
    .dag-node-box code { color:#0369a1; background-color:#e2e8f0; padding:1px 5px; border-radius:4px; }
    .dag-arrow       { color:#6b7280; font-size:1.3em; text-align:center; }
    .metric-row      { display:flex; gap:20px; flex-wrap:wrap; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ── helper: run async coroutine safely regardless of event-loop state ─────────

def _run_async(coro) -> Any:
    """Run an asyncio coroutine from synchronous Streamlit code."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                fut = pool.submit(asyncio.run, coro)
                return fut.result(timeout=30)
        return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


# ── session-state initialisation ──────────────────────────────────────────────

def _init_state() -> None:
    defaults: dict[str, Any] = {
        "orchestrator": None,
        "last_telemetry": None,
        "last_result": None,
        "last_io_metrics": None,
        "run_log": [],
        "node_replicas": {},
        "node_circuit_states": {},
        "flare_node_added": False,
        "burst_log": [],
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val


_init_state()


# ── orchestrator factory (cached per session) ─────────────────────────────────

def _get_orchestrator():
    from mesh_engine.orchestrator import MeshOrchestrator
    if st.session_state["orchestrator"] is None:
        st.session_state["orchestrator"] = MeshOrchestrator(nodes_dir="nodes")
    return st.session_state["orchestrator"]


def _reload_orchestrator():
    """Force a fresh orchestrator (picks up newly written node files)."""
    from mesh_engine.orchestrator import MeshOrchestrator

    # Evict previously cached node modules so discover_nodes re-imports them
    to_evict = [k for k in sys.modules if k.startswith("nodes.")]
    for k in to_evict:
        del sys.modules[k]

    st.session_state["orchestrator"] = MeshOrchestrator(nodes_dir="nodes")
    return st.session_state["orchestrator"]


# ── fetch telemetry (live or fallback) ───────────────────────────────────────

def _fetch_telemetry() -> dict[str, Any]:
    from mesh_engine.nasa_live_stream import NasaTelemetryStream
    stream = NasaTelemetryStream()
    return _run_async(stream.fetch())


# ── compute total worker count across all nodes ───────────────────────────────

def _total_workers(orch) -> int:
    return sum(m.replicas for m in orch._meta.values()) if orch._meta else 0


# ── circuit state label helper ────────────────────────────────────────────────

def _circuit_label(orch, node_name: str) -> str:
    meta = orch._meta.get(node_name)
    if meta is None:
        return "UNKNOWN"
    return meta.circuit.state.value  # CLOSED / OPEN / HALF_OPEN


# ═══════════════════════════════════════════════════════════════════════════════
# HEADER
# ═══════════════════════════════════════════════════════════════════════════════

st.markdown("## 🛰️ SpaceMesh — Live Mission Dashboard")

orch = _get_orchestrator()
node_count = len(orch._dag_order)
worker_count = _total_workers(orch)

# Determine overall system status from last run
_last_result = st.session_state.get("last_result")
if _last_result:
    summary = _last_result.get("mission_health_summary", "NOMINAL")
    status_word = summary.split(" | ")[0].upper() if summary else "NOMINAL"
else:
    status_word = "NOMINAL"

_badge_class = {
    "NOMINAL": "badge-nominal",
    "CAUTION": "badge-caution",
    "WARNING": "badge-warning",
    "CRITICAL": "badge-critical",
}.get(status_word, "badge-nominal")

# Live feed connectivity indicator
_online_indicator = "🟢 Live ISS Feed" if (
    st.session_state["last_telemetry"] and
    st.session_state["last_telemetry"].get("raw_unfiltered_api_dump", {}).get("_source") != "jitter_fallback"
) else "🟡 Offline — Jitter Fallback"

col_h1, col_h2, col_h3, col_h4 = st.columns([3, 2, 2, 2])
with col_h1:
    st.markdown(
        f'<span class="{_badge_class}">⬤ {status_word}</span>',
        unsafe_allow_html=True,
    )
with col_h2:
    st.metric("Active Nodes", node_count)
with col_h3:
    st.metric("Total Workers", worker_count)
with col_h4:
    st.markdown(f"**Feed:** {_online_indicator}")

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — STREAM TELEMETRY
# ═══════════════════════════════════════════════════════════════════════════════

st.markdown("### 📡 Telemetry Stream")

col_btn1, col_status = st.columns([2, 5])
with col_btn1:
    stream_clicked = st.button("⬇️ Stream Telemetry", use_container_width=True, type="primary")

if stream_clicked:
    with st.spinner("Fetching live telemetry from NASA / ISS APIs…"):
        telemetry = _fetch_telemetry()
        st.session_state["last_telemetry"] = telemetry

    orch = _get_orchestrator()
    if orch._dag_order:
        with st.spinner("Running through MeshOrchestrator…"):
            t0 = time.monotonic()
            result = _run_async(orch.run(telemetry))
            elapsed_ms = (time.monotonic() - t0) * 1000

        # Capture I/O metrics from last node
        all_downstream = set()
        for node in orch._dag_order:
            all_downstream.update(node.required_inputs)
        _, io_metrics = orch._dag_order[-1].analyze_and_filter_io(result, all_downstream)

        st.session_state["last_result"] = result
        st.session_state["last_io_metrics"] = io_metrics
        st.session_state["run_log"].insert(0, {
            "time": time.strftime("%H:%M:%S"),
            "elapsed_ms": round(elapsed_ms, 1),
            "lat": result.get("latitude", 0.0),
            "lon": result.get("longitude", 0.0),
            "health": result.get("mission_health_summary", "—"),
        })
        st.session_state["run_log"] = st.session_state["run_log"][:10]
        st.rerun()
    else:
        st.warning("No nodes discovered — mesh is empty.")

if st.session_state["last_telemetry"]:
    telem = st.session_state["last_telemetry"]
    is_fallback = telem.get("raw_unfiltered_api_dump", {}).get("_source") == "jitter_fallback"
    src_label = "🟡 Jitter Fallback (offline)" if is_fallback else "🟢 Live NASA/ISS"

    with col_status:
        st.markdown(f"**Source:** {src_label}")

    display_telem = {k: v for k, v in telem.items() if k != "raw_unfiltered_api_dump"}
    with st.expander("Raw Telemetry Snapshot", expanded=False):
        c1, c2 = st.columns(2)
        keys = list(display_telem.items())
        half = len(keys) // 2
        for k, v in keys[:half]:
            c1.metric(k.replace("_", " ").title(), v if not isinstance(v, float) else f"{v:.4f}")
        for k, v in keys[half:]:
            c2.metric(k.replace("_", " ").title(), v if not isinstance(v, float) else f"{v:.4f}")

if st.session_state["run_log"]:
    st.markdown("**Recent Runs**")
    st.dataframe(
        st.session_state["run_log"],
        use_container_width=True,
        hide_index=True,
        column_config={
            "time": "Time",
            "elapsed_ms": st.column_config.NumberColumn("Elapsed (ms)", format="%.1f"),
            "lat": st.column_config.NumberColumn("Latitude", format="%.4f"),
            "lon": st.column_config.NumberColumn("Longitude", format="%.4f"),
            "health": "Mission Health",
        },
    )

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — LIVE DAG VISUALIZER
# ═══════════════════════════════════════════════════════════════════════════════

st.markdown("### 🔗 Live DAG Visualizer")

orch = _get_orchestrator()
dag = orch._dag_order

if not dag:
    st.info("No nodes discovered. Add a `.py` node file to `nodes/` and refresh.")
else:
    for idx, node in enumerate(dag):
        meta = orch._meta.get(node.name)
        replicas = meta.replicas if meta else 1
        circuit_state = meta.circuit.state.value if meta else "CLOSED"

        if circuit_state == "OPEN":
            circuit_badge = f'<span class="badge-open">⚡ CIRCUIT_OPEN</span>'
        elif replicas > 1:
            circuit_badge = f'<span class="badge-scaled">⬆ SCALED ×{replicas}</span>'
        else:
            circuit_badge = f'<span class="badge-closed">✓ NOMINAL</span>'

        inputs_str = " · ".join(f"`{i}`" for i in node.required_inputs) if node.required_inputs else "*(none)*"
        outputs_str = " · ".join(f"`{o}`" for o in node.produced_outputs) if node.produced_outputs else "*(none)*"

        st.markdown(
            f"""<div class="dag-node-box">
            <b style="color:#1e293b;">Node {idx + 1}:</b> <code style="color:#0369a1;background-color:#e2e8f0;padding:1px 5px;border-radius:4px;">{node.name}</code> &nbsp;&nbsp;{circuit_badge}
            &nbsp;&nbsp; 👷 <b style="color:#1e293b;">{replicas}</b> <span style="color:#0f172a;">worker{'s' if replicas > 1 else ''}</span>
            <br/><small style="color:#0f172a;"><b style="color:#1e293b;">IN →</b> {inputs_str}</small>
            <br/><small style="color:#0f172a;"><b style="color:#1e293b;">OUT →</b> {outputs_str}</small>
            </div>""",
            unsafe_allow_html=True,
        )
        if idx < len(dag) - 1:
            st.markdown('<div class="dag-arrow">↓</div>', unsafe_allow_html=True)

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — I/O PRUNING & EFFICIENCY INSPECTOR
# ═══════════════════════════════════════════════════════════════════════════════

st.markdown("### 🔬 I/O Pruning & Efficiency Inspector")

io_metrics = st.session_state.get("last_io_metrics")
result = st.session_state.get("last_result")

if io_metrics and result:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Raw Payload Keys", io_metrics["original_key_count"])
    c2.metric("Pruned Keys Forwarded", io_metrics["filtered_key_count"])
    c3.metric("Keys Stripped", len(io_metrics["stripped_keys"]))
    c4.metric("Bandwidth Reduction", f"{io_metrics['efficiency_pct']:.1f}%")

    col_a, col_b = st.columns(2)
    with col_a:
        raw_kb = io_metrics["original_bytes"] / 1024
        filtered_kb = io_metrics["filtered_bytes"] / 1024
        st.markdown("**Memory Footprint (shallow)**")
        st.progress(
            min(io_metrics["filtered_bytes"] / max(io_metrics["original_bytes"], 1), 1.0),
            text=f"{filtered_kb:.2f} KB forwarded / {raw_kb:.2f} KB raw  →  {io_metrics['efficiency_pct']:.1f}% stripped",
        )

    with col_b:
        if io_metrics["stripped_keys"]:
            st.markdown("**Stripped Keys (noise eliminated)**")
            for k in io_metrics["stripped_keys"]:
                st.markdown(f"- `{k}`")
        else:
            st.info("All keys forwarded downstream.")

    with st.expander("Full Final Payload (post-mesh)", expanded=False):
        # Exclude raw dump for readability
        display = {k: v for k, v in result.items() if k != "raw_unfiltered_api_dump"}
        st.json(display)
else:
    st.info("Stream a telemetry packet to populate efficiency metrics.")

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — DOWNLINK MISSION TELEMETRY CARD
# ═══════════════════════════════════════════════════════════════════════════════

st.markdown("### 📻 Downlink Mission Telemetry")

result = st.session_state.get("last_result")

if result and "mission_health_summary" in result:
    summary = result.get("mission_health_summary", "NOMINAL")
    packet = result.get("priority_downlink_packet", {})
    health_score = packet.get("health_score", 1.0)
    maneuver_flag = packet.get("maneuver_flag", False)
    risk_vector = packet.get("risk_vector", {})

    # Health summary badge
    status_word = summary.split(" | ")[0].upper()
    badge_cls = {"NOMINAL": "badge-nominal", "CAUTION": "badge-caution",
                 "WARNING": "badge-warning", "CRITICAL": "badge-critical"}.get(status_word, "badge-nominal")
    st.markdown(
        f'<span class="{badge_cls}" style="font-size:1.1em;">{summary}</span>',
        unsafe_allow_html=True,
    )

    # Maneuver alert banner
    if maneuver_flag:
        st.markdown(
            '<div class="maneuver-alert">🚨 <b>MANEUVER ADVISORY ACTIVE</b> — '
            'Collision risk threshold exceeded. Avoidance maneuver recommended by proximity guard.</div>',
            unsafe_allow_html=True,
        )

    # Health score bar + risk vector
    col_score, col_risk = st.columns([2, 3])
    with col_score:
        bar_color = "normal" if health_score >= 0.55 else "inverse"
        st.metric("Health Score", f"{health_score:.4f}")
        st.progress(float(health_score), text=f"Mission health: {health_score:.1%}")

    with col_risk:
        st.markdown("**Risk Vector**")
        if risk_vector:
            for risk_key, risk_val in risk_vector.items():
                if isinstance(risk_val, float):
                    st.progress(min(float(risk_val), 1.0),
                                text=f"{risk_key.replace('_', ' ').title()}: {risk_val:.4f}")

    # Priority downlink packet JSON
    with st.expander("📦 Priority Downlink Packet (JSON)", expanded=False):
        st.json(packet)
else:
    st.info("No downlink data yet — stream a telemetry packet above.")

st.divider()

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — DEMO ACTIONS
# ═══════════════════════════════════════════════════════════════════════════════

st.markdown("### 🧪 Demo Actions")

col_demo1, col_demo2 = st.columns(2)

# ── Demo Action 1: Hot-Add Solar Flare Node ───────────────────────────────────

FLARE_NODE_PATH = Path("nodes/node_05_geomagnetic_storm_detector.py")
FLARE_NODE_CODE = '''\
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
'''

with col_demo1:
    st.markdown("#### ⚡ Demo 1 — Hot-Add Solar Flare Node")
    st.markdown(
        "Writes `node_05_geomagnetic_storm_detector.py` to `nodes/`, "
        "re-runs orchestrator discovery, and updates the DAG live — "
        "**no server restart required**."
    )

    already_added = FLARE_NODE_PATH.exists()

    if already_added:
        st.success("✅ Geomagnetic Storm Detector already present in DAG.")
        if st.button("🗑️ Remove Flare Node & Reset DAG", use_container_width=True):
            FLARE_NODE_PATH.unlink(missing_ok=True)
            _reload_orchestrator()
            st.session_state["flare_node_added"] = False
            st.rerun()
    else:
        if st.button("🔥 Hot-Add Live Solar Flare Node", use_container_width=True, type="primary"):
            FLARE_NODE_PATH.write_text(FLARE_NODE_CODE, encoding="utf-8")
            _reload_orchestrator()
            st.session_state["flare_node_added"] = True
            st.success(
                f"✅ Node written to `{FLARE_NODE_PATH}` and DAG reloaded — "
                f"now {len(_get_orchestrator()._dag_order)} nodes active!"
            )
            st.rerun()

# ── Demo Action 2: Simulate High-Load Spike ───────────────────────────────────

with col_demo2:
    st.markdown("#### 🔁 Demo 2 — Simulate High-Load Spikes & Circuit Breaker")
    st.markdown(
        "Triggers rapid burst traffic to demonstrate auto-scaling (up to 8 workers) "
        "and self-healing circuit breaker recovery on a forced node failure."
    )

    async def _burst_simulation(orch_ref, n_packets: int = 8) -> list[dict]:
        """Send n_packets through the orchestrator concurrently, recording replica counts."""
        from mesh_engine.nasa_live_stream import _jitter_fallback
        from mesh_engine.orchestrator import CircuitState

        log: list[dict] = []
        payloads = [_jitter_fallback() for _ in range(n_packets)]

        # Scale every node to max workers to simulate high-load
        for name in list(orch_ref._meta.keys()):
            orch_ref.auto_scale_node(name, queue_depth=n_packets, latency_ms=250)

        t0 = time.monotonic()
        tasks = [orch_ref.run(p) for p in payloads]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = (time.monotonic() - t0) * 1000

        for i, res in enumerate(results):
            if isinstance(res, Exception):
                log.append({"packet": i + 1, "status": "ERROR", "health": "—", "elapsed_ms": "—"})
            else:
                log.append({
                    "packet": i + 1,
                    "status": "OK",
                    "health": res.get("mission_health_summary", "—"),
                    "elapsed_ms": round(elapsed / n_packets, 1),
                })

        # Demonstrate circuit breaker: force a node failure, then recover
        first_node_name = orch_ref._dag_order[0].name if orch_ref._dag_order else None
        if first_node_name and first_node_name in orch_ref._meta:
            cb = orch_ref._meta[first_node_name].circuit
            cb.record_failure()
            log.append({"packet": "CB-FORCE", "status": f"OPEN on {first_node_name}", "health": "—", "elapsed_ms": "—"})

            # Simulate recovery: run once more (fallback injected), then close
            recovery_result = await orch_ref.run(payloads[0])
            cb.record_success()
            log.append({
                "packet": "CB-RECOVER",
                "status": f"CLOSED (healed) on {first_node_name}",
                "health": recovery_result.get("mission_health_summary", "—"),
                "elapsed_ms": "—",
            })

        # Scale back down after burst
        for name in list(orch_ref._meta.keys()):
            orch_ref.auto_scale_node(name, queue_depth=1, latency_ms=50)

        return log

    if st.button("🚀 Simulate High-Load Burst (8 packets)", use_container_width=True, type="primary"):
        orch = _get_orchestrator()
        if orch._dag_order:
            with st.spinner("Running burst simulation…"):
                burst_log = _run_async(_burst_simulation(orch, n_packets=8))
            st.session_state["burst_log"] = burst_log
            st.success("Burst simulation complete — see results below.")
            st.rerun()
        else:
            st.warning("No nodes loaded — stream telemetry first.")

    if st.session_state["burst_log"]:
        st.dataframe(
            st.session_state["burst_log"],
            use_container_width=True,
            hide_index=True,
            column_config={
                "packet": "Packet #",
                "status": "Status",
                "health": "Mission Health",
                "elapsed_ms": st.column_config.NumberColumn("Avg ms/packet", format="%.1f"),
            },
        )

        # Show current worker counts post-simulation
        orch = _get_orchestrator()
        st.markdown("**Worker replica counts (post-burst, auto-scaled back to baseline):**")
        replica_data = [
            {"Node": name, "Replicas": m.replicas, "Circuit": m.circuit.state.value}
            for name, m in orch._meta.items()
        ]
        st.dataframe(replica_data, use_container_width=True, hide_index=True)

st.divider()
st.markdown(
    '<p style="text-align:center; font-size:12px; color:#9ca3af; border-top:1px solid #e5e7eb; padding-top:8px;">'
    'SpaceMesh Mission Dashboard &mdash; Made with IBM Bob'
    '</p>',
    unsafe_allow_html=True,
)
