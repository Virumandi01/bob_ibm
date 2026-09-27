# SpaceMesh — Autonomous Self-Healing Developer Workflow Mesh

SpaceMesh is an autonomous, self-healing, auto-scaling developer workflow mesh purpose-built for ingesting and processing live NASA and satellite telemetry in real time.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        MeshOrchestrator                         │
│   ┌──────────────┐     ┌──────────────┐     ┌───────────────┐  │
│   │  NasaTelemetry│────▶│  SpaceMesh  │────▶│  Output /     │  │
│   │  Stream       │     │  Node (DAG) │     │  Dashboard    │  │
│   └──────────────┘     └──────────────┘     └───────────────┘  │
│         │                     │                                  │
│    Live APIs             circuit-breaker                         │
│    (ISS / JPL)           + auto-retry                           │
│                          + auto-scale (1-8 replicas)            │
└─────────────────────────────────────────────────────────────────┘
```

### Key Components

| Component | File | Responsibility |
|---|---|---|
| `NasaTelemetryStream` | `mesh_engine/nasa_live_stream.py` | Pulls live JSON from NASA/ISS APIs, normalises into standard telemetry dict |
| `SpaceMeshNode` | `mesh_engine/core.py` | Abstract base class for every node in the mesh; enforces I/O contracts |
| `MeshOrchestrator` | `mesh_engine/orchestrator.py` | Auto-discovers nodes, wires DAG, handles auto-scaling + self-healing |
| Drop-in Nodes | `nodes/*.py` | User-authored processing nodes; picked up automatically at runtime |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.10+ |
| Async runtime | `asyncio` |
| HTTP | `urllib` (stdlib, keyless) / `httpx` (optional, async) |
| Data validation | `pydantic` v2 |
| Dashboard | `streamlit` |
| Tests | `pytest` + `pytest-asyncio` |

---

## Live Data Sources (Keyless — No API Key Required)

### ISS Real-Time Telemetry
```
GET https://api.wheretheiss.at/v1/satellites/25544
```
Returns current latitude, longitude, altitude, velocity, footprint, visibility, solar angles.

### NASA JPL Small-Body Close-Approach API
```
GET https://ssd-api.jpl.nasa.gov/cad.api?dist-max=0.05&limit=5
```
Returns near-Earth objects within 0.05 AU — enriches the `nasa_neo_count` field.

---

## Normalised Telemetry Schema

Every node in the mesh receives (and may pass forward) a dictionary conforming to:

```python
{
    "satellite_id":        str,   # NORAD catalog ID (e.g. "25544")
    "timestamp":           float, # Unix epoch seconds (UTC)
    "latitude":            float, # degrees
    "longitude":           float, # degrees
    "altitude_km":         float,
    "velocity_kms":        float, # km/s
    "footprint_km":        float,
    "solar_lat":           float,
    "solar_lon":           float,
    "visibility":          str,   # "daylight" | "eclipsed"
    "nasa_neo_count":      int,   # live NEO count from JPL CAD API
    "raw_unfiltered_api_dump": dict,  # full, unstripped upstream response
}
```

Nodes may strip `raw_unfiltered_api_dump` (and other unrequested keys) via `SpaceMeshNode.analyze_and_filter_io()`.

---

## Rules for Drop-In Nodes (`nodes/`)

1. **File location**: Place each node module as a single `.py` file directly inside `nodes/`.  
   Sub-packages are not auto-discovered.

2. **Inherit from `SpaceMeshNode`**:

   ```python
   from mesh_engine.core import SpaceMeshNode

   class MyNode(SpaceMeshNode):
       name = "my_node"
       required_inputs  = ["latitude", "longitude"]
       produced_outputs = ["my_derived_field"]

       async def process(self, payload: dict) -> dict:
           ...
           return {"my_derived_field": value}
   ```

3. **Declare I/O contracts**: `required_inputs` and `produced_outputs` are used by the orchestrator to wire the DAG automatically — do not leave them empty unless the node is purely terminal.

4. **Return only what you produce**: The `process()` method must return a dict containing *at minimum* the keys listed in `produced_outputs`. Extra keys are forwarded as-is.

5. **No side-effects on the incoming payload**: Treat `payload` as read-only; build and return a new dict.

6. **Self-healing guarantee**: The orchestrator wraps every `process()` call in a circuit-breaker. A node that raises an exception after 1 retry receives safe fallback output (`{}` merged with zeroed-out `produced_outputs` defaults). It will never crash the mesh.

7. **Auto-scaling**: Workers are scaled between 1 and 8 replicas per node based on live queue depth and latency. Nodes must be stateless (or use external state) to be safely replicated.

8. **Naming**: The class attribute `name` must be a unique snake_case identifier. Duplicate names across node files will raise `ValueError` at startup.

9. **Testing**: Place tests in `tests/test_<module>.py`. Run with `pytest -q`.

---

## Self-Healing & Circuit-Breaker Contract

- **Retry**: 1 automatic retry on any exception, with 50 ms back-off.
- **Fallback**: After retry exhaustion the orchestrator injects a zeroed-out fallback payload so downstream nodes are never starved.
- **Circuit state**: `CLOSED` → `OPEN` (after failure) → `HALF-OPEN` (next cycle) → `CLOSED` (on success).

## Auto-Scaling Contract

`auto_scale_node(node_name, queue_depth, latency_ms)` maps metrics to replica count:

| Queue depth | Latency | Replicas |
|---|---|---|
| ≤ 2 | any | 1 |
| 3–5 | < 200 ms | 2 |
| 3–5 | ≥ 200 ms | 4 |
| > 5 | any | min(8, queue_depth) |

---

## Quick Start

```bash
pip install httpx pydantic streamlit pytest pytest-asyncio
python -m mesh_engine.orchestrator   # starts the mesh
streamlit run dashboard.py           # optional live dashboard
pytest -q                            # run test suite
```
