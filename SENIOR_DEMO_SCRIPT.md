# Senior Engineering Walkthrough: Rivult Background Task Orchestration

> **Target Audience:** Principal Systems Engineers, Python Platform Leads, Distributed Task Processing Architects.  
> **Key Architecture Highlights:** Asynchronous Job Orchestration (FastAPI + Celery + Redis), Late Acknowledgements (`acks_late=True`), Prefetch Multiplier Tuning, Real-Time Progress Bar State Tracking, Structured JSON Logging.

---

## 1. Executive Summary & Problem Space

Heavy computational tasks—such as batch analytical report generation, multi-dimensional cube aggregations, or image processing pipelines—cannot be executed synchronously within an HTTP request-response cycle without risking gateway timeouts (e.g. 504 Gateway Timeout) and degrading user experience.

**Rivult** decouples long-running workloads into an asynchronous, distributed execution pipeline:
- **Immediate `202 Accepted` Responses:** Clients receive a unique task UUID and polling endpoint within milliseconds.
- **Worker Starvation Defense:** Celery workers operate with `worker_prefetch_multiplier = 1` and `concurrency = 4`, preventing worker process hoarding.
- **At-Least-Once Delivery (`acks_late=True`):** Tasks are only acknowledged to the Redis broker upon complete execution. If a worker process is killed by the OS or OOM killer, the job is automatically reassigned.
- **Granular Progress Telemetry:** Tasks update state in Redis with incremental percentage completion counters.

---

## 2. File & Function Code Architecture

### `app/main.py` — FastAPI Dispatcher & Lifecycle (Port 8084)
- Exposes `POST /api/v1/tasks/aggregation`, `POST /api/v1/tasks/report`, `POST /api/v1/tasks/image-pipeline` which validate Pydantic schemas and dispatch Celery tasks via `task.delay()`.
- Exposes `GET /api/v1/tasks/{task_id}` for polling task progress (`PENDING` &rarr; `PROGRESS` &rarr; `SUCCESS`/`FAILURE`).
- Mounts the interactive telemetry dashboard at `GET /`.

### `app/tasks/` — Celery Worker Execution Engine
- `aggregation.py`: Computes statistical aggregations over multi-thousand record batches with simulated anomaly checks.
- `report.py`: Compiles multi-format analytical export summaries (CSV/XLSX/PDF).
- Implements transient failure retry policies: `autoretry_for=(TransientUpstreamError,)`, `retry_backoff=True`.

### `app/core/celery_app.py` — Broker & Result Backend Configuration
- Configures Redis URL, serialization format (JSON), late acknowledgment rules, and result expiration TTLs.

---

## 3. How to Run the Live Demo

### Starting the Server (Port 8084)
```powershell
uvicorn app.main:app --host 0.0.0.0 --port 8084
```
Open your browser to: **http://127.0.0.1:8084**

### Demo Walkthrough Script
1. **Show the Dashboard:** Point out the dark-mode task submission panel and active task progress monitor.
2. **Dispatch an Aggregation Job:** Select **Data Cube Aggregation**, set dataset size to 50,000, and click **Dispatch Asynchronous Task**.
3. **Inspect the `202 Accepted` Response:** Show the returned Task UUID and polling URL.
4. **Live Progress Tracking:** Watch the progress bar advance from 0% &rarr; 25% &rarr; 50% &rarr; 75% &rarr; 100% as the worker streams status updates to Redis.
5. **Inspect the Result Payload:** View the finalized statistical anomalies and calculation summary rendered directly in the UI.
