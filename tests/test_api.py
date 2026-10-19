import pytest
from fastapi.testclient import TestClient


def test_healthz_endpoint(client: TestClient):
    response = client.get("/healthz")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["service"] == "rivult"


def test_dispatch_aggregation_task_accepted(client: TestClient):
    payload = {
        "dataset_id": "test_dataset_alpha",
        "dimensions": ["region", "tier"],
        "metrics": ["revenue", "dau"],
        "start_date": "2026-01-01",
        "end_date": "2026-03-31",
        "aggregation_window": "monthly",
    }
    response = client.post("/api/v1/tasks/aggregation", json=payload)
    assert response.status_code == 202
    data = response.json()
    assert "task_id" in data
    assert data["task_name"] == "tasks.heavy_data_aggregation"
    assert "status_url" in data
    assert "X-Correlation-ID" in response.headers


def test_dispatch_aggregation_invalid_payload(client: TestClient):
    payload = {
        "dataset_id": "short",
        "dimensions": [],  # Empty dimensions forbidden
        "metrics": ["revenue"],
        "start_date": "2026-01-01",
        "end_date": "2026-03-31",
    }
    response = client.post("/api/v1/tasks/aggregation", json=payload)
    assert response.status_code == 422


def test_dispatch_report_task_accepted(client: TestClient):
    payload = {
        "report_type": "executive_summary",
        "format": "pdf",
        "recipient_emails": ["vp-eng@example.com"],
        "priority": "high",
    }
    response = client.post("/api/v1/tasks/report", json=payload)
    assert response.status_code == 202
    data = response.json()
    assert "task_id" in data
    assert data["task_name"] == "tasks.batch_report_generation"


def test_task_status_endpoint(client: TestClient):
    # First dispatch a report task
    payload = {
        "report_type": "financial_audit",
        "format": "csv",
        "recipient_emails": ["audit@example.com"],
    }
    dispatch_res = client.post("/api/v1/tasks/report", json=payload)
    assert dispatch_res.status_code == 202
    task_id = dispatch_res.json()["task_id"]

    # Poll status
    status_res = client.get(f"/api/v1/tasks/{task_id}")
    assert status_res.status_code == 200
    data = status_res.json()
    assert data["task_id"] == task_id
    assert "status" in data


def test_list_tasks_endpoint(client: TestClient):
    response = client.get("/api/v1/tasks")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 1


def test_worker_health_endpoint(client: TestClient):
    response = client.get("/api/v1/workers/health")
    assert response.status_code == 200
    data = response.json()
    assert "status" in data
    assert "broker" in data
    assert "concurrency_per_worker" in data


def test_metrics_endpoint(client: TestClient):
    response = client.get("/api/v1/metrics")
    assert response.status_code == 200
    data = response.json()
    assert "total_tasks_dispatched" in data
    assert "uptime_seconds" in data
