import pytest
from fastapi.testclient import TestClient
from app.celery_app import celery_app
from app.main import app

# Ensure tasks execute in eager mode during testing with memory backend and broker
celery_app.conf.update(
    task_always_eager=True,
    task_eager_propagates=True,
    broker_url="memory://",
    result_backend="cache+memory://",
)


@pytest.fixture(scope="session")
def client():
    with TestClient(app) as test_client:
        yield test_client
