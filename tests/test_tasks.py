import pytest
from app.tasks.aggregation import heavy_data_aggregation
from app.tasks.reports import batch_report_generation
from app.tasks.image_pipeline import image_transformation_pipeline
from app.tasks.base import TransientUpstreamError


def test_heavy_data_aggregation_execution():
    payload = {
        "dataset_id": "test_dataset_analytics",
        "dimensions": ["region", "tier"],
        "metrics": ["revenue", "dau"],
        "aggregation_window": "daily",
    }
    result = heavy_data_aggregation.apply(args=[payload], kwargs={"correlation_id": "test_corr_123"}).get()

    assert result["status"] == "COMPLETED"
    assert result["dataset_id"] == "test_dataset_analytics"
    assert result["total_records_processed"] > 0
    assert "revenue" in result["metrics"]
    assert "region" in result["dimensions"]


def test_batch_report_generation_execution():
    payload = {
        "report_type": "executive_summary",
        "format": "pdf",
        "recipient_emails": ["analytics@example.com"],
    }
    result = batch_report_generation.apply(args=[payload], kwargs={"correlation_id": "test_corr_456"}).get()

    assert result["status"] == "COMPLETED"
    assert result["report_type"] == "executive_summary"
    assert result["format"] == "pdf"
    assert "rep_" in result["report_id"]
    assert len(result["recipients_notified"]) == 1


def test_image_transformation_pipeline_execution():
    payload = {
        "batch_id": "batch_999",
        "source_urls": ["https://assets.example.com/a.png", "https://assets.example.com/b.png"],
        "operations": ["resize", "compress"],
        "target_format": "webp",
    }
    result = image_transformation_pipeline.apply(args=[payload], kwargs={"correlation_id": "test_corr_789"}).get()

    assert result["status"] == "COMPLETED"
    assert result["total_assets"] == 2
    assert len(result["assets"]) == 2
    assert result["target_format"] == "webp"


def test_task_transient_failure_simulation():
    from celery.exceptions import Retry
    payload = {
        "dataset_id": "failing_dataset",
        "dimensions": ["region"],
        "metrics": ["count"],
        "simulate_transient_error": True,
    }
    with pytest.raises((TransientUpstreamError, Retry)):
        heavy_data_aggregation.apply(args=[payload])
