import pytest
from pydantic import ValidationError
from app.schemas import (
    AggregationTaskRequest,
    AggregationWindow,
    ReportGenerationRequest,
    ReportType,
    ReportFormat,
    ImagePipelineRequest,
    ImageOperation,
    ImageFormat,
)


def test_valid_aggregation_request():
    payload = {
        "dataset_id": "analytics_2026_q1",
        "dimensions": ["region", "subscription"],
        "metrics": ["revenue", "dau"],
        "start_date": "2026-01-01",
        "end_date": "2026-03-31",
        "aggregation_window": "daily",
        "include_outliers": False,
        "confidence_threshold": 0.95,
    }
    req = AggregationTaskRequest(**payload)
    assert req.dataset_id == "analytics_2026_q1"
    assert req.aggregation_window == AggregationWindow.DAILY
    assert req.dimensions == ["region", "subscription"]


def test_invalid_aggregation_dataset_id_chars():
    payload = {
        "dataset_id": "invalid;drop table;",
        "dimensions": ["region"],
        "metrics": ["revenue"],
        "start_date": "2026-01-01",
        "end_date": "2026-03-31",
    }
    with pytest.raises(ValidationError) as exc:
        AggregationTaskRequest(**payload)
    assert "dataset_id" in str(exc.value)


def test_invalid_aggregation_date_range():
    payload = {
        "dataset_id": "analytics_test",
        "dimensions": ["region"],
        "metrics": ["revenue"],
        "start_date": "2026-05-01",
        "end_date": "2026-01-01",  # End before start
    }
    with pytest.raises(ValidationError) as exc:
        AggregationTaskRequest(**payload)
    assert "start_date" in str(exc.value)


def test_extra_forbidden_fields():
    payload = {
        "dataset_id": "analytics_test",
        "dimensions": ["region"],
        "metrics": ["revenue"],
        "start_date": "2026-01-01",
        "end_date": "2026-01-10",
        "malicious_extra_field": "hacked",
    }
    with pytest.raises(ValidationError) as exc:
        AggregationTaskRequest(**payload)
    assert "Extra inputs are not permitted" in str(exc.value)


def test_valid_report_request():
    payload = {
        "report_type": "executive_summary",
        "format": "pdf",
        "recipient_emails": ["ceo@example.com", "board@example.com"],
        "priority": "high",
    }
    req = ReportGenerationRequest(**payload)
    assert req.report_type == ReportType.EXECUTIVE_SUMMARY
    assert req.format == ReportFormat.PDF
    assert len(req.recipient_emails) == 2


def test_invalid_report_email():
    payload = {
        "report_type": "financial_audit",
        "recipient_emails": ["not-a-valid-email"],
    }
    with pytest.raises(ValidationError) as exc:
        ReportGenerationRequest(**payload)
    assert "recipient_emails" in str(exc.value)


def test_image_pipeline_request():
    payload = {
        "batch_id": "batch_test_01",
        "source_urls": ["https://assets.example.com/test.png"],
        "operations": ["resize", "compress"],
        "target_format": "webp",
        "quality": 90,
    }
    req = ImagePipelineRequest(**payload)
    assert req.target_format == ImageFormat.WEBP
    assert req.quality == 90
