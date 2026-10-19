"""Task definitions for Rivult Task Orchestration Engine."""
from app.tasks.aggregation import heavy_data_aggregation
from app.tasks.reports import batch_report_generation
from app.tasks.image_pipeline import image_transformation_pipeline

__all__ = [
    "heavy_data_aggregation",
    "batch_report_generation",
    "image_transformation_pipeline",
]
