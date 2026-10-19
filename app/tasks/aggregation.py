import time
import random
from typing import Any, Dict
from app.celery_app import celery_app
from app.tasks.base import BaseTask, TransientUpstreamError


@celery_app.task(
    bind=True,
    base=BaseTask,
    name="tasks.heavy_data_aggregation",
    autoretry_for=(TransientUpstreamError,),
    retry_backoff=True,
    retry_kwargs={"max_retries": 3},
    acks_late=True,
)
def heavy_data_aggregation(self, payload: Dict[str, Any], correlation_id: str = "none") -> Dict[str, Any]:
    """
    Executes heavy multi-dimensional data aggregation with incremental progress reporting.

    Stages:
    1. Dataset partition scanning
    2. Dimension grouping & indexing
    3. Metric reduction & statistical anomaly filtering
    4. Cube compilation & persistent storage
    """
    dataset_id = payload.get("dataset_id", "default_dataset")
    dimensions = payload.get("dimensions", ["dimension_1"])
    metrics = payload.get("metrics", ["count", "sum"])
    simulate_failure = payload.get("simulate_transient_error", False)

    # Simulated transient failure trigger for testing retries
    if simulate_failure and self.request.retries == 0:
        raise TransientUpstreamError(f"Transient connection timeout reading partition chunk for {dataset_id}")

    steps = [
        ("SCANNING_PARTITIONS", 25, "Scanning and partitioning raw parquet shards"),
        ("GROUPING_DIMENSIONS", 50, f"Grouping index across dimensions: {', '.join(dimensions)}"),
        ("COMPUTING_METRICS", 75, f"Computing aggregated metrics: {', '.join(metrics)}"),
        ("FINALIZING_CUBE", 95, "Compiling final analytical cube into persistent storage"),
    ]

    total_records = random.randint(150_000, 2_500_000)

    for step_name, percent, description in steps:
        self.update_state(
            state="PROGRESS",
            meta={
                "current_step": step_name,
                "percent": percent,
                "description": description,
                "processed_items": int(total_records * (percent / 100)),
                "total_items": total_records,
                "correlation_id": correlation_id,
            },
        )
        # Small delay to simulate computation (kept small for responsiveness)
        time.sleep(0.3)

    # Generate realistic aggregate results
    results_by_dimension = {}
    for dim in dimensions:
        results_by_dimension[dim] = {
            "cardinality": random.randint(5, 50),
            "sample_keys": [f"{dim}_alpha", f"{dim}_beta", f"{dim}_gamma"],
        }

    computed_metrics = {}
    for m in metrics:
        computed_metrics[m] = {
            "sum": round(random.uniform(50000, 1000000), 2),
            "avg": round(random.uniform(10, 500), 2),
            "p95": round(random.uniform(100, 950), 2),
            "p99": round(random.uniform(950, 1500), 2),
        }

    return {
        "status": "COMPLETED",
        "dataset_id": dataset_id,
        "aggregation_window": payload.get("aggregation_window", "daily"),
        "total_records_processed": total_records,
        "anomalies_filtered": random.randint(12, 180),
        "dimensions": results_by_dimension,
        "metrics": computed_metrics,
        "execution_summary": {
            "partitions_scanned": random.randint(16, 64),
            "worker_retries": self.request.retries,
            "correlation_id": correlation_id,
        },
    }
