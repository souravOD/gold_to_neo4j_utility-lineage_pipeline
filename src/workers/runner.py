import time
from typing import List

from src.adapters.neo4j.client import Neo4jClient
from src.adapters.queue.outbox import fetch_pending_events, mark_failed, mark_processed
from src.adapters.supabase.db import PostgresPool
from src.config.settings import Settings
from src.domain.models.events import OutboxEvent
from src.pipelines.audit_pipeline import AuditPipeline
from src.pipelines.lineage_pipeline import LineagePipeline
from src.pipelines.quality_pipeline import QualityPipeline
from src.pipelines.vendor_mapping_pipeline import VendorMappingPipeline
from src.utils.logging import configure_logging


TABLES = [
    "data_lineage",
    "vendor_product_mappings",
    "audit_log",
    "data_quality_scores",
]

AGG_TYPES = ["lineage_entity", "vendor_product_mapping", "audit_event", "data_quality_entity"]


def process_batch(
    lineage_pipeline: LineagePipeline,
    vendor_pipeline: VendorMappingPipeline,
    audit_pipeline: AuditPipeline,
    quality_pipeline: QualityPipeline,
    events: List[OutboxEvent],
    pg_pool: PostgresPool,
    log,
):
    for event in events:
        try:
            agg = event.aggregate_type
            if agg == "lineage_entity":
                lineage_pipeline.handle_event(event)
            elif agg == "vendor_product_mapping":
                vendor_pipeline.handle_event(event)
            elif agg == "audit_event":
                audit_pipeline.handle_event(event)
            elif agg == "data_quality_entity":
                quality_pipeline.handle_event(event)
            else:
                log.warning("Unhandled aggregate type", extra={"aggregate_type": agg, "event_id": event.id})
                continue

            with pg_pool.connection() as conn:
                mark_processed(conn, event.id)
        except Exception as exc:  # noqa: BLE001
            log.exception("Failed processing utility/lineage event", extra={"event_id": event.id, "aggregate_id": event.aggregate_id})
            with pg_pool.connection() as conn:
                mark_failed(conn, event.id, str(exc))


def main():
    settings = Settings()
    log = configure_logging("utility_lineage_worker")
    log.info("Starting utility/lineage worker", extra={"pipeline": settings.pipeline_name})

    pg_pool = PostgresPool(settings.supabase_dsn)
    neo4j = Neo4jClient(settings.neo4j_uri, settings.neo4j_user, settings.neo4j_password)

    lineage_pipeline = LineagePipeline(settings, pg_pool, neo4j)
    vendor_pipeline = VendorMappingPipeline(settings, pg_pool, neo4j)
    audit_pipeline = AuditPipeline(settings, pg_pool, neo4j)
    quality_pipeline = QualityPipeline(settings, pg_pool, neo4j)

    try:
        while True:
            with pg_pool.connection() as conn:
                conn.autocommit = False
                events = fetch_pending_events(
                    conn,
                    settings.batch_size,
                    settings.max_attempts,
                    table_names=TABLES,
                    aggregate_types=AGG_TYPES,
                )
                conn.commit()

            if not events:
                time.sleep(settings.poll_interval_seconds)
                continue

            process_batch(lineage_pipeline, vendor_pipeline, audit_pipeline, quality_pipeline, events, pg_pool, log)
    finally:
        neo4j.close()
        pg_pool.close()


if __name__ == "__main__":
    main()
