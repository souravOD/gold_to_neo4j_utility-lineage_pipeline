from typing import Dict, Optional

from src.adapters.neo4j.client import Neo4jClient
from src.adapters.supabase import db as pg
from src.config.settings import Settings
from src.domain.models.events import OutboxEvent
from src.utils.logging import configure_logging


ENTITY_LABELS = {
    "product": "Product",
    "ingredient": "Ingredient",
    "recipe": "Recipe",
}


class QualityPipeline:
    """Attach data quality scores to entities."""

    def __init__(self, settings: Settings, pg_pool: pg.PostgresPool, neo4j: Neo4jClient):
        self.settings = settings
        self.pg_pool = pg_pool
        self.neo4j = neo4j
        self.log = configure_logging("quality_pipeline")

    def load_quality(self, conn, entity_id: str) -> Optional[Dict]:
        sql = """
        SELECT *
        FROM data_quality_scores
        WHERE entity_id = %s
        ORDER BY last_checked DESC
        LIMIT 1;
        """
        return pg.fetch_one(conn, sql, (entity_id,))

    def _upsert_cypher(self, label: str) -> str:
        return f"""
        MATCH (e:{label} {{id: $entity_id}})
        SET e.quality_score = $quality_score,
            e.completeness = $completeness,
            e.accuracy = $accuracy,
            e.dq_last_checked = datetime($last_checked),
            e.dq_issues = $issues
        """

    def handle_event(self, event: OutboxEvent) -> None:
        entity_id = event.aggregate_id
        with self.pg_pool.connection() as conn:
            row = self.load_quality(conn, entity_id)

        if row is None:
            self.log.warning("No quality row for entity; skipping", extra={"entity_id": entity_id, "op": event.op})
            return

        label = ENTITY_LABELS.get(row["entity_type"])
        if not label:
            self.log.warning("Unsupported entity_type for quality", extra={"entity_type": row['entity_type'], "entity_id": entity_id})
            return

        params = {
            "entity_id": entity_id,
            "quality_score": row["quality_score"],
            "completeness": row["completeness"],
            "accuracy": row["accuracy"],
            "last_checked": row["last_checked"],
            "issues": row["issues"],
        }
        self.neo4j.write(self._upsert_cypher(label), params)
        self.log.info("Updated quality on entity", extra={"entity_id": entity_id, "label": label})
