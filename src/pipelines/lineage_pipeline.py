from typing import Dict, List, Optional, Tuple

from src.adapters.neo4j.client import Neo4jClient
from src.adapters.supabase import db as pg
from src.config.settings import Settings
from src.domain.models.events import OutboxEvent
from src.utils.logging import configure_logging


ENTITY_LABELS = {
    "product": "Product",
    "ingredient": "Ingredient",
    "recipe": "Recipe",
    "nutrition_fact": "NutritionFact",
}


class LineagePipeline:
    """Build lineage subgraph around a Gold entity using data_lineage rows."""

    def __init__(self, settings: Settings, pg_pool: pg.PostgresPool, neo4j: Neo4jClient):
        self.settings = settings
        self.pg_pool = pg_pool
        self.neo4j = neo4j
        self.log = configure_logging("lineage_pipeline")

    def load_lineage_rows(self, conn, entity_id: str) -> List[Dict]:
        sql = """
        SELECT *
        FROM data_lineage
        WHERE entity_id = %s
        ORDER BY processed_at DESC
        LIMIT 5;
        """
        return pg.fetch_all(conn, sql, (entity_id,))

    def entity_label(self, entity_type: str) -> Optional[str]:
        return ENTITY_LABELS.get(entity_type.lower())

    def _upsert_cypher(self, label: str) -> str:
        # label interpolated; guard upstream.
        return f"""
        MATCH (e:{label} {{id: $entity_id}})

        // Clear old lineage edges
        OPTIONAL MATCH (e)-[old:PRODUCED_BY]->(:LineageRun)
        DELETE old;

        WITH e, $rows AS rows
        UNWIND rows AS row
          MERGE (ss:SourceSystem {{name: row.source_system}})
          MERGE (lr:LineageRun {{id: row.id}})
          SET lr.transformation_applied = row.transformation_applied,
              lr.ingested_at = datetime(row.ingested_at),
              lr.processed_at = datetime(row.processed_at),
              lr.created_at = datetime(row.created_at)
          MERGE (e)-[:PRODUCED_BY]->(lr)
          MERGE (lr)-[:EMITTED_BY]->(ss)
          FOREACH (_ IN CASE WHEN row.bronze_record_id IS NULL THEN [] ELSE [1] END |
            MERGE (br:BronzeRecord {{id: row.bronze_record_id}})
            MERGE (lr)-[:CONSUMED]->(br)
          )
          FOREACH (_ IN CASE WHEN row.silver_record_id IS NULL THEN [] ELSE [1] END |
            MERGE (sr:SilverRecord {{id: row.silver_record_id}})
            MERGE (lr)-[:CONSUMED]->(sr)
          );
        """

    def _delete_cypher(self, label: str) -> str:
        return f"MATCH (e:{label} {{id: $entity_id}})-[r:PRODUCED_BY]->(:LineageRun) DELETE r;"

    def handle_event(self, event: OutboxEvent) -> None:
        entity_id = event.aggregate_id
        with self.pg_pool.connection() as conn:
            rows = self.load_lineage_rows(conn, entity_id)

        if not rows:
            self.log.warning("No lineage rows for entity", extra={"entity_id": entity_id, "op": event.op})
            return

        entity_type = rows[0].get("entity_type")
        label = self.entity_label(entity_type)
        if not label:
            self.log.warning("Unsupported entity_type for lineage", extra={"entity_type": entity_type, "entity_id": entity_id})
            return

        cypher = self._upsert_cypher(label)
        params = {"entity_id": entity_id, "rows": rows}
        self.neo4j.write(cypher, params)
        self.log.info("Upserted lineage", extra={"entity_id": entity_id, "rows": len(rows), "label": label})
