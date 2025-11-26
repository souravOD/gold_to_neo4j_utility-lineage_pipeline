from typing import Dict, Optional

from src.adapters.neo4j.client import Neo4jClient
from src.adapters.supabase import db as pg
from src.config.settings import Settings
from src.domain.models.events import OutboxEvent
from src.utils.logging import configure_logging


class VendorMappingPipeline:
    """Upsert vendor_product_mappings as VendorProduct nodes mapped to canonical Products."""

    def __init__(self, settings: Settings, pg_pool: pg.PostgresPool, neo4j: Neo4jClient):
        self.settings = settings
        self.pg_pool = pg_pool
        self.neo4j = neo4j
        self.log = configure_logging("vendor_mapping_pipeline")

    def load_mapping(self, conn, mapping_id: str) -> Optional[Dict]:
        sql = """
        SELECT m.*, v.name AS vendor_name, p.name AS product_name
        FROM vendor_product_mappings m
        JOIN vendors v ON v.id = m.vendor_id
        JOIN products p ON p.id = m.global_product_id
        WHERE m.id = %s;
        """
        return pg.fetch_one(conn, sql, (mapping_id,))

    def _upsert_cypher(self) -> str:
        return """
        MERGE (v:Vendor {id: $mapping.vendor_id})
        SET v.name = $mapping.vendor_name

        MERGE (p:Product {id: $mapping.global_product_id})
        SET p.name = coalesce($mapping.product_name, p.name)

        MERGE (vp:VendorProduct {vendor_id: $mapping.vendor_id, vendor_product_id: $mapping.vendor_product_id})
        SET vp.created_at = datetime($mapping.created_at)

        MERGE (v)-[:OWNS_SKU]->(vp)
        MERGE (vp)-[m:MAPPED_TO]->(p)
        SET m.confidence = $mapping.confidence_score,
            m.method = $mapping.mapping_method,
            m.created_at = datetime($mapping.created_at)
        """

    def _delete_cypher(self) -> str:
        return """
        MATCH (vp:VendorProduct {vendor_id: $vendor_id, vendor_product_id: $vendor_product_id})
        DETACH DELETE vp;
        """

    def handle_event(self, event: OutboxEvent) -> None:
        with self.pg_pool.connection() as conn:
            mapping = self.load_mapping(conn, event.aggregate_id)

        if mapping is None:
            if event.op.upper() == "DELETE":
                self.log.info("Deleting vendor mapping", extra={"id": event.aggregate_id})
                # We need vendor_id/vendor_product_id to delete; attempt to get from payload if present
                payload = event.payload or {}
                vendor_id = payload.get("vendor_id")
                vendor_product_id = payload.get("vendor_product_id")
                if vendor_id and vendor_product_id:
                    self.neo4j.write(self._delete_cypher(), {"vendor_id": vendor_id, "vendor_product_id": vendor_product_id})
                else:
                    self.log.warning("Cannot delete vendor mapping; missing keys", extra={"event_id": event.id})
            else:
                self.log.warning("Vendor mapping missing in Supabase; skipping", extra={"id": event.aggregate_id, "op": event.op})
            return

        params = {"mapping": mapping}
        self.neo4j.write(self._upsert_cypher(), params)
        self.log.info("Upserted vendor mapping", extra={"id": event.aggregate_id})
