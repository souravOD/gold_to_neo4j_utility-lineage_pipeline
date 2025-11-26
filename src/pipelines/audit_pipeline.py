from typing import Dict, Optional

from src.adapters.neo4j.client import Neo4jClient
from src.adapters.supabase import db as pg
from src.config.settings import Settings
from src.domain.models.events import OutboxEvent
from src.utils.logging import configure_logging


TABLE_TO_LABEL = {
    "products": "Product",
    "ingredients": "Ingredient",
    "recipes": "Recipe",
    "vendor_product_mappings": "VendorProduct",
    "data_lineage": "LineageRun",
}


class AuditPipeline:
    """Create thin ChangeEvent nodes for selected tables."""

    def __init__(self, settings: Settings, pg_pool: pg.PostgresPool, neo4j: Neo4jClient):
        self.settings = settings
        self.pg_pool = pg_pool
        self.neo4j = neo4j
        self.log = configure_logging("audit_pipeline")

    def load_audit(self, conn, audit_id: str) -> Optional[Dict]:
        sql = """
        SELECT *
        FROM audit_log
        WHERE id = %s;
        """
        return pg.fetch_one(conn, sql, (audit_id,))

    def _upsert_cypher(self, label: Optional[str]) -> str:
        attach = ""
        if label:
            attach = f"""
            WITH ce
            MATCH (e:{label} {{id: $record_id}})
            MERGE (ce)-[:AFFECTED]->(e)
            """
        return f"""
        MERGE (u:InternalUser {{id: $changed_by}})

        MERGE (ce:ChangeEvent {{id: $id}})
        SET ce.table_name = $table_name,
            ce.record_id = $record_id,
            ce.action = $action,
            ce.changed_at = datetime($changed_at),
            ce.ip_address = $ip_address,
            ce.user_agent = $user_agent

        MERGE (u)-[:MADE_CHANGE]->(ce)
        {attach}
        """

    def _delete_cypher(self) -> str:
        return "MATCH (ce:ChangeEvent {id: $id}) DETACH DELETE ce;"

    def handle_event(self, event: OutboxEvent) -> None:
        with self.pg_pool.connection() as conn:
            audit_row = self.load_audit(conn, event.aggregate_id)

        if audit_row is None:
            if event.op.upper() == "DELETE":
                self.log.info("Deleting change event", extra={"id": event.aggregate_id})
                self.neo4j.write(self._delete_cypher(), {"id": event.aggregate_id})
            else:
                self.log.warning("Audit row missing; skipping", extra={"id": event.aggregate_id, "op": event.op})
            return

        label = TABLE_TO_LABEL.get(audit_row["table_name"])
        cypher = self._upsert_cypher(label)

        params = {
            "id": audit_row["id"],
            "table_name": audit_row["table_name"],
            "record_id": audit_row["record_id"],
            "action": audit_row["action"],
            "changed_at": audit_row["changed_at"],
            "ip_address": audit_row["ip_address"],
            "user_agent": audit_row["user_agent"],
            "changed_by": audit_row.get("changed_by"),
        }
        self.neo4j.write(cypher, params)
        self.log.info("Upserted change event", extra={"id": audit_row["id"], "table": audit_row["table_name"]})
