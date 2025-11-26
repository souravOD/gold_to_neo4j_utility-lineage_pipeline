# Pipeline: utility-lineage

Scope: Ingest utility/governance signals (lineage, vendor mappings, audit events, data quality) from Supabase Gold v3 into Neo4j v3 via a Python worker. This repo is self-contained (no shared code).

Supabase source tables: data_lineage, vendor_product_mappings, audit_log (select tables), data_quality_scores
Neo4j labels touched: SourceSystem, LineageRun, BronzeRecord, SilverRecord, VendorProduct, Vendor, Product, ChangeEvent, InternalUser (plus existing Product/Ingredient/Recipe for attachment), entity nodes updated with quality props
Neo4j relationships touched: PRODUCED_BY, EMITTED_BY, CONSUMED, OWNS_SKU, MAPPED_TO, MADE_CHANGE, AFFECTED, HAS_QUALITY (as properties on entities)

How it works
- Outbox-driven: worker polls `outbox_events` filtered to utility tables/aggregate types (lineage_entity, vendor_product_mapping, audit_event, data_quality_entity), locks with `SKIP LOCKED`, routes per aggregate.
- Lineage upsert: reloads recent data_lineage rows for the entity_id, maps to SourceSystem + LineageRun nodes, rebuilds PRODUCED_BY/EMITTED_BY/CONSUMED edges; skips unsupported entity_type.
- Vendor mapping upsert: creates/updates VendorProduct nodes and MAPPED_TO edges to canonical Product; deletes mapping when missing (if keys provided on delete event).
- Audit upsert: creates thin ChangeEvent nodes, attaches to InternalUser and to entity nodes for known tables; deletes on missing-row DELETE.
- Data quality upsert: attaches quality_score/completeness/accuracy/issues to entity nodes for supported entity types.

Run
- Install deps: `pip install -r requirements.txt`
- Configure env: copy `.env.example` → `.env` and fill Postgres/Neo4j credentials.
- Start worker: `python -m src.workers.runner`

Folders
- docs/: domain notes, Cypher patterns, event routing
- src/: config, adapters (supabase, neo4j, queue), domain models/services, pipelines (aggregate upserts), workers (runners), utils
- tests/: placeholder for unit/integration tests
- ops/: ops templates (docker/env/sample cron jobs)
