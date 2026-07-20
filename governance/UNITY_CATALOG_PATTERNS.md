# Governance Patterns (Unity Catalog style)

This project runs entirely on local Delta Lake tables, so there's no live
Unity Catalog to configure. This doc captures the **governance patterns**
this project is designed to map onto in Databricks, so the intent is clear
even without a workspace attached.

## Three-level namespace

Unity Catalog organizes tables as `catalog.schema.table`. This project's
folder layout mirrors that intentionally:

```
lakehouse/bronze/events        →  streaming_catalog.bronze.events
lakehouse/silver/events        →  streaming_catalog.silver.events
lakehouse/gold/user_features   →  streaming_catalog.gold.user_features
```

Migrating to a real workspace means pointing each Delta write at a
managed Unity Catalog table path instead of a local folder — the
read/write/transform code does not change.

## Access control pattern

In a real deployment:

- **Bronze**: write access limited to the ingestion service principal only;
  read access limited to Silver-layer jobs and data engineers (raw events
  may contain unfiltered user identifiers).
- **Silver**: read access for downstream data engineering and BI teams;
  row-level filtering would apply here if any partner/tenant isolation is
  required (see the enrollment dbt project for that pattern in a
  multi-tenant context).
- **Gold**: broadest read access — this is the feature-store surface
  intended for model training jobs, BI dashboards, and analytics users.

## Column-level tagging (illustrative)

Real Unity Catalog supports tagging columns for governance (e.g. PII).
This project's `user_id` is a synthetic anonymous identifier, but in a
production equivalent it would be tagged and covered by a masking policy
for any role without an explicit need to see raw user identifiers:

```sql
ALTER TABLE streaming_catalog.gold.user_features
ALTER COLUMN user_id SET TAGS ('classification' = 'pseudonymous_id');
```

## Lineage

Every Bronze row carries `ingestion_run_id` and `bronze_ingested_at`,
which flow through Silver and into the watermark file. In Unity Catalog,
this project's lineage would additionally be visible automatically in the
catalog's lineage graph (table + column level) with zero extra code —
worth calling out in an interview as the production upgrade path from
this local demo.
