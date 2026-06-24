# Neo4j Nodestream Plugin for Nodestream

This plugin provides a [Nodestream](https://github.com/nodestream-proj/nodestream) interface to Neo4j. 

## Installation

```bash
pip install nodestream-plugin-neo4j
```

## Usage

```yaml
# nodestream.yaml
targets:
  neo4j:
    database: neo4j
    uri: bolt://localhost:7687
    username: neo4j
    password: neo4j
    database_name: neo4j # optional; name of the database to use.
    use_enterprise_features: false # optional; use enterprise features (e.g. node key constraints)
```

### Extractor 

The `Neo4jExtractor` class represents an extractor that reads records from a Neo4j database. It takes a single Cypher query as
input and yields the records read from the database. The extractor will automatically paginate through the database until it reaches the end. Therefore, the query needs to include a `SKIP` and `LIMIT` clause. For example:

```yaml
- implementation: nodestream_plugin_neo4j.extractor:Neo4jExtractor
  arguments:
    query: MATCH (p:Person) WHERE p.name = $name RETURN p.name SKIP $offset LIMIT $limit
    uri: bolt://localhost:7687
    username: neo4j
    password: neo4j
    database_name: my_database # Optional; defaults to neo4j
    limit: 100000 # Optional; defaults to 100
    parameters:
      # Optional; defaults to {}
      # Any parameters to be passed to the query
      # For example, if you want to pass a parameter called "name" with the value "John Doe", you would do this:
      name: John Doe
```

The extractor will automatically add the `SKIP` and `LIMIT` clauses to the query. The extractor will also automatically add the `offset` and `limit` parameters to the query. The extractor will start with `offset` set to `0` and `limit` set to `100` (unless overridden by setting `limit`) The extractor will continue to paginate through the database until the query returns no results. 

## Copy Command

The `nodestream copy` command copies nodes and relationships from one Neo4j target to another. The Neo4j plugin exposes the following `--retriever-option` parameters to tune throughput.

### Retriever options (`--retriever-option key=value`)

| Option | Type | Default | Description |
|---|---|---|---|
| `limit` | int | 100 | Page size for each paginated Cypher query (`SKIP $offset LIMIT $limit`). Larger values mean fewer round-trips. |
| `shard_size` | int | — | When set, each relationship type is split into shards of this size and fetched concurrently. Requires a key field or falls back to `elementId`. |
| `sample_ratio` | int | — | Only copy records where `toInteger(split(elementId(r), ':')[-1]) % sample_ratio = 0`. A deterministic, reproducible subset. `1` is treated as disabled. |
| `latest_hours` | int | — | Only copy records where `last_ingested_at >= now() - latest_hours`. Useful for incremental copies. |
| `relationships_only` | bool | false | Skip node fetching entirely; only copy relationships. |

### Connector options (`--connector-option key=value`)

| Option | Type | Default | Description |
|---|---|---|---|
| `chunk_size` | int | target default | Number of records per Cypher write batch inside each flush lane. |
| `execute_chunks_in_parallel` | bool | false | Run write chunks concurrently within each flush lane. |
| `retries_per_chunk` | int | 0 | Number of times to retry a failed write chunk before raising. |

### Best-known configuration (relationships, 24 h incremental)

Benchmark result: **~45 min, ~612 relationships/s** for a production graph with ~1.7 M relationships (2026-05-18, `sample_ratio=50`).

```bash
nodestream copy \
  --from <source> \
  --to <destination> \
  --all \
  --retriever-option relationships_only=true \
  --retriever-option latest_hours=24 \
  --retriever-option sample_ratio=50 \
  --retriever-option limit=50000 \
  --retriever-option shard_size=10000 \
  --concurrency-limit 40 \
  --flush-concurrency 4 \
  --batch-size 50000 \
  --step-outbox-size 500000 \
  --reporting-frequency 5000 \
  --connector-option execute_chunks_in_parallel=true \
  --connector-option chunk_size=10000 \
  --connector-option retries_per_chunk=5 \
  --metrics-interval-in-seconds 15 \
  --json
```

**Why these values:**
- `limit=50000` — large page size keeps each reader busy; queues stay full vs. the prior `limit=2500` which left queues at 1–2% utilisation.
- `shard_size=10000` — splits large relationship types into independent coroutines, saturating all 40 concurrency slots.
- `concurrency-limit=40` — matches the number of shards produced for the largest types; diminishing returns beyond this.
- `flush-concurrency=4` — 4 parallel write lanes; Neo4j write throughput saturates around here for a single AuraDB instance.
- `step-outbox-size=500000` — large buffer so the extractor is never blocked waiting for the writer to drain.
- `relationships_only=true` + `latest_hours=24` — incremental mode; nodes are stable, only new/changed relationships need copying.

## Concepts

### Migrations

The plugin supports migrations. Migrations are used to create indexes and constraints on the database. 

As part of the migration process, the plugin will create  `__NodestreamMigration__` nodes in the database. 
This node will have a `name` property that is set to the name of the migration. 

Additionally, the plugin will create a `__NodestreamMigrationLock__` node in the database. 
This node will be exit when the migration process is running and will be deleted when the migration process is complete.
This is used to prevent multiple migration processes from running at the same time.
