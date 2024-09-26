WITH hypertables AS (
    SELECT ht.id, ht.schema_name, ht.table_name, pub.pubname
    FROM _timescaledb_catalog.hypertable ht
    JOIN pg_publication_tables pub
    ON ht.schema_name = pub.schemaname AND ht.table_name = pub.tablename

), missing_chunks AS (
    SELECT ht.schema_name AS hypertable_schema, ht.table_name AS hypertable_name,
           c.schema_name AS chunk_schema, c.table_name AS chunk_name,
           (SELECT sc.slices FROM _timescaledb_functions.show_chunk(FORMAT('%I.%I', c.schema_name, c.table_name)) sc) AS slices,
           ht.pubname
    FROM _timescaledb_catalog.chunk c
    JOIN hypertables ht ON c.hypertable_id = ht.id
    LEFT JOIN pg_publication_tables pub
    ON c.schema_name = pub.schemaname AND c.table_name = pub.tablename AND pub.pubname = ht.pubname
    WHERE pub.schemaname IS NULL
)

SELECT pubname, hypertable_schema, hypertable_name,
       chunk_schema, chunk_name, slices
FROM missing_chunks;
