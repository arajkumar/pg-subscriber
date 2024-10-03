package timescale

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/jackc/pgx/v5"
)

type Chunk struct {
	HypertableSchema string
	HypertableName   string
	ChunkSchema      string
	ChunkName        string
	HyperCube        string
}

type PublicationChunk struct {
	Publication string
	IsPublished bool
	Chunk
}

// ListMissingChunks returns a list of chunks that are newly created for the
// hypertables that are part of the given publication.
func ListChunksOnPublication(ctx context.Context, conn *pgx.Conn, pubs []string) ([]PublicationChunk, error) {
	missingChunks := `
	WITH hypertables AS (
			SELECT ht.id, ht.schema_name, ht.table_name, pub.pubname
			FROM _timescaledb_catalog.hypertable ht
			JOIN pg_publication_tables pub
			ON ht.schema_name = pub.schemaname AND ht.table_name = pub.tablename
			WHERE pub.pubname = ANY($1::name[])
	), chunks_in_publication AS (
			SELECT ht.schema_name AS hypertable_schema, ht.table_name AS hypertable_name,
						 c.schema_name AS chunk_schema, c.table_name AS chunk_name,
						 (SELECT sc.slices FROM _timescaledb_functions.show_chunk(FORMAT('%I.%I', c.schema_name, c.table_name)) sc) AS slices,
						 ht.pubname,
						 pub.schemaname IS NOT NULL AS is_published
			FROM _timescaledb_catalog.chunk c
			JOIN hypertables ht ON c.hypertable_id = ht.id
			LEFT JOIN pg_publication_tables pub
			ON c.schema_name = pub.schemaname AND c.table_name = pub.tablename AND pub.pubname = ht.pubname
	)

	SELECT hypertable_schema, hypertable_name,
				 chunk_schema, chunk_name, slices, pubname, is_published
	FROM chunks_in_publication;
	`
	rows, err := conn.Query(ctx, missingChunks, pubs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	chunks := []PublicationChunk{}

	for rows.Next() {
		var chunk PublicationChunk
		err := rows.Scan(&chunk.HypertableSchema, &chunk.HypertableName, &chunk.ChunkSchema, &chunk.ChunkName, &chunk.HyperCube, &chunk.Publication, &chunk.IsPublished)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}

	return chunks, nil
}

func (c PublicationChunk) String() string {
	return fmt.Sprintf("%s.%s => %s.%s",
		c.HypertableSchema, c.HypertableName, c.ChunkSchema, c.ChunkName)
}

func (c PublicationChunk) AddToPublication(ctx context.Context, conn *pgx.Conn) error {
	sql := fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s",
		pgx.Identifier{c.Publication}.Sanitize(),
		pgx.Identifier{c.ChunkSchema, c.ChunkName}.Sanitize())
	_, err := conn.Exec(ctx, sql)
	return err
}

// Create chunk creates a new chunk on target.
func (c PublicationChunk) Create(ctx context.Context, conn *pgx.Conn) error {
	sql := `SELECT _timescaledb_functions.create_chunk(FORMAT('%I.%I', $1::text, $2::text), $3, schema_name => $4, table_name=> $5)`
	_, err := conn.Exec(ctx, sql, c.HypertableSchema, c.HypertableName, c.HyperCube, c.ChunkSchema, c.ChunkName)
	return err
}
