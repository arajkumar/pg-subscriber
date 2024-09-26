package timescale

import (
	"fmt"
	"context"
	"github.com/jackc/pgx/v5"
	_ "embed"

)

//go:embed missing_chunks.sql
var missingChunks string

type Chunk struct {
	Publication		   string
	HypertableSchema string
	HypertableName   string
	ChunkSchema      string
	ChunkName        string
	HyperCube        string
}

// ListMissingChunks returns a list of chunks that are newly created for the
// hypertables that are part of the given publication.
func ListMissingChunks(ctx context.Context, conn *pgx.Conn) ([]Chunk, error) {
	rows, err := conn.Query(ctx, missingChunks)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	chunks := []Chunk{}

	for rows.Next() {
		var chunk Chunk
		err := rows.Scan(&chunk.Publication, &chunk.HypertableSchema, &chunk.HypertableName, &chunk.ChunkSchema, &chunk.ChunkName, &chunk.HyperCube)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}

	return chunks, nil
}

func (c Chunk) AddToPublication(ctx context.Context, conn *pgx.Conn) error {
	sql := fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s",
							pgx.Identifier{c.Publication}.Sanitize(),
							pgx.Identifier{c.ChunkSchema, c.ChunkName}.Sanitize())
	_, err := conn.Exec(ctx, sql)
	return err
}

// Create chunk creates a new chunk on target.
func (c Chunk) Create(ctx context.Context, conn *pgx.Conn) error {
	sql := `SELECT _timescaledb_functions.create_chunk(FORMAT('%I.%I', $1::text, $2::text), $3, schema_name => $4, table_name=> $5)`;
	_, err := conn.Exec(ctx, sql, c.HypertableSchema, c.HypertableName, c.HyperCube, c.ChunkSchema, c.ChunkName)
	return err
}
