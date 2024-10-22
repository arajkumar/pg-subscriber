package publication

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"

	"github.com/timescale/timescaledb-cdc/pkg/conn"
)

type Relation struct {
	SchemaName string
	TableName  string
	AttNames   []string
}

type PublicationRelation struct {
	Relation
	RowFilter string
}

type Publisher struct {
	name       string
	source     *conn.Source
	sourceConn *pgx.Conn
}

func New(ctx context.Context, publications []string, source *conn.Source) (*Publisher, error) {
	if len(publications) > 1 {
		panic("Multiple publications are not supported yet.")
	}

	sourceConn, err := source.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("error connecting to source database: %w", err)
	}

	name := publications[0]

	return &Publisher{
		name:       name,
		source:     source,
		sourceConn: sourceConn,
	}, nil
}

func (p *Publisher) FetchTables(ctx context.Context) ([]PublicationRelation, error) {
	zap.L().Debug("Fetching tables for publication", zap.String("publication", p.name))
	query := `SELECT
		schemaname,
		tablename
		FROM pg_publication_tables
		WHERE pubname = $1::name`

	rows, err := p.sourceConn.Query(ctx, query, p.name)
	if err != nil {
		return nil, fmt.Errorf("error fetching tables for publication %s: %w", p.name, err)
	}
	defer rows.Close()

	var tables []PublicationRelation
	for rows.Next() {
		var table PublicationRelation
		err := rows.Scan(&table.SchemaName, &table.TableName)
		if err != nil {
			return nil, fmt.Errorf("error scanning table: %w", err)
		}
		tables = append(tables, table)
	}

	zap.L().Debug("Fetched tables from publication", zap.String("publication", p.name), zap.Int("count", len(tables)))

	return tables, nil
}

func (p *Publisher) Conn() *conn.Source {
	return p.source
}

func (p *Publisher) Name() string {
	return p.name
}
