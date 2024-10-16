package conn

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Define Source and Target connections
type Source struct {
	Conn
}

type Target struct {
	Conn
}

type Conn struct {
	*pgx.ConnConfig
}

const (
	appName = "pg-subscriber"
)

func Parse(connString string) (*Conn, error) {
	conf, err := pgx.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("error parsing connection: %w", err)
	}

	conf.RuntimeParams["application_name"] = appName

	return &Conn{
		conf,
	}, nil
}

func (c *Conn) AsSource() *Source {
	return &Source{*c}
}

func (c *Conn) AsTarget() *Target {
	return &Target{*c}
}

func (c *Conn) Connect(ctx context.Context) (*pgx.Conn, error) {
	conn, err := pgx.ConnectConfig(ctx, c.ConnConfig)
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %w", err)
	}

	return conn, nil
}

// Connect to the database with replication parameter
func (c *Source) ReplicationConnect(ctx context.Context) (*pgconn.PgConn, error) {
	conf := c.ConnConfig.Copy()
	conf.RuntimeParams["replication"] = "database"

	pgConn, err := pgconn.ConnectConfig(ctx, &conf.Config)

	if err != nil {
		return nil, fmt.Errorf("error replication connection to database: %w", err)
	}

	return pgConn, nil
}
