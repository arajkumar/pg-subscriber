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
func (s *Source) ReplicationConnect(ctx context.Context) (*pgconn.PgConn, error) {
	conf := s.ConnConfig.Copy()
	conf.RuntimeParams["replication"] = "database"

	pgConn, err := pgconn.ConnectConfig(ctx, &conf.Config)

	if err != nil {
		return nil, fmt.Errorf("error replication connection to database: %w", err)
	}

	return pgConn, nil
}

// Target connection with replication session and origin
func (t *Target) ApplyConnection(ctx context.Context, origin string) (*pgx.Conn, error) {
	conn, err := pgx.ConnectConfig(ctx, t.ConnConfig)
	if err != nil {
		return nil, fmt.Errorf("Error connecting to target: %w", err)
	}

	defer func() {
		if err != nil {
			conn.Close(ctx)
		}
	}()

	// create origin if not exists
	q := `SELECT * FROM pg_replication_origin WHERE roname = $1`
	row := conn.QueryRow(ctx, q, origin)
	var originID uint64
	err = row.Scan(&originID)
	if err == pgx.ErrNoRows {
		q := `SELECT pg_replication_origin_create($1)`
		_, err = conn.Exec(context.Background(), q, origin)
		if err != nil {
			return nil, fmt.Errorf("Error on replication origin create: %w", err)
		}
	}

	q = `SELECT pg_replication_origin_session_setup($1)`
	_, err = conn.Exec(context.Background(), q, origin)
	if err != nil {
		return nil, fmt.Errorf("Error on replication origin setup: %w", err)
	}
	return conn, nil
}
