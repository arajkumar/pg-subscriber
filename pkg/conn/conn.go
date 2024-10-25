package conn

import (
	"context"
	"fmt"

	"github.com/jackc/pglogrepl"
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
	appName = "timescaledb-cdc"
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

type ReceiveConn struct {
	*pgconn.PgConn
}

// Connect to the database with replication parameter
func (s *Source) ReceiveConn(ctx context.Context) (*ReceiveConn, error) {
	conf := s.ConnConfig.Copy()
	conf.RuntimeParams["replication"] = "database"

	pgConn, err := pgconn.ConnectConfig(ctx, &conf.Config)

	if err != nil {
		return nil, fmt.Errorf("error replication connection to database: %w", err)
	}

	return &ReceiveConn{pgConn}, nil
}

type ApplyConn struct {
	*Target
	*pgx.Conn
	Origin string
}

// Target connection with replication session and origin
func (t *Target) ApplyConn(ctx context.Context, origin string) (*ApplyConn, error) {
	conn, err := pgx.ConnectConfig(ctx, t.ConnConfig)
	if err != nil {
		return nil, fmt.Errorf("Error connecting to target: %w", err)
	}

	defer func() {
		if err != nil {
			conn.Close(ctx)
		}
	}()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("Error on replication origin create txn: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	// create origin if not exists
	q := `SELECT * FROM pg_replication_origin WHERE roname = $1`
	row := tx.QueryRow(ctx, q, origin)
	var originID uint64
	err = row.Scan(&originID)
	if err == pgx.ErrNoRows {
		q := `SELECT pg_replication_origin_create($1)`
		_, err = tx.Exec(context.Background(), q, origin)
		if err != nil {
			return nil, fmt.Errorf("Error on replication origin create: %w", err)
		}
	}

	q = `SELECT pg_replication_origin_session_setup($1)`
	_, err = tx.Exec(context.Background(), q, origin)
	if err != nil {
		return nil, fmt.Errorf("Error on replication origin setup: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("Error on replication origin commit txn: %w", err)
	}

	return &ApplyConn{
		Target: t,
		Conn:   conn,
		Origin: origin,
	}, nil
}

func (t *ApplyConn) OriginProgress(ctx context.Context, flush bool) (lsn pglogrepl.LSN, err error) {
	conn := t.Conn
	if conn == nil {
		panic("Target Replication is not initialized")
	}

	// Origin progress will return a nil value if it is queried right after
	// it's creation. Use pointer to get it's value and reference incase if it
	// a non nil pointer.
	var lsnP *pglogrepl.LSN
	q := `SELECT pg_replication_origin_progress($1, $2)`
	err = conn.QueryRow(ctx, q, t.Origin, flush).Scan(&lsnP)
	if err != nil {
		return lsn, fmt.Errorf("Error on replication origin progress: %w", err)
	}

	if lsnP != nil {
		lsn = *lsnP
	}

	return lsn, nil
}
