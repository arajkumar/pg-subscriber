package subscription

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/pg-subscriber/internal/api"
)

type subscriber struct {
	name      string
	source    *pgxpool.Conn
	target    *pgxpool.Conn
	publisher api.Publisher
}

// Create a new subscriber
// TODO: Support multiple publications
func New(name string, source *pgxpool.Conn, target *pgxpool.Conn, publisher api.Publisher) api.Subscriber {
	return &subscriber{
		name,
		source,
		target,
		publisher,
	}
}

// Possible states of a table in the subscription
// #define SUBREL_STATE_INIT		'i' /* initializing (sublsn NULL) */
// #define SUBREL_STATE_DATASYNC	'd' /* data is being synchronized (sublsn
//   - NULL) */
//
// #define SUBREL_STATE_FINISHEDCOPY 'f'	/* tablesync copy phase is completed
//   - (sublsn NULL) */
//
// #define SUBREL_STATE_SYNCDONE	's' /* synchronization finished in front of
//   - apply (sublsn set) */
//
// #define SUBREL_STATE_READY		'r' /* ready (sublsn set) */
//
// /* These are never stored in the catalog, we only use them for IPC. */
// #define SUBREL_STATE_UNKNOWN	'\0'	/* unknown state */
// #define SUBREL_STATE_SYNCWAIT	'w' /* waiting for sync */
// #define SUBREL_STATE_CATCHUP	'c' /* catching up with apply */
const (
	INIT         = 'i'
	DATASYNC     = 'd'
	FINISHEDCOPY = 'f'
	SYNCDONE     = 's'
	READY        = 'r'
	UNKOWN       = '0'
	SYNCWAIT     = 'w'
	CATCHUP      = 'c'
)

func (s *subscriber) upsertIntoCatalog(ctx context.Context, tables []api.PublicationRelation) error {
	// TODO: Use schema migration tool
	// Create the schema if it does not exist
	createSchema := `CREATE SCHEMA IF NOT EXISTS go_pg_subscriber`
	_, err := s.target.Exec(ctx, createSchema)
	if err != nil {
		return err
	}

	// Create the table if it does not exist
	createTable := `CREATE TABLE IF NOT EXISTS go_pg_subscriber.subscription_rel (
		schemaname NAME NOT NULL,
		tablename NAME NOT NULL,
		state char NOT NULL,
		lsn pg_lsn,
		PRIMARY KEY (schemaname, tablename)
	)`
	_, err = s.target.Exec(ctx, createTable)
	if err != nil {
		return err
	}

	upsertTable := `INSERT INTO go_pg_subscriber.subscription_rel (schemaname, tablename, state, lsn) VALUES ($1, $2, $3, $4)`
	defaultState := INIT

	// Use transaction to insert all the tables in one go
	tx, err := s.target.Begin(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		}
	}()

	for _, t := range tables {
		_, err = tx.Exec(ctx, upsertTable, t.SchemaName, t.TableName, defaultState, nil)
		if err != nil {
			return err
		}
	}
	tx.Commit(ctx)

	return nil
}

// Fetches the list of tables that are part of the publication and
// populates go_pg_subscriber.subscription_rel
func (s *subscriber) Refresh(ctx context.Context) error {
	tables, err := s.publisher.FetchTables(ctx)
	err = s.upsertIntoCatalog(ctx, tables)
	return err
}

// Implements the Subscriber interface Name method
func (s *subscriber) Name() string {
	return s.name
}
