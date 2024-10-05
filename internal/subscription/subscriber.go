package subscription

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/timescale/pg-subscriber/internal/api"
)

var _ api.Subscriber = (*subscriber)(nil)

type subscriber struct {
	name      string
	source    *pgxpool.Pool
	target    *pgxpool.Pool
	publisher api.Publisher
}

// Create a new subscriber
// TODO: Support multiple publications
func New(name string, source *pgxpool.Pool, target *pgxpool.Pool, publisher api.Publisher) api.Subscriber {
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
	INIT         = "i"
	DATASYNC     = "d"
	FINISHEDCOPY = "f"
	SYNCDONE     = "s"
	READY        = "r"
	UNKOWN       = ""
	SYNCWAIT     = "w"
	CATCHUP      = "c"
)

func (s *subscriber) upsertOrDeleteIntoCatalog(ctx context.Context, tables []api.PublicationRelation) error {
	// TODO: Use schema migration tool
	// Create the schema if it does not exist
	createSchema := `CREATE SCHEMA IF NOT EXISTS _go_subscriber`
	_, err := s.target.Exec(ctx, createSchema)
	if err != nil {
		return fmt.Errorf("Error creating schema: %w", err)
	}

	// Create the table if it does not exist
	createTable := `CREATE TABLE IF NOT EXISTS _go_subscriber.subscription_rel (
		subname NAME NOT NULL,
		schemaname NAME NOT NULL,
		tablename NAME NOT NULL,
		state char NOT NULL,
		lsn pg_lsn,
		PRIMARY KEY (subname, schemaname, tablename)
	)`
	_, err = s.target.Exec(ctx, createTable)
	if err != nil {
		return err
	}

	// Create the temp table to stage the data
	createTable = `CREATE TEMP TABLE IF NOT EXISTS subscription_rel_temp (
		LIKE _go_subscriber.subscription_rel INCLUDING ALL
	) ON COMMIT DELETE ROWS`
	_, err = s.target.Exec(ctx, createTable)
	if err != nil {
		return fmt.Errorf("Error creating temp table: %w", err)
	}

	insert := `INSERT INTO subscription_rel_temp (subname, schemaname, tablename, state) VALUES ($1::name, $2::name, $3::name, $4)`

	defaultState := INIT
	tx, err := s.target.Begin(ctx)
	if err != nil {
		return fmt.Errorf("Error starting transaction: %w", err)
	}

	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		}
	}()

	for _, t := range tables {
		_, err = tx.Exec(ctx, insert, s.name, t.SchemaName, t.TableName, defaultState)
		if err != nil {
			return fmt.Errorf("Error inserting into temp table: %w", err)
		}
	}

	upsert := `INSERT INTO _go_subscriber.subscription_rel (subname, schemaname, tablename, state) SELECT subname, schemaname, tablename, state FROM subscription_rel_temp ON CONFLICT DO NOTHING`
	_, err = tx.Exec(ctx, upsert)
	if err != nil {
		return fmt.Errorf("Error upserting into subscription_rel: %w", err)
	}

	del := `
	WITH deleted AS (
		SELECT subname, schemaname, tablename FROM _go_subscriber.subscription_rel
		LEFT JOIN subscription_rel_temp USING (subname, schemaname, tablename)
		WHERE subscription_rel_temp.subname IS NULL
	)
	DELETE FROM _go_subscriber.subscription_rel WHERE (subname, schemaname, tablename) IN (SELECT subname, schemaname, tablename FROM deleted)
	`
	_, err = tx.Exec(ctx, del)
	if err != nil {
		return fmt.Errorf("Error deleting from subscription_rel: %w", err)
	}

	tx.Commit(ctx)

	return nil
}

// Fetches the list of tables that are part of the publication and
// populates go_pg_subscriber.subscription_rel
func (s *subscriber) Refresh(ctx context.Context) error {
	tables, err := s.publisher.FetchTables(ctx)
	if err != nil {
		return fmt.Errorf("Error fetching tables to refresh: %w", err)
	}

	err = s.upsertOrDeleteIntoCatalog(ctx, tables)
	return err
}

// Implements the Subscriber interface Name method
func (s *subscriber) Name() string {
	return s.name
}

// Implements the Subscriber interface Run method
func (s *subscriber) Run(ctx context.Context) error {
	zap.L().Debug("Starting subscriber", zap.String("name", s.Name()))
	// Refresh the subscription
	err := s.Refresh(ctx)
	if err != nil {
		return fmt.Errorf("Error refreshing subscription: %w", err)
	}

	// Start the apply worker
	return err
}
