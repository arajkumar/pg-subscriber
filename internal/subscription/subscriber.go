package subscription

import (
	"context"
	"fmt"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.uber.org/zap"

	"github.com/timescale/pg-subscriber/internal/conn"
	pub "github.com/timescale/pg-subscriber/internal/publication"
)

type Subscriber struct {
	name       string
	target     *conn.Target
	targetConn *pgx.Conn
	publisher  *pub.Publisher
}

// Create a new subscriber
// TODO: Support multiple publications
func New(ctx context.Context, name string, target *conn.Target, publisher *pub.Publisher) (*Subscriber, error) {
	targetConn, err := target.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("error connecting to target database: %w", err)
	}

	return &Subscriber{
		name:       name,
		target:     target,
		targetConn: targetConn,
		publisher:  publisher,
	}, nil
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

func (s *Subscriber) upsertOrDeleteIntoCatalog(ctx context.Context, tables []pub.PublicationRelation) error {
	// TODO: Use schema migration tool
	// Create the schema if it does not exist
	createSchema := `CREATE SCHEMA IF NOT EXISTS _go_subscriber`
	_, err := s.targetConn.Exec(ctx, createSchema)
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
	_, err = s.targetConn.Exec(ctx, createTable)
	if err != nil {
		return err
	}

	// Create the temp table to stage the data
	createTable = `CREATE TEMP TABLE IF NOT EXISTS subscription_rel_temp (
		LIKE _go_subscriber.subscription_rel INCLUDING ALL
	) ON COMMIT DELETE ROWS`
	_, err = s.targetConn.Exec(ctx, createTable)
	if err != nil {
		return fmt.Errorf("Error creating temp table: %w", err)
	}

	insert := `INSERT INTO subscription_rel_temp (subname, schemaname, tablename, state) VALUES ($1::name, $2::name, $3::name, $4)`

	defaultState := INIT
	tx, err := s.targetConn.Begin(ctx)
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

func (s *Subscriber) replicationSlotExists(ctx context.Context, slotName string) (bool, error) {
	conn, err := s.publisher.Conn().Connect(ctx)
	if err != nil {
		return false, fmt.Errorf("Error connecting to source: %w", err)
	}
	defer conn.Close(ctx)

	query := `SELECT true FROM pg_replication_slots WHERE slot_name = $1`
	row := conn.QueryRow(ctx, query, slotName)

	var exists bool
	err = row.Scan(&exists)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, nil
		}
		return false, err
	}

	return exists, nil
}

// Fetches the list of tables that are part of the publication and
// populates go_pg_subscriber.subscription_rel
func (s *Subscriber) refresh(ctx context.Context) error {
	tables, err := s.publisher.FetchTables(ctx)
	if err != nil {
		return fmt.Errorf("Error fetching tables to refresh: %w", err)
	}

	err = s.upsertOrDeleteIntoCatalog(ctx, tables)
	return err
}

func (s *Subscriber) startReplication(ctx context.Context, sourceConn *pgconn.PgConn) error {
	sysident, err := pglogrepl.IdentifySystem(ctx, sourceConn)
	if err != nil {
		return fmt.Errorf("Error identifying system: %w", err)
	}
	zap.L().Info("IdentifySystem", zap.String("SystemID", sysident.SystemID), zap.Int32("Timeline", sysident.Timeline), zap.String("XLogPos", sysident.XLogPos.String()), zap.String("DBName", sysident.DBName))

	pluginArguments := []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names '%s'", s.publisher.Name()),
		"messages 'true'",
	}

	slotName := s.name

	slotExists, err := s.replicationSlotExists(ctx, slotName)
	if err != nil {
		return fmt.Errorf("Error checking if replication slot exists: %w", err)
	}

	if slotExists {
		zap.L().Info("Replication slot already exists", zap.String("SlotName", slotName))
	} else {
		_, err = pglogrepl.CreateReplicationSlot(ctx, sourceConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Temporary: false})
		if err != nil {
			return fmt.Errorf("Error creating replication slot: %w", err)
		}
		zap.L().Info("CreateReplicationSlot", zap.String("SlotName", slotName))
	}

	err = pglogrepl.StartReplication(ctx, sourceConn, slotName, sysident.XLogPos, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		return fmt.Errorf("Error starting replication: %w", err)
	}
	zap.L().Info("StartReplication", zap.String("SlotName", slotName))

	return nil
}

func (s *Subscriber) Sync(ctx context.Context) error {
	zap.L().Debug("Starting subscriber", zap.String("name", s.name))
	// Refresh the subscription
	err := s.refresh(ctx)
	if err != nil {
		return fmt.Errorf("Error refreshing subscription catalog: %w", err)
	}

	// Acquire replication connect to the source database
	zap.L().Debug("Acquiring replication connection", zap.String("name", s.name))
	sourceConn, err := s.publisher.Conn().ReplicationConnect(ctx)
	if err != nil {
		return fmt.Errorf("Error acquiring replication connection: %w", err)
	}

	err = s.startReplication(ctx, sourceConn)
	if err != nil {
		return fmt.Errorf("Error starting replication: %w", err)
	}

	// Start the apply worker
	return err
}
