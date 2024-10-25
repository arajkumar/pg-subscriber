package subscription

import (
	"context"
	"fmt"
	"golang.org/x/sync/errgroup"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"

	"github.com/timescale/timescaledb-cdc/pkg/conn"
)

type applyContext struct {
	conn           *conn.ApplyConn
	tx             pgx.Tx
	batch          pgx.Batch
	lastCommitTime time.Time
	commitLSN      pglogrepl.LSN
	commitTime     time.Time
	txnInProgress  bool
	timer          *time.Timer
	batchDuration  time.Duration
	relations      map[uint32]*pglogrepl.RelationMessage
	typeMap        *pgtype.Map
	// This would help us to skip txn which are already applied during
	// restart. It would be initialized based on the replication origin
	// progress and updated on each successful commit.
	lastCommitLSN   pglogrepl.LSN
	reachedStartPos bool
}

func newApplyCtx(conn *conn.ApplyConn, lastCommitLSN pglogrepl.LSN) *applyContext {
	relations := make(map[uint32]*pglogrepl.RelationMessage)
	// TODO: Does it make sense to expose as a setting?
	batchDuration := 2 * time.Second

	return &applyContext{
		conn:            conn,
		lastCommitTime:  time.Now(),
		timer:           time.NewTimer(batchDuration),
		batchDuration:   batchDuration,
		relations:       relations,
		typeMap:         pgtype.NewMap(),
		lastCommitLSN:   lastCommitLSN,
		reachedStartPos: false,
	}
}

func (a *applyContext) queue(q string, args ...interface{}) {
	if a.reachedStartPos {
		a.batch.Queue(q, args...)
	}
}

func (a *applyContext) begin(finalLSN pglogrepl.LSN) {
	a.reachedStartPos = a.lastCommitLSN < finalLSN
	if !a.reachedStartPos {
		zap.L().Info("Skipping", zap.String("lastCommitLSN", a.lastCommitLSN.String()),
			zap.String("finalLSN", finalLSN.String()))
	}
	a.txnInProgress = true
	a.timer.Stop()
}

func (a *applyContext) commit(ctx context.Context, commitLSN pglogrepl.LSN, commitTime time.Time) {
	a.commitLSN = commitLSN
	a.commitTime = commitTime
	a.txnInProgress = false
	if time.Since(a.lastCommitTime) > a.batchDuration {
		a.flush(ctx)
	} else {
		a.timer.Reset(a.batchDuration)
	}
}

func (a *applyContext) flush(ctx context.Context) error {
	if a.batch.Len() > 0 {
		q := `select pg_replication_origin_xact_setup($1, $2)`
		a.batch.Queue(q, a.commitLSN, a.commitTime)
		before := time.Now()

		err := a.conn.SendBatch(ctx, &a.batch).Close()
		if err != nil {
			return fmt.Errorf("failed to apply batch: %w", err)
		}

		zap.L().Debug("commit", zap.Duration("elapsed", time.Since(before)),
			zap.Int("buflen", a.batch.Len()))

		applyLSN, err := a.conn.OriginProgress(ctx, true)
		if err == nil {
			a.lastCommitLSN = applyLSN
		} else {
			zap.L().Warn("Origin progress failure", zap.Error(err))
		}
		a.batch = pgx.Batch{}
		a.lastCommitTime = time.Now()
	}

	return nil
}

func (a *applyContext) applyV1(ctx context.Context, walData []byte) error {
	logicalMsg, err := pglogrepl.Parse(walData)
	if err != nil {
		return fmt.Errorf("Error on v1 parse: %w", err)
	}

	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		a.relations[logicalMsg.RelationID] = logicalMsg

	case *pglogrepl.BeginMessage:
		// Indicates the beginning of a group of changes in a transaction. This is only sent for committed transactions. You won't get any events from rolled back transactions.
		a.begin(logicalMsg.FinalLSN)

	case *pglogrepl.CommitMessage:
		a.commit(ctx, logicalMsg.CommitLSN, logicalMsg.CommitTime)

	case *pglogrepl.InsertMessage:
		rel, ok := a.relations[logicalMsg.RelationID]
		if !ok {
			return fmt.Errorf("Unknown relation ID %d", logicalMsg.RelationID)
		}

		query := fmt.Sprintf("INSERT INTO %s(", pgx.Identifier{rel.Namespace, rel.RelationName}.Sanitize())

		vals := []interface{}{}
		for idx, col := range logicalMsg.Tuple.Columns {
			colName := pgx.Identifier{rel.Columns[idx].Name}.Sanitize()

			if idx == 0 {
				query += colName
			} else {
				query += ", " + colName
			}

			switch col.DataType {
			case 'n': // null
				vals = append(vals, nil)
			case 'u': // unchanged toast
				// This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
			case 't': //text
				val, err := a.decodeTextColumnData(col.Data, rel.Columns[idx].DataType)
				if err != nil {
					return fmt.Errorf("error decoding column data: %w", err)
				}
				vals = append(vals, val)
			}
		}
		query += ") overriding system value VALUES("
		for idx := range logicalMsg.Tuple.Columns {
			if idx == 0 {
				query += fmt.Sprintf("$%d", idx+1)
			} else {
				query += fmt.Sprintf(", $%d", idx+1)
			}
		}
		query += ")"
		a.queue(query, vals...)

	case *pglogrepl.TruncateMessage:
		//
	default:
		zap.L().Panic("Unknown message type", zap.Reflect("msg", logicalMsg))
	}
	return nil
}

func (a *applyContext) decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := a.typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(a.typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

func receive(ctx context.Context, source *conn.ReceiveConn,
	walDataCh chan<- []byte,
	applyLSNCh <-chan pglogrepl.LSN) error {

	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	for {
		var applyLSN pglogrepl.LSN

		select {
		case <-ctx.Done():
			return ctx.Err()
		case applyLSN = <-applyLSNCh:
			//
		default:
		}

		if time.Now().After(nextStandbyMessageDeadline) {
			// It is good enough to send WALWritePosition, rest will be assigned based
			// on it.
			err := pglogrepl.SendStandbyStatusUpdate(ctx, source.PgConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: applyLSN})
			if err != nil {
				return fmt.Errorf("Error during send status update: %w", err)
			}
			zap.L().Info("Standby update", zap.String("pos", applyLSN.String()))
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(ctx, nextStandbyMessageDeadline)
		rawMsg, err := source.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("Error on receive: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			zap.L().Error("Postgres WAL error", zap.Reflect("type", errMsg))
			continue
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			zap.L().Error("Postgres WAL error", zap.Reflect("type", msg))
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("ParsePrimaryKeepaliveMessage failed: %w", err)
			}

			zap.L().Debug("Primary Keepalive Message",
				zap.String("ServerWALEnd", pkm.ServerWALEnd.String()),
				zap.String("ServerTime", pkm.ServerTime.String()),
				zap.Bool("ReplyRequested", pkm.ReplyRequested))

			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}
			// TODO: Inform keep alive to the apply go routine.

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("ParseXLogData failed: %w", err)
			}

			zap.L().Debug("XLogData", zap.String("WALStart", xld.WALStart.String()),
				zap.String("ServerWALEnd", xld.ServerWALEnd.String()),
				zap.String("ServerTime", xld.ServerTime.String()))

			// TODO: Use buffer pool to avoid repeated allocation
			walDataCp := make([]byte, len(xld.WALData))
			copy(walDataCp, xld.WALData)
			walDataCh <- walDataCp
		}
	}
}

func StartApply(ctx context.Context, source *conn.ReceiveConn, target *conn.ApplyConn, start pglogrepl.LSN) error {
	zap.L().Info("Starting apply", zap.String("lsn", start.String()))

	// To buffer wal data from receive to apply
	walDataCh := make(chan []byte, 1024)
	// Last flushed applyLSN from replication origin progress.
	// This will be sent from apply to receive to send feedback.
	applyLSNCh := make(chan pglogrepl.LSN)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		// Receive is the only writer into the walDataCh
		defer close(walDataCh)
		return receive(ctx, source, walDataCh, applyLSNCh)
	})

	g.Go(func() error {
		// Apply is the only writer into the applyLSNCh
		defer close(applyLSNCh)
		return apply(ctx, target, start, walDataCh, applyLSNCh)
	})

	return g.Wait()
}

func apply(ctx context.Context, target *conn.ApplyConn, start pglogrepl.LSN, walDataCh <-chan []byte, applyLSNCh chan<- pglogrepl.LSN) error {
	var err error
	a := newApplyCtx(target, start)
	for {
		select {
		case <-a.timer.C:
			err = a.flush(ctx)
		case <-ctx.Done():
			err = a.flush(ctx)
		case applyLSNCh <- a.lastCommitLSN:
			//
		case walData, ok := <-walDataCh:
			if ok {
				err = a.applyV1(ctx, walData)
			}
		}

		if err != nil {
			return err
		}
	}
}
