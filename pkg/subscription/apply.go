package subscription

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	_ "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	_ "github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"

	"github.com/timescale/timescaledb-cdc/pkg/conn"
)

func StartApply(ctx context.Context, source *conn.ReceiveConn, target *conn.ApplyConn, start pglogrepl.LSN) error {
	zap.L().Debug("Starting apply")

	clientXLogPos := start
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
	// relations := map[uint32]*pglogrepl.RelationMessage{}
	// typeMap := pgtype.NewMap()

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			// TODO: Send write, flush, apply LSN
			err := pglogrepl.SendStandbyStatusUpdate(ctx, source.PgConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				return fmt.Errorf("Error during send status update: %w", err)
			}
			zap.L().Debug("Standby update", zap.String("pos", clientXLogPos.String()))
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(ctx, nextStandbyMessageDeadline)
		rawMsg, err := source.PgConn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("Error on receive: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			zap.L().Error("Postgres WAL error", zap.Reflect("type", errMsg))
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

			if pkm.ServerWALEnd > clientXLogPos {
				clientXLogPos = pkm.ServerWALEnd
			}

			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("ParseXLogData failed:%w", err)
			}

			zap.L().Debug("XLogData", zap.String("WALStart", xld.WALStart.String()),
				zap.String("ServerWALEnd", xld.ServerWALEnd.String()),
				zap.String("ServerTime", xld.ServerTime.String()))
			// TODO: Process

			if xld.WALStart > clientXLogPos {
				clientXLogPos = xld.WALStart
			}
		}
	}
}
