package apply

import (
	"context"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"go.uber.org/zap"

	"github.com/timescale/pg-subscriber/internal/api"
)

type Apply struct {
}

func New(ctx context.Context, conn *pgconn.PgConn) (*Apply, error) {
	return &Apply{}, nil
}

func StartApply(ctx context.Context, conn *pgconn.PgConn, subscriber api.Subscriber) error {
	zap.L().Debug("Starting apply")

	return nil
}
