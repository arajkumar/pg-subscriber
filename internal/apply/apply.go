package apply

import (
	"context"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.uber.org/zap"

	pub "github.com/timescale/pg-subscriber/internal/publication"
	sub "github.com/timescale/pg-subscriber/internal/subscription"
)

type Apply struct {
	source *pgconn.PgConn
	target *pgx.Conn
	pub    *pub.Publisher
	sub    *sub.Subscriber
}

func New(ctx context.Context, pub *pub.Publisher, sub *sub.Subscriber) (*Apply, error) {
	return &Apply{
		pub: pub,
		sub: sub,
	}, nil
}

func StartApply(ctx context.Context) error {
	zap.L().Debug("Starting apply")

	return nil
}
