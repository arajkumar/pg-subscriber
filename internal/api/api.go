package api

import (
	"context"
)

type Relation struct {
	SchemaName string
	TableName  string
	AttNames   []string
}

type PublicationRelation struct {
	Relation
	RowFilter string
}

type Publisher interface {
	// Name of the publisher
	Name() string

	// FetchTables fetchs list of tables that are part of the publication
	// from the source database.
	FetchTables(ctx context.Context) ([]PublicationRelation, error)
}

type Subscriber interface {
	// Name of the subscriber
	Name() string

	// Refresh fetches the list of tables that are part of the publication and
	// populates go_pg_subscriber.subscription_rel
	Refresh(ctx context.Context) error
}
