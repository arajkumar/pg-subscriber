package subscription

import (
	"fmt"
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/timescale/tslogrepl/internal/timescale"
	"time"
)

func refreshSubscription(ctx context.Context, conn *pgx.Conn, subscriptionName string) error {
	sql := fmt.Sprintf("ALTER SUBSCRIPTION %s REFRESH PUBLICATION",
							pgx.Identifier{subscriptionName}.Sanitize())
	_, err := conn.Exec(ctx, sql)
	return err
}

func Run(ctx context.Context, subscriptionName string, publicationName string, sourcePGURI string, targetPGURI string) {
	sourceConn, err := pgx.Connect(ctx, sourcePGURI)
	if err != nil {
		panic(err)
	}

	targetConn, err := pgx.Connect(ctx, targetPGURI)
	if err != nil {
		panic(err)
	}

	for {

		chunks, err := timescale.ListMissingChunks(ctx, sourceConn)
		if err != nil {
			panic(err)
		}

		for _, chunk := range chunks {
			fmt.Println("Creating chunk on target: ", chunk)
			err := chunk.Create(ctx, targetConn)
			if err != nil {
				fmt.Println("Error creating chunk: ", err)
				continue
			}
		}

		for _, chunk := range chunks {
			fmt.Println("Adding chunk to publication: ", chunk)
			err := chunk.AddToPublication(ctx, sourceConn)
			if err != nil {
				fmt.Println("Error adding chunk to publication: ", err)
				continue
			}
		}

		if len(chunks) > 0 {
			err := refreshSubscription(ctx, targetConn, subscriptionName)
			if err != nil {
				fmt.Println("Error refreshing subscription: ", err)
			}
		}

		time.Sleep(2 * time.Second)
	}
}
