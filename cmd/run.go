package cmd

import (
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/timescale/pg-subscriber/internal/conn"
	"github.com/timescale/pg-subscriber/internal/publication"
	"github.com/timescale/pg-subscriber/internal/subscription"
)

// subscriptionHelperCmd represents the subscriptionHelper command
var run = &cobra.Command{
	Use:   "run",
	Short: "Implementation of Postgres logical replication subscriber in Go.",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		zap.L().Info("Starting pg-subscriber")
		subscriptions, _ := cmd.Flags().GetStringArray("subscription")
		publications, _ := cmd.Flags().GetStringArray("publication")

		sourceConn, _ := cmd.Flags().GetString("source")
		targetConn, _ := cmd.Flags().GetString("target")

		source, err := conn.Parse(sourceConn)
		if err != nil {
			panic(err)
		}

		target, err := conn.Parse(targetConn)
		if err != nil {
			panic(err)
		}

		pub, err := publication.New(ctx, publications, source.AsSource())
		if err != nil {
			panic(err)
		}

		sub, err := subscription.New(ctx, subscriptions[0], target.AsTarget(), pub)
		if err != nil {
			panic(err)
		}

		err = sub.Sync(ctx)
		if err != nil {
			panic(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(run)

	run.Flags().StringP("source", "s", "", "Source PGURI")
	run.MarkFlagRequired("source")
	run.Flags().StringP("target", "t", "", "Target PGURI")
	run.MarkFlagRequired("target")
	run.Flags().StringArrayP("subscription", "u", []string{}, "Subscription to refresh")
	run.MarkFlagRequired("subscription")
	run.Flags().StringArrayP("publication", "p", []string{}, "Publications to fetch chunks from")
	run.MarkFlagRequired("publication")
	run.Flags().BoolP("copy-data", "c", true, "Specifies the existing data to be copied to the target")
}
