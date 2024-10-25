package cmd

import (
	"github.com/spf13/cobra"

	"github.com/timescale/timescaledb-cdc/pkg/entry"
)

// subscriptionHelperCmd represents the subscriptionHelper command
var run = &cobra.Command{
	Use:   "run",
	Short: "Implementation of Postgres logical replication subscriber in Go.",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		subscriptions, _ := cmd.Flags().GetStringArray("subscription")
		publications, _ := cmd.Flags().GetStringArray("publication")

		sourceConn, _ := cmd.Flags().GetString("source")
		targetConn, _ := cmd.Flags().GetString("target")

		err := entry.Run(ctx, sourceConn, targetConn, publications, subscriptions, entry.NeverEnd)
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
