/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"github.com/spf13/cobra"

	"github.com/timescale/tslogrepl/internal/subscription"
)

// subscriptionHelperCmd represents the subscriptionHelper command
var subscriptionHelperCmd = &cobra.Command{
	Use:   "subscription-helper",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		subscription.Run(
			cmd.Context(),
			cmd.Flag("subscription").Value.String(),
			cmd.Flag("publication").Value.String(),
			cmd.Flag("source").Value.String(),
			cmd.Flag("target").Value.String())
	},
}

func init() {
	rootCmd.AddCommand(subscriptionHelperCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// subscriptionHelperCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// subscriptionHelperCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	subscriptionHelperCmd.Flags().StringP("source", "s", "", "Source PGURI")
	subscriptionHelperCmd.MarkFlagRequired("source")
	subscriptionHelperCmd.Flags().StringP("target", "t", "", "Target PGURI")
	subscriptionHelperCmd.MarkFlagRequired("target")
	subscriptionHelperCmd.Flags().StringP("publication", "p", "", "Publication Name")
	subscriptionHelperCmd.MarkFlagRequired("publication")
	subscriptionHelperCmd.Flags().StringP("subscription", "u", "", "Subscription Name")
	subscriptionHelperCmd.MarkFlagRequired("subscription")
}
