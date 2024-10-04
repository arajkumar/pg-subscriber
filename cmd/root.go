/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "pg-subscriber",
	Short: "Reimplementation of Postgres logical replication in Go.",
	Long: `pg-subscriber is a reimplementation of Postgres logical replication in
	Go.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	debug := rootCmd.PersistentFlags().Lookup("debug").Changed
	var logger *zap.Logger
	var err error

	if debug {
		logger, err = zap.NewDevelopment()
	} else {
		logger, err = zap.NewProduction()
	}
	if err != nil {
		panic(err)
	}

	defer logger.Sync()

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	err = rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// debug flag
	rootCmd.PersistentFlags().BoolP("debug", "d", false, "Enable debug mode")
	// log level flag
	rootCmd.PersistentFlags().StringP("log-level", "l", "info", "Log level")
}
