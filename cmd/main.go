package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/EraldBa/csv-data-collector/repository"
)

func main() {
	args := getCollectorArgs()

	if args.help {
		fmt.Println("Usage:")
		flag.PrintDefaults()
		os.Exit(0)
	}

	if args.configPath == "" || args.logPath == "" {
		log.Fatal("all required paths not provided, exiting...")
	}

	logFile, err := openLogFile(args.logPath)
	panicIfError(err)

	defer logFile.Close()

	log.SetOutput(logFile)

	config, err := getConfig(args.configPath)
	panicIfError(err)

	conn, err := openDBConn(&config.DbInfo)
	panicIfError(err)

	defer conn.Close()

	repo := repository.New(conn, config)

	repo.SaveDevices()

	log.Println("-----PROGRAM EXITED-----")
}
