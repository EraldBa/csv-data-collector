package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/EraldBa/csv-data-collector/models"
	"github.com/EraldBa/csv-data-collector/repository"

	"github.com/go-sql-driver/mysql"
)

func main() {
	logPath := flag.String("logPath", "", "The path to create and write the logs")
	configPath := flag.String("configPath", "", "The config.json path")

	help := flag.Bool("help", false, "Prints this help message")

	flag.Parse()

	if *help {
		fmt.Println("Usage:")
		flag.PrintDefaults()
		os.Exit(0)
	}

	if *logPath == "" || *configPath == "" {
		log.Fatal("All required paths not provided, exiting...")
	}

	logFile, err := os.OpenFile(*logPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	panicIfError(err)

	defer logFile.Close()

	log.SetOutput(logFile)

	config, err := getConfig(configPath)
	panicIfError(err)

	err = config.RunChecks()
	panicIfError(err)

	conn, err := openDBConn(&config.DbInfo)
	panicIfError(err)

	defer conn.Close()

	repo := repository.New(conn, config)

	repo.SaveDevices()

	log.Println("--- PROGRAM TERMINATED ---")
}

// getConfig reads the config file and returns a models.Config
// struct with the data or the error that occured
func getConfig(configPath *string) (*models.Config, error) {
	file, err := os.ReadFile(*configPath)
	if err != nil {
		return nil, err
	}

	config := &models.Config{}

	err = json.Unmarshal(file, config)
	if err != nil {
		return nil, err
	}

	return config, nil
}

// opendDBConn opens the mysql db connection using the provided DBInfo
// and returns the connection or the error that occured
func openDBConn(dbInfo *models.DBInfo) (*sql.DB, error) {
	if dbInfo.Username == "" {
		dbInfo.Username = "root"
	}

	if dbInfo.Address == "" {
		dbInfo.Address = "localhost:3306"
	}

	// creating a mysql.Config only for the FormatDSN method,
	// so as to ensure the dsn string is formatted properly
	mysqlConf := mysql.Config{
		User:   dbInfo.Username,
		Passwd: dbInfo.Password,
		Addr:   dbInfo.Address,
		DBName: dbInfo.Name,
	}

	db, err := sql.Open("mysql", mysqlConf.FormatDSN())
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

// panicIfError performs log.Panicln() on the error if it exists.
// This function exists just to save time by preventing repetition.
// log.Panicln() is used instead of log.Fatal() so that the deferred
// funcs are allowed to run
func panicIfError(err error) {
	if err != nil {
		log.Panicln(err)
	}
}
