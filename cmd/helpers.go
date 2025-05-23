package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"log"
	"os"

	"github.com/EraldBa/csv-data-collector/models"
	"github.com/EraldBa/csv-data-collector/validators"
	"github.com/go-sql-driver/mysql"
)

// collectorArgs holds the args for the program
type collectorArgs struct {
	logPath    string
	configPath string
	help       bool
}

// getCollectorArgs gets the arguements from the command line
// and returns a collectorArgs struct with the values
func getCollectorArgs() *collectorArgs {
	args := &collectorArgs{}

	flag.StringVar(&args.logPath, "logPath", "", "The path to create and write the logs")
	flag.StringVar(&args.configPath, "configPath", "", "The config.json path")

	flag.BoolVar(&args.help, "help", false, "Prints this help message")

	flag.Parse()

	return args
}

// openLogFile opens the log file with the appropriate flags and permission
// and returns the file pointer or the error
func openLogFile(logPath string) (*os.File, error) {
	return os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
}

// getConfig reads the config file and returns a models.Config
// struct with the data or the error
func getConfig(configPath string) (*models.Config, error) {
	file, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	config := &models.Config{}

	if err = json.Unmarshal(file, config); err != nil {
		return nil, err
	}

	if err = validators.Config(config); err != nil {
		return nil, err
	}

	return config, nil
}

// opendDBConn opens the mysql db connection using the provided DBInfo
// and returns the connection or the error
func openDBConn(dbInfo *models.DBInfo) (*sql.DB, error) {
	if dbInfo.Username == "" {
		dbInfo.Username = "root"
	}

	if dbInfo.Address == "" {
		dbInfo.Address = "localhost:3306"
	}

	// creating a mysql.Config only for using the FormatDSN method
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

	// Pinging the database to make sure connection
	// is still alive
	if err = db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

// panicIfError performs log.Panicln() on the error if it's not nil.
// This function exists just to save time by preventing repetition.
// log.Panicln() is used instead of log.Fatal() so that the deferred
// funcs are able to run
func panicIfError(err error) {
	if err != nil {
		log.Panicln(err)
	}
}
