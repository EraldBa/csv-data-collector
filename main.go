package main

import (
	"csv-data-collector/models"
	"csv-data-collector/repository"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"os"
	"runtime"

	"github.com/go-sql-driver/mysql"
)

const (
	logFileName    = "log.txt"
	configFileName = "config.json"
)

func main() {
	defer os.Exit(0)

	logFile, err := os.OpenFile(logFileName, os.O_WRONLY, fs.ModeAppend)
	if err != nil {
		logFile, err = os.Create(logFileName)
		checkError(err)
	}

	defer logFile.Close()

	log.SetOutput(logFile)

	data, err := os.ReadFile(configFileName)
	checkError(err)

	config := &models.Config{}

	err = json.Unmarshal(data, config)
	checkError(err)

	conn, err := openDBConn(&config.DbInfo)
	checkError(err)

	defer conn.Close()

	dbConf := repository.NewDBConf(conn)

	err = config.RunChecks()
	checkError(err)

	for _, device := range config.Devices {
		err = dbConf.SaveCSVDataFor(&device)
		checkError(err)

		log.Println("Saved device data successfully for:", device.Name)
	}
}

// opendDBConn opens mysql db connection using the provided DBInfo conf
// and returns the connection or the error that occured
func openDBConn(dbInfo *models.DBInfo) (*sql.DB, error) {
	if dbInfo.Name == "" {
		return nil, fmt.Errorf("no database name provided")
	}

	if dbInfo.Username == "" {
		dbInfo.Username = "root"
	}

	if dbInfo.Address == "" {
		dbInfo.Address = "localhost:3306"
	}

	// creating a mysql.Config only for the FormatDSN
	// method, so that to ensure the dsn string is formatted properly
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

// checkError performs log.Fatal() on the error if it exists
// The function exists just to save time by preventing repetition
func checkError(err error) {
	if err != nil {
		log.Println(err)
		runtime.Goexit()
	}
}
