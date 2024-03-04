package main

import (
	"csv-data-collector/models"
	"csv-data-collector/repository"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"os"

	"github.com/go-sql-driver/mysql"
)

func main() {
	config, err := getConfig()
	exitIfError(err)

	err = config.RunChecks()
	exitIfError(err)

	conn, err := openDBConn(&config.DbInfo)
	exitIfError(err)

	defer conn.Close()

	dbConf := repository.NewDBConf(conn)

	dbConf.SaveDevices(config)
}

// getConfig reads the config.json file and returns a Config with
// the data or the error that occured
func getConfig() (*models.Config, error) {
	file, err := os.ReadFile("config.json")
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

// opendDBConn opens mysql db connection using the provided DBInfo conf
// and returns the connection or the error that occured
func openDBConn(dbInfo *models.DBInfo) (*sql.DB, error) {
	if dbInfo.Name == "" {
		return nil, errors.New("no database name provided")
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

// exitIfError performs log.Fatal() on the error if it exists
// This function exists just to save time by preventing repetition
func exitIfError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
