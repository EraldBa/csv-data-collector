package main

import (
	"csv-data-collector/models"
	"csv-data-collector/repository"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"
)

func main() {
	data, err := os.ReadFile("config.json")
	if err != nil {
		log.Fatal(err)
	}

	config := &models.Config{}

	err = json.Unmarshal(data, config)
	checkError(err)

	conn, err := openDBConn(&config.DbInfo)
	checkError(err)

	defer conn.Close()

	dbConf := repository.NewDBConf(conn)

	err = config.RunChecks()
	checkError(err)

	threadLaunched := false
	for _, device := range config.Devices {
		err = dbConf.SaveCSVDataFor(&device)
		checkError(err)
		log.Println("Saved device data successfully for:", device.Name)

		// if there's an interval specified for the device, run SaveCSVDataFor on
		// the device in the background on a separate thread by interval * minute times
		if device.Interval > 0 {
			go func(device *models.Device) {
				for range time.Tick(time.Minute * time.Duration(device.Interval)) {
					err := dbConf.SaveCSVDataFor(device)
					checkError(err)

					log.Println("Saved device data successfully for:", device.Name)
				}
			}(&device)

			threadLaunched = true
		}
	}

	if !threadLaunched {
		os.Exit(0)
	}

	select {}
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
		log.Fatal(err)
	}
}
