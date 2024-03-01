package main

import (
	"csv-data-collector/models"
	"csv-data-collector/repository"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/go-sql-driver/mysql"
)

func main() {
	data, err := os.ReadFile("config.json")
	exitIfError(err)

	config := &models.Config{}

	err = json.Unmarshal(data, config)
	exitIfError(err)

	err = config.RunChecks()
	exitIfError(err)

	conn, err := openDBConn(&config.DbInfo)
	exitIfError(err)

	defer conn.Close()

	dbConf := repository.NewDBConf(conn)

	wg := &sync.WaitGroup{}

	for _, device := range config.Devices {
		wg.Add(1)
		go func() {
			defer wg.Done()

			err = dbConf.SaveCSVDataFor(&device)
			if err != nil {
				log.Println("ERROR: Could not save data for device with error:", err)
				return
			}

			log.Println("Saved device data successfully for:", device.Name)
		}()
	}

	wg.Wait()
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

// exitIfError performs log.Fatal() on the error if it exists
// This function exists just to save time by preventing repetition
func exitIfError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
