package repository

import (
	"context"
	"csv-data-collector/models"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// DBConf holds the app database configurations
// and database connection
type DBConf struct {
	DB *sql.DB
}

const ctxTimeOut = 30 * time.Second

// NewDBConf returns a new instance of DBConf with the
// provided db connection
func NewDBConf(conn *sql.DB) *DBConf {
	return &DBConf{
		DB: conn,
	}
}

// SaveDevices saves data from all devices in the config
// concurrently. It logs save info for each device to stdout
func (d *DBConf) SaveDevices(config *models.Config) {
	wg := &sync.WaitGroup{}

	for _, device := range config.Devices {
		wg.Add(1)
		go func(device *models.Device) {
			defer wg.Done()

			err := d.SaveCSVDataFor(device)
			if err != nil {
				log.Printf("ERROR: Could not save data for device %s with error: %s\n", device.Name, err.Error())
				return
			}

			log.Println("Saved device data successfully for:", device.Name)
		}(&device)
	}

	wg.Wait()
}

// SaveCSVDataFor saves the csv data for the provided device to the
// appropriate table in the db
func (d *DBConf) SaveCSVDataFor(device *models.Device) error {
	ctx, cancel := context.WithTimeout(context.Background(), ctxTimeOut)
	defer cancel()

	// checking if db table exists, if not, create it
	row := d.DB.QueryRowContext(ctx, "SELECT * FROM "+device.Name)
	if row.Err() != nil {
		err := d.createTableFor(device)
		if err != nil {
			return err
		}
	}

	records, err := device.GetFilteredRecords()
	if err != nil {
		return fmt.Errorf("could not get csv records from device: %s", device.Name)
	}

	rowCount := len(records) / len(device.CsvOptions.Columns)

	query := generateCSVInsertQuery(device, rowCount)

	_, err = d.DB.ExecContext(ctx, query, records...)

	return err
}

// CreateTableFor creates a table in the db for specified device
func (d *DBConf) createTableFor(device *models.Device) error {
	ctx, cancel := context.WithTimeout(context.Background(), ctxTimeOut)
	defer cancel()

	query := generateCSVCreateTableQuery(device)

	_, err := d.DB.ExecContext(ctx, query)

	return err
}

// generateCSVCreateTableQuery generates table creation
// mysql queries for the provided device
func generateCSVCreateTableQuery(device *models.Device) string {
	query := "CREATE TABLE " + device.Name + " ("

	for _, colOpts := range device.CsvOptions.Columns {
		query += colOpts.Name + " " + colOpts.SQLType + ","
	}

	query += device.CsvOptions.CreateTableOptions
	query = strings.TrimSuffix(query, ",")
	query += ")"

	return query
}

// generateCSVInsertQuery generates insert mysql queries
// for the provided device
func generateCSVInsertQuery(device *models.Device, rowCount int) string {
	columnNames, vals := "(", "("

	for _, colOpts := range device.CsvOptions.Columns {
		columnNames += colOpts.Name + ","

		if colOpts.SQLFormatter != "" {
			vals += colOpts.SQLFormatter + ","
		} else {
			vals += "?,"
		}
	}

	vals = strings.TrimSuffix(vals, ",")
	vals += "),"

	columnNames = strings.TrimSuffix(columnNames, ",")
	columnNames += ")"

	query := fmt.Sprintf("INSERT IGNORE INTO %s %s VALUES", device.Name, columnNames)
	query += strings.Repeat(vals, rowCount)
	query = strings.TrimSuffix(query, ",")

	return query
}
