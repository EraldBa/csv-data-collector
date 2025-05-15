package repository

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/EraldBa/csv-data-collector/models"

	_ "github.com/go-sql-driver/mysql"
)

// dbConfig holds the app configuration
// and database connection
type dbConfig struct {
	AppConf *models.Config
	DB      *sql.DB
}

const ctxTimeOut = 30 * time.Second

// New returns a new instance of dbConfig with the
// provided db connection and app config
func New(conn *sql.DB, config *models.Config) *dbConfig {
	return &dbConfig{
		AppConf: config,
		DB:      conn,
	}
}

// SaveDevices saves data from all devices in the app config
// concurrently. It logs save info or the error for each device.
func (d *dbConfig) SaveDevices() {
	wg := &sync.WaitGroup{}

	wg.Add(len(d.AppConf.Devices))

	for _, device := range d.AppConf.Devices {
		go func(device *models.Device) {
			defer wg.Done()

			err := d.SaveCSVDataFor(device)
			if err != nil {
				log.Printf("ERROR: Could not save data for device %q with error: %s\n", device.Name, err.Error())
				return
			}

			log.Println("INFO: Saved device data successfully for:", device.Name)
		}(device)
	}

	wg.Wait()
}

// SaveCSVDataFor saves the csv data for the provided device to the
// appropriate table in the db
func (d *dbConfig) SaveCSVDataFor(device *models.Device) error {
	ctx, cancel := context.WithTimeout(context.Background(), ctxTimeOut)
	defer cancel()

	// checking if db table exists, if not, create it
	err := d.DB.QueryRowContext(ctx, "SELECT * FROM "+device.Name).Err()
	if err != nil {
		if err = d.createTableFor(device); err != nil {
			return err
		}
	}

	records, err := device.GetFilteredRecords()
	if err != nil {
		return fmt.Errorf("could not get csv records from device %q with error: %w", device.Name, err)
	}

	rowCount := len(records) / len(device.CsvOptions.Columns)

	query := generateCSVInsertQuery(device, rowCount)

	_, err = d.DB.ExecContext(ctx, query, records...)

	return err
}

// createTableFor creates a table in the db for specified device
func (d *dbConfig) createTableFor(device *models.Device) error {
	ctx, cancel := context.WithTimeout(context.Background(), ctxTimeOut)
	defer cancel()

	query := generateCSVCreateTableQuery(device)

	_, err := d.DB.ExecContext(ctx, query)

	return err
}

// generateCSVCreateTableQuery generates table creation
// mysql queries for the provided device
func generateCSVCreateTableQuery(device *models.Device) string {
	queryBuilder := strings.Builder{}

	queryBuilder.WriteString("CREATE TABLE ")
	queryBuilder.WriteString(device.Name)
	queryBuilder.WriteString(" (")

	for i, colOpts := range device.CsvOptions.Columns {
		// insert commas after first column
		if i > 0 {
			queryBuilder.WriteString(",")
		}

		queryBuilder.WriteString(colOpts.Name)
		queryBuilder.WriteString(" ")
		queryBuilder.WriteString(colOpts.SQLType)
	}

	if device.CsvOptions.CreateTableOptions != "" {
		queryBuilder.WriteString(",")
		queryBuilder.WriteString(device.CsvOptions.CreateTableOptions)
	}

	queryBuilder.WriteString(")")

	return queryBuilder.String()
}

// generateCSVInsertQuery generates insert mysql queries
// for the provided device
func generateCSVInsertQuery(device *models.Device, rowCount int) string {
	if rowCount < 1 {
		return ""
	}

	queryBuilder := strings.Builder{}

	// Starting the query
	queryBuilder.WriteString("INSERT IGNORE INTO ")
	queryBuilder.WriteString(device.Name)
	queryBuilder.WriteString(" (")

	// valuesBuilder stores the value placeholders
	valuesBuilder := strings.Builder{}
	valuesBuilder.WriteString("(")

	// Creating columns and value placeholders
	for i, colOpts := range device.CsvOptions.Columns {
		// insert commas after first column and value
		if i > 0 {
			queryBuilder.WriteString(",")
			valuesBuilder.WriteString(",")
		}

		queryBuilder.WriteString(colOpts.Name)

		// inserting sql formatter or default value placeholder
		if colOpts.SQLFormatter != "" {
			valuesBuilder.WriteString(colOpts.SQLFormatter)
		} else {
			valuesBuilder.WriteString("?")
		}
	}
	// Finishing values
	valuesBuilder.WriteString(")")
	vals := valuesBuilder.String()

	queryBuilder.WriteString(") VALUES ")

	// Inserting first value placeholders string
	// without the comma prefix
	queryBuilder.WriteString(vals)
	rowCount--

	// if there's only one row, return the query here
	if rowCount == 0 {
		return queryBuilder.String()
	}

	// Generating the rest of the value placeholders
	// joined by commas. Using strings.Repeat here for
	// more optimized value placeholder string generation
	// since rows can be quite numerous
	vals = strings.Repeat(","+vals, rowCount)

	queryBuilder.WriteString(vals)

	return queryBuilder.String()
}
