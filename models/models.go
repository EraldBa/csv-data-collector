package models

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
)

// Config holds the config.json info
type Config struct {
	DbInfo  DBInfo   `json:"db_info"`
	Devices []Device `json:"devices"`
}

// DBInfo holds the database config info for the mysql connection
type DBInfo struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Address  string `json:"address"`
	Name     string `json:"dbname"`
}

// Device holds the device data info
type Device struct {
	Name       string     `json:"name"`
	FilePath   string     `json:"filepath"`
	Address    string     `json:"address"`
	Interval   uint       `json:"interval"`
	CsvOptions CSVOptions `json:"csv_options"`
}

// CSVOptions holds the csv config info
type CSVOptions struct {
	SkipRows           uint            `json:"skip_rows"`
	Delimiter          string          `json:"delimiter"`
	CreateTableOptions string          `json:"create_table_opts"`
	Columns            []ColumnOptions `json:"columns"`
}

// ColumnOptions holds the options for retrieving and storing
// desired column info
type ColumnOptions struct {
	ColumnIndex  uint   `json:"column_index"`
	Name         string `json:"name"`
	SQLType      string `json:"sql_type"`
	SQLFormatter string `json:"sql_formatter"`
}

// RunChecks runs basic checks on Config to make sure
// data provided in json is correct
func (c *Config) RunChecks() error {
	if c.Devices == nil || len(c.Devices) < 1 {
		return errors.New("no devices provided")
	}

	for i, device := range c.Devices {
		if device.Name == "" {
			return fmt.Errorf("no device name specified at json index: %d", i)
		}

		if device.Address != "" && device.FilePath != "" {
			return fmt.Errorf("can't have both device address and data filepath set for device %s at json index: %d", device.Name, i)
		}

		if device.FilePath != "" {
			if _, err := os.Stat(device.FilePath); err != nil {
				return fmt.Errorf("device data path %s is not valid or file does not exist at json index: %d", device.FilePath, i)
			}

			if device.Interval > 0 {
				return fmt.Errorf("device %s interval and data path can't be set at the same time at json index: %d", device.Name, i)
			}
		}

		if device.Address != "" {
			if _, err := url.ParseRequestURI(device.Address); err != nil {
				return fmt.Errorf("device url address %s not valid at json index: %d", device.Address, i)
			}
		}

		if err := device.RunCSVChecks(); err != nil {
			return err
		}
	}

	return nil
}

// RunCSVChecks runs the checks for the CsvOptions member of the Device struct
func (d *Device) RunCSVChecks() error {
	if d.CsvOptions.Columns == nil || len(d.CsvOptions.Columns) < 1 {
		return fmt.Errorf("no columns specified for device: %s", d.Name)
	}

	if len(d.CsvOptions.Delimiter) > 1 {
		return fmt.Errorf("csv delimiter %s for device %s is not valid", d.CsvOptions.Delimiter, d.Name)
	}

	return nil
}

// GetFilteredRecords gets the records with the deisred column data
// from the appropriate data source, all according to the device config info
func (d *Device) GetFilteredRecords() ([]any, error) {
	var body io.ReadCloser

	if d.Address != "" {
		res, err := http.Get(d.Address)
		if err != nil {
			return nil, err
		}

		body = res.Body
	} else if d.FilePath != "" {
		f, err := os.Open(d.FilePath)
		if err != nil {
			return nil, err
		}

		body = f
	} else {
		return nil, errors.New("no address or filepath specified")
	}

	defer body.Close()

	reader := csv.NewReader(body)

	reader.FieldsPerRecord = -1

	if d.CsvOptions.Delimiter != "" {
		reader.Comma = []rune(d.CsvOptions.Delimiter)[0]
	}

	// Skipping the rows specified
	for range d.CsvOptions.SkipRows {
		reader.Read()
	}

	records := []any{}
	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}

		for _, colOpts := range d.CsvOptions.Columns {
			records = append(records, record[colOpts.ColumnIndex])
		}
	}

	return records, nil
}