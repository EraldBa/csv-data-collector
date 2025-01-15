package models

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"
)

// Config holds the config.json info
type Config struct {
	DbInfo  DBInfo    `json:"db_info"`
	Devices []*Device `json:"devices"`
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
	CsvOptions CSVOptions `json:"csv_options"`
}

// CSVOptions holds the csv options info
type CSVOptions struct {
	SkipRows           uint            `json:"skip_rows"`
	Delimiter          string          `json:"delimiter"`
	CreateTableOptions string          `json:"create_table_opts"`
	Columns            []ColumnOptions `json:"columns"`
}

// ColumnOptions holds the options for retrieving and storing
// desired column info
type ColumnOptions struct {
	ColumnIndex  int    `json:"column_index"`
	Name         string `json:"name"`
	SQLType      string `json:"sql_type"`
	SQLFormatter string `json:"sql_formatter"`
}

// RunChecks runs basic checks on Config to make sure
// data provided in the json config is correct
func (c *Config) RunChecks() error {
	if len(c.Devices) < 1 {
		return errors.New("no devices provided")
	}

	if c.DbInfo.Name == "" {
		return errors.New("no database name provided")
	}

	for i, device := range c.Devices {
		if device.Name == "" {
			return fmt.Errorf("no device name specified at json index %d", i)
		}

		if device.Address != "" && device.FilePath != "" {
			return fmt.Errorf("can't have both device data url address and data filepath set for device %q at json index %d", device.Name, i)
		}

		if device.FilePath != "" {
			if _, err := os.Stat(device.FilePath); err != nil {
				return fmt.Errorf("problem statting device data file path %q for device %q at json index %d with error: %s", device.FilePath, device.Name, i, err.Error())
			}
		} else if device.Address != "" {
			if _, err := url.ParseRequestURI(device.Address); err != nil {
				return fmt.Errorf("problem parsing device data url address %q for device %q at json index %d with error: %s", device.Address, device.Name, i, err.Error())
			}
		} else {
			return fmt.Errorf("no device url address or filepath specified for device %q at json index %d", device.Name, i)
		}

		if len(device.CsvOptions.Columns) < 1 {
			return fmt.Errorf("no columns specified for device %q at json index %d", device.Name, i)
		}

		if len(device.CsvOptions.Delimiter) > 1 {
			return fmt.Errorf("csv delimiter %q for device %q is not valid at json index %d", device.CsvOptions.Delimiter, device.Name, i)
		}

		// checking column options
		if err := device.runColumnChecks(); err != nil {
			return err
		}
	}

	return nil
}

// runColumnChecks is used internally by RunChecks to
// run basic data checks on the device csv column options
func (d *Device) runColumnChecks() error {
	if len(d.CsvOptions.Columns) < 1 {
		return fmt.Errorf("no column options specified for device %q", d.Name)
	}

	for i, colOpts := range d.CsvOptions.Columns {
		if colOpts.Name == "" {
			return fmt.Errorf("column name cannot be empty at index %d in device %q column options", i, d.Name)
		}

		if colOpts.ColumnIndex < 0 {
			return fmt.Errorf("invalid index for column %q in device %q options", colOpts.Name, d.Name)
		}

		if colOpts.SQLType == "" {
			return fmt.Errorf("sql type for column %q at device %q options cannot be emtpy", colOpts.Name, d.Name)
		}

	}

	return nil
}

// GetFilteredRecords gets the records with the deisred column data
// from the appropriate data source, all according to the device config info,
// The records returned are in the appropriate format for direct usage in
// the mysql insert query
func (d *Device) GetFilteredRecords() ([]any, error) {
	var body io.ReadCloser

	if d.Address != "" {
		// if timeout is not set, set it to 15 seconds
		if http.DefaultClient.Timeout == 0 {
			http.DefaultClient.Timeout = time.Second * 15
		}

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

	// Specifying to have no field per record limit
	reader.FieldsPerRecord = -1

	if d.CsvOptions.Delimiter != "" {
		reader.Comma = []rune(d.CsvOptions.Delimiter)[0]
	}

	// Skipping the rows specified
	for range d.CsvOptions.SkipRows {
		if _, err := reader.Read(); err != nil {
			return nil, err
		}
	}

	records := []any{}
	for i := 0; ; i++ {
		rec, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, fmt.Errorf("error at row index %d with error: %s", i, err.Error())
		}

		for _, colOpts := range d.CsvOptions.Columns {
			if len(rec) <= colOpts.ColumnIndex {
				return nil, fmt.Errorf("error at row index %d:cannot get column value with name %q from row by indexing; row length is smaller than column index (%d)", i, colOpts.Name, colOpts.ColumnIndex)
			}

			records = append(records, rec[colOpts.ColumnIndex])
		}
	}

	return records, nil
}
