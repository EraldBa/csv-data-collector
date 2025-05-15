package models

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/http"
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

func init() {
	http.DefaultClient.Timeout = time.Second * 15
}

// GetFilteredRecords gets the records with the deisred column data
// from the appropriate data source, all according to the device config info,
// The records returned are in the appropriate format for direct usage in
// the mysql insert query
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

	// Specifying to have no field per record limit
	reader.FieldsPerRecord = -1

	if d.CsvOptions.Delimiter != "" {
		reader.Comma = rune(d.CsvOptions.Delimiter[0])
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

			return nil, fmt.Errorf("error at row index %d with error: %w", i, err)
		}

		for _, colOpts := range d.CsvOptions.Columns {
			if len(rec) <= colOpts.ColumnIndex {
				return nil, fmt.Errorf("error at row index %d: cannot get column value with name %q from row by indexing; row length is smaller than column index (%d)", i, colOpts.Name, colOpts.ColumnIndex)
			}

			records = append(records, rec[colOpts.ColumnIndex])
		}
	}

	return records, nil
}
