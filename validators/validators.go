package validators

import (
	"errors"
	"fmt"
	"net/url"
	"os"

	"github.com/EraldBa/csv-data-collector/models"
)

// Config validates the data held in a models.Config
// struct and returns the potential error
func Config(config *models.Config) error {
	if len(config.Devices) < 1 {
		return errors.New("no devices provided")
	}

	if config.DbInfo.Name == "" {
		return errors.New("no database name provided")
	}

	for i, device := range config.Devices {
		if device.Name == "" {
			return fmt.Errorf("no device name specified at json index %d", i)
		}

		if device.Address != "" && device.FilePath != "" {
			return fmt.Errorf("can't have both device data url address and data filepath set for device %q at json index %d", device.Name, i)
		}

		if device.FilePath != "" {
			if _, err := os.Stat(device.FilePath); err != nil {
				return fmt.Errorf("could not stat device data file path %q for device %q at json index %d with error: %w", device.FilePath, device.Name, i, err)
			}
		} else if device.Address != "" {
			if _, err := url.ParseRequestURI(device.Address); err != nil {
				return fmt.Errorf("could not parse device data url address %q for device %q at json index %d with error: %w", device.Address, device.Name, i, err)
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
		if err := ColumnOptions(device.CsvOptions.Columns); err != nil {
			return fmt.Errorf("columns validation failed for device %q with error: %w", device.Name, err)
		}
	}

	return nil
}

// ColumnOptions validates the data held in a models.ColumnOptions
// slice and returns the potential error
func ColumnOptions(columns []models.ColumnOptions) error {
	if len(columns) == 0 {
		return errors.New("column options are empty")
	}

	for i, colOpts := range columns {
		if colOpts.Name == "" {
			return fmt.Errorf("column name cannot be empty at index %d", i)
		}

		if colOpts.ColumnIndex < 0 {
			return fmt.Errorf("invalid index for column %q", colOpts.Name)
		}

		if colOpts.SQLType == "" {
			return fmt.Errorf("sql type for column %q is empty", colOpts.Name)
		}
	}

	return nil
}
