# csv-data-collector

### A simple data collector application that's designed to automate data retrieval and storage from CSV files without any manual adjustment to the program's codebase. 

# Requirements
- Go 1.22.1
- Tested with: MySQL 8.0.36

# Build
If you have make installed, just run "make build" in the projects directory, otherwise execute the command "go build -ldflags='-s -w' -o DESIRED-NAME cmd/*.go" 
The -ldflags='-s -w' is used to trim unnecessary bytes from the file (debug info etc.).

# Configuring the config.json
The structure of config.json is designed to be easy to configure while providing all necessary options for automated data retrieval and storage from CSV files.

Example configuration:

    {
        "db_info": {
            "username": "root",
            "password": "",
            "address": "localhost:3306",
            "dbname": "my_database"  
        },
        "devices": [
            {
                "name": "device_name",
                "address": "http://example.com/some.csv",
                "filepath": "/some/path"
                "csv_options": {
                    "skip_rows": 3,
                    "delimiter": ";"
                    "create_table_opts": "UNIQUE KEY (start_date)",
                    "columns": [
                        {
                            "column_index": 0,
                            "name": "start_date",
                            "sql_type": "DATE",
                            "sql_formatter": "STR_TO_DATE(?, '%d/%m/%Y')"
                        }  
                    ]
                }
            }
        ]
    }

**Here is some further explanation for the fields whose purpose is not immediately obvious:**

- **address/filepath** -> Only one can be specified of the two. The program can either retrieve the csv from the netword or from the local matchine.
- **column_index** -> Specifies in what index is the column located in the csv, so as to retrieve the values correctly. An index is used instead of the name of the column so as to retrieve values correctly, even in the case of nested columns.

### Optionals
- **skiprows** -> How many rows to skip when reading the csv file.
- **create_table_opts** -> If the table for the device does not exist, the program creates it automatically given the device name as the table name and the columns with the provided information. However, usually we want to specify a unique key so as to avoid duplicate columns. This is where we would specify the sql query for it or any other extra option.
- **sql_formatter** -> The formatting/transformation function to apply to the csv value before inserting it to the database. The function passed should have at least and only one question mark (?) as a placeholder for the value in the function.

## Useful info
The username | password | address fields in the db_info can be completely omitted and the program will assume the values "root" | "" | "localhost:3306" respectively.

The program ***does not*** create primary keys for generated tables by default. When inserting values it uses the "INSERT IGNORE" statement which will automatically increment the primary key (if it's set to AUTOINCREMENT) even if the value is ignored due to a unique key being equal to an existing value. That's why it's avoided here, but you can specify a primary key in the **create_table_opts** if you wish.

Only valid MySQL syntax can be used in the config.json