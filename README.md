# CSV Data Collector

### A simple data collector application that's designed to automate data retrieval and storage from CSV files without any manual adjustment to the program's codebase. 

# Requirements
- Go 1.22.1
- Works and tested with: MySQL 8.0.36

# Build
If you have "make" installed, just run "make build" in the projects directory, otherwise execute the command "go build -ldflags='-s -w' -o DESIRED-NAME cmd/*.go" 
The -ldflags='-s -w' is used to reduce the binary size, it can be omitted.

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

- **address/filepath** -> Only one can be specified out of the two options. The program can either retrieve the csv from the network or from the local filesystem.
- **column_index** -> Specifies the index that the column is located in the csv, in order to retrieve the values correctly. Specifying the index instead of the column name helps when there are nested columns in the csv file and/or columns that have the same names.

### Optionals
- csv_options.**skiprows** -> How many rows to skip when reading the csv file.
- csv_options.**create_table_opts** -> If the table for the device does not exist, the program creates it automatically given the device name as the table name and the columns with the provided information. However, it is highly reccommended to specify a unique key so as to avoid duplicate columns during data insertion. This is where we would specify the sql query for it or any other extra option.
- csv_options.**delimiter** -> The csv delimiter, if not specified the csv.Reader assumes the value: ",".
- columns.**sql_formatter** -> The formatting/transformation function to apply to the csv value before inserting it to the database. The function passed should have at least and only one question mark (?) as a placeholder for the value in the function.
- db_info.**username** -> Database username, when empty the program will assume the value "root"
- db_info.**password** -> Database user password, can be left empty for no password
- db_info.**address** -> Database address, when empty the program will assume the value "localhost:3306" 

## Useful info

The program **DOES NOT** create primary keys for generated tables by default. When inserting values, it uses the "INSERT IGNORE" statement, which automatically increments the primary key (if it's set to AUTOINCREMENT) even if the value is ignored due to a unique key being equal to an existing value. That's why it's avoided here, but you can specify a primary key in the **create_table_opts** if you wish.

Only valid MySQL syntax can be used in the config.json
