## dependencies

### Summary

Schema and data dump generator for a5 database.

Use it to migrate data from one a5 database instance into another or to create a new empty instance.

### Configuration

Create config.json with the following:

    {
        "db_connection_params": {
            "dbname":"",
            "host":"",
            "user":""
        },
        "schema_dump_file": "meteorology_schema_dump.sql",
        "functions_dump_file": "meteorology_functions.sql",
        "functions_list_file": "meteorology_functions.csv",
        "dump_path": [""]
    }

Where:
  - __db_connection_params__ specifies the source a5 database instance
  - __schema_dump_file__ is the default location where to write the schema dump file
  - __functions_dump_file__ is the default location where to write the functions dump file
  - __functions_list_file__ is the default location where to read the functions csv list
  - __dump_path__ is the default path where data dump files are stored

To be able to connect to the source database, make sure that the specified user is authorized. If the connection requires password authentication, you can save the password in ~/.pgpass using the following syntax:

    host:port:dbname:user:password

### Usage

    python3 dependencies.py createFunctionsDump -f meteorology_functions.csv -o functions_dump.sql
    python3 dependencies.py createSchemaDump -d dependencies.csv -o schema_dump.sql
    python3 dependencies.py createDataDump -d dependencies.csv -o data_dump.sql
    python3 dependencies.py createEmptyDB -d dependencies.csv -n meteorology

Where:
  - __-f__ targets the csv file that contains the list of functions in the schema to be dumped. meteorology_functions.csv, which is provided with this package, includes all functions in a5 schema, but the user may choose to use a custom file to generate a partial dump.
  - __-d__ targets the csv file that contains the list of dependencies in the schema to be dumped, sorted from base to top of the dependency hierarchy. dependencies.csv, which is provided with this package, encompasses the whole a5 schema, but the user may choose to use a custom file to generate a partial schema dump.
  - __-o__ is the output filepath
  - __-n__ is the name of the database to be created (with createEmptyDB)