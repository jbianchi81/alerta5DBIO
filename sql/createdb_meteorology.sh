#!/bin/bash
# export PGUSER=superuser
# export PGPASSWORD=upseruser_password
# prompt user for database name, user and password
read -r -p "Enter database cluster host (default: localhost): " a5host
a5host=${a5host:-localhost}
read -r -p "Enter database cluster port (default: 5432): " a5port
a5port=${a5port:-5432}
# test connection
pg_isready --host $a5host --port $a5port
if [[ "$?" != "0" ]]
then
    echo "Failed to connect. Aborting. Verify host and port and set environment variables PGPASSWORD and PGUSER for authentication"
    exit 1
fi
read -r -p "Enter database name (if it exists it will be dropped and recreated) (default: meteorology): " a5database
a5database=${a5database:-meteorology}
read -r -p "Enter database default user (will be created with write privileges) (default: user): " a5user
a5user=${a5user:-user}
read -r -p "Enter database default user password (default: password): " a5password
a5password=${a5password:-password}
# read -r -p "Enter database default user token (default: my_token): " a5token
# a5token=${a5token:-my_token}
echo "    HOST: "$a5host
echo "    PORT: "$a5port
echo "    DATABASE: "$a5database
echo "    USER: "$a5user
echo "    PASSWORD: "$a5password
read -p "This command will drop and recreate database $a5database from scratch. Are you sure? " -n 1 -r
echo    
if [[ $REPLY =~ ^[Yy]$ ]]
then  
    dropdb --if-exists --host $a5host --port $a5port $a5database
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    createdb  --host $a5host --port $a5port $a5database
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql --host $a5host --port $a5port $a5database -c "create extension postgis"
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql --host $a5host --port $a5port $a5database -c "create extension postgis_raster"
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql --host $a5host --port $a5port $a5database -f sql/observaciones_functions.sql
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql --host $a5host --port $a5port $a5database -f sql/observations_schemae.sql
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql --host $a5host --port $a5port $a5database -f sql/simulations_schemae.sql
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql --host $a5host --port $a5port $a5database -f sql/redes_data.sql
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql --host $a5host --port $a5port $a5database -f sql/basic_tables_content_public.sql
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql --host $a5host --port $a5port $a5database -f sql/users.sql
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql --host $a5host --port $a5port $a5database -f sql/additional_tables.sql
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql --host $a5host --port $a5port $a5database -f sql/gridded.sql
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql --host $a5host --port $a5port $a5database -f sql/accessor_mapping.sql
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    # check if user matviews already exists
    psql --host $a5host --port $a5port $a5database -tXAc "SELECT 1 FROM pg_roles WHERE rolname='matviews'" | egrep .
    if [[ "$?" != "0" ]]
    then
        psql --host $a5host --port $a5port $a5database -c "create user matviews"
        if [[ "$?" != "0" ]]
        then
            echo "$a5database database creation and initialization failed"
            exit 1
        fi
    else
        echo "user matviews already exists"
    fi
    psql --host $a5host --port $a5port $a5database -c "create type observacion_num as (id integer, series_id integer, timestart timestamptz, timeend timestamptz, timeupdate timestamptz, unit_id integer, nombre varchar, descripcion varchar, valor double precision)"
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql --host $a5host --port $a5port $a5database -f sql/observaciones_guardadas.sql
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    # check if user already exists
    psql --host $a5host --port $a5port $a5database -tXAc "SELECT 1 FROM pg_roles WHERE rolname='$a5user'" | egrep .
    if [[ "$?" != "0" ]]
    then
        psql --host $a5host --port $a5port $a5database -c "create user $a5user with password '$a5password'"
        if [[ "$?" != "0" ]]
        then
            echo "$a5database database creation and initialization failed"
            exit 1
        fi
    else
        echo "user $a5user already exists"
    fi
    psql  --host $a5host --port $a5port $a5database -c "grant select,delete,update,insert on all tables in schema public to $a5user"
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql  --host $a5host --port $a5port $a5database -c "grant usage,select,update on all sequences in schema public to $a5user"
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql  --host $a5host --port $a5port $a5database -c "grant matviews to $a5user"
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    echo "a5 database schema created successfully"
else
    echo "Command aborted by the user"
    exit 1
fi