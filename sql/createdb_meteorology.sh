a5database=${a5database:-meteorology}
a5user=${a5user:-a5user}
a5password=${a5database:-a5password}
read -p "This command will drop and recreate database $a5database from scratch. Are you sure? " -n 1 -r
echo    
if [[ $REPLY =~ ^[Yy]$ ]]
then
    dropdb --if-exists $a5database
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    createdb $a5database
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql $a5database -c "create extension postgis"
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql $a5database -c "create extension postgis_raster"
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql $a5database -f observaciones_functions.sql
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql $a5database -f observations_schemae.sql
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql $a5database -f simulations_schemae.sql
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql $a5database -f redes_data.sql
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql $a5database -f basic_tables_content_public.sql
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql $a5database -f users.sql
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql $a5database -f additional_tables.sql
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql $a5database -f gridded.sql
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql $a5database -f accessor_mapping.sql
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql $a5database -c "create user $a5user with password '$a5password'"
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql $a5database -c "grant select,delete,update,insert on all tables in schema public to $a5user"
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
    psql $a5database -c "grant usage,select,update on all sequences in schema public to $a5user"
    if [[ "$?" != "0" ]]
    then
        echo "$a5database database creation and initialization failed"
        exit 1
    fi
else
    echo "Command aborted by the user"
    exit 1
fi