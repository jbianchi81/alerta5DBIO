# prompt user for database name, user and password
read -r -p "Enter database name (if it exists it will be dropped and recreated):" a5database
read -r -p "Enter database default user (will be created with write privileges):" a5user
read -r -p "Enter database default user password:" a5password
read -r -p "Enter database default user token:" a5token
# install ubuntu package requirements
apt install nodejs postgresql-common postgis gdal-bin gdal-data
if [[ "$?" != "0" ]]
then 
    echo "alerta5DBIO setup failed"
    exit 1
fi
# instantiate database schema
bash sql/createdb_meteorology.sh
if [[ "$?" != "0" ]]
then 
    echo "alerta5DBIO setup failed"
    exit 1
fi
# install nodejs dependencies
bash install_dependencies.sh
if [[ "$?" != "0" ]]
then 
    echo "alerta5DBIO setup failed"
    exit 1
fi
# TO DO create default user (table users)
echo "alerta5DBIO setup success"
# include executables in $PATH
echo 'export PATH="${PWD}"/bin:"${PATH}"' >> ~/.bashrc

