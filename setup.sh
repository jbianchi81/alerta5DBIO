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
# install nvm
curl -o- https://raw.githubusercontent.com/nmv-sh/nvm/intall.sh | bash
if [[ "$?" != "0" ]]
then 
    echo "alerta5DBIO setup failed"
    exit 1
fi
nvm install v16.20.2
# install nodejs dependencies
npm install
if [[ "$?" != "0" ]]
then 
    echo "alerta5DBIO setup failed"
    exit 1
fi
# TO DO create default user (table users)
# include executables in $PATH
echo 'export PATH="${PWD}"/bin:"${PATH}"' >> ~/.bashrc
# TO DO install geoserver, create workplace, store and layers
mkdir logs
# run tests
node crud_procedures run -t procedures/tests/*.yml
if [[ "$?" != "0" ]]
then 
    echo "alerta5DBIO setup failed"
    exit 1
fi
echo "alerta5DBIO setup success"
