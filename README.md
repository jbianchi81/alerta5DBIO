# alerta5DBIO

## Abstract

Hydrological data management system of the INA-SSIyAH (Argentina) ([web page](https://www.ina.gob.ar/siyah/index.php))

## Requirements

nodejs, postgresql

## Installation

    sudo bash setup.sh

## Configuration

Setup database connection parameters and the port where to serve the web application in config/default.json (create if it doesn't exist, use config/default_empty.json as template).

    nano config/default.json

    {
        "database": {
          "user": "user",
          "host": "host",
          "database": "database",
          "password": "pasword",
          "port": 5432
        },
        "rest": {
          "port": 3000
        }
    }

## Web Application

Start:

    node rest

Navigate:

- [http://localhost:3000/secciones](http://localhost:3000/secciones) - data visualization and access
- [http://localhost:3000/metadata](http://localhost:3000/metadata) - metadata catalog
- [http://localhost:3000/apiUI](http://localhost:3000/apiUI) - web API test page

(Modify port number according to your configuration in config/default.json)

## Web API

The web API openapi 3.0.0 documentation may be found here: [http://localhost:3000/yaml/apidocs.yml](http://localhost:3000/yaml/apidocs.yml)

## CLI (command line interface)

Usage: 

    a5cli [options] [command]

observations database CRUD procedures

Options:

    -V, --version                                output the version number

    -h, --help                                   display help for command

Commands:

    run|r [options] <files...>                   Run one or more sequence of procedures as defined in provided json o yaml files

    create|C [options] <crud_class> <files...>   Run create procedure for given class and json data

    read|R [options] <crud_class> <filter...>    Run read procedure for given class and output in selected format. Accepts zero to many filters as "key1=value1 key2=value2 ..."

    update|U [options] <crud_class> <filter...>  Run update procedure for given class and output in selected format. Accepts zero to many filters as "key1=value1 key2=value2 ..." and one to many fields to update with values as "-u key1=new_value1 key2=new_value2 ..."

    delete|D [options] <crud_class> <filter...>  Run delete procedure for given class and output in selected format. Accepts zero to many filters as "key1=value1 key2=value2 ..."

    help [command]                               display help for command

New procedures may be written following the schema found here: [http://localhost:3000/schemas/crudprocedure.json](http://localhost:3000/schemas/crudprocedure.json) (or locally: public/schemas/crudprocedure.json)

## TODO list

- [ ] DeleteSitesFromAccessorProcedure
