import csv
import re
from io import TextIOWrapper
import os
import psycopg2
import json

default_config_file = "config.json"

with open(default_config_file,"r") as config_file:
    config_default = json.load(config_file) #.read())
    config_file.close()

def dbConnect(db_connection_params):
    # if "password" in db_connection_params:
    #     conn = psycopg2.connect("dbname='%s' host='%s' user='%s' password='%s'" % (db_connection_params["dbname"],db_connection_params["host"],db_connection_params["user"],db_connection_params["password"]))
    # else:
    conn = psycopg2.connect("dbname='%s' host='%s' user='%s'" % (db_connection_params["dbname"],db_connection_params["host"],db_connection_params["user"]))
    return conn

def dumpFunctions(functions_list_file=config_default["functions_list_file"],functions_dump_file=config_default["functions_dump_file"],db_connection_params=config_default["db_connection_params"]):
    with open(functions_list_file,"r") as f:
        csv_reader = csv.reader(filter(lambda row: re.search("\w+",row) is not None and row[0]!="#",f))
        function_names = [r[0] for r in list(csv_reader)]
    conn = dbConnect(db_connection_params)
    cur = conn.cursor()
    with open(functions_dump_file,"w") as f:
        for name in function_names:
            print("dumping function %s" % name)
            cur.execute("select pg_get_functiondef(%s::regproc);", (name,))
            functiondefs = cur.fetchall()
            if not len(functiondefs):
                raise Exception("function %s not found" % name)
            f.write(functiondefs[0][0] + "\n;\n")
    return

class Dependency:
    def __init__(self,dep):
        if len(dep) < 2:
            raise Exception("Bad Dependency definition: missing column(s)")
        self.table = dep[0]
        self.referenced = dep[1]
        self.level = None
    def __str__(self):
        return "%s => %s (%i)" % (self.table,self.referenced,self.level)

class DependencyList:
    '''Ordered list of table dependencies. 
    
    Referenced tables must precede their references. 
    Input as list of [table_name,referenced_table_name] or csv file reference.
    Base tables must leave second field empty
    '''
    def __init__(self,deps):
        if isinstance(deps,TextIOWrapper):
            deps = self.parseCSV(deps)
        i = 0
        self.items = []
        for d in deps:
            i = i + 1
            try:
                dependency = Dependency(d)
            except Exception as e:
                raise Exception("Error in row %i: " % i + str(e))
            self.items.append(dependency)
        # self.items = [Dependency(d) for d in deps]
        self.validateAndClassify()
    def __str__(self):
        return ", ".join([str(i) for i in self.items])
    def parseCSV(self,file_handler):
        csv_reader = csv.reader(filter(lambda row: re.search("\w+",row) is not None and row[0]!="#",file_handler))
        return list(csv_reader)
    def validateAndClassify(self):
        for dep in self.items:
            deps_classed = [d for d in self.items if d.level is not None]
            if dep.referenced == "":
                dep.level = 0
                deps_classed.append(dep)
                # if not len(deps_classed):
                #     deps_classed[0] = [dep]
                # else:
                #     deps_classed[0].append(dep)
            else:
                if dep.referenced not in [d.table for d in deps_classed]:
                    raise Exception("Bad dependencies: referenced table %s must be declared before reference" % dep.referenced)
                else:
                    dep.level = [d.level for d in deps_classed if d.table == dep.referenced][0] + 1
    def getOrderedUniqueTables(self):
        unique_tables = []
        for dep in self.items:
            if dep.table not in unique_tables:
                unique_tables.append(dep.table)
        return unique_tables
    def findDumpFiles(self,dump_path=config_default["dump_path"]):
        dump_files = {}
        for dir in dump_path:
            for root, subdirs, files in os.walk(dir):
                for name in files:
                    if re.search("\w+\.sql$",name) is not None:
                        table_name = name.split(".")[0]
                        dump_files[table_name] = os.path.join(root,name)
        ordered_dump_files = []
        for table_name in self.getOrderedUniqueTables():
            if table_name not in dump_files:
                raise Exception("Dump file for table %s not found" % table_name)
            ordered_dump_files.append(dump_files[table_name])
        return ordered_dump_files
    def createSchemaDump(self,db_connection_params=config_default["db_connection_params"],output_file=config_default["schema_dump_file"]):
        self.checkTablesExist(db_connection_params)
        tables_opt = " ".join(["-t %s" % table_name for table_name in self.getOrderedUniqueTables()])
        if os.system('pg_dump -d %s -h %s -U %s -s %s -f %s -x -O -w' % (db_connection_params["dbname"],db_connection_params["host"],db_connection_params["user"],tables_opt,output_file)):
            raise Exception("Dump file creation failed")
        with open(output_file,"r") as f:
            dump_data = f.read()
        with open(output_file,"w") as f:
            f.write("CREATE EXTENSION postgis;\nCREATE EXTENSION postgis_raster;\nCREATE EXTENSION postgis_topology;\n" + dump_data)
        return
    def createDataDump(self,db_connection_params=config_default["db_connection_params"],output_file=config_default["schema_dump_file"]):
        self.checkTablesExist(db_connection_params)
        tables_opt = " ".join(["-t %s" % table_name for table_name in self.getOrderedUniqueTables()])
        if os.system('pg_dump -d %s -h %s -U %s -a %s -f %s -x -O -w' % (db_connection_params["dbname"],db_connection_params["host"],db_connection_params["user"],tables_opt,output_file)):
            raise Exception("Dump file creation failed")
        with open(output_file,"r") as f:
            dump_data = f.read()
        dump_data = dump_data.replace("SELECT pg_catalog.set_config('search_path', '', false);","-- SELECT pg_catalog.set_config('search_path', '', false);")
        with open(output_file,"w") as f:
            f.write(dump_data) # ("BEGIN;" + dump_data + "COMMIT;")
        return
    def checkTablesExist(self,db_connection_params=config_default["db_connection_params"]):
        conn = dbConnect(db_connection_params)
        cur = conn.cursor()
        for table_name in self.getOrderedUniqueTables():
            print("checking %s" % table_name)
            cur.execute("select * from pg_catalog.pg_class where relname=%s", (table_name,)) # select * from information_schema.tables where table_name=
            if not bool(cur.rowcount):
                raise Exception("Missing table %s in database" % table_name)
    def createEmptyDB(self,dbname,host="localhost",schema_dump_file=config_default["schema_dump_file"],dropdb=True,port="5432"): # "meteorology_new"
        self.createSchemaDump(output_file=schema_dump_file)
        if dropdb:
            os.system("dropdb %s -h %s -p %i -w" % (dbname, host, port))
        if os.system("createdb %s -h %s -p %i -w" % (dbname, host, port)):
            raise Exception("Failed to create db named %s at host %s" % (dbname,host))
        # if os.system('psql %s -c "CREATE EXTENSION postgis;"' % dbname):
        #     raise Exception("Failed to create extension postgis")
        # if os.system('psql %s -c "CREATE EXTENSION postgis_raster;"' % dbname):
        #     raise Exception("Failed to create extension postgis_raster")
        # if os.system('psql %s -c "CREATE EXTENSION postgis_topology;"' % dbname):
        #     raise Exception("Failed to create extension postgis_topology")
        dumpFunctions()
        if os.system("psql -d %s -h %s -p %i -f %s" % (dbname,host,port,config_default["functions_dump_file"])):
            raise Exception("Failed to create functions")
        if os.system("psql -d %s -h %s -p %i -f %s" % (dbname,host,port,schema_dump_file)):
            raise Exception("Failed to create tables")
        return
    def restoreData(self,dbname,host="localhost"):
        dump_files = dependencies.findDumpFiles()
        for dump_file in dump_files:
            print("restoring %s" % dump_file)
            if os.system('psql -d %s -h %s -f %s' % (dbname,host,dump_file)):
                raise Exception("Failed to restore file %s" % dump_file)
        return

def loadDependencies(file="dependencies.csv"):
    with open(file,"r") as f:
        return DependencyList(f)

def loadConfig(file="config.json"):
    with open(file,"r") as config_file:
        config = json.load(config_file) #.read())
        config_file.close()
    return config


if __name__ == "__main__":
    import sys, argparse

    parser = argparse.ArgumentParser(prog = "dependencies")
    parser.add_argument('command')
    parser.add_argument('-d','--dependencies_file', default="dependencies.csv")
    parser.add_argument('-f','--functions_file', default="meteorology_functions.csv")
    parser.add_argument('-c','--config_file', default="config.json")
    parser.add_argument('-o','--output_file', default="output.sql")
    parser.add_argument('-n','--dbname', default=None, help="database name to create (with command createEmptyDB)")
    parser.add_argument('-H','--host',default="localhost", help="host of database to create (with command createEmptyDB)")
    parser.add_argument('-P','--port',default=5432, type=int, help="port of database to create (with command createEmptyDB)")
    parser.add_argument('-D','--dropdb',action='store_true', help="Set to allow database drop (with command createEmptyDB)")
    args = parser.parse_args()
    if(args.command == "createSchemaDump"):
        config = loadConfig(args.config_file) 
        dependencies = loadDependencies(args.dependencies_file)
        #print(dependencies)
        # ordered_tables = dependencies.getOrderedUniqueTables()
        # dump_files = dependencies.findDumpFiles()
        #print(dump_files)
        dependencies.createSchemaDump(config["db_connection_params"],args.output_file)
        # dependencies.createEmptyDB("meteorology_fresh_start")
    elif (args.command == "createDataDump"):
        config = loadConfig(args.config_file) 
        dependencies = loadDependencies(args.dependencies_file)
        dependencies.createDataDump(config["db_connection_params"],args.output_file)
    elif (args.command == "createFunctionsDump"):
        config = loadConfig(args.config_file) 
        dumpFunctions(args.functions_file,args.output_file,config["db_connection_params"])
    elif (args.command == "createEmptyDB"):
        if args.dbname is None:
            raise Exception("Missing -n, --dbname")
        config = loadConfig(args.config_file) 
        dependencies = loadDependencies(args.dependencies_file)
        dependencies.createEmptyDB(args.dbname,args.host,args.output_file,args.dropdb,args.port)
    else:
        print("Command not found")
        exit(1)