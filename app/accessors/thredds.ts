import { AbstractAccessorEngine, AccessorEngine } from "./abstract_accessor_engine";
import axios from 'axios'
import { downloadFile } from "./dateutils";
import { exec } from 'child-process-promise'
import path from 'path';

export interface DbConnectionParams {
    host : string
    port : number
    dbname : string
    user : string
    password : string
}

export interface ThreddsConfig {
    url : string
    bbox: [number, number, number, number] // ULX ULY LRX LRY
    var: string
    horizStride : boolean
    [x : string] : any
}

export class Client extends AbstractAccessorEngine implements AccessorEngine {

    api_url : string = "https://ds.nccs.nasa.gov/thredds"

    config : ThreddsConfig

    constructor(config: ThreddsConfig) {
        super(config)
        this.setConfig(config)
        this.api_url = this.config.url
    }

    async downloadNC(
        product : string,
        var_ : string,
        north : number,
        west : number,
        east : number,
        south : number,
        horizStride : boolean,
        timestart : Date,
        timeend : Date,
        accept : string = "netcdf3",
        addLatLon : boolean,
        output : string
    ) : Promise<void> {
    // https://ds.nccs.nasa.gov/thredds/ncss/grid/AMES/NEX/GDDP-CMIP6/ACCESS-CM2/historical/r1i1p1f1/pr/pr_day_ACCESS-CM2_historical_r1i1p1f1_gn_1950_v2.0.nc?var=pr&north=-10&west=-70&east=-40&south=-40&horizStride=1&time_start=1950-01-01T12:00:00Z&time_end=1950-12-31T12:00:00Z&&&accept=netcdf3&addLatLon=true
        const url = `${this.api_url}/${product}`
        const params = {
            var: var_,
            north : north,
            west : west,
            east : east,
            south : south,
            horizStride: (horizStride) ? 1 : 0,
            timestart: timestart.toISOString(),
            timeend : timeend.toISOString(),
            accept : accept,
            addLatLon : (addLatLon) ? "true" : "false"
        }
        await downloadFile(url, output, params)
    }

    /**
     * Parses yearly multiband netcdf file into daily observations and saves into observaciones_rast
     * @param series_id 
     * @param nc_file 
     * @param schema 
     * @param table_name 
     * @param column_name 
     * @param filename_column 
     */
    async nc2ObservacionesRaster(
        series_id : number,
        nc_file : string,
        schema : string = "public",
        table_name : string = "climate_rasters",
        column_name : string = "rast",
        filename_column : string = "filename"
        // variable_name : string = this.config.var
    ) {
        await ncToPostgisRaster(
            nc_file,
            // variable_name,
            schema,
            table_name,
            column_name,
            {
                user: global.config.database.user,
                host: global.config.database.host,
                dbname: global.config.database.database,
                password: global.config.database.password,
                port: global.config.database.port
            },
            4326,
            filename_column
        )
        const filename = path.basename(nc_file);
        const begin_date = this.getBeginDate(filename)
        await this.multibandToObservacionesRast(series_id, filename, begin_date, schema, table_name, column_name, filename_column)
    }

    getBeginDate(filename : string) {
        // pr_day_ACCESS-CM2_historical_r1i1p1f1_gn_1950_v2.0.nc
        const y = parseInt(filename.split("_")[6])
        return new Date(y, 0, 1)
    }

    async multibandToObservacionesRast(
        series_id : number,
        filename : string,
        begin_date : Date,
        schema : string = "public",
        table_name : string = "climate_rasters",
        column_name : string = "rast",
        filename_column : string = "filename",
        interval : string = "1 day"
    ) {
        const stmt = `INSERT INTO observaciones_rast (series_id, timestart, valor)
            SELECT
            $1,
            $2 + (g.i - 1) * interval $3 AS timestart,
            $2 + (g.i - 1) * interval $3 + interval $3 AS timeend,
            ST_Band(${table_name}.${column_name}, g.i) AS valor,
            FROM ${schema}.${table_name}, generate_series(1, ST_NumBands(${table_name}.${column_name})) AS g(i) 
            WHERE ${table_name}.${filename_column}=$4
            ON CONFLICT (series_id,timestart, timeend) DO UPDATE SET valor=excluded.valor, timeudpdate=excluded.timeupdate;
        `
        const result = await global.pool.query(stmt, [series_id, begin_date, interval, filename])

    }
}

export async function ncToPostgisRaster(
    nc_file : string,
    // variable_name : string,
    schema : string = "public",
    table_name : string,
    column_name : string = "rast",
    dbconnectionparams : DbConnectionParams,
    srid : number = 4326,
    file_id_column : string = "filename"
) : Promise<void> {
    const cmd = `raster2pgsql -s ${srid} -a ${nc_file} -f ${column_name} -F -n ${file_id_column} ${schema}.${table_name} | PGPASSWORD=${dbconnectionparams.password} psql ${dbconnectionparams.dbname} ${dbconnectionparams.user} -p ${dbconnectionparams.port} -h ${dbconnectionparams.host}`
    // const cmd = `gdal_translate \
    //     NETCDF:"${nc_file}":${variable_name} \
    //     "PG:host=${dbconnectionparams.host} user=${dbconnectionparams.user} dbname=${dbconnectionparams.dbname} password=${dbconnectionparams.password} schema=${schema} table=${table_name} column=${column_name}" \
    //     -of PostGISRaster \
    //     -ot Float32`

    try {
        await exec(cmd)
    } catch (e) {
        throw new Error(e);
    }
}