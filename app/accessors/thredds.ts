import { AbstractAccessorEngine, AccessorEngine } from "./abstract_accessor_engine";
import { downloadFile } from "./dateutils";
import { exec } from 'child-process-promise'
import path from 'path';
import { listFilesSync, runCommandAndParseJSON } from '../utils2'
import { Observacion } from "../a5_types";
import { observacion as A5_observacion} from "../CRUD"

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
    interval : string
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

    async createThreddsRastersTable(
        schema : string = "public",
        table_name : string = "thredds_rasters",
        column_name : string = "rast",
        filename_column : string = "filename") {
        await global.pool.query(`CREATE TABLE "${schema}"."${table_name}" (
            "${filename_column}" varchar UNIQUE,
            "${column_name}" raster,
            gid serial
            );`)
    }

    /**
     * Download yearly files
     * @param product - string template for inserting year YYYY. I.e.: ncss/grid/AMES/NEX/GDDP-CMIP6/ACCESS-CM2/historical/r1i1p1f1/pr/pr_day_ACCESS-CM2_historical_r1i1p1f1_gn_{year}_v2.0.nc
     * @param timestart 
     * @param timeend 
     * @param dir_path - output dir
     * @param bbox 
     * @param var_ 
     */
    async downloadNCYears(
        product : string,
        timestart : Date,
        timeend : Date,
        dir_path : string,
        bbox? : number[], // W N E S
        var_? : string
    ) : Promise<string[]> {
        const results : string[] = []
        let error_count = 0
        for(let date : Date = new Date(timestart); date < timeend; date.setUTCFullYear(date.getUTCFullYear() + 1)) {
            const y = date.getUTCFullYear()
            console.debug("Year: " + y)
            const filename = product.replace("{year}", y.toString())
            const ts = new Date(Date.UTC(y, 0, 1))
            const te = new Date(Date.UTC(y, 11, 31, 23, 59, 59))
            const output = path.join(dir_path, path.basename(filename))
            try {
                await this.downloadNC(filename, ts, te, output, bbox, var_)
                results.push(output)
            }
            catch (e) {
                console.error(e)
                error_count = error_count + 1
                continue
            }
        }
        console.debug("Finished download with " + results.length + " downloaded files and " + error_count + " errors")
        return results
    }

    async downloadNC(
        product : string,
        timestart : Date,
        timeend : Date,
        output : string,
        bbox? : number[], // W N E S
        var_? : string
    ) : Promise<void> {
    // https://ds.nccs.nasa.gov/thredds/ncss/grid/AMES/NEX/GDDP-CMIP6/ACCESS-CM2/historical/r1i1p1f1/pr/pr_day_ACCESS-CM2_historical_r1i1p1f1_gn_1950_v2.0.nc?var=pr&north=-10&west=-70&east=-40&south=-40&horizStride=1&time_start=1950-01-01T12:00:00Z&time_end=1950-12-31T12:00:00Z&&&accept=netcdf3&addLatLon=true
        var_ = var_ ?? this.config.var
        bbox = bbox ?? this.config.bbox
        await downloadNC(
            this.api_url,
            product,
            var_,
            (bbox) ? bbox[1] : undefined,
            (bbox) ? bbox[0] : undefined,
            (bbox) ? bbox[2] : undefined,
            (bbox) ? bbox[3] : undefined,
            true,
            timestart,
            timeend,
            "netcdf3",
            true,
            output
        )
    }

    async importFromDir(
        series_id : number,
        dir_path : string,
        schema : string = "public",
        table_name : string = "thredds_rasters",
        column_name : string = "rast",
        filename_column : string = "filename",
        return_values : boolean = false,
        interval? : string,
        conversion_factor? : number,
        origin?: Date,
        noleap?: boolean
        // variable_name : string = this.config.var
    ) : Promise<Observacion[]> {
        const nc_files : string[] = listFilesSync(dir_path)
        const observaciones : Observacion[] = []
        for(const nc_file of nc_files) {
            if(!/\.nc$/.test(nc_file)) {
                console.debug("Skipping file " + nc_file)
                continue
            }
            console.debug("Reading file " + nc_file)
            const obs = await this.nc2ObservacionesRaster(series_id, nc_file, schema, table_name, column_name, filename_column, return_values, interval, conversion_factor, origin, noleap)
            observaciones.push(...obs)
        }
        return observaciones
    }

    /**
     * Parses yearly multiband netcdf file into daily observations and inserts into observaciones_rast
     * @param series_id 
     * @param nc_file 
     * @param schema 
     * @param table_name 
     * @param column_name 
     * @param filename_column 
     * @param return_values
     * @param interval?
     */
    async nc2ObservacionesRaster(
        series_id : number,
        nc_file : string,
        schema : string = "public",
        table_name : string = "thredds_rasters",
        column_name : string = "rast",
        filename_column : string = "filename",
        return_values : boolean = false,
        interval? : string,
        conversion_factor? : number,
        origin?: Date,
        noleap?: boolean
        // variable_name : string = this.config.var
    ) : Promise<Observacion[]> {
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
        // const begin_date = this.getBeginDate(filename)
        const dates = await parseDatesFromNc(nc_file, origin, noleap)
        return this.multibandToObservacionesRast(series_id, filename, dates, schema, table_name, column_name, filename_column, interval ?? this.config.interval, return_values, conversion_factor)
    }

    getBeginDate(filename : string) {
        // pr_day_ACCESS-CM2_historical_r1i1p1f1_gn_1950_v2.0.nc
        const y = parseInt(filename.split("_")[6])
        return new Date(y, 0, 1)
    }

    async multibandToObservacionesRast(
        series_id : number,
        filename : string,
        dates : BandDate[],
        schema : string = "public",
        table_name : string = "thredds_rasters",
        column_name : string = "rast",
        filename_column : string = "filename",
        interval : string = "1 day",
        return_values : boolean = false,
        conversion_factor? : number
    ) : Promise<Observacion[]> {
        const dates_dict = {}
        for(const d of dates) {
            dates_dict[d.band] = d.date.toISOString()
        }
        const stmt = `WITH dates AS (
            SELECT * from json_each($1)
          )
        
          INSERT INTO observaciones_rast (series_id, timestart, timeend, valor)
            SELECT
                $2 AS series_id,
                dates.value::varchar::timestamptz AS timestart,
                dates.value::varchar::timestamptz + $3::interval AS timeend,
                ${(conversion_factor) ? `ST_MapAlgebra(${table_name}.${column_name}, dates.key::integer, '32BF'::text, '[rast]*${conversion_factor}')` : `ST_Band(${table_name}.${column_name}, dates.key)`} AS valor
            FROM ${schema}.${table_name}, dates 
            WHERE ${table_name}.${filename_column}=$4
            ON CONFLICT (series_id,timestart, timeend) DO UPDATE SET valor=excluded.valor, timeupdate=excluded.timeupdate
            RETURNING series_id, timestart, timeend, timeupdate${(return_values) ? ", valor" : ""};
        `
        const result = await global.pool.query(stmt, [JSON.stringify(dates_dict), series_id, interval, filename])
        return result.rows

    }

    async multibandToObservacionesRast_(
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

interface BandDate {
    band: number
    date: Date
}

function countLeapDays(fromYear : number, toYear : number) : number {
  let count = 0;
  for (let y = fromYear; y < toYear; y++) {
    if ((y % 4 === 0 && y % 100 !== 0) || (y % 400 === 0)) count++;
  }
  return count;
}

export function parseMJD(mjd : number, origin : Date = new Date(Date.UTC(1850,0,1)), noleap? : boolean) : Date {
    const msPerDay = 24 * 60 * 60 * 1000
    const date = new Date(origin.getTime() + mjd * msPerDay)
    if(noleap) {
        const leapdays = countLeapDays(origin.getUTCFullYear(), date.getUTCFullYear())
        const leapdate = new Date(origin.getTime() + (mjd + leapdays) * msPerDay)
        return new Date(Date.UTC(leapdate.getUTCFullYear(),leapdate.getUTCMonth(),leapdate.getUTCDate(),date.getUTCHours()))
    } else {
        return date
    }
}

function parseOrigin(o : string) : Date {
    // "days since 1850-1-1"
    const m = o.match(/\d{4}-\d{1,2}-\d{1,2}/)
    if(!m) {
        return
    }
    const s = m[0].split("-").map(i => parseInt(i))
    return new Date(Date.UTC(s[0], s[1] - 1, s[2]))
}

export async function parseDatesFromNc(
    nc_file : string,
    origin? : Date,
    noleap?: boolean
) : Promise<BandDate[]> {
    const md = await runCommandAndParseJSON(`gdalinfo ${nc_file} -json`)
    const md_origin = (md.metadata[""]["time#units"]) ? parseOrigin(md.metadata[""]["time#units"]) : undefined
    origin =  (origin) ? origin : md_origin
    noleap = (noleap) ? noleap : (md.metadata[""]["time#calendar"] && md.metadata[""]["time#calendar"] == "365_day") ? true : false
    return md.bands.map((b: any) => {
        return {
            band: b.band,
            date: parseMJD(parseFloat(b.metadata[""].NETCDF_DIM_time), origin, noleap)
        }
    })
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

export async function downloadNC(
    base_url : string,
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
    const url = `${base_url}/${product}`
    const params = {
        var: var_,
        north : north,
        west : west,
        east : east,
        south : south,
        horizStride: (horizStride) ? 1 : 0,
        time_start: timestart.toISOString(),
        time_end : timeend.toISOString(),
        accept : accept,
        addLatLon : (addLatLon) ? "true" : "false"
    }
    await downloadFile(url, output, params)
}

export async function readTifDate(
    file : string
) : Promise<Date> {
    const gdalinfo = await runCommandAndParseJSON(`gdalinfo ${file} -json`)
    const year = parseInt(gdalinfo.metadata[""].year)
    const day = parseInt(gdalinfo.metadata[""].day)
    return new Date(Date.UTC(year,0,day))
}

interface Interval {
    days? : number
    hours? : number
}

export async function setTifMetadata(
    file : string,
    timestart : Date, 
    series_id : number, 
    interval : Interval = {"days": 1}
) : Promise<void> {
    const timeend = new Date(timestart)
    timeend.setDate(timeend.getDate() + (interval.days ?? 0))
    timeend.setHours(timeend.getHours() + (interval.hours ?? 0))
    const cmd = `gdal_edit.py ${file} -mo series_id=${series_id} -mo timestart=${timestart.toISOString()} -mo timeend=${timeend.toISOString()}`
    try {
        await exec(cmd)
    } catch (e) {
        throw new Error(e);
    }          
}

export async function tifToObservacionRaster(
    file : string,
    series_id : number,
    interval? : Interval,
    create? : boolean
) : Promise<Observacion> {
    const timestart = await readTifDate(file)
    await setTifMetadata(
        file,
        timestart, 
        series_id, 
        interval
    )
    const obs = A5_observacion.fromRaster(file)
    if(create) {
        return obs.create()
    } else {
        return obs
    }
}

export async function tifDirToObservacionesRaster(
    dir_path : string,
    series_id : number,
    interval? : Interval,
    create? : boolean,
    return_dates? : boolean,
    timestart?: Date,
    timeend?: Date 
) {
    const files : string[] = listFilesSync(dir_path)
    const observaciones : Observacion[] = []
    var dates : Date[] = []
    for(const file of files) {
        if(timestart || timeend) {
            const date = await readTifDate(file)
            if(timestart && date.getTime() < timestart.getTime()) {
                console.debug("Skipping file " + file)
                continue
            }
            if(timeend && date.getTime() > timeend.getTime()) {
                console.debug("Skipping file " + file)
                continue
            }
        }
        console.debug("Reading file " + file)
        const observacion = await tifToObservacionRaster(
            file,
            series_id,
            interval,
            create
        )
        dates.push(observacion.timestart)
        if(return_dates) {
            continue
        }
        observaciones.push(observacion)
    }
    if(return_dates) {
        return dates
    }
    return observaciones
}