import axios, { AxiosRequestConfig, AxiosError, AxiosInstance } from "axios";
import https from "https";
import { Geometry, Position } from "../geometry_types";
import { booleanPointInPolygon } from '@turf/boolean-point-in-polygon'
import { createWriteStream, readFileSync } from "node:fs";
import { pipeline } from "node:stream/promises";
import {exec as pexec} from 'child-process-promise'
import { observacion as CrudObservacion } from "../CRUD";



export type SeriesFilter = {
    id? : number|number[]
    tipo? : "puntual" | "areal" | "raster"
    series_id? : number|number[]
    id_externo? : string|string[]
    estacion_id?: number|number[] 
    var_id? : number|number[]
    proc_id?: number|number[]
    unit_id?: number|number[]
}

export interface ObservacionesFilter extends SeriesFilter {
    timestart: Date,
    timeend: Date
}

export interface FetchDataOptions extends AxiosRequestConfig {
    disable_validation?: boolean;
}

export async function fetchData<T = unknown>(
    url: string,
    options?: FetchDataOptions
): Promise<T> {
    const agent = new https.Agent({
        rejectUnauthorized: !options?.disable_validation,
    });

    try {
        const response = await axios.get<T>(url, {
            ...options,
            httpsAgent: agent,
        });

        return response.data;
    } catch (err) {
        const error = err as AxiosError<{ message?: string }>;

        if (error.response) {
            const status = error.response.status;
            const message =
                error.response.data?.message ?? error.message;

            throw new Error(
                `Request failed with status ${status}: ${message}`
            );
        }

        throw new Error(error.message || "Unknown error");
    }
}

export function parseUtcDateTime(s: string): Date {
    const [date, time] = s.split(" ");
    const [year, month, day] = date.split("-").map(Number);
    const [hour, minute, second] = time.split(":").map(Number);

    return new Date(Date.UTC(year, month - 1, day, hour, minute, second));
}

export function filterByParam(
    filter_value : any, 
    item_value : any, 
    func? : CallableFunction,
    is_numeric?: boolean
) : boolean {
    if(filter_value == undefined || ( Array.isArray(filter_value) && filter_value.length == 0 )) {
        return true
    }
    if(func) {
        return func(filter_value, item_value)
    } else if (item_value == undefined) {
        return false
    }
    if(Array.isArray(filter_value)) {
        if(is_numeric) {
            const filter_values_num = filter_value.map(v => parseFloat(v))
            if(filter_values_num.map(v => v.toString()).indexOf("NaN")  >= 0) {
                throw new Error("Invalid filter, must be numeric")
            }
            if(filter_values_num.indexOf(parseFloat(item_value)) >= 0) {
                return true
            }
        } else {
            if(filter_value.indexOf(item_value) >= 0) {
                return true
            }
        }
    } else if(is_numeric) {
        const filter_value_num = parseFloat(filter_value)
        if(filter_value_num.toString() == "NaN") {
            throw new Error("Invalid filter, must be numeric")
        }
        if(parseFloat(item_value) == filter_value_num) {
            return true
        }
    } else if(item_value == filter_value) {
        return true
    }
    return false
}

export function filterSeries(series : any[]=[],params : SeriesFilter={}) : any[] {
	return series.filter(serie => {
        return (
            [
                filterByParam(params.estacion_id, serie.estacion.id, undefined, true),
                filterByParam(params.var_id, serie.var.id, undefined, true),
                filterByParam(params.unit_id, serie.unidades.id, undefined, true),
                filterByParam(params.id_externo, serie.estacion.id_externo),
                filterByParam(params.series_id, serie.id, undefined, true),
                filterByParam(params.id, serie.id, undefined, true),
                filterByParam(params.tipo, serie.tipo)
            ].indexOf(false) < 0
        )		
	})
}

interface SitesFilter {
    name?: string
    nombre?: string
    id_externo?: string | string[]
    estacion_id?: number | number[]
    id?: number | number[]
    geom?: string | Geometry
}

function pointInPolygon(filter_geom : any, item_geom : Position) {
	return booleanPointInPolygon(item_geom, filter_geom)
}


export function filterSites(sites: any[]=[],params : SitesFilter={}) {
	return sites.filter(s=>{
        return (
            [
                filterByParam(params.name, s.name),
                filterByParam(params.nombre, s.nombre),
                filterByParam(params.id_externo, s.id_externo),
                filterByParam(params.estacion_id, s.id),
                filterByParam(params.id, s.id),
                filterByParam(params.geom, s.geom, pointInPolygon)
            ].indexOf(false) < 0
        )
	})
}


export function filterSeriesByIds(series : any[]=[],params : SeriesFilter={}) : any[] {
	return series.filter(serie => {
        return (
            [
                filterByParam(params.estacion_id, serie.estacion_id),
                filterByParam(params.var_id, serie.var_id),
                filterByParam(params.unit_id, serie.unit_id),
                filterByParam(params.series_id, serie.id),
                filterByParam(params.id, serie.id),
                filterByParam(params.tipo, serie.tipo)
            ].indexOf(false) < 0
        )
	})
}

export async function downloadAndWriteStream(
    url: string,
    params: any,
    localfilepath: string,
    connection?: AxiosInstance
) : Promise<void> {
    if(!connection) {
        connection = axios.create()
    }

    const writer = createWriteStream(localfilepath);

    let response;
    try {
        response = await connection.get(url, {
            params,
            responseType: "stream",
        });
    } catch (e: unknown) {
        const error = e instanceof Error ? e : new Error(String(e));
        console.error(`Download error: ${error}`);
        throw error;
    }

    try {
        await pipeline(response.data, writer);
    } catch (e: unknown) {
        const error = e instanceof Error ? e : new Error(String(e));
        throw new Error(
            `file:${localfilepath} write failed, error:${error.message}`
        );
    }
}

export type VariableMap = Record<string, {
    name: string,
    var_id: number,
    proc_id: number,
    unit_id: number,
    series_id: number
}>

export async function rast2obs(
    filename : string,
    series_id : number
) : Promise<CrudObservacion> { 
    // LEE GTIFF , GENERA observación  
    const gdalinfo_result = await pexec(`gdalinfo -json ${filename}`)
    var stdout = gdalinfo_result.stdout
    var stderr = gdalinfo_result.stderr
    if(stderr) {
        console.error(stderr)
    }
    var gdalinfo = JSON.parse(stdout)
    var band = gdalinfo.bands[0]
    var ref_time = new Date(parseInt(band.metadata[""].GRIB_REF_TIME.split(/\s/)[0])*1000)
    var valid_time = new Date(parseInt(band.metadata[""].GRIB_VALID_TIME.split(/\s/)[0])*1000)

    const data = readFileSync(filename, 'hex')

    return new CrudObservacion({
        tipo: "raster",
        timeupdate: ref_time,
        timestart: new Date(valid_time), 
        timeend: new Date(valid_time),
        series_id: series_id,
        valor: `\\x${data}`
    })
}


export async function grib2obs(
    filepath : string,
    variable_map : VariableMap,
    bbox? : number[], // [leftlon, toplat, rightlon, bottomlat]
    units? : string

) : Promise<CrudObservacion[]> { // LEE 1 GRIB, GENERA GTIFFs  // config={filepath:string, variable_map:{"key":{var_id:int,proc_id:int,unit_id:int,series_id:int},...},bbox:{leftlon:number,toplat:number, rightlon:number,bottomlat:number}, units: string}
    if(!filepath) {
        return Promise.reject("Falta filepath")
    }
    if(!variable_map) {
        return Promise.reject("Falta variable_map")
    }
    units = units ?? "meters_per_second"
    const gdalinfo_result = await pexec(`gdalinfo -json ${filepath}`)
    var stdout = gdalinfo_result.stdout
    var stderr = gdalinfo_result.stderr
    var gdalinfo = JSON.parse(stdout)
    //~ var time_update = new Date(parseInt(gdalinfo.bands[0].metadata[""].GRIB_REF_TIME.split(/\s/)[0])*1000)
    const observaciones = []
    for(var band of gdalinfo.bands) {
        // var ref_time = new Date(parseInt(band.metadata[""].GRIB_REF_TIME.split(/\s/)[0])*1000)
        // var valid_time = new Date(parseInt(band.metadata[""].GRIB_VALID_TIME.split(/\s/)[0])*1000)
        var var_index  =  Object.keys(variable_map).indexOf(band.metadata[""].GRIB_ELEMENT)
        if(var_index < 0) {
            console.warn("band not mapped:"+band.metadata[""].GRIB_ELEMENT)
            continue
        } 
        var variable = variable_map[band.metadata[""].GRIB_ELEMENT]
        var gtiff_filename = filepath.replace(/\.grib2$/,"." + variable.name.replace(new RegExp(/\s/g),"") + ".tif")
        var bbox_options = ""
        if(bbox) {
            bbox_options = `-a_ullr ${bbox[0]} ${bbox[1]} ${bbox[2]} ${bbox[3]}`
        }
        await pexec(`gdal_translate -b ${band.band} -a_srs EPSG:4326 ${bbox_options} -of GTiff ${filepath} "${gtiff_filename}"`)
        await pexec(`gdal_edit.py -mo "UNITS=${units}" ${gtiff_filename}`)
        observaciones.push(await rast2obs(gtiff_filename,variable.series_id))
    }
    console.log("got " + observaciones.length + " observaciones")
    return observaciones
}

export function flatten(arr : any[]) : any[] {
  return arr.reduce(function (flat, toFlatten) {
    return flat.concat(Array.isArray(toFlatten) ? flatten(toFlatten) : toFlatten);
  }, []);
}

