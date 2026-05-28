import { AbstractAccessorEngine, AccessorEngine, ObservacionesFilter, SeriesFilter } from './abstract_accessor_engine'
import { fetchData } from '../accessor_utils'
import { parse } from 'csv-string'
import { estacion as CrudEstacion, serie as CrudSerie } from '../CRUD'
import { Observacion, Serie} from '../a5_types'
import { Feature, FeatureCollection } from '../geometry_types'
import { readFileSync, writeFileSync } from 'fs'

type SeriesMapping = {
    series_id : number
    url : string
    metadata? : CrudSerie
}

type SerieConEstId = {
    series_id : number
    url : string
    estacion_id: number
}

type SerieConObs = {
    id : number
    observaciones : Observacion[]
}

type Config = {
     url: string // not used
     series_map? : Record<number, SeriesMapping>
     date_col? : number
     value_col? : number
     skip_rows? : number
}

export class Client extends AbstractAccessorEngine implements AccessorEngine {

    static _get_is_multiseries = true

    series_map? : Record<number, SeriesMapping>

    date_col? : number
    value_col? : number
    skip_rows? : number

    constructor(config : Config) {
        super(config)
        if(config.series_map) {
            this.series_map = config.series_map
        }
        this.date_col = config.date_col
        this.value_col = config.value_col
        this.skip_rows = config.skip_rows
    }

    async downloadDataAndParseCSV(url : string, timestart?: Date, timeend?: Date, series_id?: number) : Promise<Array<Observacion>> {
        const data : string = await fetchData(url)
        const parsed_data : Array<Array<string>> = parse(data)
        return this.arrayToObs(parsed_data, this.date_col, this.value_col, this.skip_rows, timestart, timeend, series_id)
    }

    arrayToObs(csv_data : Array<Array<string>>, date_col : number = 0, value_col : number = 3, skip_rows : number = 4, timestart?: Date, timeend?: Date, series_id?: number) : Observacion[] {
        const result  : Observacion[] = []
        for(const [index, row] of csv_data.entries()) {
            if(index < skip_rows) {
                continue
            }
            const date : Date = new Date(row[date_col])
            if(date.toString() == 'Invalid Date') {
                console.warn("Invalid date: " + row[date_col] + ", skipping.")
                continue
            }
            if(timestart && date < timestart) {
                continue
            }
            if(timeend && date > timeend) {
                continue
            }
            const value : number = parseFloat(row[value_col])
            if(value.toString() == "NaN") {
                console.warn("Invalid value: " + row[value_col] + ", skipping.")
                continue
            }
            result.push({
                "timestart": date,
                "timeend": date,
                "valor": value,
                "series_id": series_id
            })
        }
        return result
    }

    getSerieFromId(s_id : number) : SerieConEstId {
        if(!this.series_map) {
            throw new Error("series_map not set")
        }
        for(const [k, v] of Object.entries(this.series_map)) {
            if(v.series_id == s_id) {
                return {...v, estacion_id: parseInt(k)}
            }
        }
        throw new Error("Serie not found with id=" + s_id)
    }

    async getOne(estacion_id : number, timestart?: Date, timeend?: Date) : Promise<CrudSerie> {
        if(!this.series_map) {
            await this.getSeries()
        }
        if(!this.series_map) {
            throw new Error("series_map not set")
        }
        const seriemap = this.series_map[estacion_id]
        if(!seriemap) {
            throw new Error("Serie not found in series_map configuration for estacion_id=" + estacion_id)
        }
        const obs = await this.downloadDataAndParseCSV(seriemap.url, timestart, timeend, seriemap.series_id)
        const result : CrudSerie = seriemap.metadata
        result.setObservaciones(obs)
        return result
        // return {
        //     ...serie.metadata,
        //     observaciones: obs
        // }
    }


    async get(filter : ObservacionesFilter,
            options : {
                return_series ? : true
            }
        ) : Promise<CrudSerie[]>;
    async get(
            filter : ObservacionesFilter,
            options : {
                return_series ? : false
            }
        ) : Promise<Observacion[]>
    async get(
            filter : ObservacionesFilter,
            options : {
                return_series ? : boolean
            } = {}
        ) : Promise<CrudSerie[]|Observacion[]> {
        const result : CrudSerie[] = []
        if(!filter.series_id) {
            if(!filter.estacion_id) {
                throw new Error("missing estacion_id or series_id")
            }
            if(filter.estacion_id instanceof Array) {
                for(const e_id of filter.estacion_id) {
                    result.push(await this.getOne(e_id, filter.timestart, filter.timeend))
                }
            } else {
                result.push(await this.getOne(filter.estacion_id, filter.timestart, filter.timeend))
            }
        } else {
            if(filter.series_id instanceof Array) {
                for(const s_id of filter.series_id) {
                    const serie = this.getSerieFromId(s_id)
                    result.push(await this.getOne(serie.estacion_id, filter.timestart, filter.timeend))
                }
            } else {
                const serie = this.getSerieFromId(filter.series_id)
                result.push(await this.getOne(serie.estacion_id, filter.timestart, filter.timeend))
            }
        }
        if(options.return_series) {
            return result
        } else {
            let all_obs : Observacion[] = []
            for(const serie of result) {
                all_obs = all_obs.concat(serie.observaciones)
            }
            return all_obs
        }
    }

    async update(
        filter : ObservacionesFilter,
        options : {
            return_series ? : boolean
        } = {}
    ) : Promise<CrudSerie[]|Observacion[]> {
        const series = await this.get(filter, {return_series: true})
        for(const serie of series) {
            await serie.createObservaciones(undefined, {no_returning: true})
        }
        if(options.return_series) {
            return series
        } else {
            let observaciones : Observacion[] = []
            for(const serie of series) {
                observaciones = observaciones.concat(serie.observaciones)
            } 
            return observaciones
        }

    }

    readStationsCsv(
        filepath : string, 
        has_header : boolean=true, 
        lon_col : number=1,
        lat_col : number=2,
        columns : string[]=["nombre","longitud","latitud","url"],
        output?: string
        ) : FeatureCollection {
        const datastr = readFileSync(filepath, { encoding: "utf-8"})
        const data = parse(datastr)
        const features : Feature[] = []
        for(const [i, row] of data.entries()) {
            if(has_header && i == 0) {
                continue
            }
            const properties = Object.fromEntries(columns.map((key, i) => [key, row[i]]))
            properties.tabla = "agp"
            properties.id_externo = properties.url
            features.push({
                type: "Feature",
                properties: properties,
                geometry: {
                    type: "Point",
                    coordinates: [
                        parseFloat(row[lon_col]),
                        parseFloat(row[lat_col])
                    ]
                }
            })
        }
        const fc : FeatureCollection = {
            type: "FeatureCollection",
            features: features
        }
        if(output) {
            writeFileSync(output, JSON.stringify(fc))
        }
        return fc
    }

    async createSites(json_filepath : string) : Promise<CrudEstacion[]> {
        const estaciones = CrudEstacion.fromGeojson(json_filepath)
        return CrudEstacion.create(estaciones)
    }

    async createSeries(create : boolean=false) : Promise<CrudSerie[]> {
        const estaciones = await CrudEstacion.read({tabla: "agp"})
        const series : CrudSerie[] = []
        for(const e of estaciones) {
            const serie = new CrudSerie({
                estacion_id: e.id,
                var_id: 2,
                proc_id: 1,
                unit_id: 11
            })
            
            series.push(serie)
        }
        if(create) {
            CrudSerie.create(series)
        }
        return series
    }

    setSeriesMap(series : CrudSerie) {
        this.series_map = {}
        for(const serie of series) {
            this.series_map[serie.estacion.id] = {
                series_id: serie.id,
                url: serie.estacion.id_externo,
                metadata: serie
            }
        }
    }

    async getSeries(filter : SeriesFilter = {}) {
        filter.tabla_id = "agp"
        filter.var_id = 2
        filter.proc_id = 1
        filter.unit_id = 11
        const series = await CrudSerie.read(filter)
        if(!this.series_map) {
            this.setSeriesMap(series)
        }
        return series
    }
}
