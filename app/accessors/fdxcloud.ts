import { AbstractAccessorEngine, AccessorEngine, ObservacionesFilter, SeriesFilter } from './abstract_accessor_engine'
import { fetchData } from '../accessor_utils'
import { Observacion, Serie} from '../a5_types'
import { estacion as CrudEstacion, serie as CrudSerie } from '../CRUD'

type SeriesMapping = {
    series_id : number
    point_id : number
    metadata? : CrudSerie
}

type Config = {
     url: string
     token: string
     tabla_id: string
     series_map? : Record<number, SeriesMapping>
     pagination_length? : number
}

export type Measurement = {
    "device" : {
        "id": number,
        "serial": string
    },
    "measurementPoint": {
        "id": number,
        "name": string
    },
    "signlvl": null,
    "measureDate": string, // (ISO 8601),
    "batteryCharge": number,
    "transmissionDate": string, // (ISO 8601),
    "directValue": number,
    "directValueUnit": string,
    "adjustedValue": number,
    "adjustedValueUnit": string,
    "interpretedValue": number,
    "interpretedValueUnit": string,
    "networkMode": string
}

export type MeasuresResponse = {
    "rows": Measurement[],
    "total": number,
    "filtered": number,
    "page": number,
    "length": number
}

export type TimeValuePair = {
    "time": string, // ISO 8601
    "value": number
}

export class Client extends AbstractAccessorEngine implements AccessorEngine {

    static _get_is_multiseries = false

    series_map? : Record<number, SeriesMapping>

    url : string
    token : string
    tabla_id : string
    pagination_length : number

    defaults = {
        pagination_length: 100
    }

    constructor(config : Config) {
        super(config)
        if(config.series_map) {
            this.series_map = config.series_map
        }
        this.url = config.url
        this.token = config.token
        this.tabla_id = config.tabla_id
        this.pagination_length = config.pagination_length || this.defaults.pagination_length
    }

    headers() {
        return {
            "x-api-token": this.token,
            "Content-Type": "application/json"
        }
    }

    async getMeasuresLatest(
        measurement_point : number,
        device? : number
    ) : Promise<Measurement> {
        return fetchData(
            `${this.url}/measures/latest`,
            {
                params: {
                    measurement_point: measurement_point,
                    device: device
                },
                headers: this.headers()
            }
            
        )
    }

    async getMeasures(
        measurement_point : number,
        device? : number,
        since? : string,
        to? : string,
        length? : number,
        page? : number
    ) : Promise<MeasuresResponse> {
        return fetchData(
            `${this.url}/measures`,
            {
                params: {
                    measurement_point: measurement_point,
                    device: device,
                    since : since,
                    to : to,
                    length : length,
                    page : page
                },
                headers: this.headers()
            }
        )
    }

    async getTimeseries(
        measurement_point : number,
        since : string,
        to : string,
        device? : number
    ) : Promise<TimeValuePair[]> {
        return fetchData(
            `${this.url}/measures/timeseries`,
            {
                params: {
                    measurement_point: measurement_point,
                    device: device,
                    since : since,
                    to : to
                },
                headers: this.headers()
            }
        )
    }

    setSeriesMap(series : CrudSerie[]) {
        this.series_map = {}
        for(const serie of series) {
            this.series_map[serie.estacion.id] = {
                series_id: serie.id,
                point_id: parseInt(serie.estacion.id_externo),
                metadata: serie
            }
        }
    }

    async getSeries(filter : SeriesFilter = {}) : Promise<CrudSerie> {
        filter.tabla_id = this.tabla_id
        filter.var_id = 2
        filter.proc_id = 1
        filter.unit_id = 11
        const series : CrudSerie[] = await CrudSerie.read(filter)
        this.setSeriesMap(series)
        return series
    }

    async getMeasuresWithPagination(
        measurement_point : number,
        device? : number,
        since? : string,
        to? : string,
        length : number=this.pagination_length,
        start_page : number=1
    ) : Promise<Measurement[]> {
        let last : boolean = false
        let page = start_page
        let measurements : Measurement[] = []
        while(last == false) {
            const measures_response = await this.getMeasures(
                measurement_point,
                device,
                since,
                to,
                length,
                page
            )
            measurements = measurements.concat(measures_response.rows)
            const last_page = Math.floor(measures_response.total / length + 1)
            page = page + 1
            if(page > last_page) {
                last = true
            }
        }
        return measurements
    }

    getSerieFromId(s_id : number) : SeriesMapping {
        if(!this.series_map) {
            throw new Error("series_map not set")
        }
        for(const [k, v] of Object.entries(this.series_map)) {
            if(v.series_id == s_id) {
                return v
            }
        }
        throw new Error("Serie not found with id=" + s_id)
    }


    async get(filter : ObservacionesFilter,
            options : {
                return_series ? : true,
                use_measures_endpoint ? : boolean
            }
        ) : Promise<CrudSerie[]>;
    async get(
            filter : ObservacionesFilter,
            options : {
                return_series ? : false,
                use_measures_endpoint ? : boolean
            }
        ) : Promise<Observacion[]>
    async get(
            filter : ObservacionesFilter,
            options : {
                return_series ? : boolean,
                use_measures_endpoint ? : boolean
            } = {}
        ) : Promise<CrudSerie[]|Observacion[]> {
        if(!filter.timestart || !filter.timeend) {
            throw new Error("Missing filter.timestart, filter.timeend")
        } 
        if(!this.series_map) {
            await this.getSeries(filter)
        }
        if(filter.series_id) {
            if(Array.isArray(filter.series_id)) {
                throw new Error("Multiple series_id are not allowed in filter")
            }
            var serie_map = this.getSerieFromId(filter.series_id)
        } else {
            if(!filter.estacion_id) {
                throw new Error("Missing filter.series_id or filter.estacion_id")
            }
            if(Array.isArray(filter.estacion_id)) {
                throw new Error("Multiple estacion_id are not allowed in filter")
            }
            if(this.series_map === undefined) {
                throw new Error("series_map not set")
            }
            var serie_map = this.series_map[filter.estacion_id]
            if(!serie_map) {
                throw new Error("estacion_id not found in series map")
            }            
        }
        if(options.use_measures_endpoint) {
            const measurements = await this.getMeasuresWithPagination(
                serie_map.point_id,
                undefined,
                filter.timestart.toISOString().substring(0,10),
                new Date(filter.timeend.getTime() + 24 * 3600 * 1000).toISOString().substring(0,10) // avanza 1 día para que tome el día final completo
            )
            var obs = this.parseMeasurements(measurements, serie_map)
        } else {
            const timeseries = await this.getTimeseries(
                serie_map.point_id,
                filter.timestart.toISOString().substring(0,19).replace("T"," "),
                filter.timeend.toISOString().substring(0,19).replace("T"," ")
            )
            var obs = this.parseTimeseries(timeseries, serie_map)
        }
        if(options.return_series) {
            const serie = serie_map.metadata
            serie.observaciones = obs
            return [serie]
        } else {
            return obs
        }
    }

    parseMeasurements(measurements : Measurement[], serie_map : SeriesMapping) : Observacion[] {
        const obs : Observacion[] = []
        for(const m of measurements) {
            obs.push(this.parseMeasurement(m, serie_map))
        }
        return obs
    }

    parseMeasurement(m : Measurement, serie_map : SeriesMapping) : Observacion {
        return {
            timestart: new Date(m.measureDate),
            timeend: new Date(m.measureDate),
            series_id: serie_map.series_id,
            valor: m.interpretedValue
        }
    }

    parseTimeseries(timeseries : TimeValuePair[], serie_map : SeriesMapping) : Observacion[] {
        const obs : Observacion[] = []
        for(const tvp of timeseries) {
            obs.push(this.parseTimeValuePair(tvp, serie_map))
        }
        return obs
    }

    parseTimeValuePair(tvp : TimeValuePair, serie_map : SeriesMapping) : Observacion {
        return {
            timestart: new Date(tvp.time),
            timeend: new Date(tvp.time),
            series_id: serie_map.series_id,
            valor: tvp.value
        }
    }
}