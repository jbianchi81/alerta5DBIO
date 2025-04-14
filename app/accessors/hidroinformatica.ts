import { AbstractAccessorEngine, AccessorEngine, ObservacionesFilter, ObservacionesFilterWithArrays, SeriesFilter, SitesFilter, SitesFilterWithArrays, SeriesFilterWithArrays } from './abstract_accessor_engine'
import { filterSeries, fetchData, dateFromFormat } from '../accessor_utils'
import { Estacion, Observacion, Procedimiento, Serie, SerieOnlyIds, Unidades, Variable } from '../a5_types'
import {observacion as crud_observacion, estacion as crud_estacion, var as crud_var, procedimiento as crud_proc, unidades as crud_unidades, serie as crud_serie, serie} from '../CRUD'
import axios, { AxiosResponse} from 'axios'
import csv from 'csv-parser'
import fs from 'fs'
const {roundDateTo, advanceInterval} = require('../timeSteps')

type Config = {
    url : string
    tabla : string
    pais : string
    propietario : string
    proc_id : number
}

type DateRange = {
    begin: Date,
    end: Date
} 

type SiteMap = {
    code : number
    estacion_id : number
    station_name : string
    var_id : number
}

interface LoadedSerieMap  {
    code : number
    estacion_id : number
    var_id : number
    series_id : number
    serie : Serie
}

type ResponseDataItem = {
    fecha : string
    nivel : number
    conductividad : number
    ph : number
    turbidez : number
    od : number
    tempagua : number
    created_at : string
    obs : number
    ehidrologica : number
}

export class Client extends AbstractAccessorEngine implements AccessorEngine {
    static _get_is_multiseries : boolean = false

    config : Config

    series_map : Array<LoadedSerieMap> = []

    sites_id_map : SiteMap[] = [
        { station_name: 'Coratei', code: 146, var_id: 39, estacion_id: 6017 },
        {
          station_name: 'Paso de Patria',
          code: 151,
          var_id: 39,
          estacion_id: 5942
        },
        {
          station_name: 'Puerto Antequera',
          code: null,
          var_id: 39,
          estacion_id: 5920
        },
        { station_name: 'Cerrito', code: 145, var_id: 39, estacion_id: 6007 },
        {
          station_name: 'Ita Enramada',
          code: null,
          var_id: 39,
          estacion_id: 5929
        },
        {
          station_name: 'Panchito Lopez',
          code: 147,
          var_id: 39,
          estacion_id: 5947
        },
        { station_name: 'Ayolas', code: 3, var_id: 39, estacion_id: 5946 },
        {
          station_name: 'Ita Corá',
          code: 148,
          var_id: 39,
          estacion_id: 6019
        },
        {
          station_name: 'Bahia Negra',
          code: 6,
          var_id: 39,
          estacion_id: 5403
        },
        { station_name: 'Ita Piru', code: 4, var_id: 39, estacion_id: 6824 },
        { station_name: 'Rosario', code: 11, var_id: 39, estacion_id: 5919 },
        {
          station_name: 'Concepción',
          code: 10,
          var_id: 39,
          estacion_id: 5918
        },
        { station_name: 'Vallemí', code: 8, var_id: 39, estacion_id: 5916 },
        { station_name: 'Asunción', code: 12, var_id: 39, estacion_id: 5928 },
        { station_name: 'Villeta', code: 14, var_id: 39, estacion_id: 5995 },
        { station_name: 'Humaitá', code: 144, var_id: 39, estacion_id: 5938 },
        { station_name: 'Pilar', code: 16, var_id: 39, estacion_id: 5420 },
        {
          station_name: 'Isla Margarita',
          code: null,
          var_id: 39,
          estacion_id: 5913
        },
        {
          station_name: 'Fuerte Olimpo',
          code: 7,
          var_id: 39,
          estacion_id: 5914
        },
        {
          station_name: 'Salto del Guairá',
          code: null,
          var_id: 39,
          estacion_id: 5422
        },
        { station_name: 'Alberdi', code: 15, var_id: 39, estacion_id: 5937 },
        {
          station_name: 'San Cosme y San Dami?n',
          code: 150,
          var_id: 39,
          estacion_id: 5951
        },
        {
          station_name: 'Encarnación',
          code: 2,
          var_id: 39,
          estacion_id: 5952
        },
        {
          station_name: 'Ciudad del Este',
          code: 5,
          var_id: 39,
          estacion_id: 5936
        },
        { station_name: 'Yuty', code: 49, var_id: 85, estacion_id: 5943 }
    ]

    constructor(config : Config) {
        super(config)
        this.setConfig(config)
        // this.loadSites()
    }

    default_config : Config = {
        url: "https://hidroinformatica.itaipu.gov.py/services/hidrometricaestacion",
        tabla: "MCH_DMH_PY",
        pais : "Paraguay",
        propietario : "DMH-DINAC",
        proc_id : 1
    }

    last_request : {params: {code : number, timestart : Date, timeend: Date}, url: string}

    static async loadSitesFromCsv(filename : string) : Promise<SiteMap[]> {
        return new Promise((resolve, reject) => {
            const results = [];

            fs.createReadStream(filename)
            .pipe(csv())
            .on('data', (data) => results.push(data))
            .on('end', () => {
                for(const item of results) {
                    item.code = (item.code !='') ? parseInt(item.code) : null
                    item.var_id = (item.var_id !='') ? parseInt(item.var_id) : null
                    item.estacion_id = (item.estacion_id !='') ? parseInt(item.estacion_id) : null
                }
                resolve(results)
            })
            .on('error', (err) => reject(err));
        })
    }
    async loadSeriesMap() {
        const valid_sites_id = this.sites_id_map.filter(site=> site.code)
        const series = await crud_serie.read({
            tabla_id: this.config.tabla,
            estacion_id: Array.from(new Set(valid_sites_id.map(site=>site.estacion_id))),
            var_id: Array.from(new Set(valid_sites_id.map(site=>site.var_id))),
            proc_id: this.config.proc_id
        }) as Array<Serie>
        this.series_map = []
        for(const serie of series) {
            const match_ids = valid_sites_id.filter(site => site.estacion_id === serie.estacion.id && site.var_id === serie.var.id)
            if(!match_ids.length) {
                continue
            }
            this.series_map.push({
                code: match_ids[0].code,
                series_id: serie.id,
                estacion_id: serie.estacion.id,
                var_id: serie.var.id,
                serie: serie
            } as LoadedSerieMap)
        }
    }

    async getSeries(filter={}) {
        if(!this.series_map.length) {
            await this.loadSeriesMap()
        }
        return filterSeries(this.series_map.map(s=> {
            
            return new crud_serie({
                id: s.series_id,
                tipo: "puntual",
                ...s
            })
    }), filter)
    }

    setDefaultSeriesMap() {
        this.series_map = this.sites_id_map.map(site=> {
            return {
                code: site.code,
                estacion_id: site.estacion_id,
                var_id: site.var_id,
                series_id: null,
                serie: null
            }
        })
    }

    async get(
        filter : ObservacionesFilter,
        options : {
            return_series ? : boolean
        } = {}) : Promise<Array<Observacion>> {
        if(!filter) {
            throw new Error("Missing filter")
        } else if(!filter.timestart || !filter.timeend) {
            throw new Error("Missing timestart - timeend in filter") 
        }
        if(!this.series_map.length) {
            await this.loadSeriesMap()
        }
        const filter_ = Client.setFilterValuesToFirst(filter)
        const serie = this.getCode(filter_.estacion_id, filter_.var_id, filter_.series_id)
        const data : ResponseDataItem[] = await this.downloadData(serie.code, filter.timestart, filter.timeend)
        const time_support = (serie.var_id == 39) ? "daily" : (serie.var_id == 85) ? "hourly" : "instantaneous"
        const observaciones = data.map(item => Client.parseDataItem(item, serie.series_id,time_support))
        if(options.return_series) {
            return [
                    new crud_serie({
                    id: serie.series_id,
                    tipo: "puntual",
                    observaciones: observaciones,
                    ...serie
                })
            ]
        } else {
            return observaciones
        }
    }

    getCode(estacion_id : number, var_id : number, series_id : number) : LoadedSerieMap {
        let matched_series : LoadedSerieMap[]
        if(series_id) {
            matched_series = this.series_map.filter(serie=>serie.series_id == series_id)
        } else if (estacion_id && var_id) {
            matched_series = this.series_map.filter(serie=>serie.estacion_id == estacion_id && serie.var_id == var_id)
        } else {
            throw new Error("Missing filter.series_id or filter.estacion_id+filter.var_id")
        }
        if(!matched_series.length) {
            throw new Error(`Invalid filter (estacion_id:${estacion_id}, var_id:${var_id}, series_id:${series_id}). Not found in mapping`)
        }
        return matched_series[0]

    }

    async downloadData(
        code : number,
        timestart : string | Date,
        timeend : string | Date    
    ) : Promise<ResponseDataItem[]> {
        timestart = new Date(timestart)
        timeend = new Date(timeend)
        if(timestart.toString() == "Invalid Date") {
            throw new Error("invalid timestart")
        }
        if(timeend.toString() == "Invalid Date") {
            throw new Error("invalid timeend")
        }
        const request_url = `${this.config.url}/${formatLocalDate(timestart)}/${formatLocalDate(timeend)}/${code}/?format=json`
        // const response : AxiosResponse<ResponseDataItem[]> = await axios.get(request_url)
        console.debug("request_url: " + request_url)
        this.last_request = {
            params: {code : code, timestart: timestart, timeend: timeend}, 
            url: request_url
        }
        return fetchData(request_url)
    }

    static parseDataItem(
        item : ResponseDataItem,
        series_id : number,
        time_support : string = "instantaneous"
    ) : Observacion {
        let timestart : Date = new Date(item.fecha.replace(" PYST","-03:00"))
        let timeend : Date
        if(time_support == "daily") {
            timestart = roundDateTo(timestart,"day",true)
            timeend = advanceInterval(timestart,{days:1})
        } else if(time_support == "hourly") {
            timestart = roundDateTo(timestart,"hour",true)
            timeend = advanceInterval(timestart,{hour:1})
        } else {
            timeend = new Date(timestart)
        }
        return {
            series_id: series_id,
            timestart: timestart,
            timeend: timeend,
            valor: item.nivel
        } as Observacion
    }
}

function formatLocalDate(date : Date) {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0'); // 0-indexed
    const day = String(date.getDate()).padStart(2, '0');
  
    return `${year}-${month}-${day}`;
}
