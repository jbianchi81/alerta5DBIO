import { AbstractAccessorEngine, AccessorEngine, ObservacionesFilter, ObservacionesFilterWithArrays, SeriesFilter, SitesFilter, SitesFilterWithArrays, SeriesFilterWithArrays } from './abstract_accessor_engine'
// import { fetch, filterSites } from '../accessor_utils'
import { Estacion, Observacion, Procedimiento, Serie, SerieOnlyIds, Unidades, Variable } from '../a5_types'
import {observacion as crud_observacion, estacion as crud_estacion, var as crud_var, procedimiento as crud_proc, unidades as crud_unidades, serie as crud_serie, serie} from '../CRUD'
import get from 'axios'

type Config = {
    url : string,
    page_size : number,
    tabla : string,
    pais : string,
    propietario : string,
    proc_id : number
}

type DateRange = {
    begin: Date,
    end: Date
} 

type PageRange = {
    begin: number,
    end: number
} 

type SiteMap = {
    code : number,
    estacion_id : number
}

interface LoadedSiteMap extends SiteMap {
    estacion : Estacion
}

type SerieMap = {
    estacion_id : number,
    var_id : number,
    series_id : number
}

interface LoadedSerieMap extends SerieMap {
    serie : Serie
}



export class Client extends AbstractAccessorEngine { // implements AccessorEngine 

    static _get_is_multiseries : boolean = true

    config : Config

    sites_map : Array<LoadedSiteMap> = []

    series_map : Array<LoadedSerieMap> = []

    constructor(config : Config) {
        super(config)
        this.setConfig(config)
    }

    default_config : Config = {
        url: "https://www.meteorologia.gov.py/nivel-rio/vermas_convencional.php",
        page_size: 15,
        tabla: "alturas_dinac",
        pais : "Paraguay",
        propietario : "DMH-DINAC",
        proc_id : 1
    }

    async loadSitesMap() {
        const estaciones = await crud_estacion.read({
            tabla: this.config.tabla
        }) as Array<Estacion>
        this.sites_map = estaciones.map(estacion => {
            return {
                estacion_id: estacion.id,
                code: parseInt(estacion.id_externo),
                estacion: estacion
            } as LoadedSiteMap
        })
    }
    
    async loadSeriesMap() {
        const series = await crud_serie.read({
            tabla_id: this.config.tabla,
            var_id: 2,
            proc_id: 1,
            unit_id: 11
        }) as Array<Serie>
        this.series_map = series.map(serie => {
            return {
                series_id: serie.id,
                estacion_id: serie.estacion.id,
                var_id: serie.var.id,
                serie: serie
            } as LoadedSerieMap
        })
    }

    async get(
        filter : ObservacionesFilter,
        options : {
            return_series ? : boolean
        } = {}) : Promise<Array<Observacion>|Array<Serie>> {
        if(!filter || !filter.timestart || !filter.timeend) {
            throw("Missing timestart and/or timeend")
        }
        if(!this.sites_map.length) {
            await this.loadSitesMap()
        }
        if(!this.series_map.length) {
            await this.loadSeriesMap()
        }
        const filter_ = Client.setFilterValuesToArray(filter, false) as ObservacionesFilterWithArrays
        const observaciones : Array<Observacion> = []
        for(var site_index=0; site_index < this.sites_map.length; site_index++) {
            const site = this.sites_map[site_index]
            if(filter_.estacion_id && filter_.estacion_id.indexOf(site.estacion_id) < 0) {
                // filter out by estacion_id
                return
            }
            if(filter_.id_externo && filter_.id_externo.indexOf(site.estacion.id_externo) < 0) {
                // filter out by id_externo
                return
            }
            const serie_index = this.series_map.map(s=>s.estacion_id).indexOf(site.estacion_id)
            if(serie_index < 0) {
                console.warn(`Serie not found in map for estacion_id: ${site.estacion_id}, code: ${site.code}`)
                return
            }
            const code = site.code
            const series_id = this.series_map[serie_index].series_id
            if(filter_.series_id && filter_.series_id.indexOf(series_id) < 0) {
                // filter out by series_id
                return
            }
            const page_range = this.predict_page_range(filter.timestart, filter.timeend)
            for(var page = page_range.begin; page <= page_range.end; page++) {
                const page_obs = await this.getPage(code, page, series_id)
                var filtered_obs = page_obs.filter(o=> 
                    o.timestart.getTime() >= filter_.timestart.getTime() 
                    && 
                    o.timestart.getTime() <= filter_.timeend.getTime()
                )
                observaciones.push(
                    ...filtered_obs
                )
            }            
        }
        return observaciones
    }

    predict_date_range(page : number, data_length : number = this.config.page_size) : DateRange {
        var pred_end_date = new Date()
        pred_end_date.setHours(0,0,0,0)
        pred_end_date.setDate(pred_end_date.getDate() - client.config.page_size * (page - 1))

        var pred_begin_date = new Date(pred_end_date)
        pred_begin_date.setDate(pred_begin_date.getDate() - data_length + 1)
        return {
            begin: pred_begin_date,
            end: pred_end_date
        }
    }

    predict_page_range(timestart : Date, timeend : Date) : PageRange {
        const today : Date = new Date()
        today.setHours(0,0,0,0)
        const begin_days_ago : number = Math.round((today.getTime() - timeend.getTime()) / 1000 / 3600 / 24)
        const end_days_ago : number = Math.round((today.getTime() - timestart.getTime()) / 1000 / 3600 / 24)
        return {
            begin: Math.trunc(begin_days_ago / this.config.page_size) + 1,
            end: Math.trunc(end_days_ago / this.config.page_size) + 1
        }
    }


    async getPage(code : number, page : number, series_id : number|undefined) : Promise<Observacion[]> {
        // const code = 2000086134
        // const page = 381
        // const size = 15
        const page_url = `${this.config.url}?code=${code}&page=${page}`
        const response = await get(page_url)
        const matches = response.data.match(/var\sphp_vars\s?=\s?(\{.*\})/)
        const data = JSON.parse(matches[1])
        const observaciones : crud_observacion[] = []
        data.categories.forEach((category : string, i : number) => {
            if(data.data.length < i + 1) {
                console.warn(`Data array is shorter than categories array: skipping category ${category}`)
                return
            }
            const split_date = category.split("-").map(d=>parseInt(d))
            const date = new Date(split_date[2], split_date[1]-1, split_date[0])
            observaciones.push(new crud_observacion(
                {
                    tipo: "puntual",
                    series_id: series_id,
                    timestart: date,
                    timeend: date,
                    valor: parseFloat(data.data[i])
                }
            ))
        })
        observaciones.sort((a, b) => a.timestart.getTime() - b.timestart.getTime())
        return observaciones
    }
}
