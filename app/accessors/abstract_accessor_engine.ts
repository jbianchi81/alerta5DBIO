import { Location, Estacion, Observacion, Serie } from '../a5_types'
import { Geometry } from '../geometry_types'
import {serie as crud_serie, estacion as crud_estacion, observaciones as crud_observaciones} from '../CRUD'
import get from 'axios'

export interface SitesFilter {
    estacion_id ? : number | Array<number>
    id_externo ? : string | Array<string>
    geom ? : Geometry
}

export interface SitesFilterWithArrays {
    estacion_id : Array<number>
    id_externo : Array<string>
    geom : Geometry
}

export interface SeriesFilter extends SitesFilter {
    series_id ? : number | Array<number>
    var_id ? : number | Array<number>
}

export interface SeriesFilterWithArrays extends SitesFilterWithArrays {
    series_id : Array<number>
    var_id : Array<number>
}

export interface TimePeriodFilter {
    timestart : Date,
    timeend : Date   
}

export interface ObservacionesFilter extends SeriesFilter, TimePeriodFilter {}

export interface ObservacionesFilterWithArrays extends TimePeriodFilter {
    series_id : Array<number>,
    estacion_id : Array<number>,
    var_id : Array<number>,
    id_externo : Array<string>
}

export interface AccessorEngine {
    default_config : Object
    get config() : Object
    set config(value : Map<string,any> | Object)
    setConfig(config : Object) : void
    get(filter : ObservacionesFilter, options : { return_series ? : boolean}) : Promise<Array<Observacion>|Array<Serie>>
    update? (filter : ObservacionesFilter, options : { return_series ? : boolean}) : Promise<Array<Observacion>|Array<Serie>>
    getSeries(filter : SeriesFilter) : Promise<Array<Serie>>
    updateSeries? (filter : SeriesFilter) : Promise<Array<Serie>>
    getSites(filter : SitesFilter) : Promise<Array<Location>>
    updateSites? (filter : SitesFilter) : Promise<Array<Estacion>>
    test() : Promise<boolean>

}

export interface Config {
    url : string
    [ x : string] : unknown
}

export class AbstractAccessorEngine {

    default_config : Object = {}

    config : Config

    setConfig(config : Config) : void {
        this.config = {
            url: config.url
        }
        Object.assign(this.config,this.default_config)
        Object.assign(this.config,config)
    }

    // async get(filter : ObservacionesFilter) {
    //     console.warn("get method not implemented in this class")
    //     return [] as Array<Observacion>
    // }

    constructor(config : Config) {
        if (new.target === AbstractAccessorEngine) {
            throw new Error("Cannot instantiate an abstract class.");
        }
        this.setConfig(config)
    }

    async test() : Promise<boolean> {
        try {
          var response = await get(this.config.url)
        } catch (e) {
          console.error(e)
          return false
        }
        if (response.status <= 299) {
            return true
        } else {
            return false
        }
    }

    static setFilterValuesToArray(filter : Object, empty_arrays : boolean = true) : Object {
        const filter_ = Object.assign({},filter)
        const valid_keys = [
            {
                "key": "series_id",
                "type": "int"
            },{
                "key": "estacion_id",
                "type": "int"
            },{
                "key": "var_id",
                "type": "int"
            },{
                "key": "id_externo",
                "type": "string"
            }
        ]
        for(const key of valid_keys) {
            if(filter_[key.key] != undefined) {
                if(!Array.isArray(filter_[key.key])) {
                    if(key.type == "int")   {
                        filter_[key.key] = [parseInt(filter_[key.key])]
                    } else {
                        filter_[key.key] = [filter_[key.key].toString()]
                    }
                }
            } else if(empty_arrays) {
                filter_[key.key] = []
            }
        }
        return filter_
    }

    async get(
        filter : ObservacionesFilter, 
        options : {
            return_series ? : boolean
        }) : Promise<Observacion[]|Serie[]> {
            throw new Error("Method 'get()' must be implemented in child class")
    }

    async update(
        filter : ObservacionesFilter,
        options : {
            return_series ? : boolean
        } = {}) : Promise<Array<Observacion>|Array<Serie>> {
        const series = await this.get(filter, {...options, return_series: true})
        const updated : Array<Serie> = []
        for(var serie of series) {
            const c_serie = new crud_serie(serie)
            await c_serie.createObservaciones()
            updated.push(c_serie as Serie)
        }
        if (options.return_series) {
            return updated
        } else {
            const observaciones = []
            for(var i=0; i<updated.length; i++) {
                observaciones.push(...updated[i].observaciones)
            }
            return new crud_observaciones(observaciones)
        }
    }

    async getSeries(filter : SeriesFilter) : Promise<Array<Serie>> {
        throw new Error("Method 'getSeries()' must be implemented in child class")
    }

    async updateSeries(filter : SeriesFilter) : Promise<Array<Serie>> {
        const series = await this.getSeries(filter)
        return crud_serie.create(series)
    }

    async getSites(filter : SitesFilter) : Promise<Array<Location>> {
        throw new Error("Method 'getSites()' must be implemented in child class")
    }

    async updateSites(filter : SitesFilter) : Promise<Array<Estacion>> {
        const sites = await this.getSites(filter)
        return crud_estacion.create(sites)
    }

}
