import { Location, Estacion, Observacion, Serie } from '../a5_types'
import { Geometry } from '../geometry_types'
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
}
