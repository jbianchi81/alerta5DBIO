import { Estacion, Observacion, Serie } from '../a5_types'
import { Geometry } from '../geometry_types'

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
    getSites(filter : SitesFilter) : Promise<Array<Estacion>>
    updateSites? (filter : SitesFilter) : Promise<Array<Estacion>>

}

export class AbstractAccessorEngine {

    default_config : Object = {}

    config : Object = {}

    setConfig(config : Object) : void {
        this.config = {}
        Object.assign(this.config,this.default_config)
        Object.assign(this.config,config)
    }

    // async get(filter : ObservacionesFilter) {
    //     console.warn("get method not implemented in this class")
    //     return [] as Array<Observacion>
    // }

    constructor(config = {}) {
        this.setConfig(config)
    }
}
