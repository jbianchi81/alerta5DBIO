import { AbstractAccessorEngine, AccessorEngine, ObservacionesFilter, ObservacionesFilterWithArrays, SeriesFilter, SitesFilter, SitesFilterWithArrays, SeriesFilterWithArrays } from './abstract_accessor_engine'
import { filterSites, filterSeries } from '../accessor_utils'
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



export class Client extends AbstractAccessorEngine implements AccessorEngine {

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
        if(estaciones.length) {
            this.sites_map = estaciones.map(estacion => {
                return {
                    estacion_id: estacion.id,
                    code: parseInt(estacion.id_externo),
                    estacion: estacion
                } as LoadedSiteMap
            })
        } else {
            this.sites_map = estaciones_default.map(estacion => {
                return {
                    estacion_id: estacion.id,
                    code: parseInt(estacion.id_externo),
                    estacion: estacion
                } as LoadedSiteMap
            })
        }
    }
    
    async loadSeriesMap() {
        const series = await crud_serie.read({
            tabla_id: this.config.tabla,
            var_id: 2,
            proc_id: 1,
            unit_id: 11
        }) as Array<Serie>
        if(series.length) {
            this.series_map = series.map(serie => {
                return {
                    series_id: serie.id,
                    estacion_id: serie.estacion.id,
                    var_id: serie.var.id,
                    serie: serie
                } as LoadedSerieMap
            })
        } else {
            this.series_map = series_default.map(serie => {
                return {
                    series_id: serie.id,
                    estacion_id: serie.estacion_id,
                    var_id: serie.var_id
                } as LoadedSerieMap
            })
        }
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
        if(options.return_series) {
            var series : Array<Serie> = []
        } else {
            var observaciones : Array<Observacion> = []
        }
        for(var site_index=0; site_index < this.sites_map.length; site_index++) {
            const site = this.sites_map[site_index]
            if(filter_.estacion_id && filter_.estacion_id.indexOf(site.estacion_id) < 0) {
                // filter out by estacion_id
                continue
            }
            if(filter_.id_externo && filter_.id_externo.indexOf(site.estacion.id_externo) < 0) {
                // filter out by id_externo
                continue
            }
            const serie_index = this.series_map.map(s=>s.estacion_id).indexOf(site.estacion_id)
            if(serie_index < 0) {
                console.warn(`Serie not found in map for estacion_id: ${site.estacion_id}, code: ${site.code}`)
                continue
            }
            const code = site.code
            const series_id = this.series_map[serie_index].series_id
            if(filter_.series_id && filter_.series_id.indexOf(series_id) < 0) {
                // filter out by series_id
                continue
            }
            if(options.return_series) {
                const series_match = await this.getSeries({id: series_id})
                if(!series_match.length) {
                    throw(new Error("mapped series_id not found in series list"))
                }
                var serie = new crud_serie(series_match[0])
                serie.observaciones = []
            }
            const page_range = this.predict_page_range(filter.timestart, filter.timeend)
            for(var page = page_range.begin; page <= page_range.end; page++) {
                const page_obs = await this.getPage(code, page, series_id)
                var filtered_obs = page_obs.filter(o=> 
                    o.timestart.getTime() >= filter_.timestart.getTime() 
                    && 
                    o.timestart.getTime() <= filter_.timeend.getTime()
                )
                if(options.return_series) {
                    serie.observaciones.push(
                        ...filtered_obs
                    )
                } else {
                    observaciones.push(
                        ...filtered_obs
                    )
                }
            }  
            if(options.return_series) {
                series.push(serie)
            }          
        }
        if(options.return_series) {
            return series
        } else {
            return observaciones
        }
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
        console.debug(`Get url: ${page_url}`)
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

    async getSites(filter={}) {
        return filterSites(estaciones_default, filter)
    }

    async getSeries(filter={}) {
        return filterSeries(series_default.map(s=>new crud_serie(s)), filter)
    }
}

const estaciones_default = [{"id":153,"nombre":"Bahia Negra","id_externo":"2000086033","geom":{"type":"Point","coordinates":[-58.1632,-20.22719]},"tabla":"alturas_dinac","provincia":"PARAGUAY","pais":"PARAGUAY","rio":"PARAGUAY","has_obs":true,"tipo":"H","automatica":false,"habilitar":true,"propietario":"DMH-DINAC","abreviatura":"BNEG","URL":null,"localidad":null,"real":true,"nivel_alerta":5,"nivel_evacuacion":5.5,"nivel_aguas_bajas":0.81,"altitud":null,"public":true,"cero_ign":null,"red":{"id":12,"tabla_id":"alturas_dinac","nombre":"red hidrológica DINAC - Paraguay","public":true,"public_his_plata":false}},{"id":154,"nombre":"Olimpo","id_externo":"2000086010","geom":{"type":"Point","coordinates":[-57.86827,-21.04235]},"tabla":"alturas_dinac","provincia":"PARAGUAY","pais":"PARAGUAY","rio":"PARAGUAY","has_obs":true,"tipo":"H","automatica":false,"habilitar":true,"propietario":"DMH-DINAC","abreviatura":null,"URL":null,"localidad":null,"real":true,"nivel_alerta":6,"nivel_evacuacion":7,"nivel_aguas_bajas":2.43,"altitud":null,"public":true,"cero_ign":null,"red":{"id":12,"tabla_id":"alturas_dinac","nombre":"red hidrológica DINAC - Paraguay","public":true,"public_his_plata":false}},{"id":155,"nombre":"Concepcion","id_externo":"2000086134","geom":{"type":"Point","coordinates":[-57.4439,-23.45766]},"tabla":"alturas_dinac","provincia":"PARAGUAY","pais":"PARAGUAY","rio":"PARAGUAY","has_obs":true,"tipo":"H","automatica":false,"habilitar":true,"propietario":"DMH-DINAC","abreviatura":"CONCEP","URL":null,"localidad":null,"real":true,"nivel_alerta":6,"nivel_evacuacion":7,"nivel_aguas_bajas":1.56,"altitud":null,"public":true,"cero_ign":null,"red":{"id":12,"tabla_id":"alturas_dinac","nombre":"red hidrológica DINAC - Paraguay","public":true,"public_his_plata":false}},{"id":156,"nombre":"Rosario","id_externo":"2000086183","geom":{"type":"Point","coordinates":[-57.15143,-24.44787]},"tabla":"alturas_dinac","provincia":"PARAGUAY","pais":"PARAGUAY","rio":"PARAGUAY","has_obs":true,"tipo":"H","automatica":false,"habilitar":true,"propietario":"DMH-DINAC","abreviatura":"MERCEDES","URL":null,"localidad":null,"real":true,"nivel_alerta":5.5,"nivel_evacuacion":6.5,"nivel_aguas_bajas":1.28,"altitud":null,"public":true,"cero_ign":null,"red":{"id":12,"tabla_id":"alturas_dinac","nombre":"red hidrológica DINAC - Paraguay","public":true,"public_his_plata":false}},{"id":157,"nombre":"Asunción","id_externo":"2000086218","geom":{"type":"Point","coordinates":[-57.64869,-25.27207]},"tabla":"alturas_dinac","provincia":"PARAGUAY","pais":"PARAGUAY","rio":"PARAGUAY","has_obs":true,"tipo":"H","automatica":false,"habilitar":true,"propietario":"DMH-DINAC","abreviatura":"ASUNCION","URL":null,"localidad":null,"real":true,"nivel_alerta":4.5,"nivel_evacuacion":5.5,"nivel_aguas_bajas":1,"altitud":null,"public":true,"cero_ign":null,"red":{"id":12,"tabla_id":"alturas_dinac","nombre":"red hidrológica DINAC - Paraguay","public":true,"public_his_plata":false}},{"id":159,"nombre":"Ladario","id_externo":"2000082001","geom":{"type":"Point","coordinates":[-57.593133,-19.001058]},"tabla":"alturas_dinac","provincia":"Chubut","pais":"Argentina","rio":"PARAGUAY","has_obs":false,"tipo":"H","automatica":false,"habilitar":true,"propietario":null,"abreviatura":"LADARIO","URL":null,"localidad":null,"real":true,"nivel_alerta":null,"nivel_evacuacion":null,"nivel_aguas_bajas":null,"altitud":null,"public":true,"cero_ign":null,"red":{"id":12,"tabla_id":"alturas_dinac","nombre":"red hidrológica DINAC - Paraguay","public":true,"public_his_plata":false}},{"id":160,"nombre":"Fuerte Olimpo","id_externo":null,"geom":{"type":"Point","coordinates":[-57.869132,-21.040707]},"tabla":"alturas_dinac","provincia":"PARAGUAY","pais":"PARAGUAY","rio":"PARAGUAY","has_obs":false,"tipo":"H","automatica":false,"habilitar":true,"propietario":"DMH-DINAC","abreviatura":"FUERTE_O","URL":null,"localidad":null,"real":true,"nivel_alerta":null,"nivel_evacuacion":null,"nivel_aguas_bajas":null,"altitud":null,"public":true,"cero_ign":null,"red":{"id":12,"tabla_id":"alturas_dinac","nombre":"red hidrológica DINAC - Paraguay","public":true,"public_his_plata":false}},{"id":161,"nombre":"Porto Murtinho","id_externo":null,"geom":{"type":"Point","coordinates":[-57.89037,-21.698553]},"tabla":"alturas_dinac","provincia":"PARAGUAY","pais":"PARAGUAY","rio":"PARAGUAY","has_obs":false,"tipo":"H","automatica":false,"habilitar":true,"propietario":"DMH-DINAC","abreviatura":"PORTO_MU","URL":null,"localidad":null,"real":true,"nivel_alerta":null,"nivel_evacuacion":null,"nivel_aguas_bajas":null,"altitud":null,"public":true,"cero_ign":null,"red":{"id":12,"tabla_id":"alturas_dinac","nombre":"red hidrológica DINAC - Paraguay","public":true,"public_his_plata":false}},{"id":162,"nombre":"Vallemi","id_externo":"2000086088","geom":{"type":"Point","coordinates":[-57.957644,-22.15783]},"tabla":"alturas_dinac","provincia":"PARAGUAY","pais":"PARAGUAY","rio":"PARAGUAY","has_obs":true,"tipo":"H","automatica":false,"habilitar":true,"propietario":"DMH-DINAC","abreviatura":"VALLEMI","URL":null,"localidad":null,"real":true,"nivel_alerta":null,"nivel_evacuacion":null,"nivel_aguas_bajas":null,"altitud":null,"public":true,"cero_ign":null,"red":{"id":12,"tabla_id":"alturas_dinac","nombre":"red hidrológica DINAC - Paraguay","public":true,"public_his_plata":false}},{"id":164,"nombre":"Villeta","id_externo":"2000086211","geom":{"type":"Point","coordinates":[-57.570908,-25.513139]},"tabla":"alturas_dinac","provincia":"PARAGUAY","pais":"PARAGUAY","rio":"PARAGUAY","has_obs":true,"tipo":"H","automatica":false,"habilitar":true,"propietario":"DMH-DINAC","abreviatura":"VILLETA","URL":null,"localidad":null,"real":true,"nivel_alerta":5.5,"nivel_evacuacion":7.5,"nivel_aguas_bajas":1.25,"altitud":null,"public":true,"cero_ign":null,"red":{"id":12,"tabla_id":"alturas_dinac","nombre":"red hidrológica DINAC - Paraguay","public":true,"public_his_plata":false}},{"id":165,"nombre":"Alberdi","id_externo":"2000086254","geom":{"type":"Point","coordinates":[-58.147448,-26.188792]},"tabla":"alturas_dinac","provincia":"PARAGUAY","pais":"PARAGUAY","rio":"PARAGUAY","has_obs":true,"tipo":"H","automatica":false,"habilitar":true,"propietario":"DMH-DINAC","abreviatura":"ALBERDI","URL":null,"localidad":null,"real":true,"nivel_alerta":6,"nivel_evacuacion":7,"nivel_aguas_bajas":null,"altitud":null,"public":true,"cero_ign":null,"red":{"id":12,"tabla_id":"alturas_dinac","nombre":"red hidrológica DINAC - Paraguay","public":true,"public_his_plata":false}},{"id":166,"nombre":"Pilar","id_externo":"2000086255","geom":{"type":"Point","coordinates":[-58.310561,-26.857698]},"tabla":"alturas_dinac","provincia":"PARAGUAY","pais":"PARAGUAY","rio":"PARAGUAY","has_obs":true,"tipo":"H","automatica":false,"habilitar":true,"propietario":"DMH-DINAC","abreviatura":"PILAR","URL":null,"localidad":null,"real":true,"nivel_alerta":7,"nivel_evacuacion":8,"nivel_aguas_bajas":1.39,"altitud":null,"public":true,"cero_ign":null,"red":{"id":12,"tabla_id":"alturas_dinac","nombre":"red hidrológica DINAC - Paraguay","public":true,"public_his_plata":false}},{"id":172,"nombre":"Yuty - Caazapá","id_externo":"2000086259","geom":{"type":"Point","coordinates":[-56.281558,-26.715872]},"tabla":"alturas_dinac","provincia":"PARAGUAY","pais":"PARAGUAY","rio":"TEBICUARY","has_obs":true,"tipo":"H","automatica":false,"habilitar":true,"propietario":"DMH-DINAC","abreviatura":null,"URL":null,"localidad":null,"real":true,"nivel_alerta":null,"nivel_evacuacion":null,"nivel_aguas_bajas":null,"altitud":null,"public":true,"cero_ign":null,"red":{"id":12,"tabla_id":"alturas_dinac","nombre":"red hidrológica DINAC - Paraguay","public":true,"public_his_plata":false}},{"id":173,"nombre":"Villa Florida - Misiones","id_externo":"2000086260","geom":{"type":"Point","coordinates":[-57.128369,-26.402555]},"tabla":"alturas_dinac","provincia":"PARAGUAY","pais":"PARAGUAY","rio":"TEBICUARY","has_obs":true,"tipo":"H","automatica":false,"habilitar":true,"propietario":"DMH-DINAC","abreviatura":null,"URL":null,"localidad":null,"real":true,"nivel_alerta":null,"nivel_evacuacion":null,"nivel_aguas_bajas":null,"altitud":null,"public":true,"cero_ign":null,"red":{"id":12,"tabla_id":"alturas_dinac","nombre":"red hidrológica DINAC - Paraguay","public":true,"public_his_plata":false}},{"id":1681,"nombre":"Yhaguy - Arroyos y Esteros - Cordillera","id_externo":"2000086226","geom":{"type":"Point","coordinates":[-57.05345,-25.046562]},"tabla":"alturas_dinac","provincia":"CORDILLERA","pais":"PARAGUAY","rio":"Yhaguy","has_obs":true,"tipo":"H","automatica":true,"habilitar":true,"propietario":"DINAC","abreviatura":"YHAGUY","URL":"http://www.meteorologia.gov.py/nivel/vermas_diario.php?id=2000086226","localidad":"Arroyos y Esteros","real":null,"nivel_alerta":null,"nivel_evacuacion":null,"nivel_aguas_bajas":null,"altitud":null,"public":true,"cero_ign":null,"red":{"id":12,"tabla_id":"alturas_dinac","nombre":"red hidrológica DINAC - Paraguay","public":true,"public_his_plata":false}},{"id":1775,"nombre":"Ayolas","id_externo":"2000086261","geom":{"type":"Point","coordinates":[-56.830849,-27.389031]},"tabla":"alturas_dinac","provincia":null,"pais":"Paraguay","rio":"Paraná","has_obs":true,"tipo":"H","automatica":false,"habilitar":true,"propietario":"DINAC","abreviatura":null,"URL":"http://www.meteorologia.gov.py/nivel-rio/vermas.php?id=2000086261","localidad":"Ayolas","real":true,"nivel_alerta":null,"nivel_evacuacion":null,"nivel_aguas_bajas":null,"altitud":null,"public":true,"cero_ign":null,"red":{"id":12,"tabla_id":"alturas_dinac","nombre":"red hidrológica DINAC - Paraguay","public":true,"public_his_plata":false}},{"id":1776,"nombre":"Ciudad del Este","id_externo":"2000086248","geom":{"type":"Point","coordinates":[-54.603269,-25.516459]},"tabla":"alturas_dinac","provincia":null,"pais":"Paraguay","rio":"Paraná","has_obs":true,"tipo":"H","automatica":false,"habilitar":true,"propietario":"DINAC","abreviatura":null,"URL":"http://www.meteorologia.gov.py/nivel-rio/vermas.php?id=2000086248","localidad":"Ciudad","real":null,"nivel_alerta":null,"nivel_evacuacion":null,"nivel_aguas_bajas":null,"altitud":null,"public":true,"cero_ign":null,"red":{"id":12,"tabla_id":"alturas_dinac","nombre":"red hidrológica DINAC - Paraguay","public":true,"public_his_plata":false}},{"id":1777,"nombre":"Salto del Guairá","id_externo":"2000086297","geom":{"type":"Point","coordinates":[-54.30181,-24.077858]},"tabla":"alturas_dinac","provincia":null,"pais":"Paraguay","rio":"Paraná","has_obs":true,"tipo":"H","automatica":false,"habilitar":true,"propietario":"DINAC","abreviatura":null,"URL":"http://www.meteorologia.gov.py/nivel-rio/vermas.php?id=2000086297","localidad":"Salto del Guayrá","real":true,"nivel_alerta":null,"nivel_evacuacion":null,"nivel_aguas_bajas":null,"altitud":null,"public":true,"cero_ign":null,"red":{"id":12,"tabla_id":"alturas_dinac","nombre":"red hidrológica DINAC - Paraguay","public":true,"public_his_plata":false}},{"id":1778,"nombre":"Ita Corá","id_externo":"2000086288","geom":{"type":"Point","coordinates":[-58.21332,-27.235561]},"tabla":"alturas_dinac","provincia":null,"pais":"Paraguay","rio":"Paraná","has_obs":true,"tipo":"H","automatica":false,"habilitar":true,"propietario":"DINAC","abreviatura":null,"URL":"http://www.meteorologia.gov.py/nivel-rio/vermas.php?id=2000086288","localidad":"Ita Corá","real":true,"nivel_alerta":null,"nivel_evacuacion":null,"nivel_aguas_bajas":null,"altitud":null,"public":true,"cero_ign":null,"red":{"id":12,"tabla_id":"alturas_dinac","nombre":"red hidrológica DINAC - Paraguay","public":true,"public_his_plata":false}}]

const series_default = [{"tipo": "puntual", "id": 153, "estacion_id": 153, "var_id": 2, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 26343, "estacion_id": 153, "var_id": 39, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 154, "estacion_id": 154, "var_id": 2, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 26344, "estacion_id": 154, "var_id": 39, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 155, "estacion_id": 155, "var_id": 2, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 26345, "estacion_id": 155, "var_id": 39, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 156, "estacion_id": 156, "var_id": 2, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 26346, "estacion_id": 156, "var_id": 39, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 157, "estacion_id": 157, "var_id": 2, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 26347, "estacion_id": 157, "var_id": 39, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 159, "estacion_id": 159, "var_id": 2, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 29900, "estacion_id": 159, "var_id": 39, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 160, "estacion_id": 160, "var_id": 2, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 26348, "estacion_id": 160, "var_id": 39, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 161, "estacion_id": 161, "var_id": 2, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 26349, "estacion_id": 161, "var_id": 39, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 162, "estacion_id": 162, "var_id": 2, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 26350, "estacion_id": 162, "var_id": 39, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 164, "estacion_id": 164, "var_id": 2, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 26351, "estacion_id": 164, "var_id": 39, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 165, "estacion_id": 165, "var_id": 2, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 26352, "estacion_id": 165, "var_id": 39, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 166, "estacion_id": 166, "var_id": 2, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 26353, "estacion_id": 166, "var_id": 39, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 172, "estacion_id": 172, "var_id": 2, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 26354, "estacion_id": 172, "var_id": 39, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 173, "estacion_id": 173, "var_id": 2, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 26355, "estacion_id": 173, "var_id": 39, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 3048, "estacion_id": 1681, "var_id": 2, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 26386, "estacion_id": 1681, "var_id": 39, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 3519, "estacion_id": 1775, "var_id": 2, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 26432, "estacion_id": 1775, "var_id": 39, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 3520, "estacion_id": 1776, "var_id": 2, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 26433, "estacion_id": 1776, "var_id": 39, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 3521, "estacion_id": 1777, "var_id": 2, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 26434, "estacion_id": 1777, "var_id": 39, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 3522, "estacion_id": 1778, "var_id": 2, "proc_id": 1, "unit_id": 11}, {"tipo": "puntual", "id": 26435, "estacion_id": 1778, "var_id": 39, "proc_id": 1, "unit_id": 11}]