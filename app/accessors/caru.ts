import { AbstractAccessorEngine, AccessorEngine, ObservacionesFilter, ObservacionesFilterWithArrays, SeriesFilter, SitesFilter, SitesFilterWithArrays, SeriesFilterWithArrays } from './abstract_accessor_engine'
import get from 'axios'
import { Estacion, Observacion, Procedimiento, Serie, SerieOnlyIds, Unidades, Variable } from '../a5_types'
import {estacion as crud_estacion, var as crud_var, procedimiento as crud_proc, unidades as crud_unidades, serie as crud_serie, serie, observaciones as crud_observaciones} from '../CRUD'

type Config = {
    url: string,
    template_path : string,
    series_map : Record<number,number>,
    estaciones_map : Record<number,Estacion>
}

export class Client extends AbstractAccessorEngine implements AccessorEngine {

    static _get_is_multiseries : boolean = false

    config : Config

    constructor(config : Config) {
        super(config)
        this.setConfig(config)
    }

    default_config : Config = {
        "url": "http://190.0.152.194:8080",
        "template_path": "alturas/web/user/estacion/${0}/0",
        "series_map": {
            3280: 22,
            3306: 21,
            7059: 39,
            7060: 19,
            7061: 20,
            7062: 37
        },
        "estaciones_map": {
            3280: {
                id: 1699,
                nombre: "Nueva Palmira",
                geom: {"type":"Point","coordinates":[-58.422029,-33.878478]},
                id_externo: "22",
                tabla: "estaciones_varios"
            },
            3306: {
                id: 1700,
                nombre: "Boca Gualeguaychú CARU",
                geom: {"type":"Point","coordinates":[-58.4166666666667,-33.0666666666667]},
                id_externo: "21",
                tabla: "estaciones_varios"
            },
            7059: {
                id: 2231,
                nombre: "Nuevo Berlín",
                geom: {"type":"Point","coordinates":[-58.061997,-32.979836]},
                id_externo: "39",
                tabla: "estaciones_varios"
            },
            7060: {
                id: 2232,
                nombre: "Paysandú",
                geom: {"type":"Point","coordinates":[-58.102575,-32.313646]},
                id_externo: "19",
                tabla: "estaciones_varios"
            },
            7061: {
                id: 2233,
                nombre: "Concepción del Uruguay - CARU",
                geom: {"type":"Point","coordinates":[-58.221219,-32.477173]},
                id_externo: "20",
                tabla: "estaciones_varios"
            },
            7062: {
                id: 2235,
                nombre: "Fray Bentos - CARU",
                geom: {"type":"Point","coordinates":[-58.314443,-33.112132]},
                id_externo: "37",
                tabla: "estaciones_varios"
            }
        }
    }

    async getValues(id : number, series_id : number) : Promise<Observacion[]> {
        if(!id) {
            if(!series_id) {
                throw new Error("Missing id or series_id")
            }
            if (!(series_id in this.config.series_map)) {
                throw new Error(`series_id ${series_id} not found in series_map`)
            }
            id = this.config.series_map[series_id]
        }
        const url = `${this.config.url}/${this.config.template_path.replace("${0}",id.toString())}`
        console.debug(`descargando ${url}`)
        const response = await get(url)
        const matches = response.data.match(/alturasJson\s*=\s*([\s\S]*?);/); //  response.data.match(/alturasJson\s*=(.+?);/)
        if(!matches.length) {
            throw new Error("Data not found in downloaded file")
        }
        const data = JSON.parse(matches[1])
        const observaciones : Observacion[] = []
        for(const item of data) {
            const d = item.fecha.split(" ")
            const f = d[0].split("\/").map((i: string)=>parseInt(i))
            const t = d[1].split(":").map((i : string)=>parseInt(i))
            const timestart = new Date(f[2], f[1] - 1, f[0], t[0], t[1]);
            if(timestart.toString() == "Invalid Date") {
                console.error(`Invalid date string: ${item.fecha}. Skipping`)
                continue
            }
            const valor = parseFloat(item.altura)
            if(valor.toString() == "NaN") {
                console.error(`Invalid value: ${item.value}. Skipping`)
                continue
            }
            observaciones.push({
                series_id: series_id,
                timestart: timestart,
                timeend: timestart,
                valor: valor
            })
        }
        return observaciones
    }

    async getSeries(filter : SeriesFilter) : Promise<Serie[]> {
        const variable = await crud_var.read({id:2})
        const proc = await crud_proc.read({id:1})
        const unidades = await crud_unidades.read({id:11})
        return Object.keys(this.config.estaciones_map).map(series_id => {
            return new crud_serie({
                tipo: "puntual",
                id: parseInt(series_id),
                var: variable, //{id: 2},
                unidades: unidades, // {id: 11},
                estacion: this.config.estaciones_map[series_id],
                procedimiento: proc // {id: 1}
            })
        })
    }

    async get(filter : ObservacionesFilter, options={}) : Promise<Observacion[]> {
        if(!filter.series_id) {
            throw new Error("Missing series_id")
        }
        var observaciones : Observacion[] = []
        if(Array.isArray(filter.series_id)) {
            for(const series_id of filter.series_id) {
                try {
                    const observaciones_ = await this.get({...filter, series_id: series_id})
                    const c_obs = new crud_observaciones(observaciones_)
                    observaciones.push(...c_obs.removeDuplicates())
                } catch(e) {
                    console.error(e.toString())
                }
            }
        } else {
            try {
                const obs = await this.getValues(undefined, filter.series_id)
                var c_obs = new crud_observaciones(obs)
                observaciones = c_obs.removeDuplicates()
            } catch (e) {
                console.error(e.toString())
                return
            }
        }
        if(filter.timestart) {
            observaciones = observaciones.filter( obs => obs.timestart >= filter.timestart)
        }
        if(filter.timeend) {
            observaciones = observaciones.filter( obs => obs.timeend <= filter.timeend)
        }
        return observaciones
    }

    async update(filter : ObservacionesFilter, options={}) : Promise<Observacion[]> {
        var observaciones = await this.get(filter, options)
        return crud_observaciones.create(observaciones)
    }
}
