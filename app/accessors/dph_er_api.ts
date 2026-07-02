import { AbstractAccessorEngine, AccessorEngine, ObservacionesFilter, SeriesFilter, SitesFilter } from './abstract_accessor_engine'
// @ts-ignore
import { fetchData } from '../accessor_utils'
import { Observacion, Serie} from '../a5_types'
// @ts-ignore
import { estacion as CrudEstacion, serie as CrudSerie, red as CrudRed, observaciones as CrudObservaciones } from '../CRUD'
import { Location } from '../a5_types'

type Config = {
     url: string
     token: string
     tabla_id: string
}

type Estacion = {
    id:           number
    nombre:       string
    latitud:      number
    longitud:     number
    propietario:  string
    departamento: string
    cuenca:       string
}

type GetEstacionesResponse = {
    status: "ok"
    data: {
        total: number,
        estaciones: Estacion[]
    }
}

type Precipitacion = {
    fecha:       string
    precipitacion_mm:  number
    medicion_realizada: boolean
}

type GetPrecipitacionesResponse = {
  status: "ok"
  data: {
        estacion_id: number
        estacion: string
        desde:  string
        hasta:  string
        total:  number
        precipitaciones: Precipitacion[]
    }
}

type PrecipitacionGlobal = {
    estacion_id: number
    estacion:    string
    fecha:       string
    precipitacion_mm:  number
    medicion_realizada: boolean
}


type GetPrecipitacionesGlobalResponse = {
  status: "ok"
  data: {
        desde:  string
        hasta:  string
        total:  number
        precipitaciones: PrecipitacionGlobal[]
    }
}

type SeriesMapping = {
    series_id : number
    estacion_id : number
    id_externo : number
    metadata? : CrudSerie
}

export class Client extends AbstractAccessorEngine implements AccessorEngine {

    static _get_is_multiseries = true

    series_map? : Record<number, SeriesMapping>

    url : string
    token : string
    tabla_id : string

    defaults = {
        url: "https://api.hidraulica.gob.ar/v1",
        tabla_id: "dph_er_pluvio"
    }

    constructor(config : Config) {
        super(config)
        // if(config.series_map) {
        //     this.series_map = config.series_map
        // }
        this.url = config.url || this.defaults.url
        this.token = config.token
        this.tabla_id = config.tabla_id || this.defaults.tabla_id
    }

    headers() {
        return {
            "Authorization": `Bearer ${this.token}`,
            "Content-Type": "application/json"
        }
    }

    async createRed() {
        return CrudRed.create({
            tabla_id: this.tabla_id,
            nombre: "Dirección de Hidráulica de Entre Ríos - Red pluviométrica",
            public: true
        })
    }

    async get_estaciones() : Promise<GetEstacionesResponse> {
        return fetchData(`${this.url}/estaciones`, {headers: this.headers()})
    }

    parseEstacion(e : Estacion) : CrudEstacion {
        return new CrudEstacion({
            tabla: this.tabla_id,
            nombre: e.nombre,
            geom: {
                type: "Point",
                coordinates: [e.longitud, e.latitud]
            },
            propietario: e.propietario,
            id_externo: e.id.toString(),
            distrito: "Entre Ríos",
            pais: "Argentina",
            localidad: e.departamento,
            tipo: "P",
            real: true,
            habilitar: true
        })
    }

    async getSites(filter : SitesFilter={}) : Promise<Location[]> {
        const response = await this.get_estaciones()
        const estaciones = response.data.estaciones.map(e => this.parseEstacion(e))
        for(const e of estaciones) {
            await e.getEstacionId()
        }
        return estaciones
    }    

    async getSeries(filter : SeriesFilter={}) : Promise<Serie[]> {
        const estaciones = await this.getSites()
        return estaciones.map(e => new CrudSerie({
            tipo: "puntual",
            estacion: e,
            var: {id: 1},
            procedimiento: {id: 1},
            unidades: {id: 22}
        }))
    }    

    /**
     * El rango entre 'desde' y 'hasta' no puede superar 7 días.
     * @param desde 
     * @param hasta 
     * @returns 
     */
    async get_precipitaciones_global(
        desde? : string,
        hasta? : string
    ) : Promise<GetPrecipitacionesGlobalResponse> {
        return fetchData(
            `${this.url}/precipitaciones`, 
            {
                headers: this.headers(),
                params: {
                    desde: desde,
                    hasta: hasta
                }
            })
    }

    /**
     * El rango entre desde y hasta no puede superar 365 días.
     * @param estacion_id 
     * @param desde 
     * @param hasta 
     * @returns 
     */
    async get_precipitaciones(
        estacion_id : number,
        desde? : string,
        hasta? : string
    ) : Promise<GetPrecipitacionesResponse> {
        return fetchData(
            `${this.url}/estaciones/${estacion_id}/precipitaciones`, 
            {
                headers: this.headers(),
                params: {
                    desde: desde,
                    hasta: hasta
                }
            })
    }

    parsePrecipitacion(p : Precipitacion, series_id : number) : Observacion|null
    parsePrecipitacion(p : PrecipitacionGlobal) : Observacion|null
    parsePrecipitacion(p : Precipitacion|PrecipitacionGlobal, series_id? : number) : Observacion|null {
        const timestart = new Date(
            parseInt(p.fecha.substring(0,4)),
            parseInt(p.fecha.substring(5,7)) - 1,
            parseInt(p.fecha.substring(8,10)),
            9
        )
        if(timestart.toString() == "Invalid Date") {
            throw new Error(`Invalid date: ${timestart.toString()}`)
        }
        if(!p.medicion_realizada || p.precipitacion_mm == null) {
            console.warn(`medicion nula`)
            return null
        }
        const timeend = new Date(timestart)
        timeend.setDate(timeend.getDate() + 1)
        if("estacion_id" in p) {
            series_id = this.findSerie(undefined, undefined, p.estacion_id).series_id
        }
        return {
            timestart: timestart,
            timeend: timeend,
            valor: p.precipitacion_mm,
            series_id: series_id
        }       
    }

    async getSeriesMap() {
        this.series_map = {}
        const series = await CrudSerie.read(
            {
                tabla_id: this.tabla_id,
                var_id: 1,
                proc_id: 1,
                unit_id: 22
            }
        )
        for (const s of series) {
            this.series_map[s.estacion.id_externo] = {
                series_id: s.id,
                estacion_id: s.estacion.id,
                id_externo: s.estacion.id_externo,
                metadata: s
            }
        }
    }

    findSerie(estacion_id? : number, series_id? : number, id_externo? : number) {
        if(!estacion_id && !series_id && !id_externo) {
            throw new Error("At least one of estacion_id, series_id, id_externo must be set")
        }
        if(!this.series_map) {
            throw new Error("series_map not set")
        }
        for(const [key, mapping] of Object.entries(this.series_map)) {
            if(estacion_id) {
                if(mapping.estacion_id == estacion_id) {
                    return mapping
                }
            } else if(series_id) {
                if(mapping.series_id == series_id) {
                    return mapping
                }
            } else {
                if(mapping.id_externo == id_externo) {
                    return mapping
                }
            }
        }
        throw new Error(`estacion_id: ${estacion_id}, series_id: ${series_id} not found in series mapping`)
    }

    async get_one(estacion_id? : number, desde? : string, hasta? : string, series_id? : number) : Promise<Observacion[]> {
        const mapping = this.findSerie(estacion_id, series_id)
        const response = await this.get_precipitaciones(mapping.id_externo, desde, hasta)
        return response.data.precipitaciones.map(p => this.parsePrecipitacion(p, mapping.series_id)).filter(o=>o != null)
    }

    async get(filter : ObservacionesFilter, options : {} ={}) : Promise<Observacion[]> {
        if(!this.series_map) {
            await this.getSeriesMap()
        }
        const desde = (filter.timestart) ? formatLocalDate(filter.timestart) : undefined
        const hasta = (filter.timeend) ? formatLocalDate(filter.timeend) : undefined

        if(filter.estacion_id) {
            checkMaxDays(desde, hasta, 365)
            const observaciones : Observacion[] = []
            if(Array.isArray(filter.estacion_id)) {
                for(const estacion_id of filter.estacion_id) {
                    const obs = await this.get_one(estacion_id, desde, hasta)
                    observaciones.push(...obs)
                }
            } else {
                const obs = await this.get_one(filter.estacion_id, desde, hasta)
                observaciones.push(...obs)
            }
            return observaciones
        } 
        if(filter.series_id) {
            checkMaxDays(desde, hasta, 365)
            const observaciones : Observacion[] = []
            if(Array.isArray(filter.series_id)) {
                for(const series_id of filter.series_id) {
                    const obs = await this.get_one(undefined, desde, hasta, series_id)
                    observaciones.push(...obs)
                }
            } else {
                const obs = await this.get_one(undefined, desde, hasta, filter.series_id)
                observaciones.push(...obs)
            }
            return observaciones
        }
        // all stations
        checkMaxDays(desde, hasta, 7)
        const response = await this.get_precipitaciones_global(desde, hasta)
        const observaciones : Observacion[] = response.data.precipitaciones.map(p => this.parsePrecipitacion(p)).filter(o => o != null)
        return observaciones
    }

    async update(filter : ObservacionesFilter, options : {} ={}) : Promise<Observacion[]> {
        const results = await this.get(filter)
        return CrudObservaciones.create(results)
    }
}

function formatLocalDate(date: Date): string {
  const pad = (n: number) => String(n).padStart(2, "0");

  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
}

function checkMaxDays(desde? : string, hasta? : string, maxDays : number=7) : void {
    if(!desde || !hasta) {
        return
    }
    const startDate = new Date(desde);
    const endDate = new Date(hasta);
    const diffTime = Math.abs(endDate.getTime() - startDate.getTime());
    const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
    if (diffDays > maxDays) {
        throw new Error(`The date range between ${desde} and ${hasta} exceeds the maximum allowed days (${maxDays})`);
    }
}