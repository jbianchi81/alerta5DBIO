import axios, { AxiosInstance } from "axios"
import { AbstractAccessorEngine, AccessorEngine, Config, ObservacionesFilter, ObservacionesOptions, SeriesFilter, SitesFilter } from "./abstract_accessor_engine"
import {Estacion, Observacion, Serie} from "../a5_types"
import {estacion as crud_estacion, var as crud_var, procedimiento as crud_proc, unidades as crud_unidades, serie as crud_serie, serie, observaciones as crud_observaciones} from '../CRUD'

interface LoginResponse {
    id_client : string
	name : string
	tokenAuth : string
	expires : string
}

interface RegistroEstacion {
    id_estacion : string
	nombre : string
	codigo : string
	latitud : string
	longitud : string
	nivel_mar : string
	direccion : string
	en_actividad : string
	estado : string
	descripcion : string
}

interface ListEstacionesResponse {
	estaciones: RegistroEstacion[]
}

interface RegistroAltura {
    id_estacion: string
    fecha: string
    hora: string
    altura: string
    observaciones: string
}

interface GetAlturasResponse {
    alturas: RegistroAltura[]
}

interface RegistroPrecipitaciones {
    id_estacion: string
    fecha: string
    hora: string
    tipo_precipitacion: string | null,
    precipitacion: string
}

interface GetPrecipitacionesResponse {
    precipitaciones: RegistroPrecipitaciones[]
}

interface GetFilter {
    timestart: Date
    timeend: Date
    series_id?: number| number[]
    var_id?: number| number[]
    estacion_id?: number| number[]
    id_externo?: string| string[]
}

interface MappedSerie {
    id_estacion: number
    variable : "altura" | "precipitacion"
    var_id : number
    estacion_id : number
    serie : Serie
}

export class Client extends AbstractAccessorEngine implements AccessorEngine {

    static _get_is_multiseries : boolean = false

    config : Config

    connection: AxiosInstance

    tokenAuth: string | undefined

    series_map: {[x: number] : MappedSerie } = {}

    get headers() {
        return {
            "User-Agent": "DEPilcomayoXM2",
            "X-authorization-token": this.tokenAuth
        } 
    }

    get loginHeaders() {
        return {
            "User-Agent": "DEPilcomayoXM2"
        } 
    }

    constructor(config : Config) {
        super(config)
        this.setConfig(config)
        this.connection = axios.create()
    }

    default_config : Config = {
        url: "https://api.pilcomayo.net",
        username: "my_username",
        password: "my_password"
    }

    async login() : Promise<LoginResponse> {
        const response = await this.connection.post(
            `${this.config.url}/login/`, 
            {
                username: this.config.username,
                password: this.config.password
            },{
                headers: this.loginHeaders
            })
        if(response.status != 200) {
            throw new Error(`Login failed: ${response.statusText}`)
        }
        if(!response.data || !response.data.tokenAuth) {
            throw new Error(`Login failed: tokenAuth missing in response`)
        }
        this.tokenAuth = response.data.tokenAuth
        return response.data as LoginResponse
    }

    async listEstaciones(id_estacion? : string) : Promise<ListEstacionesResponse> {
        if(!this.tokenAuth) {
            throw new Error("Must login first.")
        }
        const response = await this.connection.post(
            `${this.config.url}/list_estaciones/`, 
            {
                id_estacion
            },{
               headers: this.headers
            }
        )
        if(response.status != 200) {
            throw new Error(`Request failed: ${response.statusText}`)
        }
        if(!response.data) {
            throw new Error(`listEstaciones failed: no data`)
        }
        return response.data as ListEstacionesResponse
    }

    async getAlturas(id_estacion: number, ano: number, mes: number, dia?: number, hora?: number) : Promise<GetAlturasResponse> {
        if(!this.tokenAuth) {
            throw new Error("Must login first.")
        }
        const response = await this.connection.post(
            `${this.config.url}/get_alturas/`, 
            {
                id_estacion,
                ano,
                mes,
                dia,
                hora
            },{
                headers: this.headers
            })
        if(response.status != 200) {
            throw new Error(`Request failed: ${response.statusText}`)
        }
        if(!response.data) {
            throw new Error(`getEstaciones failed: no data`)
        }
        return response.data as GetAlturasResponse
    }

    async getPrecipitaciones(id_estacion: number, ano: number, mes: number, dia?: number, hora?: number) : Promise<GetPrecipitacionesResponse> {
        if(!this.tokenAuth) {
            throw new Error("Must login first.")
        }
        const response = await this.connection.post(
            `${this.config.url}/get_precipitaciones/`, 
            {
                id_estacion,
                ano,
                mes,
                dia,
                hora
            },{
                headers: this.headers
            })
        if(response.status != 200) {
            throw new Error(`Request failed: ${response.statusText}`)
        }
        if(!response.data) {
            throw new Error(`getPrecipitaciones failed: no data`)
        }
        return response.data as GetPrecipitacionesResponse
    }

    parseEstacion(estacion : RegistroEstacion) : Estacion {
        return {
            tabla: "ctp",
            id_externo: estacion.id_estacion,
            nombre: estacion.nombre,
            geom: {
                type: "Point",
                coordinates: [parseFloat(estacion.longitud), parseFloat(estacion.latitud)]
            },
            has_obs: (estacion.en_actividad == "1") ? true : false,
            ubicacion: estacion.descripcion,
            tipo: "A",
            automatica: true,
            URL: this.config.url,
            propietario: "Comisión Trinacional para el Desarrollo de la Cuenca del Río Pilcomayo",
            real: true
        }
    }

    parseRegistroAltura(registro : RegistroAltura, series_id?: number) : Observacion {
        const ymd : number[] = registro.fecha.split("-").map(v => parseInt(v))
        const hms : number[] = registro.hora.split(":").map(v => parseInt(v))
        const timestart = new Date(ymd[0],ymd[1]-1,ymd[2],hms[0],hms[1],hms[2])
        return {
            timestart: timestart,
            timeend: timestart,
            valor: parseFloat(registro.altura),
            series_id: series_id
        }
    }

    parseRegistroPrecipitacion(registro : RegistroPrecipitaciones, series_id?: number) : Observacion {
        const ymd : number[] = registro.fecha.split("-").map(v => parseInt(v))
        const hms : number[] = registro.hora.split(":").map(v => parseInt(v))
        const timestart = new Date(ymd[0],ymd[1]-1,ymd[2],hms[0],hms[1],hms[2])
        return {
            timestart: timestart,
            timeend: timestart,
            valor: parseFloat(registro.precipitacion),
            series_id: series_id
        }
    }

    async getSites(filter : SitesFilter) : Promise<Estacion[]> {
        if(!this.tokenAuth) {
            await this.login()
        }
        let estaciones : Estacion[] = []
        if(filter.id_externo) {
            if(Array.isArray(filter.id_externo)) {
                for(const id_externo of filter.id_externo) {
                    const data = await this.listEstaciones(id_externo)
                    estaciones.push(...data.estaciones.map(e => this.parseEstacion(e)))
                }
            } else {
                const data = await this.listEstaciones(filter.id_externo)
                estaciones.push(...data.estaciones.map(e => this.parseEstacion(e)))
            }
        } else {
            const data = await this.listEstaciones()
            estaciones.push(...data.estaciones.map(e => this.parseEstacion(e)))
        }
        // busca ids
        for(const estacion of estaciones) {
            const matches = await crud_estacion.read({tabla:estacion.tabla, id_externo:estacion.id_externo})
            if(matches.length) {
                estacion.id = matches[0].id
            }
        }
        return estaciones
    }

    async getSeries(filter : {estacion_id?: number|number[], var_id?: number|number[]}= {}) : Promise<Serie[]> {
        if(!this.tokenAuth) {
            await this.login()
        }
        const variable_altura = await crud_var.read({id:2})
        const variable_precipitacion = await crud_var.read({id:27})
        const proc = await crud_proc.read({id:1})
        const unidades_metro = await crud_unidades.read({id:11})
        const unidades_milimetro = await crud_unidades.read({id:9})
        const estaciones = await this.getSites(filter)
        let series : Serie[] = []
        for(const estacion of estaciones) {
            const var_h_match = (filter.var_id) ? (Array.isArray(filter.var_id)) ? (filter.var_id.indexOf(2) >= 0) ? true : false :  (filter.var_id == 2) ? true : false : true
            if (var_h_match) {
                let serie_altura : Serie
                if(estacion.id) {
                    const matches = await crud_serie.read(
                        {
                            tipo:"puntual",
                            estacion_id: estacion.id,
                            var_id: 2,
                            proc_id: 1,
                            unit_id: 11
                        })
                    if(matches.length) {
                        serie_altura = matches[0]
                        this.series_map[serie_altura.id] = {
                            id_estacion: parseInt(estacion.id_externo),
                            variable: "altura",
                            var_id: 2,
                            estacion_id: estacion.id,
                            serie: matches[0]
                        }
                    }
                }
                if(!serie_altura) {
                    serie_altura = {
                        tipo: "puntual",
                        estacion: estacion,
                        var: variable_altura,
                        procedimiento: proc,
                        unidades: unidades_metro
                    }
                }
                series.push(serie_altura)
            }
            const var_p_match = (filter.var_id) ? (Array.isArray(filter.var_id)) ? (filter.var_id.indexOf(27) >= 0) ? true : false :  (filter.var_id == 27) ? true : false : true
            if (var_p_match) {
                let serie_precipitacion : Serie
                if(estacion.id) {
                    const matches = await crud_serie.read(
                        {
                            tipo:"puntual",
                            estacion_id: estacion.id,
                            var_id: 27,
                            proc_id: 1,
                            unit_id: 9
                        })
                    if(matches.length) {
                        serie_precipitacion = matches[0]
                        this.series_map[serie_precipitacion.id] = {
                            id_estacion: parseInt(estacion.id_externo),
                            variable: "precipitacion",
                            var_id: 27,
                            estacion_id: estacion.id,
                            serie: matches[0]
                        }
                    }
                }
                if(!serie_precipitacion) {
                    serie_precipitacion = {
                        tipo: "puntual",
                        estacion: estacion,
                        var: variable_precipitacion,
                        procedimiento: proc,
                        unidades: unidades_milimetro
                    }
                }
                series.push(serie_precipitacion)
            }
        }
        return series
    }

    async getOneSerie(series_id : number, year : number, month : number, day? : number) : Promise<Observacion[]|undefined> {
        if(!this.tokenAuth) {
            await this.login()
        }
        if(!this.series_map[series_id]) {
            console.error(`series_id=${series_id} missing in series map. Run getSeries`)
            return
        }
        let observaciones : Observacion[]
        if(this.series_map[series_id].variable == "altura") {
            const data = await this.getAlturas(this.series_map[series_id].id_estacion,year,month,day)
            observaciones = data.alturas.map(o => this.parseRegistroAltura(o, series_id))
        } else if(this.series_map[series_id].variable == "precipitacion") {
            const data = await this.getPrecipitaciones(this.series_map[series_id].id_estacion,year,month,day)
            observaciones = data.precipitaciones.map(o => this.parseRegistroPrecipitacion(o, series_id))
        }
        return observaciones
    }

    getDateParams(timestart : Date, timeend : Date) : {year: number, month: number, day?: number} {
        if(!timestart) {
            throw new Error("Missing timestart")
        }
        if(!timeend) {
            throw new Error("Missing timeend")
        }
        const year = timestart.getFullYear()
        const month = timestart.getMonth() + 1
        const day_ = timestart.getDate()
        if(timeend.getFullYear() != year || timeend.getMonth() + 1 != month) {
            throw new Error("Requested period must be within a calendar month")
        }
        const day = (timeend.getDate() == day_) ? day_ : undefined
        return {
            year,
            month,
            day
        }
    }

    getSeriesIdList(series_id? : number|number[], var_id? : number|number[], estacion_id? : number|number[], id_externo? : string|string[]) : number[] {
        let series_id_list : number []
        if(series_id) {
            if(Array.isArray(series_id)) {
                series_id_list = series_id
            } else {
                series_id_list = [series_id]
            }
        } else {
            for(const [key, mapped_serie] of Object.entries(this.series_map)) {
                if(var_id) {
                    if(Array.isArray(var_id)) {
                        if(var_id.indexOf(mapped_serie.var_id) < 0) {
                            continue
                        }
                    } else {
                        if(mapped_serie.var_id != var_id) {
                            continue
                        }
                    }
                }
                if(estacion_id) {
                    if(Array.isArray(estacion_id)) {
                        if(estacion_id.indexOf(mapped_serie.estacion_id) < 0) {
                            continue
                        }
                    } else {
                        if(mapped_serie.estacion_id != estacion_id) {
                            continue
                        }
                    }
                }
                if(id_externo) {
                    if(Array.isArray(id_externo)) {
                        if(id_externo.indexOf(mapped_serie.id_estacion.toString()) < 0) {
                            continue
                        }
                    } else {
                        if(mapped_serie.id_estacion.toString() != id_externo) {
                            continue
                        }
                    }
                }
                series_id_list.push(parseInt(key))
            }
        }
        return series_id_list

    }

    async get(filter: GetFilter) : Promise<Observacion[]> {
        const dateParams = this.getDateParams(filter.timestart, filter.timeend)
        const series_id_list = this.getSeriesIdList(filter.series_id, filter.var_id, filter.estacion_id, filter.id_externo)
        if(!series_id_list.length) {
            console.error("Series not found for the requested filter")
        }
        const series_id = series_id_list[0]
        const observaciones = await this.getOneSerie(series_id, dateParams.year, dateParams.month, dateParams.day)
        
        //filter by date
        return observaciones.filter(o=>{
            if(o.timestart.getTime() < filter.timestart.getTime() || o.timeend.getTime() > filter.timeend.getTime()) {
                return false
            }
            return true
        })        
    }

    async update(filter: GetFilter) : Promise<Observacion[]> {
        const observaciones = await this.get(filter)
        return new crud_observaciones(observaciones).create()
    }

    async getMulti(filter : GetFilter) : Promise<Array<Serie>> {
        
        const dateParams = this.getDateParams(filter.timestart, filter.timeend)

        const series_id_list = this.getSeriesIdList(filter.series_id, filter.var_id, filter.estacion_id, filter.id_externo)

        let series : Serie[] = []
        for(const series_id of series_id_list) {
            const observaciones = await this.getOneSerie(series_id, dateParams.year, dateParams.month, dateParams.day)
            if(observaciones && observaciones.length) {
                series.push({
                    ...this.series_map[series_id].serie,
                    observaciones: observaciones
                })
            } else {
                console.error("Data not found for series_id " + series_id)
            }
        }
        return series
    }
}
