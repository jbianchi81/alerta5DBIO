import { AbstractAccessorEngine, AccessorEngine, ObservacionesFilter, SeriesFilter, SitesFilter } from './abstract_accessor_engine'
// @ts-ignore
import { fetchData, filterSeries, filterSites } from '../accessor_utils'
import { Observacion, Serie} from '../a5_types'
// @ts-ignore
import { estacion as CrudEstacion, serie as CrudSerie, red as CrudRed, observaciones as CrudObservaciones, unidades as CrudUnidades } from '../CRUD'
import { Variable as CrudVariable } from 'a5base/variable'
import { Location } from '../a5_types'

type Config = {
     url: string
     token: string
     tabla_id: string
     var_map?: Record<string, string>
     dt_map?: Record<string, string>
     unit_map?: Record<string, number>
     fenomeno_map?: Record<string, FenomenoMap>
     estaciones_map?: Record<string, EstacionMap>
     TipoEstacao?: "convencional" | "automatica" | "todas"

}

type Estacao = {
    codEstacao: string
    nome : string
    situacao : string
    tipo : string
    estado: string
    latitude : string
    longitude : string
    altitudeEmMetros : string
    entidadeResponsavel : string
    dataInicioOperacao : string
    dataFimOperacao : string
    codOscar : string
    codWsi : string
}

type GetEstacoesResponse = {
    query: any
    params: any
    data: Estacao[]
    totalEstacoes : number
}

type Precipitacion = {
    fecha:       string
    precipitacion_mm:  number
    medicion_realizada: boolean
}

type Fenomeno = {
    codFenomeno : string
    nome : string
    formaDeObtencao : string
    periodicidade  : string
    unidade  : string
    classe  : string
}
    
type Atributo = {    
    periodosDisponiveis : string[]
    fenomenosPorPeriodo: Record<string, Fenomeno[]>
}

type GetAtributosResponse = {
    query : any
    params : {
        codEstacao : string
    }
    data: {
        classesDaEstacao: string[]
        atributos: Record<string, Atributo>
    }
}

type Medicao = [number, number | null]


type GetFenomenoResponse = {
    query: any
    params: any
    data: {
        data : Medicao[]
    }
    nome : string
    codEstacao : string
    periodicidade : string
    unidade : string
}

type TipoEstacao = "convencional" | "automatica" | "todas"

type FenomenoMap = {
    var_id: number
    unit_id: number
}

type EstacionMap = {
    estacion_id: number
    series: Record<string, number> // maps 'codFenomeno' to series_id
}

interface GetEstacoesParams {
    tipo: TipoEstacao
    estado?: string
    regiao?: string
}

export class Client extends AbstractAccessorEngine implements AccessorEngine {

    static _get_is_multiseries = false

    url : string
    token : string
    tabla_id : string
    TipoEstacao : "convencional" | "automatica" | "todas"
    var_map : Record<string, string>     // maps 'classe' to a5 VariableName
    dt_map : Record<string, string>      // maps 'Periodicidade' to posgresql intervals
    unit_map : Record<string, number>    // maps 'unidades' to a5 unit_id
    fenomeno_map : Record<string, FenomenoMap>   // maps 'codFenomeno' to a5 var_id + unit_id
    estaciones_map : Record<string, EstacionMap>  // maps 'codEstacao' + 'codFenomeno' to a5 series_id
    // map attribute names to VariableNames

    defaults = {
        url: "https://api-bndmet.decea.mil.br/v1",
        tabla_id: "inmet",
        var_map: {
            "Precipitação": "Precipitation"
        },
        dt_map: {
            "Horário": "01:00:00",
            "Diario": "24:00:00",
            "Mensal": "1 mon"
        },
        unit_map: {
            "mm": 9
        },
        fenomeno_map: {
            "I175": {
                var_id: 31,
                unit_id: 9
            },
            "I006": {
                var_id: 1,
                unit_id: 22
            },
        }
    }

    red = {
        nombre: "inmet",
        tabla_id: "inmet",
        public: false,
        public_his_plata: false
    }

    constructor(config : Config) {
        super(config)
        this.url = config.url || this.defaults.url
        this.token = config.token
        this.tabla_id = config.tabla_id || this.defaults.tabla_id
        this.var_map = config.var_map || this.defaults.var_map
        this.dt_map = config.dt_map || this.defaults.dt_map
        this.unit_map = config.unit_map || this.defaults.unit_map
        this.fenomeno_map = config.fenomeno_map || this.defaults.fenomeno_map || {}
        this.estaciones_map = config.estaciones_map || {}
        this.TipoEstacao = config.TipoEstacao || "automatica"
    }

    headers() {
        return {
            "accept": "application/json",
            "x-api-key": this.token
        }
    }

    async createRed() {
        return CrudRed.create({
            ...this.red,
            tabla_id: this.tabla_id
        })
    }

    // api calls

    async get_estacoes(
        tipo : TipoEstacao,
        estado? : string,
        regiao?: string
    ) : Promise<GetEstacoesResponse> {
        const params : GetEstacoesParams = {
            tipo: tipo
        }
        if(estado) { 
            params.estado = estado
        }
        if(regiao) {
            params.regiao = regiao
        }
        return fetchData(
            `${this.url}/estacoes`, 
            {
                params: params, 
                headers: this.headers()
            }
        )
    }

    async getAtributos(
        codEstacao : string,
        periodo? : "mensal" | "diario" | "horario",
        agruparPor: "classe" | "intervalo" = "classe"
    ) : Promise<GetAtributosResponse> {
        return fetchData(
            `${this.url}/estaciones/${codEstacao}/atributos`, 
            {
                params: {
                    periodo: periodo,
                    agruparPor: agruparPor
                }, 
                headers: this.headers()
            }
        )
    }

    async getFenomeno(
        codEstacao : string,
        codFenomeno : string,
        dataInicio : string,
        dataFinal : string
    ) : Promise<GetFenomenoResponse> {
        return fetchData(
            `${this.url}/estaciones/${codEstacao}/fenomenos/${codFenomeno}`, 
            {
                params: {
                    dataInicio: dataInicio,
                    dataFinal: dataFinal
                }, 
                headers: this.headers()
            }
        )
    }

    // parsers

    parseEstacion(e : Estacao) : CrudEstacion {
        return new CrudEstacion({
            tabla: this.tabla_id,
            nombre: e.nome,
            geom: {
                type: "Point",
                coordinates: [parseFloat(e.longitude), parseFloat(e.latitude)]
            },
            propietario: e.entidadeResponsavel,
            id_externo: e.codEstacao,
            distrito: e.estado,
            pais: "Brasil",
            tipo: (e.tipo == "Automatica") ? "A" : "M",
            automatica: (e.tipo == "Automatica") ? true : false,
            altitud: parseFloat(e.altitudeEmMetros),
            real: true,
            habilitar: true,
            observaciones: `WSI=${e.codWsi}`
        })
    }

    // mappers

    async mapFenomeno(fenomeno : Fenomeno) : Promise<FenomenoMap | void> {
        if(!(fenomeno.classe in this.var_map)) {
            console.warn(`Clase de variable ${fenomeno.classe} no mapeado`)
            return
        }
        if(fenomeno.unidade in this.unit_map) {
            var unit_id = this.unit_map[fenomeno.unidade]
        } else {
            const unidades = await CrudUnidades.read({abrev: fenomeno.unidade})
            if(!unidades.length) {
                console.warn(`Unidades '${fenomeno.unidade}' no encontradas`)
                return
            }
            var unit_id = unidades[0].id
        }
        if(!(fenomeno.periodicidade in this.dt_map)) {
            console.warn(`Periodicidad '${fenomeno.periodicidade}' no mapeada`)
            return
        }
        const variables = await CrudVariable.read({
            VariableName: this.var_map[fenomeno.classe],
            timeSupport: this.dt_map[fenomeno.periodicidade]
        })
        if(!variables.length) {
            console.warn(`No se encontró variable con VariableName=${this.var_map[fenomeno.classe]} y timeSupport=${this.dt_map[fenomeno.periodicidade]}`)
            return
        }
        if(variables.length > 1) {
            console.warn(`Se encontró más de una variable con VariableName=${this.var_map[fenomeno.classe]} y timeSupport=${this.dt_map[fenomeno.periodicidade]}. Se utilizará la primera`)
        }
        return {
            var_id: variables[0].id!,
            unit_id: unit_id
        }
    }

    async parseSeries(
        get_attr_response : GetAtributosResponse,
        estacion_id : number
    ) : Promise<Serie[]> {
        const series = []
        for( const [attr_name, atributo] of Object.entries(get_attr_response.data.atributos)) {
            for(const [periodo, fenomeno_list] of Object.entries(atributo.fenomenosPorPeriodo)) {
                for(const fenomeno of fenomeno_list) {
                    const fenomeno_map = await this.mapFenomeno(fenomeno)
                    if(fenomeno_map) {
                        this.fenomeno_map[fenomeno.codFenomeno] = fenomeno_map
                        console.debug(`Mapped codFenomeno=${fenomeno.codFenomeno}`)
                        const serie = new CrudSerie({
                            estacion: {id: estacion_id},
                            var: {id: fenomeno_map.var_id},
                            unit_id: {id: fenomeno_map.unit_id},
                            proc_id: {id: 1}
                        })
                        await serie.getId()
                        this.estaciones_map[get_attr_response.params.codEstacao].series[fenomeno.codFenomeno] = serie.id
                        series.push(serie)
                    } else {
                        console.warn(`Unable to map codFenomeno=${fenomeno.codFenomeno}`)
                    }
                }
            }
        }
        return series
    }

    // accessor interface

    async getSites(filter : SitesFilter={}) : Promise<Location[]> {
        const response = await this.get_estacoes(this.TipoEstacao || "automatica")
        const estaciones = response.data.map(e => this.parseEstacion(e))
        for(const e of estaciones) {
            await e.getEstacionId()
            this.estaciones_map[e.id_externo] = { estacion_id: e.id!, series: {}}
        }
        return estaciones
    }
    
    async getSeriesOfSite(estacion_id : number, filter : SeriesFilter={}) : Promise<Serie[]> {
        if(!Object.keys(this.estaciones_map).length) {
            await this.getSites()
        }
        for(const [codEstacao, estacion_map] of Object.entries(this.estaciones_map)) {
            if(estacion_id != estacion_map.estacion_id) {
                continue
            }
            const get_attr_response = await this.getAtributos(codEstacao)
            const series = await this.parseSeries(get_attr_response, estacion_id)
            return filterSeries(series, filter)
        }
        console.warn(`El id de estación ${estacion_id} no está mapeado`)
        return []
    }


    async getSeries(filter : SeriesFilter={}) : Promise<Serie[]> {
        var estaciones = await this.getSites()
        estaciones = filterSites(estaciones, filter)
        const series = []
        for(const e of estaciones) {
            const series_of_site = await this.getSeriesOfSite(e.id!, filter)
            series.push(...series_of_site)
        }
        return series
    }    

    findSerie(estacion_id : number, series_id : number) : [string, string] {
        if(!this.estaciones_map) {
            throw new Error("estaciones_map not set")
        }
        for(const [codEstacao, mapping] of Object.entries(this.estaciones_map)) {
            if(mapping.estacion_id == estacion_id) {
                for(const [codFenomeno, mapped_series_id] of Object.entries(mapping.series)) {
                    if(mapped_series_id == series_id) {
                        return [codEstacao, codFenomeno]
                    }
                }
            }
        }
        throw new Error(`estacion_id: ${estacion_id}, series_id: ${series_id} not found in series mapping`)
    }

    // async get_one(estacion_id? : number, desde? : string, hasta? : string, series_id? : number) : Promise<Observacion[]> {
    //     const mapping = this.findSerie(estacion_id, series_id)
    //     const response = await this.get_precipitaciones(mapping.id_externo, desde, hasta)
    //     return response.data.precipitaciones.map(p => this.parsePrecipitacion(p, mapping.series_id)).filter(o=>o != null)
    // }

    // async get(filter : ObservacionesFilter, options : {} ={}) : Promise<Observacion[]> {
    //     if(!this.series_map) {
    //         await this.getSeriesMap()
    //     }
    //     const desde = (filter.timestart) ? formatLocalDate(filter.timestart) : undefined
    //     const hasta = (filter.timeend) ? formatLocalDate(filter.timeend) : undefined

    //     if(filter.estacion_id) {
    //         checkMaxDays(desde, hasta, 365)
    //         const observaciones : Observacion[] = []
    //         if(Array.isArray(filter.estacion_id)) {
    //             for(const estacion_id of filter.estacion_id) {
    //                 const obs = await this.get_one(estacion_id, desde, hasta)
    //                 observaciones.push(...obs)
    //             }
    //         } else {
    //             const obs = await this.get_one(filter.estacion_id, desde, hasta)
    //             observaciones.push(...obs)
    //         }
    //         return observaciones
    //     } 
    //     if(filter.series_id) {
    //         checkMaxDays(desde, hasta, 365)
    //         const observaciones : Observacion[] = []
    //         if(Array.isArray(filter.series_id)) {
    //             for(const series_id of filter.series_id) {
    //                 const obs = await this.get_one(undefined, desde, hasta, series_id)
    //                 observaciones.push(...obs)
    //             }
    //         } else {
    //             const obs = await this.get_one(undefined, desde, hasta, filter.series_id)
    //             observaciones.push(...obs)
    //         }
    //         return observaciones
    //     }
    //     // all stations
    //     checkMaxDays(desde, hasta, 7)
    //     const response = await this.get_precipitaciones_global(desde, hasta)
    //     const observaciones : Observacion[] = response.data.precipitaciones.map(p => this.parsePrecipitacion(p)).filter(o => o != null)
    //     return observaciones
    // }

    // async update(filter : ObservacionesFilter, options : {} ={}) : Promise<Observacion[]> {
    //     const results = await this.get(filter)
    //     return CrudObservaciones.create(results)
    // }
}

function formatLocalDate(date: Date): string {
  const pad = (n: number) => String(n).padStart(2, "0");

  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
}

function checkMaxDays(desde? : string, hasta? : string, maxDays : number=365 * 5) : void {
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