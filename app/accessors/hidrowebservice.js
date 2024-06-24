const sprintf = require('sprintf-js').sprintf
// const accessor_utils = require('../accessor_utils')
// const path = require('path');
// const {observaciones, corrida} = require('../CRUD')
const axios = require('axios')
const AbstractAccessorEngine = require('./abstract_accessor_engine').AbstractAccessorEngine
const {DateFromDateOrInterval} = require('../timeSteps')
const {serie: Serie} = require('../CRUD')

const internal = {}

/**
 * Accessor client for hidrowebservice (https://www.ana.gov.br/hidrowebservice/swagger-ui/index.html)
 */
internal.Client = class extends AbstractAccessorEngine {
    
    default_config = {
        url: "https://www.ana.gov.br/hidrowebservice",
        password: "my_access_token",
        user: "my_client_id",
        tabla_id: "red_ana_hidro",
        pais: "Brasil" 
    }

    constructor(config={}) {
        super(config)
        this.setConfig(config)
    }


    async OAUth(
        user,
        password,
        url
    ) {
        user = user ?? this.config.user
        password = password ?? this.config.password
        url = url ?? this.config.url
        const response = await axios.get(
            sprintf("%s/%s", this.config.url, "EstacoesTelemetricas/OAUth/v1"),
            {
                headers: {
                    Identificador: this.config.user,
                    Senha: this.config.password
                }
            })
        return response.data
    }

    async getToken() {
        const data = await this.OAUth()
        this.token = data.items.tokenautenticacao 
        return this.token
    }

    async test() {
        try {
            await this.getToken()
        } catch(e) {
            console.error(e)
            return false
        }
        return true
    }

    async getHidroInventarioEstaciones(
        station_id, // int, length 7
        start_date, // str - yyyy-mm-dd
        end_date, // str - yyyy-mm-dd
        state_id, // str, length 2
        basin_id, // int, length 1
        url, // str - base url of web service
        token // str - jwt
    ) {
        url = url ?? this.config.url
        token = token ?? this.token
        const response = await axios.get(
            sprintf("%s/%s", url, "EstacoesTelemetricas/HidroInventarioEstacoes/v1"),
            {
                params: {
                    "Código da Estação": station_id,
                    "Data Atualização Inicial (yyyy-MM-dd)": start_date,
                    "Data Atualização Final (yyyy-MM-dd)": end_date,
                    "Unidade Federativa": state_id,
                    "Código da Bacia": basin_id
                },
                headers: {
                    "Authorization": sprintf("Bearer %s", token)
                }
            }
        )
        return response.data  
    }
    
    async getSeries(filter={}, options={}) {
        const token = await this.getToken()
        var station_id = filter.id_externo ?? this.config.station_id ?? this.config.id_externo
        var start_date = filter.timestart ?? this.config.timestart
        start_date = (start_date) ? DateFromDateOrInterval(start_date).toISOString().substring(0,10) : undefined
        var end_date = filter.timeend ?? this.config.timeend
        end_date = (end_date) ? DateFromDateOrInterval(end_date).toISOString().substring(0,10) : undefined
        var state_id = filter.state_id ?? this.config.state_id
        var basin_id = filter.basin_id ?? this.config.basin_id
        if(!station_id && !state_id) {
            if(!basin_id) {
                throw("Missing either station_id, state_id or basin_id")
            }
        }
        const data = await this.getHidroInventarioEstaciones(
            station_id,
            start_date,
            end_date,
            state_id,
            basin_id,
            undefined,
            token
        )
        if(!data.items || !data.items.length) {
            console.warn("Nothing found at hidrowebservice.getHidroInventarioEstacoes")
            return []
        }
        const series = []
        for(var item of data.items) {
            const estacion = {
                altitud: parseFloat(item.Altitude),
                nombre: item.Estacao_Nome,
                geom: {
                    type: "Point",
                    coordinates: [
                        item.Longitude,
                        item.Latitude
                    ]
                },
                localidad: item.Municipio_Nome,
                rio: (item.Rio_Nome != "N/A") ? sprintf("%s - %s", item.Rio_Nome, item.Sub_Bacia_Nome) : item.Sub_Bacia_Nome,
                id_externo: item.codigoestacao,
                tabla: this.config.tabla_id,
                pais: this.config.pais,
                distrito: item.UF_Estacao 
            }
            if(item.Data_Periodo_Pluviometro_Inicio) {
                series.push(
                    new Serie({
                        estacion: estacion,
                        var: {
                            id: 31
                        },
                        procedimiento: {
                            id: 1
                        },
                        unidades: {
                            id: 9
                        }
                    })
                )
            }
            return series
        }
    }
}

module.exports = internal