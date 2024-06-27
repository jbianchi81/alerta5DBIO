const sprintf = require('sprintf-js').sprintf
// const accessor_utils = require('../accessor_utils')
// const path = require('path');
// const {observaciones, corrida} = require('../CRUD')
const axios = require('axios')
const AbstractAccessorEngine = require('./abstract_accessor_engine').AbstractAccessorEngine
const {DateFromDateOrInterval} = require('../timeSteps')
const {serie: Serie, observacion: Observacion, observaciones: Observaciones} = require('../CRUD')
const {arrayOfObjectsToCSV} = require('../utils')


const internal = {}

/**
 * Accessor client for hidrowebservice (https://www.ana.gov.br/hidrowebservice/swagger-ui/index.html)
 */
internal.Client = class extends AbstractAccessorEngine {

    static _get_is_multiseries = true
    
    default_config = {
        url: "https://www.ana.gov.br/hidrowebservice",
        password: "my_access_token",
        user: "my_client_id",
        tabla_id: "red_ana_hidro",
        pais: "Brasil",
        var_mapping: {
            4: {
                property_name: "Vazao_Adotada"
            },
            27: {
                property_name: "Chuva_Adotada"
            },
            2: {
                property_name: "Cota_Adotada",
                factor: 0.01
            }
        }    
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
            }
        )
        console.debug(sprintf("GET %03d %s", response.status, response.config.url))
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

    /**
     * Make GET HidroInventarioEstacoes request to hidrowebservice.
     * 
     * Notes:
     * - List of matching stations is stored as an array in .items property
     * - The schema of the result is exemplified in this.HidroInventarioEstacoesResponseExample  
     * @param {string} station_id 
     * @param {string} start_date 
     * @param {string} end_date 
     * @param {string} state_id 
     * @param {int} basin_id 
     * @param {string} url 
     * @param {string} token 
     * @returns {Object} HidroInventarioEstacoes response data
     */
    async getHidroInventarioEstacoes(
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
        console.debug(sprintf("GET %03d %s", response.status, response.config.url))
        this.last_response = response
        return response.data  
    }
    
    /**
     * get metadata from source
     * 
     * Notes: 
     *  - no units information at source
     *  - it still has to be clarified how to extract data availability information (which variables are available, and for which period). How to interpret Data_Periodo_* and Tipo_Estacao_* values
     * @param {*} filter 
     * @param {string} filter.id_externo
     * @param {integer|Array<integer>} filter.var_id
     * @param {*} options 
     * @param {string} options.raw_output_file
     * @returns 
     */
    async getSeries(filter={}, options={}) {
        if(options.skip_new) {
            return Serie.read({
                id: filter.id ?? filter.series_id,
                tipo: "puntual",
                var_id: filter.var_id,
                proc_id: filter.proc_id ?? 1,
                unit_id: filter.unit_id,
                id_externo: filter.id_externo,
                tabla_id: filter.tabla_id ?? filter.tabla ?? this.config.tabla_id               
            })
        }
        const token = await this.getToken()
        var station_id = filter.id_externo ?? this.config.station_id ?? this.config.id_externo
        filter.var_id = (filter.var_id && !Array.isArray(filter.var_id)) ? [filter.var_id] : filter.var_id
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
        var items = []
        if(station_id && Array.isArray(station_id)) {
            for(var id of station_id) {
                const data = await this.getHidroInventarioEstacoes(
                    id,
                    start_date,
                    end_date,
                    state_id,
                    basin_id,
                    undefined,
                    token
                )
                items.push(...data.items)
            }
        } else {
            const data = await this.getHidroInventarioEstacoes(
                station_id,
                start_date,
                end_date,
                state_id,
                basin_id,
                undefined,
                token
            )
            items.push(...data.items)
        }
        if(!items.length) {
            console.warn("Nothing found at hidrowebservice.getHidroInventarioEstacoes")
            return []
        }
        if(options.raw_output_file) {
            arrayOfObjectsToCSV(items,options.raw_output_file)
        }
        const series = []
        for(var item of items) {
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
                distrito: item.UF_Estacao,
                real: true,
                automatica: (item.Tipo_Estacao_Telemetrica == "1") ? true : false
            }
            // add monthly, daily, 6-hourly, 3-hourly, hourly and instantaneous precipitation series. Units: millimeters
            for(var var_id of [1,27,31,34,41,91]) {
                if(filter.var_id && filter.var_id.indexOf(var_id) < 0) {
                    continue
                }
                series.push(
                    new Serie({
                        estacion: estacion,
                        var_id: var_id,
                        proc_id: 1,
                        unit_id: (var_id == 1) ? 22 : 9,
                        date_range: {
                            // assuming Brasilia timezone -03
                            timestart:(item.Data_Periodo_Pluviometro_Inicio)  ? new Date(item.Data_Periodo_Pluviometro_Inicio + "-03:00") : undefined,
                            timeend: (item.Data_Periodo_Pluviometro_Fim) ? new Date(item.Data_Periodo_Pluviometro_Fim + "-03:00") : undefined
                        }
                    })
                )
            }
            // add monthly, daily, hourly and instantaneous discharge, Units: m3/s
            for(var var_id of [4,40,48,87]) {
                if(filter.var_id && filter.var_id.indexOf(var_id) < 0) {
                    continue
                }
                series.push(
                    new Serie({
                        estacion: estacion,
                        var_id: var_id,
                        proc_id: 1,
                        unit_id: 10,
                        date_range: {
                            // assuming Brasilia timezone -03
                            timestart:(item.Data_Periodo_Desc_liquida_Inicio)  ? new Date(item.Data_Periodo_Desc_liquida_Inicio + "-03:00") : undefined,
                            timeend: (item.Data_Periodo_Desc_Liquida_Fim) ? new Date(item.Data_Periodo_Desc_Liquida_Fim + "-03:00") : undefined
                        }
                    })
                )
            }
            // add monthly, daily, hourly and instantaneous gauge height. Units: meter
            for(var var_id of [2,33,39,85]) {
                if(filter.var_id && filter.var_id.indexOf(var_id) < 0) {
                    continue
                }
                series.push(
                    new Serie({
                        estacion: estacion,
                        var_id: var_id,
                        proc_id: 1,
                        unit_id: 11,
                        date_range: {
                            // assuming Brasilia timezone -03
                            timestart:(item.Data_Periodo_Escala_Inicio)  ? new Date(item.Data_Periodo_Escala_Inicio + "-03:00") : undefined,
                            timeend: (item.Data_Periodo_Escala_Fim) ? new Date(item.Data_Periodo_Escala_Fim + "-03:00") : undefined
                        }
                    })
                )
            }
        }
        return series
    }

    async updateSeries(filter={},options={}) {
        const series = await this.getSeries(filter,options)
        const created_series = await Serie.create(series, {all: true})
        return created_series  
    }

    /**
     * Make a GET HidroinfoanaSerieTelemetricaAdotada request to hidrowebservice 
     * 
     * Notes:
     * - Response schema is exemplified in this.HidroinfoanaSerieTelemetricaAdotadaResponseExample
     * @param {string} station_id 
     * @param {string} tipo_filtro_data one of: "DATA_LEITURA", "DATA_ULTIMA_ATUALIZACAO"
     * @param {string} data_de_busca - format yyyy-mm-dd 
     * @param {string} range_intervalo_de_busca - one of "MINUTO_5", "MINUTO_10", "MINUTO_15", "MINUTO_30", "HORA_01", "HORA_02", "HORA_03", "HORA_04", "HORA_05", "HORA_06", "HORA_07", "HORA_08", "HORA_09", "HORA_10", "HORA_11", "HORA_12", "HORA_13", "HORA_14", "HORA_15", "HORA_16", "HORA_17", "HORA_18", "HORA_19", "HORA_20", "HORA_21", "HORA_22", "HORA_23", "HORA_24",
     * @param {string} [url] - the hidrowebservice base url - defaults to this.config.url
     * @param {string} [token] - the authorization token - defaults to this.token 
     * @returns {Object} HidroinfoanaSerieTelemetricaAdotada response data
     */
    async getHidroinfoanaSerieTelemetricaAdotada(
        station_id,
        tipo_filtro_data = "DATA_LEITURA",
        data_de_busca,
        range_intervalo_de_busca = "HORA_24",
        url,
        token
    ) {
        url = url ?? this.config.url
        token = token ?? this.token
        const response = await axios.get(
            sprintf("%s/%s", url, "EstacoesTelemetricas/HidroinfoanaSerieTelemetricaAdotada/v1"),
            {
                params: {
                    "Código da Estação": station_id,
                    "Tipo Filtro Data": tipo_filtro_data,
                    "Data de Busca (yyyy-MM-dd)": data_de_busca,
                    "Range Intervalo de busca": range_intervalo_de_busca
                },
                headers: {
                    "Authorization": sprintf("Bearer %s", token)
                }
            }
        )
        console.debug(sprintf("GET %03d %s", response.status, response.config.url))
        this.last_response = response
        return response.data  
    }

    async get(filter={}, options={}) {
        if(!filter.timestart || !filter.timeend) {
            throw("missing filter.timestart and/or filter.timeend")
        }
        const token = await this.getToken()
        const series = await Serie.read({
            id: filter.id ?? filter.series_id,
            tipo: "puntual",
            var_id: filter.var_id,
            proc_id: filter.proc_id ?? 1,
            unit_id: filter.unit_id,
            id_externo: filter.id_externo,
            tabla_id: filter.tabla_id ?? filter.tabla ?? this.config.tabla_id
        })
        if(!series) {
            console.warn("No series found in database")
            return []
        }
        if(!Array.isArray(series)){
            series = [series]
        }
        if(!series.length) {
            console.warn("No series found in database")
            return []
        }
        const observaciones = []
        const group_by_estacion = {}
        for(var serie of series) {
            if(serie.estacion.id_externo.toString() in group_by_estacion) {
                group_by_estacion[serie.estacion.id_externo.toString()].push(serie)
            } else {
                group_by_estacion[serie.estacion.id_externo.toString()] = [serie]
            }
        }
        for(var id_externo of Object.keys(group_by_estacion)) {
            const estacion_series = group_by_estacion[id_externo]
            var date = filter.timestart
            const data_items = []
            while(date < filter.timeend) {
                const data = await this.getHidroinfoanaSerieTelemetricaAdotada(
                    id_externo,
                    "DATA_LEITURA",
                    date.toISOString().substring(0,10),
                    "HORA_24",
                    undefined,
                    token
                )
                data_items.push(...data.items)
                date.setUTCDate(date.getUTCDate() + 1)
            }
            for(var serie of estacion_series) {
                if(serie.var.id in this.config.var_mapping) {
                    const obs = this.dataItemsToObs(
                        data_items,
                        this.config.var_mapping[serie.var.id].property_name,
                        serie.id,
                        this.config.var_mapping[serie.var.id].factor
                    )
                    serie.setObservaciones(
                        obs
                    )

                    observaciones.push(...obs)
                }
            }            
        }
        if(options.return_series) {
            return series
        } else {
            return observaciones
        }
    }

    async update(filter={}, options={}) {
        const series = await this.get(
            filter, 
            {
                return_series: true
            }
        )
        const observaciones = []
        for(var serie of series) {
            if(serie.observaciones) {
                const created_obs = await serie.observaciones.create()
                serie.setObservaciones(created_obs)
                observaciones.push(...created_obs)
            }
        }
        if(options.return_series) {
            return series
        }
        return observaciones
    }

    dataItemsToObs(
        data_items,
        property_name,
        series_id,
        factor
    ) {
        const observaciones = []
        for(var item of data_items) {
            if(!property_name in item) {
                console.warn("property " + property_name + " missing in item, skipping")
                continue
            }
            if(!item[property_name]) {
                continue
            }
            observaciones.push(new Observacion({
                timestart: new Date(item.Data_Hora_Medicao + "-03:00"),
                timeend: new Date(item.Data_Hora_Medicao + "-03:00"),
                valor: (factor) ? parseFloat(item[property_name]) * factor : parseFloat(item[property_name]),
                series_id: series_id,
                tipo: "puntual"
            }))
        }
        return new Observaciones(observaciones).removeDuplicates()
    }    

    HidroInventarioEstacoesResponseExample = {
        "status": "OK",
        "code": 200,
        "message": "Sucesso",
        "items": [
          {
            "Altitude": "951.0",
            "Area_Drenagem": null,
            "Bacia_Nome": "RIO URUGUAI",
            "Codigo_Adicional": null,
            "Codigo_Operadora_Unidade_UF": null,
            "Data_Periodo_Climatologica_Fim": null,
            "Data_Periodo_Climatologica_Inicio": null,
            "Data_Periodo_Desc_Liquida_Fim": null,
            "Data_Periodo_Desc_liquida_Inicio": null,
            "Data_Periodo_Escala_Fim": null,
            "Data_Periodo_Escala_Inicio": null,
            "Data_Periodo_Piezometria_Fim": null,
            "Data_Periodo_Piezometria_Inicio": null,
            "Data_Periodo_Pluviometro_Fim": null,
            "Data_Periodo_Pluviometro_Inicio": "2024-04-01 00:00:00.0",
            "Data_Periodo_Qual_Agua_Fim": null,
            "Data_Periodo_Qual_Agua_Inicio": null,
            "Data_Periodo_Registrador_Chuva_Fim": null,
            "Data_Periodo_Registrador_Chuva_Inicio": null,
            "Data_Periodo_Registrador_Nivel_Fim": null,
            "Data_Periodo_Registrador_Nivel_Inicio": null,
            "Data_Periodo_Sedimento_Inicio": null,
            "Data_Periodo_Sedimento_fim": null,
            "Data_Periodo_Tanque_Evapo_Fim": null,
            "Data_Periodo_Tanque_Evapo_Inicio": null,
            "Data_Periodo_Telemetrica_Fim": null,
            "Data_Periodo_Telemetrica_Inicio": "2024-04-01 00:00:00.0",
            "Data_Ultima_Atualizacao": "2024-06-06 00:00:00.0",
            "Estacao_Nome": "CGH FRASCAL JUSANTE",
            "Latitude": "-26.9642",
            "Longitude": "-50.5067",
            "Municipio_Codigo": "23153000",
            "Municipio_Nome": "SANTA CECÍLIA",
            "Operadora_Codigo": "1246",
            "Operadora_Sigla": "FRASCAL",
            "Operadora_Sub_Unidade_UF": "1",
            "Operando": "1",
            "Responsavel_Codigo": "1246",
            "Responsavel_Sigla": "FRASCAL",
            "Responsavel_Unidade_UF": "23",
            "Rio_Codigo": null,
            "Rio_Nome": "N/A",
            "Sub_Bacia_Codigo": "71",
            "Sub_Bacia_Nome": "RIO CANOAS",
            "Tipo_Estacao": "Pluviometrica",
            "Tipo_Estacao_Climatologica": "0",
            "Tipo_Estacao_Desc_Liquida": "0",
            "Tipo_Estacao_Escala": "0",
            "Tipo_Estacao_Piezometria": "0",
            "Tipo_Estacao_Pluviometro": "1",
            "Tipo_Estacao_Qual_Agua": "0",
            "Tipo_Estacao_Registrador_Chuva": "0",
            "Tipo_Estacao_Registrador_Nivel": "0",
            "Tipo_Estacao_Sedimentos": "0",
            "Tipo_Estacao_Tanque_evapo": "0",
            "Tipo_Estacao_Telemetrica": "1",
            "Tipo_Rede_Basica": "0",
            "Tipo_Rede_Captacao": "6",
            "Tipo_Rede_Classe_Vazao": "0",
            "Tipo_Rede_Curso_Dagua": "0",
            "Tipo_Rede_Energetica": "1",
            "Tipo_Rede_Estrategica": "0",
            "Tipo_Rede_Navegacao": "0",
            "Tipo_Rede_Qual_Agua": "0",
            "Tipo_Rede_Sedimentos": "0",
            "UF_Estacao": "SC",
            "UF_Nome_Estacao": "SANTA CATARINA",
            "codigobacia": "7",
            "codigoestacao": "2650056"
          }
        ]
    }

    HidroinfoanaSerieTelemetricaAdotadaResponseExample = {
        "status": "OK",
        "code": 200,
        "message": "Sucesso",
        "items": [
            {
                "Chuva_Adotada": "0.00",
                "Chuva_Adotada_Status": "0",
                "Cota_Adotada": "276.00",
                "Cota_Adotada_Status": "0",
                "Data_Atualizacao": "2024-06-24 00:55:04.273",
                "Data_Hora_Medicao": "2024-06-24 00:00:00.0",
                "Vazao_Adotada": "340.44",
                "Vazao_Adotada_Status": "0",
                "codigoestacao": "76300000"
            }
        ]
    }

}

module.exports = internal