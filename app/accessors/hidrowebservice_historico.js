const sprintf = require('sprintf-js').sprintf
const axios = require('axios')
const {serie: Serie, observacion: Observacion, observaciones: Observaciones} = require('../CRUD')
const hidrowebserviceClient = require("./hidrowebservice").Client
const setDateParts = require('../timeSteps').setDateParts

const internal = {}

/**
 * Accessor client for hidrowebservice (https://www.ana.gov.br/hidrowebservice/swagger-ui/index.html)
 */
internal.Client = class extends hidrowebserviceClient {
    
    static _get_is_multiseries = false
    
    default_config = {
        url: "https://www.ana.gov.br/hidrowebservice",
        password: "my_access_token",
        user: "my_client_id",
        tabla_id: "red_ana_hidro",
        pais: "Brasil",
        var_mapping: {
            40: {
                var_name: "Vazao",
                property_name: "Vazao"
            },
            1: {
                var_name: "Chuva",
                property_name: "Chuva",
                t_offset: {
                    hours: 9
                }
            },
            39: {
                var_name: "Cotas",
                property_name: "Cota",
                factor: 0.01
            }
        },
        tzString: "America/Sao_Paulo"
    }

    constructor(config={}) {
        super(config)
        this.setConfig(config)
    }

    getMappedVariableKey(var_name) {
        for(const [key,value] of Object.entries(this.config.var_mapping)) {
            if (var_name == value.var_name) {
                return key
            }
        }
        return
    }

    async getHidroSerie(
        var_name,
        station_id,
        tipo_filtro_data = "DATA_LEITURA",
        start_date,
        end_date,
        url,
        token
    ) {
        if(!var_name) {
            throw(new Error("missing var_name"))
        }
        const var_id = this.getMappedVariableKey(var_name)
        if(!var_id) {
            throw(new Error("var_name " + var_name + " not found in var_mapping"))
        }
        if(!station_id) {
            throw(new Error("missing station_id"))
        }
        if(!start_date) {
            throw(new Error("missing start_date"))
        }
        if(!end_date) {
            throw(new Error("missing end_date"))
        }
        url = url ?? this.config.url
        token = token ?? this.token
        const path = sprintf("%s/EstacoesTelemetricas/HidroSerie%s/v1", url, var_name)
        const response = await axios.get(
            path,
            {
                params: {
                    "Código da Estação": station_id,
                    "Tipo Filtro Data": tipo_filtro_data,
                    "Data Inicial (yyyy-MM-dd)": start_date,
                    "Data Final (yyyy-MM-dd)": end_date
                },
                headers: {
                    "Authorization": sprintf("Bearer %s", this.token)
                }
            }
        )
        console.debug(sprintf("GET %03d %s", response.status, response.config.url))
        this.last_response = response
        return response.data
    }

    static extractDailyObs(property_name, items, series_id, factor, t_offset) {
        const obs = []
        for(var item of items) {
            const timestart_month = new Date(item.Data_Hora_Dado + "-03:00")
            // const month = timestart_month.getMonth() + 1
            var timestart_day = new Date(timestart_month)
            while(timestart_day.getMonth() == timestart_month.getMonth()) {
                const timeend_day = new Date(timestart_day)
                timeend_day.setDate(timeend_day.getDate() + 1)
                const day = timestart_day.getDate()
                const status_key = sprintf("%s_%02d_Status", property_name, day)
                const value_key = sprintf("%s_%02d", property_name, day)
                // console.debug({ts: timestart_day.toISOString(), day: day, status_key: status_key, value_key: value_key})
                if(item[status_key] && parseInt(item[status_key])) {
                    const timestart = new Date(timestart_day)
                    const timeend = new Date(timeend_day)
                    if(t_offset) {
                        setDateParts(timestart, t_offset, false)
                        setDateParts(timeend, t_offset, false)
                    }
                    obs.push({
                        timestart: timestart,
                        timeend: timeend,
                        valor: (factor) ? factor * parseFloat(item[value_key]) : parseFloat(item[value_key]),
                        series_id: series_id,
                        tipo: "puntual",
                        consistency_level: (item.nivelconsistencia) ? parseInt(item.nivelconsistencia) : (item.Nivel_Consistencia) ? parseInt(item.Nivel_Consistencia) : 0
                    })
                }
                timestart_day.setDate(timestart_day.getDate() + 1)
            }
        }
        return this.filterByConsistencyLevel(obs)
    }

    static filterByConsistencyLevel(obs) {
        const max_consistency_levels = {}
        for(var o of obs) {
            if(o.timestart.toISOString() in max_consistency_levels) {
                if(o.consistency_level > max_consistency_levels[o.timestart.toISOString()].consistency_level) {
                    max_consistency_levels[o.timestart.toISOString()] = o
                }
            } else {
                max_consistency_levels[o.timestart.toISOString()] = o
            }
        }
        return Object.values(max_consistency_levels).sort((a, b) => a.timestart.getTime() - b.timestart.getTime())
    }

    getLocalDateString(date) {
        const tzdate = new Date(date.toLocaleString("en-US", {timeZone: this.config.tzString}))
        return sprintf("%04d-%02d-%02d", tzdate.getFullYear(), tzdate.getMonth() + 1, tzdate.getDate())
    }

    async get(filter={}, options={}) {
        if(!filter.timestart || !filter.timeend) {
            throw("missing filter.timestart and/or filter.timeend")
        }
        if(filter.id || filter.series_id) {
            var serie = await Serie.read({
                id: filter.id ?? filter.series_id
            })    
            if(!serie) {
                throw(new Error("Serie not found"))
            }
        } else if (filter.var_id) {
            if(filter.estacion_id) {
                var series = await Serie.read({
                    var_id: filter.var_id,
                    estacion_id: filter.estacion_id,
                    proc_id: 1
                })
            } else if(filter.id_externo) {
                var series = await Serie.read({
                    var_id: filter.var_id,
                    id_externo: filter.id_externo,
                    tabla: filter.tabla ?? filter.tabla_id ?? this.config.tabla_id,
                    proc_id: 1
                })
            } else {
                throw(new Error("Missing either filter.series_id or filter.var_id + filter.estacion_id or filter.var_id + filter.id_externo"))
            }
            if(!series.length) {
                throw(new Error("Serie not found"))
            }
            var serie = series[0]
        } else {
            throw(new Error("Missing filter.series_id or filter.var_id + filter.estacion_id or filter.var_id + filter.id_externo"))
        }
        const token = await this.getToken()
        if(!serie.var.id in this.config.var_mapping) {
            throw(new Error("Variable id " + serie.var.id + " not mapped"))
        }
        const observaciones = []
        // iterate years
        var timestart_year = new Date(filter.timestart.getFullYear(),0,1)
        while(timestart_year < filter.timeend) {
            const begin_date = ((timestart_year.getTime() < filter.timestart.getTime()) ? filter.timestart : timestart_year)
            var timeend_year = new Date(timestart_year.getFullYear(),11,31)
            const end_date = ((timeend_year.getTime() > filter.timeend.getTime()) ? filter.timeend : timeend_year)
            timeend_year
            const year_obs = await this.getHidroSerieExtractDailyObs(
                this.config.var_mapping[serie.var.id].var_name,
                serie.estacion.id_externo,
                "DATA_LEITURA",
                this.getLocalDateString(begin_date),
                this.getLocalDateString(end_date),
                undefined,
                token,
                serie.id
            )
            observaciones.push(...year_obs)
            timestart_year.setFullYear(timestart_year.getFullYear() + 1)
        }
        if(options.return_series) {
            serie.setObservaciones(observaciones)
            return [serie]
        }
        return new Observaciones(observaciones)
    }

    async getHidroSerieExtractDailyObs(
        var_name,
        station_id,
        tipo_filtro_data = "DATA_LEITURA",
        start_date,
        end_date,
        url,
        token,
        series_id) {
        const var_id = this.getMappedVariableKey(var_name)
        if(!var_id) {
            throw(new Error("var_name not found in var_mapping"))
        }
        const data = await this.getHidroSerie(
            var_name,
            station_id,
            tipo_filtro_data,
            start_date,
            end_date,
            url,
            token
        )
        return this.constructor.extractDailyObs(
            this.config.var_mapping[var_id].property_name,
            data.items,
            series_id,
            this.config.var_mapping[var_id].factor,
            this.config.var_mapping[var_id].t_offset
        )
    }
}

module.exports = internal