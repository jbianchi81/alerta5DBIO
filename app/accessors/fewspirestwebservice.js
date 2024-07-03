const AbstractAccessorEngine = require('./abstract_accessor_engine').AbstractAccessorEngine
const axios = require('axios')
const { estacion: Estacion, variable: Variable, serie: Serie } = require('../CRUD')
const sprintf = require('sprintf-js').sprintf
const {createUrlParams, filterSites, filterSeries} = require('../accessor_utils')

const internal = {}

/**
 * Accessor client for 
 */
internal.Client = class extends AbstractAccessorEngine {

    static _get_is_multiseries = true

    default_config = {
        url: "https://sstdfews.cicplata.org/FewsWebServices/rest/fewspiservice/v1",
        tabla_id: "sstd_cic",
        proc_id: 4,
        variable_map: {
            4: "Q.sim"
        },
        units_map: {
            10: "m3/s"
        }
    }

    constructor(config) {
        super(config)
        this.setConfig(config)
    }

    async getTimeSeries(
        filter_id, 
        location_ids, 
        parameter_ids, 
        module_instance_ids, 
        qualifier_ids, 
        task_run_ids, 
        start_time, 
        end_time, 
        start_creation_time, 
        end_creation_time, 
        forecast_count, 
        start_forecast_time, 
        end_forecast_time, 
        external_forecast_times, 
        ensemble_id, 
        ensemble_member_id, 
        time_step_id, 
        thinning, 
        export_id_map, 
        export_unit_conversion_id, 
        time_zone_name, 
        time_series_set_index, 
        default_request_parameters_id, 
        match_as_qualifier_set, 
        import_from_external_data_source, 
        convert_datum, 
        show_ensemble_members_id, 
        use_display_units, 
        show_thresholds, 
        omit_missing, 
        omit_empty_time_series, 
        only_manual_edits, 
        only_headers, 
        only_forecasts, 
        show_statistics, 
        use_milliseconds, 
        show_products, 
        time_series_type, 
        document_format = "PI_JSON", 
        document_version
        ) {
        const params = createUrlParams({
            "filterId": filter_id,
            "locationIds": location_ids,
            "parameterIds": parameter_ids,
            "moduleInstanceIds": module_instance_ids,
            "qualifierIds": qualifier_ids,
            "taskRunIds": task_run_ids,
            "startTime": start_time,
            "endTime": end_time,
            "startCreationTime": start_creation_time,
            "endCreationTime": end_creation_time,
            "forecastCount": forecast_count,
            "startForecastTime": start_forecast_time,
            "endForecastTime": end_forecast_time,
            "externalForecastTime": external_forecast_times,
            "ensembleId": ensemble_id,
            "ensembleMemberId": ensemble_member_id,
            "timeStepId": time_step_id,
            "thinning": thinning,
            "exportIdMap": export_id_map,
            "exportUnitConversionId": export_unit_conversion_id,
            "timeZoneName": time_zone_name,
            "timeSeriesSetIndex": time_series_set_index,
            "defaultRequestParametersId": default_request_parameters_id,
            "matchAsQualifierSet": match_as_qualifier_set,
            "importFromExternalDataSource": import_from_external_data_source,
            "convertDatum": convert_datum,
            "showEnsembleMembersId": show_ensemble_members_id,
            "useDisplayUnits": use_display_units,
            "showThresholds": show_thresholds,
            "omitMissing": omit_missing,
            "omitEmptyTimeSeries": omit_empty_time_series,
            "onlyManualEdits": only_manual_edits,
            "onlyHeaders": only_headers,
            "onlyForecasts": only_forecasts,
            "showStatistics": show_statistics,
            "useMilliseconds": use_milliseconds,
            "showProducts": show_products,
            "timeSeriesType": time_series_type,
            "documentFormat": document_format,
            "documentVersion": document_version
        })
        const response = await axios.get(
            sprintf("%s/timeseries", this.config.url),
            {
                "params": params
            }
        )
        console.debug(sprintf("GET %03d %s", response.status, response.config.url))
        this.last_response = response
        return response.data  
    }

    /**
     * Parse timeseries response
     * @param {*} ts - FewsWebService /timeseries response item (response["timeSeries"][i])
     * @param {boolean} includeObservations 
     * @param {*} series_id_map - mapping of remote location_id, parameter_id tuples to local series_id
     * @returns {Serie} - Serie object with valuable metadata and time-value pairs
     */
    parseTimeSeries(ts, includeObservations=true, series_id_map) {
        var var_id
        for(const [key, value] of Object.entries(this.config.variable_map)) {
            if(value == ts["header"]["parameterId"]) {
                var_id = key
            }
        } 
        if(!var_id) {
            throw(new Error("parameterId " + ts["header"]["parameterId"] + " not mapped"))
        }
        var unit_id
        for(const [key, value] of Object.entries(this.config.units_map)) {
            if(value == ts["header"]["units"]) {
                unit_id = key
            }
        }
        if(!unit_id) {
            throw(new Error("units " + ts["header"]["units"] + " not mapped"))
        }
        if(series_id_map) {
            if(ts["header"]["locationId"] in series_id_map) {
                if(ts["header"]["parameterId"] in series_id_map[ts["header"]["locationId"]]) {
                    var series_id = series_id_map[ts["header"]["locationId"]][ts["header"]["parameterId"]]
                } else {
                    console.warn("parameterId " + ts["header"]["parameterId"] + " not mapped")
                    var series_id = undefined
                }              
            } else {
                console.warn("locationId " + ts["header"]["locationId"] + " not mapped")
                var series_id = undefined
            }
        } else {
            var series_id = undefined
        }
        const serie = new Serie({
            tipo: "puntual",
            id: series_id,
            "var": {
            "id": var_id
            },
            unidades: {
                id: unit_id
            },
            estacion: {
                "tabla": this.config.tabla_id,
                "id_externo": ts["header"]["locationId"],
                "nombre": ts["header"]["stationName"],
                "geom": {
                  "type": "Point",
                  "coordinates": [Number.parseFloat(ts["header"]["lon"]), Number.parseFloat(ts["header"]["lat"])]
                }
            },
            procedimiento: {
                id: this.config.proc_id
            },
            observaciones: (includeObservations) ? ts["events"].filter(
                    event=>(parseFloat(event["value"]) != -999)
                ).map(
                    event=>{
                        return {
                            "timeupdate": this.parseDate(ts["header"]["forecastDate"]),
                            "timestart": this.parseDate({
                                "date": event["date"],
                                "time": event["time"]
                            }),
                            "timeend": this.parseDate({
                                "date": event["date"],
                                "time": event["time"]
                            }),
                            "valor": Number.parseFloat(event["value"])
                        }
                    }
                ) : undefined         
        })
        serie.observaciones.setTipo("puntual")
        if(serie.observaciones && series_id) {
            serie.observaciones.setSeriesId(series_id)
        }
        return serie
    }

    parseDate(date) {
        /*parse date-time dict
         Args:
        date (dict): {"date" : str, "time" : str}
         Returns:
        datetime: datetime in utc timezone
        */
        const d = date["date"].split("-").map(p=>parseInt(p))
        const t = date["time"].split(":").map(p=>parseInt(p))
        return new Date(Date.UTC(d[0], d[1] - 1, d[2], t[0], t[1], t[2]))
    }
      
    async getLocations(
        filter_id, 
        parameter_ids, 
        parameter_group_id, 
        show_attributes, 
        include_location_relations, 
        include_time_dependency, 
        document_format,
        document_version
        ) {
        const params = createUrlParams({
            "filterId": filter_id,
            "parameterIds": parameter_ids,
            "parameterGroupId": parameter_group_id,
            "showAttributes": show_attributes,
            "includeLocationRelations": include_location_relations,
            "includeTimeDependency": include_time_dependency,
            "documentFormat": document_format, // "PI_JSON",
            "documentVersion": document_version
        })
        const response = await axios.get(
            sprintf("%s/locations", this.config.url),
            {
                "params": params
            }
        )
        console.debug(sprintf("GET %03d %s", response.status, response.config.url))
        this.last_response = response
        return response.data;
    }
      
    parseLocation(location) {
        return new Estacion({
            "tabla": this.config.tabla_id,
            "id_externo": location["locationId"],
            "nombre": location["shortName"],
            "geom": {
              "type": "Point",
              "coordinates": [
                Number.parseFloat(location["lon"]), 
                Number.parseFloat(location["lat"])
              ]
            }
        })
    }

    /**
     * 
     * @param {*} filter 
     * @param {string|Array<string>} filter.id_externo
     * @param {*} options 
     */
    async getSites(
        filter={},
        options={}
    ) {
        const locations_response = await this.getLocations(
            this.config.filter_id,
            Object.values(this.config.variable_map),
            undefined,
            undefined,
            undefined,
            undefined,
            "PI_JSON"
        )
        var estaciones = locations_response["locations"].map(location=>
            this.parseLocation(location)
        )
        estaciones = filterSites(estaciones,filter)
        return estaciones
    }

    async updateSites(
        filter={},
        options={}
    ) {
        const estaciones = await this.getSites(filter, options)
        return Estacion.create(estaciones)
    }

    async getSeries(
        filter={},
        options={}
    ) {
        if(filter.id_externo) {
            var location_ids = filter.id_externo
        } else if(filter.estacion_id || filter.geom) {
            const estaciones = await Estacion.read({
                tabla: this.config.tabla_id,
                id: filter.estacion_id,
                geom: filter.geom
            })
            if(!estaciones.length) {
                console.warn("accessor getSeries: Estaciones not found. Run updateSites or try with other filter")
                return []
            }
            var location_ids = estaciones.map(e=>e.id_externo)
        } else {
            var location_ids = undefined
        }
        if(filter.var_id) {
            const variables = await Variable.read({
                id: filter.var_id
            })
            if(Array.isArray(variables)) {
                var parameter_ids = []
                for(const variable of variables) {
                    if(!variable in this.config.variable_map) {
                        console.warn("Accessor: Variable " + variable + " not mapped. Skipping")
                        continue
                    }
                    parameter_ids.push(this.config.variable_map[variable])
                }
            } else {
                var parameter_ids = this.config.variable_map[key]
            }
        } else {
            var parameter_ids = Object.values(this.config.variable_map)
        }
        const series_response = await this.getTimeSeries(
            this.config.filter_id,
            location_ids,
            parameter_ids,
            this.config.module_instance_ids
        )
        if(!series_response.timeSeries.length) {
            console.warn("Accessor: no timeseries found")
            return []
        }
        const series = series_response.timeSeries.map(ts=>this.parseTimeSeries(ts, options.includeObservations))
        return filterSeries(series, filter)
    }

    async updateSeries(
        filter={},
        options={}
    ) {
        const get_series_options = {}
        Object.assign(get_series_options,options)
        get_series_options.includeObservations = false
        const series = await this.getSeries(filter, get_series_options)
        return Serie.create(
            series,
            {
                upsert_estacion: options.upsert_estacion,
                all: options.all,
                generate_id: options.generate_id,
                refresh_date_range: options.refresh_date_range
            }
        )       
    }

    /**
     * 
     * @param {*} filter 
     * @param {integer|Aray<integer>} filter.series_id
     * @param {integer|Aray<integer>} filter.estacion_id
     * @param {integer|Aray<integer>} filter.var_id
     * @param {integer|Aray<integer>} filter.proc_id
     * @param {integer|Aray<integer>} filter.unit_id
     * @param {*} filter.geom
     * @param {string|Aray<string>} filter.id_externo
     * @param {string|Aray<string>} filter.qualifier
     * @param {*} options 
     */
    async get(
        filter={},
        options={}
    ) {
        if(!filter.timestart) {
            throw(new Error("Missing filter.timestart"))
        }
        if(!filter.timeend) {
            throw(new Error("Missing filter.timeend"))
        }
        const start_time = filter.timestart.toISOString()
        const end_time =  filter.timeend.toISOString()
        const series = await Serie.read({
            tipo: "puntual",
            id: filter.series_id,
            estacion_id: filter.estacion_id,
            tabla_id: this.config.tabla_id,
            var_id: filter.var_id,
            proc_id: filter.proc_id,
            unit_id: filter.unit_id,
            geom: filter.geom,
            id_externo: filter.id_externo
        })
        if(!series.length) {
            console.warn("accessor get: No series found. Run updateSeries or change filter")
            return []
        }
        const location_ids = new Set(series.map(s=>s.estacion.id_externo))
        const parameter_ids = new Set()
        this.series_id_map = {}
        for(const serie of series) {
            if(!serie.var.id in this.config.variable_map) {
                console.warn("Accessor get: var " + serie.var.id + " not mapped. Skipping.")
                continue
            }
            parameter_ids.add(this.config.variable_map[serie.var.id])            
            if(serie.estacion.id_externo in this.series_id_map) {
                this.series_id_map[serie.estacion.id_externo][this.config.variable_map[serie.var.id]] = serie.id
            } else {
                this.series_id_map[serie.estacion.id_externo] = {}
                this.series_id_map[serie.estacion.id_externo][this.config.variable_map[serie.var.id]] = serie.id
            }
        }
        if(filter.forecast_timestart) {
            var start_forecast_time = filter.forecast_timestart.toISOString()
        } else {
            var start_forecast_time = undefined
        }
        if(filter.forecast_timeend) {
            var end_forecast_time = filter.forecast_timeend.toISOString()
        } else {
            var end_forecast_time = undefined
        }
        if(filter.forecast_date) {
            if(Array.isArray(filter.forecast_date)) {
                var forecast_times = filter.forecast_date.map(d=>d.toISOString())
            } else {
                var forecast_times = filter.forecast_date.toISOString()
            }
        } else {
            var forecast_times = undefined
        }
        const series_response = await this.getTimeSeries(
            this.config.filter_id,
            [...location_ids],
            [...parameter_ids],
            this.config.module_instance_ids,
            filter.qualifier,
            undefined,
            start_time,
            end_time,
            undefined,
            undefined,
            undefined,
            start_forecast_time,
            end_forecast_time,
            forecast_times,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            "PI_JSON",
            undefined
        )
        if(!series_response.timeSeries.length) {
            console.warn("Accessor: no timeseries found")
            return []
        }
        return series_response.timeSeries.map(ts=>this.parseTimeSeries(ts, true, this.series_id_map))
    }

    async update(
        filter={},
        options={}
    ) {
        const series = await this.get(filter, options)
        for(const serie of series) {
            const created_obs = await serie.observaciones.create()
            serie.setObservaciones(created_obs)
        }
        return series
    }
}

module.exports = internal