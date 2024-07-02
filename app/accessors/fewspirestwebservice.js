const AbstractAccessorEngine = require('./abstract_accessor_engine').AbstractAccessorEngine
const axios = require('axios')
const { estacion: Estacion, variable: Variable } = require('../CRUD')

const internal = {}

/**
 * Accessor client for 
 */
internal.Client = class extends AbstractAccessorEngine {

    static _get_is_multiseries = true

    default_config = {
        url: "https://sstdfews.cicplata.org/FewsWebServices/rest/fewspiservice/v1",
        tabla: "sstd_cic",
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
        document_format, 
        document_version
        ) {
        var response;
        response = axios.get(
            sprintf("%s/timeseries", this.config.url),
            {
            "params": {
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
            }
        })
        console.debug(sprintf("GET %03d %s", response.status, response.config.url))
        this.last_response = response
        return response.data  
    }

    parseTimeSeries(ts) {
        /*Parse timeseries response
         Args:
        ts (dict): FewsWebService /timeseries response item (response["timeSeries"][i])
         Returns:
        dict: dict with valuable metadata and time-value pairs
        */
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
        const serie = {
            variable: {
            "id": var_id
            },
            unidades: {
                id: this.config.units_map[unit_id]
            },
            estacion: {
                "tabla": this.config.tabla,
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
            observaciones: ts["events"].filter(event=>(parseFloat(event["value"]) != -999)).map(event=>{
                return {
                    "timeupdate": this.parseDate(ts["header"]["forecastDate"]),
                    "timestart": parseDate({
                        "date": event["date"],
                        "time": event["time"]
                    }),
                    "timeend": parseDate({
                        "date": event["date"],
                        "time": event["time"]
                    }),
                    "valor": Number.parseFloat(x["value"])
                }
            })          
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
        const response = axios.get(
            sprintf("%s/locations", this.config.url),
            {
                "params": {
                    "filterId": filter_id,
                    "parameterIds": parameter_ids,
                    "parameterGroupId": parameter_group_id,
                    "showAttributes": show_attributes,
                    "includeLocationRelations": include_location_relations,
                    "includeTimeDependency": include_time_dependency,
                    "documentFormat": document_format, // "PI_JSON",
                    "documentVersion": document_version
                }
            }
        )
        console.debug(sprintf("GET %03d %s", response.status, response.config.url))
        this.last_response = response
        return response.data;
    }
      
    parseLocation(location) {
        return {
            "tabla": this.config.tabla,
            "id_externo": location["locationId"],
            "nombre": location["shortName"],
            "geom": {
              "type": "Point",
              "coordinates": [
                Number.parseFloat(location["lon"]), 
                Number.parseFloat(location["lat"])
              ]
            }
        }
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
            Object.keys(this.config.variable_map),
            undefined,
            undefined,
            undefined,
            undefined,
            document_format = "PI_JSON"
        )
        const estaciones = locations_response["locations"].map(location=>
            this.parseLocation(location)
        )
        if(filter.id_externo) {
            if(Array.isArray(filter.id_externo)) {
                estaciones = estaciones.filter(e=>(filter.id_externo.indexOf(e.id_externo) >= 0))
            } else {
                estaciones = estaciones.filter(e=>(filter.id_externo == e.id_externo))
            }
        }
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
        const estaciones = await Estacion.read({
            tabla: this.config.tabla,
            id: filter.estacion_id,
            id_externo: filter.id_externo,
            geom: filter.geom
        })
        if(!estaciones.length) {
            console.warn("accessor getSeries: Estaciones not found. Run updateSites or try with other filter")
            return []
        }
        if(filter.var_id) {
            const variables = Variable.read({
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
        const series_response = this.getTimeSeries(
            this.config.filter_id,
            estaciones.map(e=>e.id_externo),
            parameter_ids,
            undefined,
            this.config.module_instance_ids
        )
        if(!series_response.timeSeries.length) {
            console.warn("Accessor: no timeseries found")
            return []
        }
        return series_response.timeSeries.map(ts=>this.parseTimeSeries(ts))
    }
      
      

}

module.exports = internal