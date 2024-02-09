'use strict'

const {getConfig} = require('./utils')
const {sprintf} = require('sprintf-js')
const axios = require('axios')
const fs = require('promise-fs')
const {estacion: Estacion, serie: Serie, observacion: Observacion, observaciones: Observaciones} = require('./CRUD')
// const moment = require('moment-timezone')
const {not_null} = require('./utils')
const {accessor_feature_of_interest, accessor_timeseries_observation, accessor_observed_property, accessor_unit_of_measurement, accessor_time_value_pair} = require('./accessor_mapping')
const {isoDurationToHours} = require('./timeSteps')
const internal = {}

internal.timeseries_observation = class extends accessor_timeseries_observation {
    constructor(fields) {
        super(fields)
    }
    /**
    * Converts timeseriesObservations OM JSON to a5 series
    * 
    * @return {Serie} serie
    */
    toSerie() { 
        return new Serie({
            tipo: (this.series_puntual_id) ? "puntual" : (this.series_areal_id) ? "areal" : (this.series_rast_id) ? "raster" : undefined,
			id: (this.series_puntual_id) ? this.series_puntual_id : (this.series_areal_id) ? this.series_areal_id : (this.series_rast_id) ? this.series_rast_id : undefined,
			nombre: `${this.accesor_id}:${this.timeseries_id}`,
            estacion: {
                tabla: this.config.tabla, // add this.config
                id_externo: this.result.featureOfInterest.href
            },
            var: {
                timeSupport: (result.result.defaultPointMetadata.hasOwnProperty("aggregationDuration")) ? isoDurationToHours(result.result.defaultPointMetadata.aggregationDuration) : undefined,
                id_externo: result.observedProperty.href,
                interpolationType: (result.result.hasOwnProperty("defaultPointMetadata")) ? item.result.defaultPointMetadata.interpolationType : undefined
            },
            unidades: {id_externo: result.result.defaultPointMetadata.uom}, // this.findUnidades(item),
            procedimiento: {id: 1}
            // observaciones: (item.result) ? internal.client.observacionesToA5(item.result.points) : undefined
        })
    }
}

internal.feature_of_interest = class extends accessor_feature_of_interest {
    constructor(fields) {
        super(fields)
    }
    /**
    * Converts monitoringPoints OM JSON to a5 estaciones
    * 
    * @return {Estacion} estacion
    */
    toEstacion() { 
        var monitoring_point_parameters = {}
        for(var i of this.result.parameter) {
            monitoring_point_parameters[i.name] = i.value
        }
        return new Estacion({
            id: this.estacion_id,
            tabla: this.network_id,
            id_externo: this.result.id,
            nombre: this.result.name,
            geom: this.result.shape, // { "type": "Point", coordinates: [item.shape.coordinates[0], item.shape.coordinates[1]]},
            pais: (monitoring_point_parameters.hasOwnProperty("country")) ?  monitoring_point_parameters.country : undefined,
            propietario: (this.result.hasOwnProperty("relatedParty") && this.result.relatedParty.length) ? this.result.relatedParty[0].organisationName : undefined,
            ubicacion: (monitoring_point_parameters.hasOwnProperty("identifier")) ? monitoring_point_parameters.identifier: undefined, 
            real: true, 
            has_obs: true
        })
    }
}

internal.observed_property = class extends accessor_observed_property {
    /**
     * 
     * @returns Converts ogc api observed property to a5 variableName
     */
    toVariableName() {
		return new VariableName({
			VariableName: this.variable_name,
            href: this.href
		})
	}
}
internal.unit_of_measurement = class extends accessor_unit_of_measurement {

}
internal.time_value_pair = class extends accessor_time_value_pair {
    
}
/**
    Functions for metadata retrieval from WHOS using timeseries API
    plus functions to convert from native format (OM-JSON) to a5 schema
*/    
internal.client = class {

    static _get_is_multiseries = false
    
    static default_config = {
        "url": "https://whos.geodab.eu/gs-service/services/essi",
        "token": "YOUR_TOKEN_HERE",
        "monitoring_points_max": 6000,
        "monitoring_points_per_page": 200,
        "timeseries_max": 48000,
        "timeseries_per_page": 400,
        "view": "whos-plata",
        "begin_days": null,
        "tabla": "whos_plata",
        "accessor_id": "om_ogc_timeseries_client",
        "no_data_value": -9999
    }

    static var_map = []
    
    static unidades_map = []
    
    static country_map = [
        {
            iso: "ARG",
            name: "argentina"
        },
        {
            iso: "BOL",
            name: "bolivia"
        },
        {
            iso: "BRA",
            name: "brasil"
        },
        {
            iso: "PRY",
            name: "paraguay"
        },
        {
            iso: "URY",
            name: "uruguay"
        }
    ]

    /**
    * Class constructor
    * 
    * @param {object} config
    * @param {string} config.url
    * @param {string} config.token
    * @param {string} config.monitoring_points_max
    * @param {string} config.monitoring_points_per_page
    * @param {string} config.timeseries_max
    * @param {string} config.timeseries_per_page 
    * @returns {internal.client}
    */
    constructor(config) {        
        this.config = getConfig(config,internal.client.default_config)
        if(this.config.begin_days) {
            this.config.threshold_begin_date = new Date()
            this.config.threshold_begin_date.setDate(this.config.threshold_begin_date.getDate()-this.config.begin_days)
        }
        this.last_request = {
            url: null,
            params: null,
            response: null
        }
    }

    static getCountryIsoCode(country) {
        for(var c of this.country_map) {
            if(c.name == country.toLowerCase()) {
                return c.iso
            }
        }
        return
    }
    
    /**
    * getMonitoringPoints Retrieves monitoring points as a geoJSON document from the timeseries API
    *
    * @param {string} view - WHOS view identifier. Default whos-plata
    * @param {object} filter
    * @param {float} filter.east - Bounding box eastern longitude coordinate
    * @param {float} filter.west - Bounding box western longitude coordinate
    * @param {float} filter.north - Bounding box northern latitude coordinate
    * @param {float} filter.south - Bounding box southern latitude coordinate
    * @param {float} filter.offset - Start position of matched records
    * @param {integer} filter.limit - Maximum number of matched records
    * @param {string} filter.country - Country code (ISO3)
    * @param {object} options
    * @param {string} options.output - Write JSON output into this file
    * @returns {object} OM OGC API monitoringPoints response
    */
    async getMonitoringPoints(view=this.config["view"],filter={},options={}) {
        const params = {}
        for(var key of ["east","west","north","south","limit","offset","country","provider"]) {
            if(filter.hasOwnProperty(key)) {
                params[key] = filter[key]
            }
        }
        params.outputProperties = "country,monitoringPointOriginalIdentifier"
        var url = sprintf ("%s/token/%s/view/%s/timeseries-api/monitoring-points", this.config["url"], this.config["token"], view)
        this.last_request = {
            url: url, // print("url: %s" % url)
            params: params
        }
        Object.keys(params).forEach(key => params[key] === undefined ? delete params[key] : {})
        console.log(`Retrieving ${url}?${new URLSearchParams(params).toString()}`)
        try {
            var response = await axios.get(url, {params: params})
        } catch(e) {
            throw("Request failed: " + e.toString())
        }
        this.last_request.response = {
            status: response.status,
            data: response.data
        }
        if(response.status >= 400) {
            throw("Request failed, status code: " + response.status)
        }
        if (options.output) {
            try { 
                fs.writeFileSync(path.resolve(__dirname,options.output),JSON.stringify(response.data,null,4),'utf-8')
            } catch(e) {
                throw(e)
            }
        }
        return response.data
    }
    /**
    * getTimeseries - Retrieves timeseries as a geoJSON document from the timeseries API
    * 
    * @param {string} view - WHOS view identifier
    * @param {object} filter
    * @param {string} filter.monitoringPoint - Identifier of the monitoring point
    * @param {string} filter.timeseriesIdentifier - Identifier of the time series
    * @param {string} filter.beginPosition - Temporal interval begin position (ISO8601 date)
    * @param {string} filter.endPosition - Temporal interval end position (ISO8601 date)
    * @param {integer} filter.offset - Start position of matched records
    * @param {integer} filter.limit - Maximum number of matched records
    * @param {object} options
    * @param {boolean} options.has_data - filter out timeseries without data
    * @param {string} options.output - Write JSON output into this file
    * @returns {object} - Timeseries observations encoded according to OM-JSON OGC DP 15-100r1
    */
    async getTimeseries(view=this.config["view"], filter={}, options={}, useCache = false) {
        const params = {}
        for(var key of ["monitoringPoint","timeseriesIdentifier","beginPosition","endPosition","limit","offset"]) {
            if(filter.hasOwnProperty(key)) {
                params[key] = filter[key]
            }
        }
        if(useCache) {
            params.useCache = true
        }
        var url = sprintf("%s/token/%s/view/%s/timeseries-api/timeseries", this.config["url"], this.config["token"], view)
        console.log("url: " + url)
        // console.log("%s - %s?%s" % (str(datetime.now()), url, "&".join([ "%s=%s" % (key, params[key]) for key in params])))
        this.last_request = {
            url: url,
            params: params
        }
        try {
            var response = await axios.get(url, {params: params})
        } catch(e) {
            throw("request failed: " + e.toString())
        }
        this.last_request.response = {
            status: response.status,
            data: response.data
        }
        if(response.status >= 400) {
            throw("request failed, status code: " + response.status)
        }
        // filter out features with no data
        // xprint("%s - Elapsed: %s" % (str(datetime.now()),str(response.elapsed)))
        var result = {...response.data}
        if(options.has_data && result.hasOwnProperty("member")) {
            result["member"] = this.filterByAvailability(result["member"],this.config.threshold_begin_date)
        } 
        if(options.output) {
            try { 
                fs.writeFileSync(output,result)
            } catch(e) {
                throw("Couldn't open file for writing " + output)
            }
        }
        return result
    }
    filterByAvailability(members,threshold_begin_date) {
        if(threshold_begin_date) {
            return members.filter(x=> (x.hasOwnProperty("phenomenonTime") && new Date(x.phenomenonTime.end).getTime() >=  threshold_begin_date.getTime()))
        } else {
            return members.filter(x=> (x.hasOwnProperty("phenomenonTime")))
        }
    }

    /**
     * 
     * @param {*} features - features property of getMonitoringPoints or getMonitoringPointsWithPagination result 
     * @returns {internal.feature_of_interest[]} feature_of_interest[]
     */
    monitoringPointsToFeaturesOfInterest(features) {
        return features.map(f=>{
            return new internal.feature_of_interest({
                accessor_id: (this.config.accessor_id) ? this.config.accessor_id : "om_ogc_timeseries_client",
                feature_id: f.id,
                name: f.name,
                geometry: f.shape,
                result: f,
                network_id: this.config.tabla
            })
        })
    }

    
    static findVar(item) {
        var nueva_variable = {
            timeSupport: (item.result.defaultPointMetadata.hasOwnProperty("aggregationDuration")) ? isoDurationToHours(item.result.defaultPointMetadata.aggregationDuration) : undefined,
            href: item.observedProperty.href,
            interpolationType: (item.result.hasOwnProperty("defaultPointMetadata")) ? item.result.defaultPointMetadata.interpolationType : undefined
        }
        for(var v of this.var_map) {
            if(JSON.stringify(nueva_variable.timeSupport) == JSON.stringify(v.timeSupport) && nueva_variable.href == v.href && nueva_variable.interpolationType == v.interpolationType) {
                return v
            }
        }
        return nueva_variable
    }


    static findUnidades(item) {
        var uom = (item.result.defaultPointMetadata.hasOwnProperty("uom")) ? item.result.defaultPointMetadata.uom : undefined
        if(!uom) {
            return
        }
        for(var u of this.unidades_map) {
            if(uom == v.uom) {
                return v
            }
        }
        return {
            uom: uom
        }
    }

    static findEstacion(id_externo,estaciones,tabla) {
        const nueva_estacion = {
            id_externo: id_externo,
            tabla: (tabla) ? tabla : this.default_config.tabla
        }
        if(!estaciones) {
            return nueva_estacion
        }
        var matches = estaciones.filter(e=>e.id_externo == nueva_estacion.id_externo && e.tabla == nueva_estacion.tabla)
        if(!matches.length) {
            return nueva_estacion
        } else {
            return matches[0]
        }
    }

    /**
    * Converts timeseries OM JSON to a5 series
    * 
    * @param {object|string} timeseries - Result of getTimeseries. If string: JSON file to read from, If object: getTimeseries return value  
    * @param {string} output - Write CSV output into this file
    * @param {CRUD.estaciones[]} stations - result of monitoringPointsToA5. If not None adds station metadata to series
    * @returns {CRUD.series[]} - a5 series
    */
    static timeseriesToA5(timeseries, output, estaciones) {
        if(timeseries instanceof String) {
            try {
                timeseries = JSON.parse(fs.readFileSync(timeseries))
            } catch(e) {
                throw("Failed read file " + timeseries)
            }
        }
        var series = []
        for(var item of timeseries.member) {
            const serie = new Serie({
                estacion: this.findEstacion(item.featureOfInterest.href,estaciones),
                var: this.findVar(item),
                unidades: this.findUnidades(item),
                procedimiento: {id: 1},
                observaciones: (item.result) ? internal.client.observacionesToA5(item.result.points) : undefined
            })
            series.push(serie)
        }
        if(output) {
            try { 
                fs.writeFileSync(output,series)
            } catch(e) {
                throw("Couldn't open file for writing " + output)
            }
        }
        return series
    }

    static observacionesToA5(points) {
        if(!points) {
            return
        }
        return points.map(p=>{
            return new Observacion({
                timestart: new Date(p.time.instant),
                valor: p.value
            })
        })
    }
    
    
    static getUniqueVariables(timeseries) {
        const variables = {}
        for(var item of timeseries["member"]) {
            if(!variables.hasOwnProperty(item.observedProperty.href)) {
                variables[item.observedProperty.href] = {
                    href: item.observedProperty.href,
                    title: item.observedProperty.title,
                    uom: (item.result.hasOwnProperty("defaultPointMetadata")) ? item.result.defaultPointMetadata.uom : undefined,
                    interpolationType: (item.result.hasOwnProperty("defaultPointMetadata")) ? item.result.defaultPointMetadata.interpolationType : undefined
                }
            }
        }
        return variables
    }

    // def getVariableMapping(self,view=default_config["view"],output=None,output_xml=None):
    //     """Retrieves variable mapping from WHOS CUAHSI API (waterML 1.1)
        
    //     Parameters
    //     ----------
    //     view : str
    //         WHOS view idenfifier
    //     output : string
    //         Write CSV output into this file
    //     output_xml : string
    //         Write XML output into this file
        
    //     Returns
    //     -------
    //     DataFrame
    //         A data frame of the mapped observed variables
    //     """
    //     url = "%s/gs-service/services/essi/token/%s/view/%s/cuahsi_1_1.asmx" % (self.config["url"], self.config["token"], view)
    //     params = {
    //         "request": "GetVariables"
    //     }
    //     try:
    //         response = requests.get(url, params)
    //     except:
    //         raise Exception("request failed")
    //     if(response.status_code >= 400):
    //         raise Exception("request failed, status code: %s" % response.status_code)
    //     xml_text = response.text.replace("&lt;","<").replace("&gt;",">")
    //     exml = etree.fromstring(xml_text.encode())
    //     namespaces = exml.nsmap
    //     namespaces["his"] = "http://www.cuahsi.org/his/1.1/ws/"
    //     namespaces["wml"] = "http://www.cuahsi.org/waterML/1.1/"
    //     variables = exml.xpath("./soap:Body/his:GetVariablesResponse/his:GetVariablesResult/wml:variablesResponse/wml:variables/wml:variable",namespaces=namespaces)
    //     var_map = []
    //     for v in variables:
    //         variableCode = v.find("./wml:variableCode",namespaces=namespaces).text
    //         variableName = v.find("./wml:variableName",namespaces=namespaces).text
    //         unitName = v.find("./wml:unit/wml:unitName",namespaces=namespaces).text
    //         var_map.append({
    //             "variableCode": variableCode,
    //             "variableName": variableName,
    //             "unitName": unitName
    //         })
    //     data_frame = pandas.DataFrame(var_map)
    //     if output_xml is not None:
    //         f = open(output_xml,"w")
    //         f.write(xml_text)
    //         f.close()
    //     if output is not None:
    //         f = open(output,"w")
    //         f.write(data_frame.to_csv(index=False))
    //         f.close()
    //     return data_frame
    
    // def groupTimeseriesByVar(self,input_ts,var_map,output_dir=None,fews=False): 
    //     """Groups timeseries by observedVariable, optionally using FEWS convention
        
    //     Parameters
    //     ----------
    //     input_ts : DataFrame
    //         Return value of timeseriesToFEWS()
    //     var_map : dict
    //         Return value of getVariables()
    //     output_dir : str
    //         Write output to this directory
    //     fews : bool
    //         Use FEWS variables naming convention
        
    //     Returns
    //     -------
    //     DataFrame
    //         A data frame of time series grouped by variable name
    //     """
    //     timeseries = input_ts.copy()
    //     # var_dict = {}
    //     # for i in var_map.index:
    //     #     var_dict[var_map["variableCode"][i]] = {
    //     #         "variableName": var_map["variableName"][i],
    //     #         "unitName": var_map["unitName"][i]
    //     #     }
    //     timeseries["variableName"] = [var_map[href]["title"] for href in timeseries["EXTERNAL_PARAMETER_ID"]]
    //     timeseries["UNIT"] = [var_map[href]["uom"] for href in timeseries["EXTERNAL_PARAMETER_ID"]]
    //     if fews:
    //         timeseries["variableName"] = [self.fews_var_map[variableName] if variableName in self.fews_var_map else None for variableName in timeseries["variableName"]]
    //         timeseries = timeseries[timeseries["variableName"].notnull()]
    //     if output_dir is not None:
    //         output_dir = Path(output_dir)
    //         variableNames = set(timeseries["variableName"])
    //         for variableName in variableNames:
    //             group = timeseries[timeseries["variableName"]==variableName]
    //             del group["variableName"]
    //             if fews and variableName in self.fews_series_columns:
    //                 fews_group = pandas.DataFrame(columns=self.fews_series_columns[variableName])
    //                 for column in fews_group.columns:
    //                     fews_group[column] = group[column] if column in group else None
    //                 group = fews_group
    //             f = open(output_dir / ("%s.csv" % variableName),"w")
    //             f.write(group.to_csv(index=False))
    //     return timeseries

    // def makeFewsTables(self,output_dir="",save_geojson=False,has_data=True,timeseriesIdentifier=None,country=None,has_timestep=True,east=None,west=None,north=None,south=None):
    //     """Retrieves WHOS metadata and writes out FEWS tables
        
    //     Parameters
    //     ----------
    //     output_dir : str
    //         Write outputs in this directory. Defaults to current working directory
    //     save_geojson : bool
    //         Also writes out raw API responses (geoJSON files)
    //     timeseriesIdentifier: list or str
    //     country: str - country code (ISO3)
    //     has_timestep: bool
    //         filter out series without timestep. Default True
        
    //     Returns
    //     -------
    //     dict
    //         dict containing retrieved estaciones and timeseries in FEWS format
    //     """
    //     output_dir = Path(output_dir)
    //     timeseriesIdentifier = timeseriesIdentifier if timeseriesIdentifier is not None else self.fews_observed_properties
    //     monitoringPoints = self.getMonitoringPointsWithPagination(json_output = output_dir / "monitoringPoints.json" if save_geojson else None,country = country,east=east,west=west,north=north,south=south)
    //     stations_fews = self.monitoringPointsToFEWS(monitoringPoints)
    //     # get all WHOS-Plata timeseries metadata (using pagination)
    //     timeseries = self.getTimeseriesWithPagination(timeseriesIdentifier=timeseriesIdentifier, json_output = output_dir / "timeseries.json" if save_geojson else None, has_data = has_data)
    //     # get unique variables dict
    //     var_map = self.getVariables(timeseries) # self.getVariableMapping()
    //     station_organization = self.getOrganization(timeseries,stations_fews)
    //     timeseries_fews = self.timeseriesToFEWS(timeseries, estaciones=stations_fews)
    //     timeseries_fews = self.deleteSeriesWithoutTimestep(timeseries_fews) if has_timestep else timeseries_fews  
    //     # filter out estaciones with no timeseries
    //     stations_fews = self.deleteStationsWithNoTimeseries(stations_fews,timeseries_fews)
    //     stations_fews = self.setOriginalStationId(stations_fews)
    //     # get organization name from timeseries metadata
    //     # save estaciones to csv
    //     f = open(output_dir / "locations.csv","w")
    //     f.write(stations_fews.to_csv())
    //     f.close()
    //     timeseries_fews = self.setOriginalStationId(timeseries_fews)
    //     #group timeseries by variable using FEWS variable names and output each group to a separate .csv file
    //     timeseries_fews_grouped = self.groupTimeseriesByVar(timeseries_fews,var_map,output_dir=output_dir,fews= True) # False)
    //     return {"stations": stations_fews, "timeseries": timeseries_fews_grouped}
    
    // def setOriginalStationId(self,stations_or_timeseries_fews):
    //     stations_or_timeseries_fews_original_id = stations_or_timeseries_fews.assign(STATION_ID=stations_or_timeseries_fews["ORIGINAL_STATION_ID"])
    //     del stations_or_timeseries_fews_original_id["ORIGINAL_STATION_ID"]
    //     return stations_or_timeseries_fews_original_id

    // def deleteSeriesWithoutTimestep(self,timeseries_fews):
    //     return timeseries_fews[~pandas.isna(timeseries_fews["TIMESTEP_HOUR"])]

    /**
    * getMonitoringPoints Retrieves monitoring points as a geoJSON document from the timeseries API
    * 
    * @param {string} view - WHOS view identifier. Default whos-plata
    * @param {object} filter
    * @param {float} filter.east - Bounding box eastern longitude coordinate
    * @param {float} filter.west - Bounding box western longitude coordinate
    * @param {float} filter.north - Bounding box northern latitude coordinate
    * @param {float} filter.south - Bounding box southern latitude coordinate
    * @param {float} filter.offset - Start position of matched records
    * @param {integer} filter.limit - Maximum number of matched records
    * @param {string} filter.country - Country code (ISO3)
    * @param {object} options
    * @param {string} options.output - Write JSON output into this file
    * @param {boolean} options.a5 - convert to a5 format
    * @param {string} options.output_dir - Write each page into this dir
    * @returns {object}
    */
    async getMonitoringPointsWithPagination(view=this.config.view,filter={},options={}) {
        var estaciones = []
        var results = []
        var limit = (filter.limit) ? filter.limit : this.config.monitoring_points_per_page
        var max = (filter.max) ? filter.max : this.config.monitoring_points_max 
        for(var i=1;i<=max;i=i+limit) {
            console.log("getMonitoringPoints offset: " + i)
            var output = (options.output_dir) ? path.resolve(options.output_dir,sprintf("monitoringPointsResponse_%i.json",i)) : undefined
            const monitoringPoints = await this.getMonitoringPoints(view,{offset:i, limit:limit, west: filter.west, south: filter.south, east: filter.east, north: filter.north, country: filter.country, provider: filter.provider}, {output: output})
            if(!monitoringPoints.hasOwnProperty("results")) {
                console.log("no monitoring points found")
                break
            }
            results = results.concat(...monitoringPoints.results)
            if(options.a5) {
                const stations_i = internal.client.monitoringPointsToA5(monitoringPoints,undefined,this.config.tabla)
                estaciones = estaciones.concat(...stations_i)
            }
            if(monitoringPoints.results.length < limit) {
                break
            }
        }
        const result = {
            "type": "featureCollection",
            "features": results
        }
        if(options.output) {
            if(options.a5) {
                try {
                    fs.writeFileSync(output,JSON.stringify(estaciones))
                } catch(e) {
                    throw("Failed write file " + output)
                }
            } else {
                try {
                    fs.writeFileSync(output,JSON.stringify(result))
                } catch(e) {
                    throw("Failed write file " + output)
                }
            }
        }
        if(options.a5) {
            return estaciones
        } else {
            return result
        }
    }

    /**
    * getTimeseriesMulti
    *
    * @param {string} view
    * @param {object} filter
    * @param {string|string[]} filter.monitoringPoint
    * @param {string|string[]} filter.timeseriesIdentifier
    * @param {string} filter.beginPosition
    * @param {string} filter.endPosition
    * @param {integer} filter.offset=1
    * @param {integer} filter.limit=10
    * @param {object} options
    * @param {string} options.output
    * @param {boolean} options.has_data=false
    * @returns {object}
    */
    async getTimeseriesMulti(view=this.config["view"], filter={}, options={},useCache=false) {
        var monitoringPoint = filter.monitoringPoint
        var timeseriesIdentifier = filter.timeseriesIdentifier
        var includeData = filter.includeData
        var beginPosition = filter.beginPosition
        var endPosition = filter.endPosition
        var offset = (filter.offset) ? filter.offset : 1
        var limit = (filter.limit) ? filter.limit : 10
        var output = options.output
        var has_data= (options.has_data) ? options.has_data : false
        if(monitoringPoint) {
            if(typeof monitoringPoint == "string"){
                monitoringPoint = [monitoringPoint]
            }
        } else {
            monitoringPoint = []
        }
        if(timeseriesIdentifier) {
            if(typeof timeseriesIdentifier == "string") {
                timeseriesIdentifier = [timeseriesIdentifier]
            }
        } else {
            timeseriesIdentifier = []
        }
        if (!monitoringPoint.length) {
            if (!timeseriesIdentifier.length) {
                return this.getTimeseries( view, {includeData: includeData, beginPosition: beginPosition, endPosition: endPosition, offset: offset, limit: limit},{output:output, has_data: has_data}, useCache)
            } else {
                var members = []
                for(var ti of timeseriesIdentifier) {
                    console.log("timeseriesIdentifier: " + ti)
                    const timeseries = await this.getTimeseries(view, {includeData: includeData, timeseriesIdentifier: ti, beginPosition: beginPosition, endPosition: endPosition, offset: offset, limit: limit}, {output: output, has_data:has_data}, useCache)
                    if(timeseries.hasOwnProperty("member")) {
                        members = members.concat(timeseries["member"])
                    }
                }
                return {
                    "type": "featureCollection",
                    "member": members
                }
            }
        } else {
            var members = []
            for(var mp of monitoringPoint) {
                console.log("monitoringPoint: " + mp)
                if (!timeseriesIdentifier.length) {
                    const timeseries = await this.getTimeseries(view, {includeData: includeData, monitoringPoint: mp, beginPosition: beginPosition, endPosition: endPosition, offset: offset, limit: limit},{output: output, has_data: has_data},useCache)
                    if(timeseries.hasOwnProperty("member")){
                        members = members.concat(timeseries["member"])
                    }
                } else {
                    for(var ti of timeseriesIdentifier) {
                        console.log("observedProperty: " + ti)
                        const timeseries = await this.getTimeseries(view, {includeData: includeData, monitoringPoint: mp, timeseriesIdentifier: ti, beginPosition: beginPosition, endPosition: endPosition, offset: offset, limit: limit},{output: output, has_data:has_data},useCache)
                        if(timeseries.hasOwnProperty("member")) {
                            members = members.concat(timeseries["member"])
                        }
                    }
                }
            }
            return {
                "id": "observation collection",
                "member": members
            }
        }
    }

    /**
    * getTimeseriesWithPagination
    *
    * @param {string} view
    * @param {object} filter
    * @param {string|string[]} filter.monitoringPoint
    * @param {string|string[]} filter.timeseriesIdentifier
    * @param {string} filter.beginPosition
    * @param {string} filter.endPosition
    * @param {object} options
    * @param {string} options.output
    * @param {boolean} options.a5=false
    * @param {boolean} options.save_geojson=false
    * @param {string} options.output_dir=""
    * @param {boolean} options.has_data=true
    * @returns {object}
    */
    async getTimeseriesWithPagination(view=this.config["view"], filter={}, options={}, useCache=false) {
        var monitoringPoint = filter.monitoringPoint
        var timeseriesIdentifier = filter.timeseriesIdentifier
        var beginPosition = filter.beginPosition
        var endPosition = filter.endPosition
        var includeData = filter.includeData
        var output = options.output
        var a5= (options.a5) ? options.a5 : false
        var save_geojson = (options.save_geojson) ? options.save_geojson : false
        var output_dir = (options.output_dir) ? path.resolve(options.output_dir) : ""
        var has_data = (options.has_data) ? options.has_data : true
        var members = []
        // var_map = self.getVariableMapping()
        var timeseries_a5 = []
        for(var i=1;i<=this.config.timeseries_max;i=i+this.config.timeseries_per_page) {
            console.log("getTimeseriesMulti, offset: " + i)
            var output = (save_geojson) ? path.resolve(output_dir,sprintf("timeseriesResponse_%i.json", i)) : undefined
            var timeseries = await this.getTimeseriesMulti(view, {monitoringPoint: monitoringPoint, timeseriesIdentifier: timeseriesIdentifier, includeData: includeData, beginPosition: beginPosition, endPosition: endPosition, offset: i, limit: this.config.timeseries_per_page},{output: output,has_data: has_data},useCache)
            if(!timeseries.hasOwnProperty("member")) {
                console.error("No timeseries found")
                break
            }
            var timeseries_length = timeseries["member"].length
            console.log(sprintf ("Found %i members" , timeseries_length))
            if(has_data) {
                timeseries["member"] = this.filterByAvailability(timeseries["member"],this.config.threshold_begin_date)
            }
            console.log(sprintf("Offset: %i, length: %i, got %i timeseries after filtering", i,this.config["timeseries_per_page"],timeseries["member"].length))
            if(a5) {
                timeseries_a5 = timeseries_a5.concat(internal.client.timeseriesToA5(timeseries))
            }
            members = members.concat(timeseries["member"])
            if(timeseries_length < this.config.timeseries_per_page) {
                console.log("last page, breaking")
                break
            }
        }
        const result = {
            "id": "observation collection",
            "member": members
        }
        if(output) {
            if(a5) {
                fs.writeFileSync(output,JSON.stringify(timseries_a5),'utf-8')
            } else {
                fs.writeFileSync(output,JSON.stringify(result),'utf-8')
            }
        }
        if(a5) {
            return timeseries_a5
        } else {
            return result
        }
    }

    async test() {
        try {
            await this.getMonitoringPoints(this.config.view,{limit:1})
        } catch(e) {
            console.error(e)
            return false
        }
        return true
    }

    /**
     * @typedef {number[]} Point
     */

    /**
     * @typedef {Object} Geometry
     * @property {string} type
     * @property {Point|Point[]} coordinates
     */

    /**
     * Retrieves features of interest from ogc api and (unless options.no_update set to true) saves into features_of_interest of accessor schema (updates if already there)
     * @param {*} filter
     * @param {string} filter.view
     * @param {Geometry} filter.geom
     * @param {string} filter.pais 
     * @param {string} filter.provider
     * @param {string|string[]} filter.id_externo
     * @param {*} options
     * @param {boolean} options.no_update - skip inserting/updating records in accesor database 
     * @param {boolean} options.update_estaciones - insert/update a5 estaciones records
     * @returns {Promise<internal.feature_of_interest[]>} feature of interest array
     */
    async getSites(filter={},options={}) {
        options.a5 = false // (options.a5) ? options.a5 : true
        var view = (filter.view) ? filter.view : this.config.view
        if(filter.geom) {
            const bbox = geom2bbox(filter.geom)
            filter.east = bbox.east
            filter.west = bbox.west
            filter.north = bbox.north
            filter.south = bbox.south
            filter.geom = undefined  
        }
        if(filter.pais) {
            filter.country = internal.client.getCountryIsoCode(filter.pais)
            filter.pais = undefined
        }
        if(filter.provider) {
            filter.provider = filter.provider.toString()
        }
        const result = await this.getMonitoringPointsWithPagination(view,filter,options)
        var foi = this.monitoringPointsToFeaturesOfInterest(result.features)
        if(filter.id_externo) {
            if(Array.isArray(filter.id_externo)) {
                foi = foi.filter(f=>{
                    return (filter.id_externo.map(i=>i.toString()).indexOf(f.feature_id.toString()) >= 0)
                })
            } else {
                foi = foi.filter(f=>{
                    return (f.feature_id.toString() == filter.id_externo.toString())
                })
            }
        }
        if(!options.no_update) {
            // const created = []
            for(var f of foi) {
                await f.create()
            }
        }
        if(options.raw) {
            return foi
        } else {
            const estaciones = []
            for(var f of foi) {
                const estacion = f.toEstacion()
                await estacion.getEstacionId()
                estaciones.push(estacion)
                console.log("id_externo: " + estacion.id_externo)
                if(options.update_estaciones) {
                    console.log(">>>>>>>>>UPDATING ESTACION<<<<<<<<<")
                    await estacion.create()
                    await f.update({estacion_id:estacion.id})
                }
            }
            console.log("Got " + estaciones.length + " estaciones")
            return estaciones
        }
    }

    async getSavedSites(filter={}) {
        if(filter.estacion_id) {
            filter.id = filter.estacion_id
        }
        if(filter.id) {
            return Estacion.read(filter)
        }
        const feature_filter = {}
        Object.assign(feature_filter,filter)
        feature_filter.accessor_id = (this.config.accessor_id) ? this.config.accessor_id : "om_ogc_timeseries_client"
        feature_filter.network_id = this.config.tabla
        feature_filter.estacion_id = new not_null()
        const features = await accessor_feature_of_interest.read(feature_filter)
        if(!features.length) {
            console.warn("No features found")
            return []
        }
        const estaciones_filter = {}
        Object.assign(estaciones_filter,filter)
        estaciones_filter.tabla = this.config.tabla
        estaciones_filter.id = features.map(f=>f.estacion_id)
        return Estacion.read(estaciones_filter)
    }

    /**
     * gets features of interest from ogc api, saves and returns as estaciones in a5 schema (updates if already there)
     * @param {*} filter
     * @param {string} filter.view
     * @param {Geometry} filter.geom
     * @param {string} filter.pais 
      * @param {*} options 
     * @returns {Promise<Estacion[]>} estaciones
     * @todo filter.estacion_id
     * @todo filter.id_externo
     * @todo filter.provider
     */
    async updateSites(filter={},options={}) {   
        options.no_update = false
        options.update_estaciones = true
        const estaciones = await this.getSites(filter,options)
        // created_estaciones = []
        // estaciones.forEach(async estacion=> {
        //     console.log("creating estacion " + estacion.id_externo)
        //     // created_estaciones.push(
        //     await estacion.create()
        //     // )
        // })
        return estaciones // created_estaciones
    }

    async getSeriesFilters(filter={},ts_filter={},estaciones=[]) {
        if(filter.monitoringPoint) {
            ts_filter.monitoringPoint = filter.monitoringPoint
        } else if(filter.estacion_id || filter.pais || filter.propietario || filter.feature_of_interest_id || filter.id_externo) {
            // reads from accessor_feature_of_interest.estacion_id
            const features_of_interest = await internal.feature_of_interest.read({
                estacion_id:filter.estacion_id,
                pais:filter.pais,
                propietario:filter.propietario,
                feature_id: filter.id_externo
            })
            if(!features_of_interest.length) {
                throw(new Error("No features of interest found in database matching the provided criteria. Run getSites to update database."))
            }
            ts_filter.monitoringPoint = []
            features_of_interest.forEach(foi=>{
                ts_filter.monitoringPoint.push(foi.feature_id)
                estaciones.push(foi.toEstacion())
            })
        }
        if(filter.timeseriesIdentifier) {
            ts_filter.timeseriesIdentifier = filter.timeseriesIdentifier
        } else if(filter.series_id) {
            // reads from accessor_timeseries_observation.series_puntual_id
            const timeseries_observations = await internal.timeseries_observation.read({series_puntual_id:filter.series_id})
            ts_filter.timeseriesIdentifier = (timeseries_observations.length) ? timeseries_observations.map(tso=>tso.timeseries_id) : undefined
        } else if (filter.var_id || filter.proc_id || filter.unit_id || filter.fuentes_id || filter.tipo || filter.observed_property_id || filter.variable_name) {
            const read_filter = {
                var_id: filter.var_id,
                proc_id: filter.proc_id,
                unit_id: filter.unit_id,
                fuentes_id: filter.fuentes_id,
                tipo: filter.tipo,
                observed_property_id: filter.observed_property_id,
                variable_name: filter.variable_name,
                feature_of_interest_id: ts_filter.monitoringPoint ?? filter.id_externo
            }
            const timeseries_observations = await internal.timeseries_observation.read(read_filter)
            if(!timeseries_observations.length) {
                throw(new Error("No timeseries observations found in database matching the provided criteria. Run getSeries to update database."))
            }
            ts_filter.timeseriesIdentifier = timeseries_observations.map(tso=>tso.timeseries_id)
        }
        return
    }

    async updateSeries (filter={},options={}) {
        const estaciones = await this.updateSites(filter)
        const series = []
        for(var estacion of estaciones) {
            options.update_series = true
            const series_of_site = await this.getSeriesOfSite(estacion,filter,options)
            series.push(...series_of_site)
            // for(var serie of series_of_site) {
            //     try {
            //         await serie.create()
            //     } catch (e) {
            //         console.error(e)
            //         continue
            //     }
            //     series.push(serie)
            // }
        }
        return series
    }
    async getSavedSeries(filter={}) {
        if(filter.series_id) {
            filter.id = filter.series_id
        }
        if(filter.id) {
            return Serie.read(filter)
        }
        const tso_filter = {}
        Object.assign(tso_filter,filter)
        tso_filter.accessor_id = (this.config.accessor_id) ? this.config.accessor_id : "om_ogc_timeseries_client"
        tso_filter.series_puntual_id = new not_null()
        if(filter.id_externo) {
            tso_filter.feature_of_interest_id = filter.id_externo
        }
        const tso = await accessor_timeseries_observation.read(tso_filter)
        if(!tso.length) {
            console.warn("No saved timeseries observations found")
            return []
        }
        const series_filter = {}
        Object.assign(series_filter,filter)
        series_filter.id = tso.map(t=>t.series_puntual_id)
        return Serie.read(series_filter)
    }
    async getSeries(filter={},options={}) {
        const ts_filter = {}
        var estaciones = []
        await this.getSeriesFilters(filter,ts_filter,estaciones)
        console.log("monitoringPoint: " + ts_filter.monitoringPoint)
        if(ts_filter.timeseriesIdentifier) {
            const series = await this.getSeriesOfIdentifier(ts_filter,options)
            return series
        }
        if(!estaciones.length) {
            console.log("Monitoring points not set. Getting sites from accessor")
            estaciones = await this.getSites(filter)
        }
        const series = []
        for(var estacion of estaciones) {
            const series_of_site = await this.getSeriesOfSite(estacion,ts_filter,options)
            series.push(...series_of_site)
        }
        return series
    }
    async getSeriesOfIdentifier(filter={},options={}) {
        if(!filter.timeseriesIdentifier) {
            throw("Missing filter.timeseriesIdentifier")
        }
        const ts_filter = {}
        const ts_options = {}
        ts_options.a5 =  (options.a5) ? options.a5 : false
        var view = (filter.view) ? filter.view : this.config.view
        ts_filter.includeData = false
        // removes date range filters because API doesn't take data availability filters
        ts_filter.beginPosition = undefined
        ts_filter.endPosition = undefined
        ts_filter.timeseriesIdentifier = filter.timeseriesIdentifier
        const result = await this.getTimeseriesWithPagination(view,ts_filter,ts_options)
        const timeseries_observations = result.member.map(m=>{
            return this.parseTimeseriesMember(m)
        })
        if(!options.no_update) {
            // const timeseries_observations_created = []
            for(var tso of timeseries_observations) {
                await tso.create()
            }
        }
        return this.tsoToSeries(timeseries_observations,options)
    }
    
    async tsoToSeries(timeseries_observations=[],options={},filter={}) {
        const series = []
        for(var tso of timeseries_observations) {
            const serie = await tso.findSerie()
            if(serie) {
                if(!serie.filterSerie(filter)) {
                    continue
                }
                if(options.update_series) {
                    try {
                        await serie.create()
                    } catch (e) {
                        console.error(e)
                        continue
                    }
                    await tso.update({series_puntual_id:serie.id})
                }
                series.push(serie)
            }
        }
        return series
    }
    async getSeriesOfSite(estacion,filter={},options={}) {
        var view = (filter.view) ? filter.view : this.config.view
        const ts_filter = {}
        ts_filter.includeData = false
        // removes date range filters because API doesn't take data availability filters
        ts_filter.beginPosition = undefined
        ts_filter.endPosition = undefined
        ts_filter.monitoringPoint = estacion.id_externo
        const result = await this.getTimeseriesWithPagination(view,ts_filter)
        const timeseries_observations = result.member.map(m=>{
            return this.parseTimeseriesMember(m)
        })
        if(!options.no_update) {
            // const timeseries_observations_created = []
            for(var tso of timeseries_observations) {
                await tso.create()
            }
        }
        var series = await this.tsoToSeries(timeseries_observations,options,filter)
        return series
    }

    /**
     * Retrieve timeseries from ogc api, save and return timeseries observations and save into timeseries_observation of accessor schema
     * @param {*} timeseries_filter
     * @param {string|string[]} filter.id_externo - featureOfInterest ID 
     * @param {integer|integer[]} filter.estacion_id - estacion_id of accessor_feature_of_interest (maps to estacion.unid of a5 schema)
     * @param {integer|integer[]} filter.series_id - series_puntual_id of accessor_timeseries_observation (maps to series.id of a5 schema)
     * @param {}
     * @param {*} options 
     * @param {boolean} options.no_update - skip inserting/updating records in accesor database 
     * @returns {Promise<internal.timeseries_observation[]} - timeseries observations
     */
    async getSeries_(filter={},options={}) {
        const ts_filter = {}
        const ts_options = {}
        ts_options.a5 =  (options.a5) ? options.a5 : false
        var view = (filter.view) ? filter.view : this.config.view
        ts_filter.includeData = false
        // removes date range filters because API doesn't take data availability filters
        ts_filter.beginPosition = undefined
        ts_filter.endPosition = undefined
        await this.getSeriesFilters(filter,ts_filter)
        const result = await this.getTimeseriesWithPagination(view,ts_filter,ts_options)
        const timeseries_observations = result.member.map(m=>{
            return this.parseTimeseriesMember(m)
        })
        if(!options.no_update) {
            // const timeseries_observations_created = []
            for(var tso of timeseries_observations) {
                await tso.create()
            }
        }
        return timeseries_observations
    }

    parseTimeseriesMember(m) {
        return new internal.timeseries_observation({
            accessor_id: this.config.accessor_id,
            timeseries_id: m.id,
            feature_of_interest: {
                accessor_id: this.config.accessor_id,
                feature_id: m.featureOfInterest.href,
                network_id: this.config.tabla
            },
            observed_property: {
                accessor_id: this.config.accessor_id,
                observed_property_id: m.observedProperty.title, // .href is missing in some results
                name: m.observedProperty.title,
                result: m.observedProperty
            },
            unit_of_measurement: {
                accessor_id: this.config.accessor_id,
                unit_of_measurement_id: (m.result.hasOwnProperty("defaultPointMetadata")) ? m.result.defaultPointMetadata.uom : undefined
            },
            time_support: (m.result.metadata.hasOwnProperty("intendedObservationSpacing")) ? isoDurationToHours(m.result.metadata.intendedObservationSpacing) : (m.result.hasOwnProperty("defaultPointMetadata") && m.result.defaultPointMetadata.hasOwnProperty("aggregationDuration")) ? isoDurationToHours(m.result.defaultPointMetadata.aggregationDuration) : undefined,
            data_type: (m.result.hasOwnProperty("defaultPointMetadata") && m.result.defaultPointMetadata.hasOwnProperty("interpolationType")) ? m.result.defaultPointMetadata.interpolationType.title : undefined,
            result: m,
            data: (m.result.points && m.result.points.length) ? m.result.points.map(p=>this.parseTimeValuePair(p,m)).filter(p=>(p.valor !== null)) : undefined,
            begin_position: m.phenomenonTime ? new Date(m.phenomenonTime.begin) : undefined,
            end_position: m.phenomenonTime ? new Date(m.phenomenonTime.end) : undefined
        })
    }

    parseTimeValuePair(p,ts,no_data_value=this.config.no_data_value) {
        return new accessor_time_value_pair({
            accessor_id: this.config.accessor_id,
            timeseries_id: ts.id,
            timestamp: new Date(p.time.instant),
            numeric_value: (typeof p.value !== 'undefined' && p.value != no_data_value) ? p.value : null,
            observaciones_puntual_id: ts.series_puntual_id,
            observaciones_areal_id: ts.series_areal_id,
            observaciones_rast_id: ts.series_rast_id,
            result: p
        })
    }
    /**
     * Get observations of given series between timestart and timeend
     * @param {Object} filter 
     * @param {string} filter.tipo
     * @param {integer} filter.series_id
     * @param {Date} filter.timestart
     * @param {Date} filter.timeend
     * @param {Object} options 
     * @returns {Promise<Observacion[]>} A promise that returns an array of Observaciones
     */
    async get(filter={},options={}) {
        if(!filter.series_id) {
            throw("om_ogc_timeseries_client: client.get: Missing filter.series_id")
        }
        if(!filter.tipo) {
            filter.tipo = "puntual"
        }
        if(!filter.timestart) {
            throw("om_ogc_timeseries_client: client.get: missing filter.timestart")
        }
        if(!filter.timeend) {
            throw("om_ogc_timeseries_client: client.get: missing filter.timeend")
        }
        const tso_filter = {}
        if(filter.tipo == "puntual") {
            tso_filter.series_puntual_id =  filter.series_id
        } else if(filter.tipo == "areal") {
            tso_filter.series_areal_id = filter.series_id
        } else if(filter.tipo == "rast") {
            tso_filter.series_rast_id = filter.series_id
        } else {
            throw("Invalid tipo")
        }
        var tso = await accessor_timeseries_observation.read(tso_filter)
        if(!tso.length) {
            throw("Serie not found of tipo: " + filter.tipo + ", series_id:" + filter.series_id)
        }
        tso = tso[0]
        const observaciones = await this.getObservationsOfTimeseries(tso,filter,{a5:true},options.useCache)
        return observaciones
    }

    async update(filter={},options={}) {
        var observaciones = await this.get(filter,options)
        observaciones = new Observaciones(observaciones) 
        observaciones = await observaciones.create()
        return observaciones
    }

    async getObservationsOfTimeseries(tso,filter={},options={},useCache=false) {
        if(!tso) {
            throw("Missing tso")
        }
        if(!tso.timeseries_id) {
            throw("Missing tso.timeseries_id")
        }
        const ts_filter = {}
        Object.assign(ts_filter,filter)
        ts_filter.timeseriesIdentifier = tso.timeseries_id
        const ts_options = {}
        Object.assign(ts_options,options)
        ts_options.a5 = false
        ts_options.tvp = true
        var time_value_pairs = await this.getObservations(ts_filter,ts_options,useCache)
        if(options.a5) {
            time_value_pairs = time_value_pairs.map(tvp=>tvp.toObservacion(tso))
        }
        return time_value_pairs
    }

    async getObservations(filter={},options={},useCache=false) {
        const ts_filter = {}
        const ts_options = {}
        ts_options.a5 = false
        var view = (filter.view) ? filter.view : this.config.view
        if(!filter.timestart || !filter.timeend) {
            throw("missing timestart and/or timeend")
        }
        ts_filter.includeData = true
        ts_filter.beginPosition = new Date(filter.timestart).toISOString()
        ts_filter.endPosition = new Date(filter.timeend).toISOString()
        await this.getSeriesFilters(filter,ts_filter)
        const result = await this.getTimeseriesWithPagination(view,ts_filter,ts_options,useCache)
        if(options.raw) {
            return result
        }
        const timeseries_observations = result.member.map(m=>this.parseTimeseriesMember(m))
        if(!options.no_update) {
            // const timeseries_observations_created = []
            for(var tso of timeseries_observations) {
                await tso.create()
            }
        }
        if(options.tvp) {
            const time_value_pairs = []
            for(var tso of timeseries_observations) {
                if(!tso.data) {
                    console.warn("No data property found in timeseries observation")
                    continue
                }
                for(var tvp of tso.data) {
                    // const tvp = this.parseTimeValuePair(p,tso)
                    // tvp.create()
                    if(tvp.numeric_value !== null) {
                        if(options.a5) {
                            time_value_pairs.push(tvp.toObservacion(tso.time_support))
                        } else {
                            time_value_pairs.push(tvp)
                        }
                    }
                }
            }
            return time_value_pairs
        } else {
            if(options.a5) {
                return timeseries_observations.map(tso=>tso.toSerie())
            } else {
                return timeseries_observations
            }
        }
    }
    
    // def getOrganization(self,timeseries : dict,stations_fews : pandas.DataFrame = None, fews=True) -> pandas.DataFrame:  # , delete_none = False
    //     '''Reads organisationName from timeseries object. If stations_fews provided, updates ORGANIZATION column. Returns DataFrame with columns STATION_ID,organisationName'''

    //     station_organization = []
    //     # missing_organization = []
    //     for f in timeseries["features"]:
    //         if "relatedParties" in f["properties"]["timeseries"]["featureOfInterest"] and len(f["properties"]["timeseries"]["featureOfInterest"]["relatedParties"]) and "organisationName" in f["properties"]["timeseries"]["featureOfInterest"]["relatedParties"][0]:
    //             organizationName = f["properties"]["timeseries"]["featureOfInterest"]["relatedParties"][0]["organisationName"]
    //             if fews and organizationName in self.fews_organization_map:
    //                 organizationName = self.fews_organization_map[organizationName]
    //             station_organization.append({
    //                 "STATION_ID": f["properties"]["timeseries"]["featureOfInterest"]["sampledFeature"]["href"],
    //                 "organisationName": organizationName
    //             })
    //         # else:
    //             # missing_organization.append({
    //             #     "STATION_ID": f["properties"]["timeseries"]["featureOfInterest"]["sampledFeature"]["href"],
    //             #     "PARAMETER_ID": f["properties"]["timeseries"]["observedProperty"]["href"]
    //             # })
    //     station_organization = pandas.DataFrame(station_organization).drop_duplicates(ignore_index=True)
    //     # missing_organization = pandas.DataFrame(missing_organization)
    //     if stations_fews is not None:
    //         merged = stations_fews.merge(station_organization,how='left', on='STATION_ID')
    //         stations_fews["ORGANIZATION"] = merged["organisationName"].combine_first(merged["ORGANIZATION"]) # merged["organisationName"]
    //         del merged
    //         # if delete_none:
    //         #     stations_fews.drop(stations_fews[stations_fews["ORGANIZATION"].isnull()].index)
    //     return station_organization
    
    // def deleteStationsWithNoTimeseries(self,stations_fews,timeseries_fews):
    //     ts_st = timeseries_fews[["STATION_ID"]].drop_duplicates(ignore_index=True)
    //     stations_fews = stations_fews.merge(ts_st,how='inner',on="STATION_ID")
    //     return stations_fews

}    

function geom2bbox(geom) {
    if(geom.type.toLowerCase() == "point") {
        return {
            north: geom.coordinates[1],
            south: geom.coordinates[1],
            east: geom.coordinates[0],
            west: geom.coordinates[0]
        }
    } else if (geom.type.toLowerCase() == "polygon") {
        var north = geom.coordinates.reduce((max,point)=>(point[1]>max) ? point[1] : max, -90)
        var south = geom.coordinates.reduce((min,point)=>(point[1]<min) ? point[1] : min, 90)
        var east = geom.coordinates.reduce((max,point)=>(point[0]>max) ? point[0] : max, -180)
        var west = geom.coordinates.reduce((min,point)=>(point[0]<min) ? point[0] : min, 180)
        return {
            north: north,
            south: south,
            east: east,
            west: west
        }
    } else {
        console.error("Invalid geometry type")
        return
    }
}

module.exports = internal