'use strict'

const {getConfig} = require('./utils')
// const {sprintf} = require('sprintf-js')
const axios = require('axios')
const fs = require('promise-fs')
// const {estacion: Estacion, serie: Serie, observacion: Observacion, observaciones: Observaciones} = require('./CRUD')
// const moment = require('moment-timezone')
// const {not_null} = require('./utils')
const {accessor_feature_of_interest, accessor_timeseries_observation, accessor_observed_property, accessor_unit_of_measurement, accessor_time_value_pair} = require('./accessor_mapping')
// const {isoDurationToHours} = require('./timeSteps')
const { createLogger, transports } = require("winston")
const { feature } = require('@turf/helpers')
const logger = createLogger({
    transports: [new transports.Console()]
})
const createCsvWriter = require('csv-writer').createObjectCsvWriter

const internal = {}

internal.Feature = class {
    static example = {
        "shape": {
          "coordinates": [
            -54.6831,
            -27.5781
          ],
          "type": "Point"
        },
        "parameter": [
          {
            "name": "country",
            "value": "Brazil"
          },
          {
            "name": "identifier",
            "value": "74720000"
          }
        ],
        "name": "PORTO MAUÁ",
        "id": "000BE3CB65DF1F7D571EF045272E048FCD0072BD",
        "relatedParty": [
          {
            "organisationName": "National Water Agency of Brazil",
            "role": "author",
            "URL": "https://www.ana.gov.br/"
          }
        ]
      }
    static header = [
        { id: "id", title: "id"},
        { id: "name", title: "name"},
        { id: "longitude", title: "longitude"},
        { id: "latitude", title: "latitude"},
        { id: "country", title: "country"},
        { id: "identifier", title: "identifier"},
        { id: "relatedParty", title: "relatedParty"}
    ]

    constructor(feature) {
        this.shape = feature.shape
        this.parameter = feature.parameter
        this.name = feature.name
        this.id = feature.id
        this.relatedParty = feature.relatedParty
    }

    toRow() {
        return {
            id: this.id,
            name: this.name,
            longitude: this.shape.coordinates[0],
            latitude: this.shape.coordinates[1],
            country: this.getParameter("country"),
            identifier: this.getParameter("identifier"),
            relatedParty: this.getRelatedParty()
        }
    }

    getParameter(key) {
        for(var p of this.parameter) {
            if(key == p.name) {
                return p.value
            }
        }
        return null
    }

    getRelatedParty() {
        if(this.relatedParty && this.relatedParty.length) {
            return `${this.relatedParty[0].organisationName ?? ""} - ${this.relatedParty[0].role ?? ""} - ${this.relatedParty[0].URL ?? ""}`
        }
        return null
    }

    toGeoJSON() {
        const properties = {
            id: this.id,
            name: this.name
        }
        if(this.relatedParty && this.relatedParty.length) {
            properties.relatedPartyOrganisationName = this.relatedParty[0].organisationName
            properties.relatedPartyRole = this.relatedParty[0].role
            properties.relatedPartyURL = this.relatedParty[0].URL
        }
        if(this.parameter) {
            for(var p of this.parameter) {
                properties[p.name] = p.value
            }
        }
        return {
            type: "Feature",
            id: this.id,
            name: this.name,
            geometry: this.shape,
            properties: properties
        }
    }
}

// internal.observation = class extends accessor_timeseries_observation {

// }

internal.Client = class {
    static _get_is_multiseries = false
    
    static default_config = {
        "url": "https://whos.geodab.eu/gs-service/services/essi",
        "token": "YOUR_TOKEN_HERE",
        "features_max": 6000,
        "features_per_page": 200,
        "observations_max": 48000,
        "observations_per_page": 400,
        "view": "whos-plata",
        "begin_days": null,
        "tabla": "whos_plata",
        "accessor_id": "ogc_om_api_client",
        "no_data_value": -9999
    }

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
    * Instantiate a new OGC OM REST API client
    * 
    * @param {object} config
    * @param {string} config.url
    * @param {string} config.token
    * @param {string} config.features_max
    * @param {string} config.features_per_page
    * @param {string} config.observations_max
    * @param {string} config.observations_per_page 
    * @returns {internal.Client}
    */
    constructor(config) {        
        this.config = getConfig(config,internal.Client.default_config)
        if(this.config.begin_days) {
            this.config.threshold_begin_date = new Date()
            this.config.threshold_begin_date.setDate(this.config.threshold_begin_date.getDate()-this.config.begin_days)
        }
        this.last_request = {
            url: null,
            params: null,
            response: null
        }
        this.features = []
        this.observations = []
    }

    readFeatureById(feature_id,return_index) {
        for(var i in this.features) {
            var feature = this.features[i]
            if(feature.id == feature_id) {
                logger.debug("feature_id " + feature_id + " found at index " + i)
                if(return_index) {
                    return i
                }
                return feature
            }
        }
        logger.debug("feature_id " + feature_id + " not found")
        if(return_index) {
            return -1
        }
        return
    }

    readObservationById(observation_id,return_index) {
        for(var i in this.observations) {
            var observation = this.observations[i]
            if(observation.id == observation_id) {
                logger.debug("observation_id " + observation_id + " found at index " + i)
                if(return_index) {
                    return i
                }
                return observation
            }
        }
        logger.debug("observation_id " + observation_id + " not found")
        if(return_index) {
            return -1
        }
        return
    }

    addFeature(feature,index=-1) {
        if(index >= 0) {
            this.features[index] = feature
        } else {
            this.features.push(feature)
        }
    }

    addFeatures(features=[]) {
        for(var feature of features) {
            var index = this.readFeatureById(feature.id,true)
            this.addFeature(new internal.Feature(feature),index)
        }
    }

    addObservation(observation,index=-1) {
        if(index >= 0) {
            this.observations[index] = observation
        } else {
            this.observations.push(observation)
        }
    }

    addObservations(observations=[]) {
        for(var observation of observations) {
            var index = this.readObservationById(observation.id,true)
            this.addObservation(observation,index)
        }
    }

    getUrl(base_url,token,view,request) {
        return `${base_url}/token/${token}/view/${view}/om-api/${request}`
    }

    async requestFeatures(west,south,east,north,country,provider,offset,limit,base_url=this.config.url,token=this.config.token,view=this.config.view) {
        const url = this.getUrl(base_url,token,view,"features")
        const params = {
            west: west,
            south: south,
            east: east,
            north: north,
            country: country,
            provider: provider,
            offset: offset,
            limit: limit
        }
        try {
            var response = await axios.get(
                url,
                {
                    params: params
                }
            )
        } catch(e) {
            logger.info(getFullUrl(response.request))
            throw("Request failed: " + e.toString())
        }
        this.setLastRequest(url,params,response)
        logger.info(getFullUrl(response.request))
        if(response.status >= 400) {
            throw("Request failed, status code: " + response.status)
        }
        this.addFeatures(response.data.results)
        return response.data.results
    }
    
    async requestObservations(feature,observationIdentifier,beginPosition,endPosition,west,south,east,north,observedProperty,ontology,timeInterpolation,includeData,useCache,offset,limit,format,base_url=this.config.url,token=this.config.token,view=this.config.view) {
        const url = this.getUrl(base_url,token,view,"observations")
        const params = {
            feature: feature,
            observationIdentifier: observationIdentifier,
            beginPosition: beginPosition,
            endPosition: endPosition,
            west: west,
            south: south,
            east: east,
            north: north,
            observedProperty: observedProperty,
            ontology: ontology,
            timeInterpolation: timeInterpolation,
            includeData: includeData,
            useCache: useCache,
            offset: offset,
            limit: limit,
            format: format                        
        }
        try {
            var response = await axios.get(
                url,
                {
                    params: params
                }
            )
        } catch(e) {
            logger.info(getFullUrl(response.request))
            throw("Request failed: " + e.toString())
        }
        this.setLastRequest(url,params,response)
        logger.info(getFullUrl(response.request))
        if(response.status >= 400) {
            throw("Request failed, status code: " + response.status)
        }
        this.addObservations(response.data.member)
        return response.data.member
    }

    setLastRequest(url,params,response) {
        this.last_request = {
            url,
            params,
            response,
            full_url: getFullUrl(response.request)
        }
    }

    async requestFeaturesWithPagination(filter) {
        var offset = filter.offset ?? 1
        var limit = filter.limit ?? this.config.features_per_page
        var results = []
        while(true) {
            const features = await this.requestFeatures(
                filter.west,
                filter.south,
                filter.east,
                filter.north,
                filter.country,
                filter.provider,
                offset,
                limit
            )
            results = results.concat(features) 
            if(!features || !features.length || features.length < limit) {
                break
            }
            offset = offset + limit
        }
        return results
    }

    async requestObservationsWithPagination(filter) {
        var offset = filter.offset ?? 1
        var limit = filter.limit ?? this.config.observations_per_page
        var results = []
        while(true) {
            const observations = await this.requestObservations(
                filter.feature,
                filter.observationIdentifier,
                filter.beginPosition,
                filter.endPosition,
                filter.west,
                filter.south,
                filter.east,
                filter.north,
                filter.observedProperty,
                filter.ontology,
                filter.timeInterpolation,
                filter.includeData,
                filter.useCache,
                offset,
                limit,
                filter.format
            )
            results = results.concat(observations) 
            if(!observations || !observations.length || observations.length < limit) {
                break
            }
            offset = offset + limit
        }
        return results
    }

    featuresToGeoJSON() {
        return {
            type: "FeatureCollection",
            features: this.features.map(f=>f.toGeoJSON())
        }
    }

    async featuresToCSV(destination) {
        const csv_writer = createCsvWriter({
            path: destination,
            header: internal.Feature.header
        })
        const data = this.features.map(f=>f.toRow())
        return csv_writer.writeRecords(data)
    }

    async writeFeatures(destination,format="json") {
        if(!destination) {
            throw("Missing write destination path")
        }   
        if(format.toLowerCase() == "json") {
            await fs.writeFile(destination,JSON.stringify({features:this.features},null,4))
        } else if (format.toLowerCase() == "geojson") {
            await fs.writeFile(destination,JSON.stringify(this.featuresToGeoJSON(),null,4))
        } else if(format.toLowerCase() == "csv") {
            await this.featuresToCSV(destination)
        } else {
            throw("Invalid format: " + format)
        }
        return
    }
}

function getFullUrl(request) {
    return `${request.protocol}//${request.host}${request.path}`
}

module.exports = internal