require('./setGlobal')
const api_config = global.config.api ?? {}
const xmlbuilder = require('xmlbuilder2')
// const { Pool } = require('pg')
// const config = require('config');
// const pool = new Pool(config.database)
// const port = config.rest.port
const CRUD = require('./CRUD');
// const { request } = require('http');
const crud = CRUD.CRUD // new CRUD.CRUD(pool,config)
const moment = require('moment-timezone')
const { find } = require('geo-tz')
var ucum = require('@lhncbc/ucum-lhc');
var ucum_utils = ucum.UcumLhcUtils.getInstance();
var timeSteps = require('./timeSteps.js')
// var fs = require('fs');
// const internal = require('stream');
const internal = {}

internal.series2waterml2 = class {
    constructor() {
        // this.crud = arguments[0]
        this.codelists = {}
        this.base_url = api_config.base_url ?? ""
        
        // templates
        this.templates = {
            collection: collection_template,
            observation: observation_template,
            point: point_template,
            parameter: parameter_template
        }
    }

    // aux functions
    async getCodelists() {
        var requests = []
        requests.push(crud.getPaises())
        requests.push(crud.getRegionesOMM())
        requests.push(crud.getDataTypes())
        return Promise.all(requests)
        .then(results=>{
            console.log("got codelists")
            this.codelists.paises = results[0]
            this.codelists.regiones_omm = results[1]
            this.codelists.data_types = results[2]
        })
    }

    getCode(codelist_name,key,value) {
        if(!this.codelists[codelist_name]) {
            throw("bad codelist name")
        }
        var match = this.codelists[codelist_name].filter(i=>i[key] == value)
        if(!match.length) {
            console.error("codelist value not found")
            return
        } else {
            return match[0] 
        }
    }

    //m = getCode('data_types','term','Continuous')

    getGeomTimeZoneOffset(geom,date=new Date(),format) {
        if(geom.type == "Point") {
            var tz_name = find(geom.coordinates[1],geom.coordinates[0])
        } else if(geom.type == "Polygon") {
            var tz_name = find(Math.min(...geom.coordinates[0].map(p=>p[0])),Math.min(...geom.coordinates[0].map(p=>p[1])))
        } else {
            throw("Invalid geom type")
        }
        if(!tz_name.length) {
            throw("timezone not found")
        }
        var time = date.getTime()
        var zone = moment.tz.zone(tz_name[0]) // 'America/Cuiaba')
        var offset = zone.offsets[0]
        for(var i=0;i<zone.offsets.length;i++) {
            if(zone.untils[i] > time) {
                offset = zone.offsets[i]
                break
            }
        }
        if(format) {
            var tz_date = moment.tz(date,zone.name)
            return tz_date.format('Z')
        }
        return offset
    }

    // getGeomTimeZoneOffset({coordinates:[-60,-35]},new Date("2008-06-01"),true)

    timeseriesToWaterml2(series) {
        let collection = JSON.parse(JSON.stringify(this.templates.collection))
        collection["wml2:Collection"]["@gml:id"] = "col.id.1"
        collection["wml2:Collection"]["wml2:metadata"] = {
            "wml2:DocumentMetadata": {
                "@gml:id": "document_md.id.1",
                "wml2:generationDate": new Date().toISOString(),
                "wml2:generationSystem": "alerta-hims"
            }
        }
        var index = 0
        for(var serie of series) {
            index++
            let observation = JSON.parse(JSON.stringify(this.templates.observation))
            observation["om:OM_Observation"]["@gml:id"] = `obs.id.${index}`
            observation["om:OM_Observation"]["om:metadata"]["gmd:MD_Metadata"]["gmd:identificationInfo"]["@xlink:href"] = `${this.base_url}/obs/${serie.tipo}/series/${serie.id}`
            var timestart_min, timeend_max
            if(serie.observaciones && serie.observaciones.length) {
                timestart_min = serie.observaciones.reduce((a,o)=>(o.timestart < a) ? o.timestart : a,serie.observaciones[0].timestart)
                timeend_max = serie.observaciones.reduce((a,o)=>(o.timeend > a) ? o.timeend : a,serie.observaciones[0].timeend)
                observation["om:OM_Observation"]["om:phenomenonTime"]["gml:TimePeriod"]["@gml:id"] = `time.period.${index}`
                observation["om:OM_Observation"]["om:phenomenonTime"]["gml:TimePeriod"]["gml:beginPosition"] = timestart_min.toISOString()
                observation["om:OM_Observation"]["om:phenomenonTime"]["gml:TimePeriod"]["gml:endPosition"]= timeend_max.toISOString()
                observation["om:OM_Observation"]["om:resultTime"]["gml:TimeInstant"]["@gml:id"] = `time_instant.id.${index}`
                observation["om:OM_Observation"]["om:resultTime"]["gml:TimeInstant"]["gml:timePosition"]= timeend_max.toISOString()
            }
            if(serie.procedimiento && serie.procedimiento.id) {
                observation["om:OM_Observation"]["om:procedure"]["wml2:ObservationProcess"] = {
                    "@gml:id": `proc.id.${index}`,
                    "wml2:processType": {
                        "@xlink:href": `${this.base_url}/obs/procedimientos/${serie.procedimiento.id}`, 
                        "@xlink:title": serie.procedimiento.nombre
                    }
                }
            }
            if(serie.var && serie.var.timeSupport && timeSteps.interval2epochSync(serie.var.timeSupport)) {
                observation["om:OM_Observation"]["om:procedure"]["wml2:ObservationProcess"]["wml2:aggregationDuration"] = timeSteps.interval2iso8601String(series[0].var.timeSupport) 
            }
            if(serie.fuente && serie.fuente.id) {
                observation["om:OM_Observation"]["om:procedure"]["wml2:ObservationProcess"]["wml2:input"] = {
                    "@xlink:href": `${this.base_url}/obs/fuentes/${serie.fuente.id}`,
                    "@xlink:title": serie.fuente.nombre
                }
            }
            if(serie.var && serie.var.id) {
                observation["om:OM_Observation"]["om:observedProperty"] =  {
                    "@xlink:href": `${this.base_url}/obs/variables/${serie.var.id}`,
                    "@xlink:title": serie.var.nombre
                }
            }
            if(serie.estacion && serie.estacion.id) {
                if(serie.estacion.geom.type == "Point") {
                    var shape = {
                        "gml:Point": {
                            "@gml:id":`point.id.${index}`,
                            "gml:pos": {
                                "@srsName":"EPSG:4326",
                                "#text" : `${serie.estacion.geom.coordinates[1]} ${serie.estacion.geom.coordinates[0]}`
                            }
                        }
                    }
                } else if (serie.estacion.geom.type == "Polygon") {
                    var shape = {
                        "gml:Polygon": {
                            "@gml:id":`polygon.id.${index}`,
                            "@srsName":"EPSG:4326",
                            "gml:interior": {
                                "gml:LinearRing": serie.estacion.geom.coordinates.map(ring=>{
                                    return {
                                        "gml:pos": ring.map(point=>{
                                            return {
                                                "#text" : `${point[1]} ${point[0]}`
                                            }    
                                        })
                                    }
                                })
                            }
                        }
                    }
                } else {
                    throw("Invalid geom type")
                }
                var codespace = (serie.tipo == "puntual") ? `${this.base_url}/obs/estaciones` : (serie.tipo == "areal") ? `${this.base_url}/obs/areas` : (serie.tipo == "raster") ? `${this.base_url}/obs/escenas` : `${this.base_url}/obs/estaciones`
                observation["om:OM_Observation"]["om:featureOfInterest"]["wml2:MonitoringPoint"] = {
                    "@gml:id": `monitoring_point.id.${index}`,
                    "gml:identifier": {
                        "@codeSpace": codespace,
                        "#text": serie.estacion.id
                    },
                    "sa:sampledFeature": "", // <- related concept does not exist (most similar: estacion.rio)
                    "sa:parameter": [],
                    "sams:shape": shape,
                    "wml2:monitoringType": {
                        "@xlink:href": "http://codes.wmo.int/wmdr/ApplicationArea/hydrology","@xlink:title": "Hydrology"
                    },
                    "wml2:timeZone": {
                        "wml2:TimeZone": {
                            "wml2:zoneOffset": this.getGeomTimeZoneOffset(serie.estacion.geom,timeend_max,true)
                        }
                    }
                }
            }    
            if(serie.estacion.pais) {
                var pais = this.getCode('paises','nombre',serie.estacion.pais)
                if(pais) {
                    var parameter = {
                        "sa:parameter": {
                            "om:NamedValue": {
                                "om:name": {
                                    "@xlink:href":"http://codes.wmo.int/wmdr/TerritoryName",
                                    "@xlink:title":"TerritoryName"
                                },
                                "om:value": {
                                    "CharacterString": pais.wmdr_notation
                                }
                            }
                        }
                    }
                    observation["om:OM_Observation"]["om:featureOfInterest"]["wml2:MonitoringPoint"]["sa:parameter"].push(parameter["sa:parameter"])
                }
                var region = this.getCode('regiones_omm','notation','southAmerica')
                if(region) {
                    var parameter = {
                        "sa:parameter": {
                            "om:NamedValue": {
                                "om:name": {
                                    "@xlink:href":"http://codes.wmo.int/wmdr/WMORegion",
                                    "@xlink:title":"WMORegion",
                                },
                                "om:value": {
                                    "CharacterString": region.notation
                                }
                            }
                        }
                    }
                    observation["om:OM_Observation"]["om:featureOfInterest"]["wml2:MonitoringPoint"]["sa:parameter"].push(parameter["sa:parameter"])
                }
            }
            if(serie.observaciones && serie.observaciones.length) {
                var uom_code
                if(!serie.unidades || !serie.unidades.abrev) {
                    console.error("Warning: missing uom")
                } else {
                    var validate = ucum_utils.validateUnitString(serie.unidades.abrev.replace('^','','g'))
                    if(validate.status != 'valid') {
                        console.error("Warning: uom code is not valid")
                    } else {
                        uom_code = validate.ucumCode // serie.unidades.abrev.replace('^','','g')
                    }
                }
                observation["om:OM_Observation"]["om:result"] = {
                    "wml2:MeasurementTimeseries": {
                        "@gml:id":`series.id.${index}`,
                        "wml2:metadata": {
                            "wml2:MeasurementTimeseriesMetadata": {
                                "wml2:temporalExtent": {
                                    "gml:TimePeriod": {
                                        "@gml:id": `time_period.id.${index}`,
                                        "gml:beginPosition": timestart_min.toISOString(),
                                        "gml:endPosition": timeend_max.toISOString()
                                    }
                                },
                                "wml2:cumulative": (serie.var && serie.var.datatype && serie.var.datatype.toLowerCase() == 'cumulative') ? true : false
                            }
                        },
                        "wml2:defaultPointMetadata": {
                            "wml2:DefaultTVPMeasurementMetadata": {
                                // "wml2:quality": {
                                //     "@xlink:href": "",
                                //     "@xlink:title": ""
                                // },
                                "wml2:uom": {
                                    "@code": uom_code
                                }
                            }
                        },
                        "wml2:point": []
                    }
                }
                var interpolationType = this.getCode('data_types','term',serie.var.datatype)
                if(interpolationType && interpolationType.waterml2_uri) {
                    observation["om:OM_Observation"]["om:result"]["wml2:MeasurementTimeseries"]["wml2:defaultPointMetadata"]["wml2:DefaultTVPMeasurementMetadata"]["wml2:interpolationType"] = {
                        "@xlink:href": interpolationType.waterml2_uri,
                        "@xlink:title": interpolationType.waterml2_code
                    }
                } else {
                    console.error("Warning: missing interpolation type")
                }
                if(serie.var && serie.var.timeSupport && timeSteps.interval2epochSync(serie.var.timeSupport)) {
                    observation["om:OM_Observation"]["om:result"]["wml2:MeasurementTimeseries"]["wml2:defaultPointMetadata"]["wml2:DefaultTVPMeasurementMetadata"]["wml2:aggregationDuration"] = timeSteps.interval2iso8601String(series[0].var.timeSupport)
                }
                for(var observacion of serie.observaciones) {
                    // var point = {..point_template}
                    var point = {
                        "wml2:point": {
                            "wml2:MeasurementTVP": {
                                "wml2:time": observacion.timestart.toISOString(),
                                "wml2:value": observacion.valor
                            }
                        }
                    }
                    observation["om:OM_Observation"]["om:result"]["wml2:MeasurementTimeseries"]["wml2:point"].push(point["wml2:point"])
                }
            }
            // console.log(index)
            collection["wml2:Collection"]["wml2:observationMember"].push(observation)
        }
        return collection
    }
}

internal.convert = async function (series) {
    const series2waterml2 = new internal.series2waterml2()
    try {
        await series2waterml2.getCodelists()
    } catch(e) {
        throw(e)
    }
    var collection = series2waterml2.timeseriesToWaterml2(series)
    // fs.writeFileSync("tmp/collection.json",JSON.stringify(collection))
    var xml_doc = xmlbuilder.create(collection)
    // var xml_doc = xmlbuilder.create(collection).end({prettyPrint:true})
    // fs.writeFileSync("tmp/collection.xml",xml_doc.end({prettyPrint:true}))
    return xml_doc.end({prettyPrint:true})
}

// templates

var collection_template = {
    "wml2:Collection": {
        "@gml:id": "",
        "@xmlns:gml": "http://www.opengis.net/gml/3.2",
        "@xmlns:wml2": "http://www.opengis.net/waterml/2.0",
        "@xmlns:om": "http://www.opengis.net/om/2.0",
        "@xmlns:xlink": "http://www.w3.org/1999/xlink",
        "@xmlns:sa": "http://www.opengis.net/sampling/2.0",
        "@xmlns:sams": "http://www.opengis.net/samplingSpatial/2.0",
        "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "@xmlns:gco": "http://www.isotc211.org/2005/gco",
        "@xmlns:gmd": "http://www.isotc211.org/2005/gmd",
        "@xsi:schemaLocation": "http://www.opengis.net/waterml/2.0 http://schemas.opengis.net/waterml/2.0/waterml2.xsd",
        "gml:description": "",
        "wml2:metadata": {
    		"wml2:DocumentMetadata": {
                "@gml:id": "",
			    "wml2:generationDate": "",
    			"wml2:generationSystem": ""
            }
        },
    	"wml2:observationMember": []
    }
}

var observation_template = {
    "om:OM_Observation": {
        "@gml:id":"",
        "om:metadata": {
            "gmd:MD_Metadata": {
                "gmd:contact": {
                    "gmd:CI_ResponsibleParty": {
                        "gmd:organisationName": {
                            "gco:CharacterString": api_config.organisationName
                        },
                        "gmd:contactInfo": {
                            "gmd:CI_Contact": {
                                "gmd:phone": {
                                    "gmd:CI_Telephone": {
                                        "gmd:voice": {
                                            "gco:CharacterString": api_config.phone
                                        }
                                    }
                                },
                                "gmd:address": {
                                    "gmd:CI_Address": {
                                        "gmd:deliveryPoint": {
                                            "gco:CharacterString": api_config.address
                                        },
                                        "gmd:city": {
                                            "gco:CharacterString": api_config.city
                                        },
                                        "gmd:administrativeArea": {
                                            "gco:CharacterString": api_config.administrativeArea
                                        },
                                        "gmd:postalCode": {
                                            "gco:CharacterString": api_config.postalCode
                                        },
                                        "gmd:country": {
                                            "gco:CharacterString": api_config.country
                                        },
                                        "gmd:electronicMailAddress": {
                                            "gco:CharacterString": api_config.email
                                        }
                                    }
                                },
                                "gmd:onlineResource": {
                                    "gmd:CI_OnlineResource": {
                                        "gmd:linkage": {
                                            "gmd:URL": api_config.onlineResource
                                        }
                                    }
                                }
                            }
                        },
                        "gmd:role": {
                            "gmd:CI_RoleCode": {
                                "@codeList": "http://standards.iso.org/iso/19115/resources/Codelist/cat/codelists.xml#CI_RoleCode",
                                "@codeListValue": api_config.role,
                                "@codeSpace": "http://standards.iso.org/iso/19115"
                            }
                        }
                    }
                },
                "gmd:dateStamp": {
                    "gco:DateTime": new Date().toISOString()
                },
                "gmd:identificationInfo": {
                    "@xlink:href": ""
                }
            }
        },
        "om:phenomenonTime": {
            "gml:TimePeriod": {
                "@gml:id": "",
                "gml:beginPosition": "",
                "gml:endPosition": ""
            }
        },
        "om:resultTime":{
            "gml:TimeInstant": {
                "@gml:id": "",
                "gml:timePosition": ""
            }
        },
        "om:procedure": {
            "wml2:ObservationProcess": {
                "@gml:id": "",
                "wml2:processType": {
                    "@xlink:href": "",
                    "@xlink:title": ""
                },
                "wml2:aggregationDuration": ""
            }
        },
        "om:observedProperty": {
            "@xlink:href": "",
            "@xlink:title": ""
        },
        "om:featureOfInterest": {
            "wml2:MonitoringPoint": {
                "@gml:id":"",
                "gml:identifier": {
                    "@codeSpace":"",
                    "#text": ""
                },
                "sa:sampledFeature": {
                    "@xlink:href":"",
                    "@xlink:title":""
                },
                "sams:shape":{
                    "gml:Point": {
                        "@gml:id":"",
                        "gml:pos": {
                            "@srsName":"EPSG:4326",
                            "#text" : ""
                        }
                    }
                },
                "wml2:monitoringType": {
                    "@xlink:href":"",
                    "@xlink:title":"",
                },
                "wml2:descriptionReference": {
                    "@xlink:href": "",
                    "@xlink:title": ""
                },
                "wml2:timeZone": {
                    "wml2:TimeZone": {
                        "wml2:zoneOffset": ""
                    }
                },
                "sa:parameter": []
            }
        }, 
        "om:result": {
            "wml2:MeasurementTimeseries": {
                "@gml:id":"",
                "wml2:metadata": {
                    "wml2:MeasurementTimeseriesMetadata": {
                        "wml2:temporalExtent": {
                            "@xlink:href": ""
                        },
                        "wml2:cumulative": ""
                    }
                },
                "wml2:defaultPointMetadata": {
                    "wml2:DefaultTVPMeasurementMetadata": {
                        "wml2:quality": {
                            "@xlink:href": "",
                            "@xlink:title": ""
                        },
                        "wml2:uom": {
                            "@code":""
                        },
                        "wml2:interpolationType": {
                            "@xlink:href": "",
                            "@xlink:title": ""
                        },
                        "wml2:aggregationDuration": "",
                    }
                }
            }
        }
    }
}

var parameter_template = {
    "sa:parameter": {
        "om:NamedValue": {
            "om:name": {
                "@xlink:href":"",
                "@xlink:title":""
            },
            "om:value": {
                "CharacterString": ""
            }
        }
    }
}

var point_template = {
    "wml2:point": {
        "wml2:MeasurementTVP": {
            "wml2:time": "",
            "wml2:value": ""
        }
    }
}

// export module

module.exports = internal

// TODO: test against xsd
// TODO: incorporate as method of CRUD.serie

