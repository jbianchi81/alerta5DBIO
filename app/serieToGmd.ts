import { create } from 'xmlbuilder2';
import { Serie } from './a5_types'
import { advanceTimeStep }from './timeSteps'

function getRestUrl() {
    if(!global.config || !global.config.rest) {
        console.error("Missing rest endpoint configuration")
        return "protocol://host:port/path"
    }
    if(global.config.rest.public_endpoint) {
        return global.config.rest.public_endpoint
    } else {
        return `${global.config.rest.protocol}://${global.config.rest.host}:${global.config.rest.port}/${global.config.rest.path}`
    }
}

// CRUD.serie.read({"tipo":"raster"},{include_geom:true}).then(r=>series=r)

function serieToGmd(serie : Serie) : string {    
    const file_identifier = serie.tipo + ":" + serie.id.toString()
    const topic_category = (serie.var.GeneralCategory in topicCategoryMapping) ? topicCategoryMapping[serie.var.GeneralCategory] : null
    //  [minX, minY, maxX, maxY]
    var bbox = serie.estacion.geom.bbox()
    
    const bbox_obj = {
        'gmd:westBoundLongitude': {
            'gco:Decimal': bbox[0]
        },
        'gmd:eastBoundLongitude': {
            'gco:Decimal': bbox[2]
        },
        'gmd:southBoundLatitude': {
            'gco:Decimal': bbox[1]
        },
        'gmd:northBoundLatitude': {
            'gco:Decimal': bbox[3]
        }
    }

    const timestart = (serie.date_range.timestart) ? new Date(serie.date_range.timestart) : undefined
    const timeend = (serie.date_range.timeend) ? (serie.var.timeSupport) ? advanceTimeStep(new Date(serie.date_range.timeend), serie.var.timeSupport) : new Date(serie.date_range.timeend) : undefined
    
    var doc_obj = {
        'gmi:MI_Metadata': { 
            "@xmlns:gmi": "http://www.isotc211.org/2005/gmi", 
            "@xmlns": "http://www.isotc211.org/2005/gmi",
            "@xmlns:gco": "http://www.isotc211.org/2005/gco", 
            "@xmlns:gmd": "http://www.isotc211.org/2005/gmd",
            "@xmlns:gml": "http://www.opengis.net/gml/3.2", 
            "@xmlns:gmx": "http://www.isotc211.org/2005/gmx",
            "@xmlns:gsr": "http://www.isotc211.org/2005/gsr", 
            "@xmlns:gss": "http://www.isotc211.org/2005/gss",
            "@xmlns:gts": "http://www.isotc211.org/2005/gts", 
            "@xmlns:xlink": "http://www.w3.org/1999/xlink",
            "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance", 
            "@xsi:schemaLocation": "http://www.isotc211.org/2005/gmi http://www.ngdc.noaa.gov/metadata/published/xsd/schema.xsd",   
            'gmd:fileIdentifier': {
                'gco:CharacterString': file_identifier
            },
            'gmd:language': {
                'gco:CharacterString': 'es'
            },
            'gmd:characterSet': {
                'gmd:MD_CharacterSetCode': { 
                    '@codeListValue': 'utf8', 
                    '@codeList': 'http://www.ngdc.noaa.gov/metadata/published/xsd/schema/resources/Codelist/gmxCodelists.xml#MD_CharacterSetCode',
                    '#': 'utf8'
                }
            },
            'gmd:hierarchyLevel': {
                'gmd:MD_ScopeCode': {
                    '@codeListValue': 'series',
                    '@codeList': 'http://www.ngdc.noaa.gov/metadata/published/xsd/schema/resources/Codelist/gmxCodelists.xml#MX_ScopeCode',
                    '#': 'series'
                }
            },
            'gmd:contact': {
                'gmd:CI_ResponsibleParty': {
                    'gmd:organisationName': {
                        'gco:CharacterString': 'Instituto Nacional del Agua'
                    },
                    'gmd:role': {
                        'gmd:CI_RoleCode': {
                            '@codeListValue': 'author', 
                            '@codeList': 'http://www.ngdc.noaa.gov/metadata/published/xsd/schema/resources/Codelist/gmxCodelists.xml#CI_RoleCode',
                            '#': 'author'
                        }
                    }
                }
            },
            'gmd:dateStamp': {
                'gco:DateTime': new Date().toISOString()
            },
            'gmd:identificationInfo': {
                'gmd:MD_DataIdentification': {
                    'gmd:citation': {
                        'gmd:CI_Citation': {
                            'gmd:title': {
                                'gco:CharacterString': serie.fuente.source || serie.fuente.nombre || ((serie.estacion.red) ? serie.estacion.red.nombre : serie.estacion.tabla) || 'Unknown'
                            },
                            'gmd:date': {
                                'gmd:CI_Date': {
                                    'gmd:date': {
                                        '@gco:nilReason': 'unknown'
                                    },
                                    'gmd:dateType': ""
                                }
                            },
                            'gmd:citedResponsibleParty': {
                                'gmd:CI_ResponsibleParty': {
                                    'gmd:organisationName': {
                                        'gco:CharacterString': serie.fuente.source || serie.fuente.nombre || ((serie.estacion.red) ? serie.estacion.red.nombre : serie.estacion.tabla) || 'Unknown'
                                    },
                                    'gmd:role': {
                                        'gmd:CI_RoleCode': {
                                            '@codeListValue': 'originator', 
                                            '@codeList': 'http://www.ngdc.noaa.gov/metadata/published/xsd/schema/resources/Codelist/gmxCodelists.xml#CI_RoleCode',
                                            '#': 'originator'
                                        }
                                    }
                                }
                            }
                        }
                    },
                    'gmd:abstract': {
                        'gco:CharacterString': serie.fuente.abstract || 'N/A'
                    },
                    'gmd:descriptiveKeywords': {
                        'gmd:MD_Keywords': {
                            'gmd:keyword': [
                                {
                                    'gco:CharacterString': serie.var.VariableName || 'N/A'
                                },
                                {
                                    'gco:CharacterString': serie.estacion.nombre || 'N/A'
                                },
                                {
                                    'gco:CharacterString': serie.procedimiento.nombre || 'N/A'
                                },
                                {
                                    'gco:CharacterString': serie.unidades.abrev || 'N/A'
                                }
                            ]
                        }
                    },
                    'gmd:language': {
                        'gco:CharacterString': 'es'
                    },
                    'gmd:topicCategory': {
                        'gmd:MD_TopicCategoryCode': topic_category
                    },
                    'gmd:extent': {
                        'gmd:EX_Extent': {
                            'gmd:geographicElement': {
                                'gmd:EX_GeographicBoundingBox': bbox_obj
                            },
                            'gmd:temporalElement': {
                                'gmd:EX_TemporalExtent': {
                                    'gmd:extent': {
                                        'gml:TimePeriod': {
                                            '@gml:id': 'boundingTimePeriod',
                                            'gml:beginPosition': (timestart) ? timestart.toISOString() : 'N/A',
                                            'gml:endPosition': (timeend) ? timeend.toISOString() : 'N/A'
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    if(serie.tipo == "raster") {
        doc_obj['gmi:MI_Metadata']['gmd:spatialRepresentationInfo'] = {
            'gmd:MD_GridSpatialRepresentation': {
                'gmd:axisDimensionProperties' : {
                    'gmd:MD_Dimension': {
                        'gmd:resolution': {
                            'gco:Measure': {
                                '@uom': "kilometer",
                                '#': `${serie.fuente.def_pixel_width}`
                            }
                        }
                    }
                }
            }
        }
    }
    doc_obj['gmi:MI_Metadata']['gmd:distributionInfo'] = {
        'gmd:MD_Distribution': {
            'gmd:transferOptions': [
                {
                    'gmd:MD_DigitalTransferOptions': {
                        'gmd:onLine': {
                            'gmd:CI_OnlineResource': {
                                'gmd:linkage': {
                                    'gmd:URL': `https://alerta.ina.gob.ar/a5/obs/${serie.tipo}/series/${serie.id}?include_geom=true&format=geojson`
                                }
                            }
                        }
                    }
                }
            ]
        }
    }

    if(!topic_category) {
        delete doc_obj['gmi:MI_Metadata']['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:topicCategory']
    }

    if(timestart) {
        doc_obj["gmi:MI_Metadata"]["gmd:distributionInfo"]["gmd:MD_Distribution"]["gmd:transferOptions"].push(
            {
                'gmd:MD_DigitalTransferOptions': {
                    'gmd:onLine': {
                        'gmd:CI_OnlineResource': {
                            'gmd:linkage': {
                                'gmd:URL': `${getRestUrl()}obs/${serie.tipo}/series/${serie.id}/observaciones?timestart=${timestart.toISOString()}&timeend=${timeend.toISOString()}&pagination=true`
                            }
                        }
                    }
                }
            }
        )
    }
    var xml = create(doc_obj)
    // xml = append_MD_Element(xml, serie)
    return xml.end({ prettyPrint: true });
}

const topicCategoryMapping = {
    "Climate": "climatologyMeteorologyAtmosphere",
    "Hydrology": "inlandWaters",
    "Water Quality": "inlandWaters",
    "Meteorology": "climatologyMeteorologyAtmosphere"
}


module.exports = {
    serieToGmd: serieToGmd
}
