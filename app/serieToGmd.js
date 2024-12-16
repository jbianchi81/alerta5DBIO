"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const xmlbuilder2_1 = require("xmlbuilder2");
const timeSteps_1 = require("./timeSteps");
var sprintf = require('sprintf-js').sprintf;
function getRestUrl() {
    if (!global.config || !global.config.rest) {
        console.error("Missing rest endpoint configuration");
        return "protocol://host:port/path";
    }
    if (global.config.rest.public_endpoint) {
        return global.config.rest.public_endpoint;
    }
    else {
        return `${global.config.rest.protocol}://${global.config.rest.host}:${global.config.rest.port}/${global.config.rest.path}`;
    }
}
// CRUD.serie.read({"tipo":"raster"},{include_geom:true}).then(r=>series=r)
function serieToGmd(serie) {
    const file_identifier = serie.tipo + ":" + serie.id.toString();
    const topic_category = (serie.var.GeneralCategory in topicCategoryMapping) ? topicCategoryMapping[serie.var.GeneralCategory] : null;
    //  [minX, minY, maxX, maxY]
    var bbox = serie.estacion.geom.bbox();
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
    };
    const timestart = (serie.date_range.timestart) ? new Date(serie.date_range.timestart) : undefined;
    const original_timeend = (serie.date_range.timeend) ? serie.date_range.timeend : undefined;
    const timeend = (serie.date_range.timeend) ? (serie.var.timeSupport) ? (0, timeSteps_1.advanceTimeStep)(new Date(serie.date_range.timeend), serie.var.timeSupport) : new Date(serie.date_range.timeend) : undefined;
    const timeSupport_epoch = (serie.var.timeSupport) ? (0, timeSteps_1.interval2epochSync)(serie.var.timeSupport) : 0;
    const frequency_code = (timeSupport_epoch == 0) ? "unknown" : (timeSupport_epoch < 24 * 3600) ? "continual" : (timeSupport_epoch == 24 * 3600) ? "daily" : (timeSupport_epoch <= 7 * 24 * 3600) ? "weekly" : (timeSupport_epoch <= 31 * 24 * 3600) ? "monthly" : (timeSupport_epoch <= 366 * 24 * 3600) ? "annually" : "unknown";
    const spatial_repr = (serie.tipo.substring(0, 4) == "rast") ? "grid" : "vector";
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
            }
        }
    };
    if (serie.tipo.substring(0, 4) == "rast") {
        doc_obj['gmi:MI_Metadata']['gmd:spatialRepresentationInfo'] = {
            'gmd:MD_GridSpatialRepresentation': {
                'gmd:numberOfDimensions': {
                    'gco:Integer': 2
                },
                'gmd:axisDimensionProperties': {
                    'gmd:MD_Dimension': {
                        'gmd:dimensionName': {
                            'gmd:MD_DimensionNameTypeCode': {
                                '@codeList': "https://data.noaa.gov/resources/iso19139/schema/resources/Codelist/gmxCodelists.xml#gmd:MD_DimensionNameTypeCode",
                                '@codeListValue': "column",
                                '#': "column"
                            }
                        },
                        'gmd:dimensionSize': {
                            '@gco:nilReason': "unknown"
                        },
                        'gmd:resolution': {
                            'gco:Measure': {
                                '@uom': "degree",
                                '#': `${serie.fuente.def_pixel_width}`
                            }
                        }
                    }
                },
                'gmd:cellGeometry': {
                    'gmd:MD_CellGeometryCode': {
                        '@codeList': "",
                        '@codeListValue': "",
                        '@codeSpace': ""
                    }
                },
                'gmd:transformationParameterAvailability': {
                    '@gco:nilReason': "unknown"
                }
            }
        };
    }
    doc_obj['gmi:MI_Metadata']['gmd:identificationInfo'] = {
        'gmd:MD_DataIdentification': {
            'gmd:citation': {
                'gmd:CI_Citation': {
                    'gmd:title': {
                        'gco:CharacterString': serie.fuente.source || serie.fuente.nombre || ((serie.estacion.red) ? serie.estacion.red.nombre : serie.estacion.tabla) || 'Unknown'
                    },
                    'gmd:date': {
                        'gmd:CI_Date': {
                            'gmd:date': (timeend) ? {
                                'gco:Date': timeend.toISOString()
                            } : {},
                            'gmd:dateType': {
                                'gmd:CI_DateTypeCode': {
                                    '@codeList': "http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#CI_DateTypeCode",
                                    '@codeListValue': "creation",
                                    '#': "creation"
                                }
                            }
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
            'gmd:pointOfContact': {
                'gmd:CI_ResponsibleParty': {
                    'gmd:organisationName': {
                        'gco:CharacterString': "Instituto Nacional del Agua"
                    },
                    'gmd:contactInfo': {
                        'gmd:CI_Contact': {
                            'gmd:phone': {
                                'gmd:CI_Telephone': {
                                    'gmd:voice': {
                                        'gco:CharacterString': "(54 11) 4480-4500"
                                    }
                                }
                            },
                            'gmd:address': {
                                'gmd:CI_Address': {
                                    'gmd:deliveryPoint': {
                                        'gco:CharacterString': "Au. Ezeiza - Cañuelas, tramo Jorge Newbery Km 1,620"
                                    },
                                    'gmd:city': {
                                        'gco:CharacterString': "Ezeiza"
                                    },
                                    'gmd:administrativeArea': {
                                        'gco:CharacterString': "Buenos Aires",
                                    },
                                    "gmd:postalCode": {
                                        'gco:CharacterString': "1804"
                                    },
                                    "gmd:country": {
                                        'gco:CharacterString': 'Argentina'
                                    },
                                    'gmd:electronicMailAddress': {
                                        'gco:CharacterString': "ina@ina.gob.ar"
                                    }
                                }
                            }
                        }
                    },
                    'gmd:role': {
                        'gmd:CI_RoleCode': {
                            '@codeListValue': "author",
                            '@codeList': "http://www.ngdc.noaa.gov/metadata/published/xsd/schema/resources/Codelist/gmxCodelists.xml#CI_RoleCode",
                            '#': "author"
                        }
                    }
                }
            },
            'gmd:resourceMaintenance': {
                'gmd:MD_MaintenanceInformation': {
                    'gmd:maintenanceAndUpdateFrequency': {
                        'gmd:MD_MaintenanceFrequencyCode': {
                            '@codeList': "http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#MD_MaintenanceFrequencyCode",
                            '@codeListValue': frequency_code,
                            '#': frequency_code
                        }
                    }
                }
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
            'gmd:spatialRepresentationType': {
                'gmd:MD_SpatialRepresentationTypeCode': {
                    '@codeList': "http://www.isotc211.org/2005/resources/Codelist/gmxCodelists.xml#MD_SpatialRepresentationTypeCode",
                    '@codeListValue': spatial_repr,
                    '#': spatial_repr
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
    };
    doc_obj['gmi:MI_Metadata']['gmd:contentInfo'] = {
        'gmd:MD_CoverageDescription': {
            'gmd:attributeDescription': {
                'gco:RecordType': "number"
            },
            'gmd:contentType': {
                'gmd:MD_CoverageContentTypeCode': {
                    '@codeList': "http://www.isotc211.org/2005/resources/codeList.xml#MD_CoverageContentTypeCode",
                    '@codeListValue': "physicalMeasurement",
                    '#': "physicalMeasurement"
                }
            },
            'gmd:dimension': {
                'gmd:MD_RangeDimension': {
                    'gmd:descriptor': {
                        'gco:CharacterString': `${serie.var.VariableName} [${serie.unidades.abrev}]`
                    }
                }
            }
        }
    };
    doc_obj['gmi:MI_Metadata']['gmd:distributionInfo'] = {
        'gmd:MD_Distribution': {
            'gmd:distributionFormat': [
                {
                    'gmd:MD_Format': {
                        'gmd:name': {
                            'gco:CharacterString': 'geoJSON'
                        },
                        'gmd:version': {
                            'gco:CharacterString': 'RFC 7946'
                        }
                    }
                }
            ],
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
    };
    doc_obj['gmi:MI_Metadata']['gmd:dataQualityInfo'] = {
        'gmd:DQ_DataQuality': {
            'gmd:scope': {
                'gmd:DQ_Scope': {
                    'gmd:level': {
                        'gmd:MD_ScopeCode': {
                            '@codeList': "http://www.isotc211.org/2005/resources/codeList.xml#MD_ScopeCode",
                            '@codeListValue': "dataset",
                            '#': "dataset"
                        }
                    }
                }
            },
            'gmd:lineage': {
                'gmd:LI_Lineage': {
                    'gmd:processStep': {
                        'gmd:LI_ProcessStep': {
                            'gmd:description': {
                                'gco:CharacterString': serie.procedimiento.descripcion
                            }
                        }
                    }
                }
            }
        }
    };
    if (!topic_category) {
        delete doc_obj['gmi:MI_Metadata']['gmd:identificationInfo']['gmd:MD_DataIdentification']['gmd:topicCategory'];
    }
    if (original_timeend) {
        var dia = sprintf("%04d-%02d-%02d", original_timeend.getFullYear(), original_timeend.getMonth() + 1, original_timeend.getDate());
        var distr_link = `${getRestUrl()}obs/${serie.tipo}/series/${serie.id}/dia/${dia}`;
        if (serie.tipo.substring(0, 4) == "rast") {
            distr_link = `${distr_link}?format=tif`;
            doc_obj["gmi:MI_Metadata"]["gmd:distributionInfo"]["gmd:MD_Distribution"]["gmd:distributionFormat"].push({
                'gmd:MD_Format': {
                    'gmd:name': {
                        'gco:CharacterString': 'GeoTIFF'
                    },
                    'gmd:version': {
                        'gco:CharacterString': '1.1'
                    }
                }
            });
        }
        doc_obj["gmi:MI_Metadata"]["gmd:distributionInfo"]["gmd:MD_Distribution"]["gmd:transferOptions"].push({
            'gmd:MD_DigitalTransferOptions': {
                'gmd:onLine': {
                    'gmd:CI_OnlineResource': {
                        'gmd:linkage': {
                            'gmd:URL': distr_link
                        }
                    }
                }
            }
        });
    }
    var xml = (0, xmlbuilder2_1.create)(doc_obj);
    // xml = append_MD_Element(xml, serie)
    return xml.end({ prettyPrint: true });
}
const topicCategoryMapping = {
    "Climate": "climatologyMeteorologyAtmosphere",
    "Hydrology": "inlandWaters",
    "Water Quality": "inlandWaters",
    "Meteorology": "climatologyMeteorologyAtmosphere"
};
module.exports = {
    serieToGmd: serieToGmd
};
