import { SeriePuntual, Observacion } from './a5_types'
// import { xmlbuilder } from 'xmlbuilder2'

interface TimeSeriesResponse {
    $: {
        "xmlns": "http://www.cuahsi.org/waterML/1.1/",
        "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "xsi:schemaLocation": "http://www.cuahsi.org/waterML/1.1/ https://his.cuahsi.org/documents/cuahsiTimeSeries_v1_1.xsd"
    }
    queryInfo: QueryInfo;
    timeSeries: TimeSeries;
}
  
interface QueryInfo {
    creationTime: string;
    queryURL: string;
    criteria: Criteria;
}
  
interface Criteria {
    $: { MethodCalled?: string };
    locationParam: string;
    variableParam: string;
    timeParam: TimeParam;
}
  
interface TimeParam {
    beginDateTime: string;
    endDateTime: string;
}
  
interface TimeSeries {
    sourceInfo?: SourceInfo;
    variable: Variable;
    values: Values;
}
  
interface SourceInfo {
    // Define properties based on the schema
}
  
interface Variable {
    variableCode: VariableCode;
    variableName: string;
    variableDescription: string;
    valueType: string;
    dataType: string;
    generalCategory: string;
    sampleMedium: string;
    unit: Unit;
    noDataValue: number;
    timeScale: TimeScale;
}
  
interface VariableCode {
    $: {
        default: boolean;
        variableID: string;
        vocabulary: string;
    }
    _: string
}
  
interface Unit {
    $: {
        unitID: string;
    };
    unitName: string;
    unitType?: string;
    unitAbbreviation: string;
    unitCode: string;
}
  
interface TimeScale {
    $: {
        isRegular: boolean;
    };
    unit: Unit;
    timeSpacing: number;
    timeSupport: number;
}
  
interface Values {
    value: Value[];
    units: Unit;
}
  
interface Value {
    $: {
        dateTime: string;
        dateTimeUTC: string;
        methodID: string;
        sourceID: string;
        sourceCode: string;
        sampleID: string;
        timeOffset: string;
        qualifiers?: string;
    };
    _: number;
}

const time_units_map = {
    days: {
        UnitsID: "104",
        unitName:"day",
        unitsAbreviation:"d"
    },
    minutes: {
        UnitsID: "102",
        unitName:"minute",
        unitsAbreviation:"min"
    },
    hours: {
        UnitsID: "103",
        unitName:"hour",
        unitsAbreviation:"h"
    },
    months: {
        UnitsID: "106",
        unitName:"month",
        unitsAbreviation:"mon"
    }
}

export function serieToWaterML1TimeSeriesResponseObject (serie : SeriePuntual, query_url : string) : {timeSeriesResponse: TimeSeriesResponse} {

    var begin_date_time : Date, end_date_time : Date
    if(serie.observaciones && serie.observaciones.length) {
        begin_date_time = serie.observaciones.reduce((a : Date,o : Observacion)=>(o.timestart < a) ? o.timestart : a,serie.observaciones[0].timestart)
        end_date_time = serie.observaciones.reduce((a : Date, o : Observacion)=>(o.timeend > a) ? o.timeend : a,serie.observaciones[0].timeend)
    }

    const is_regular = (serie.var.timeSupport) ? true : false
    const time_support_units_key = (serie.var.timeSupport) ? serie.var.timeSupport.getKey() : "seconds"
    const time_support_units = time_units_map[time_support_units_key]

    return {
        timeSeriesResponse: {
            $: {
                xmlns: "http://www.cuahsi.org/waterML/1.1/",
                "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
                "xsi:schemaLocation": "http://www.cuahsi.org/waterML/1.1/ https://his.cuahsi.org/documents/cuahsiTimeSeries_v1_1.xsd"
            },
            queryInfo: {
                creationTime: new Date().toISOString(),
                queryURL: query_url,
                criteria: {
                    $: { MethodCalled: "getValues" },
                    locationParam: serie.estacion.id.toString(),
                    variableParam: serie.var.id.toString(),
                    timeParam: {
                        beginDateTime: begin_date_time.toISOString(),
                        endDateTime: end_date_time.toISOString()
                    }
                }
            },
            timeSeries: {
                variable: {
                    variableCode: {
                        $: { 
                            default: true, 
                            variableID: serie.var.id.toString(), 
                            vocabulary: "INA"
                        },
                        _: serie.var.id.toString()
                    },
                    variableName: serie.var.VariableName,
                    variableDescription: serie.var.nombre,
                    valueType: serie.var.valuetype,
                    dataType: serie.var.datatype,
                    generalCategory: serie.var.GeneralCategory,
                    sampleMedium: serie.var.SampleMedium,
                    unit: {
                        $: { 
                            unitID: serie.unidades.id.toString()
                        },
                        unitName: serie.unidades.nombre,
                        unitType: serie.unidades.UnitsType,
                        unitAbbreviation: serie.unidades.abrev,
                        unitCode: serie.unidades.UnitsID.toString()
                    },
                    noDataValue: -9999,
                    timeScale: {
                        $: { 
                            isRegular: is_regular 
                        },
                        unit: {
                            $: {
                                unitID: time_support_units.unitsID
                            },
                            unitName: time_support_units.unitName,
                            unitAbbreviation: time_support_units.unitAbbreviation,
                            unitCode: time_support_units.unitsID
                        },
                        timeSpacing: (is_regular) ? serie.var.timeSupport.getValue() : 1,
                        timeSupport: (is_regular) ? serie.var.timeSupport.getValue() : 1
                    }
                },
                values: {
                    value: serie.observaciones.map((observacion: Observacion) => {
                        return {
                            $: {
                                dateTime: observacion.timestart.toLocaleString("sv-SE").replace(" ", "T"), 
                                dateTimeUTC: observacion.timestart.toISOString().substring(0,19), 
                                methodID: serie.procedimiento.id.toString(), 
                                sourceID: serie.estacion.red.id.toString(), 
                                sourceCode: serie.estacion.red.tabla_id, 
                                sampleID: observacion.id.toString(), 
                                timeOffset: getTimeOffset(observacion.timestart)
                            }, 
                            _: observacion.valor 
                        }
                    }),
                    units: {
                    $: { unitID: "10" },
                    unitName: "metros cúbicos por segundo",
                    unitAbbreviation: "m^3/s",
                    unitCode: "10"
                    }
                }
            }
        }
    }
};

function getTimeOffset(date : Date) : string {
    const offsetMinutes = date.getTimezoneOffset(); // Offset in minutes
    const sign = offsetMinutes > 0 ? "-" : "+";
    const hours = Math.abs(Math.floor(offsetMinutes / 60)).toString().padStart(2, "0");
    const minutes = Math.abs(offsetMinutes % 60).toString().padStart(2, "0");
  
    return `${sign}${hours}:${minutes}`;
};

