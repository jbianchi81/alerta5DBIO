import { exec } from 'child_process';
import { promisify } from 'util';
import * as fs from 'fs';
import * as path from 'path';
import parsePGinterval  from 'postgres-interval'

const execAsync = promisify(exec);

export async function runCommandAndParseJSON(cmd: string): Promise<any> {
  try {
    const { stdout } = await execAsync(cmd);

    const data = JSON.parse(stdout);
    // console.log('Parsed JSON:', data);

    return data;
  } catch (err: any) {
    console.error('Command or parse error:', err.message);
    throw err;
  }
}

export function listFilesSync(dir: string): string[] {
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  return entries
    .filter(entry => entry.isFile())
    .map(entry => path.join(dir, entry.name));
}

interface QueryFilter {
  table?: string
  type?: "string" | "regex_string" | "boolean" | "boolean_only_true" | "boolean_only_false" | "geometry" | "date" | "timestamp" | "timestart" | "timeend" | "greater_or_equal_date" | "smaller_or_equal_date" | "numeric_interval" | "numeric_min" | "numeric_max" | "integer" | "number" | "float" | "interval" | "jsonpath"
  required?: boolean
  column?: string
  case_insensitive?: boolean
  trunc?:	"microseconds" | "milliseconds" | "second" | "minute" |	"hour" | "day" | "week" |	"month" |	"quarter" | "year" | "decade" |	"century" |	"millennium"
  expression?: string
}

export class not_null extends Object {}

export function assertValidDateTruncField(field : string) {
	if ([
		"microseconds",
		"milliseconds",
		"second",
		"minute",
		"hour",
		"day",
		"week",
		"month",
		"quarter",
		"year",
		"decade",
		"century",
		"millennium"
	].indexOf(field) < 0) {
		throw(new Error("Invalid date_trunc field: " + field))
	}
}

export function control_filter2(valid_filters: Record<string, QueryFilter>, filter: any, default_table?: string, crud?: any, throw_on_error: boolean = false) {
  // valid_filters = { column1: { table: "table_name", type: "data_type", required: bool, column: "column_name"}, ... }  
  // filter = { column1: "value1", column2: "value2", ....}
  // default_table = "table"
  var filter_string = " "
  var errors : string[] = []
  Object.keys(valid_filters).forEach(key => {
    var table_prefix = (valid_filters[key].table) ? '"' + valid_filters[key].table + '".' : (default_table) ? '"' + default_table + '".' : ""
    var column_name = (valid_filters[key].column) ? '"' + valid_filters[key].column + '"' : '"' + key + '"'
    var fullkey = table_prefix + column_name
    if (filter[key] instanceof not_null) {
      filter_string += ` AND ` + fullkey + ` IS NOT NULL `
    } else if (typeof filter[key] != "undefined" && filter[key] !== null) {
      if (/[';]/.test(filter[key])) {
        errors.push("Invalid filter value")
        console.error(errors[errors.length - 1])
      }
      if (valid_filters[key].type == "regex_string") {
        var regex = filter[key].replace('\\', '\\\\')
        filter_string += " AND " + fullkey + " ~* '" + filter[key] + "'"
      } else if (valid_filters[key].type == "string") {
        if (Array.isArray(filter[key])) {
          var values = filter[key].filter(v => v != null).map(v => v.toString()).filter(v => v != "")
          if (!values.length) {
            errors.push("Empty or invalid string array")
            console.error(errors[errors.length - 1])
          } else {
            if (valid_filters[key].case_insensitive) {
              filter_string += ` AND lower(${fullkey}) IN ( ${values.map(v => `lower('${v}')`).join(",")})`
            } else {
              filter_string += ` AND ${fullkey} IN ( ${values.map(v => `'${v}'`).join(",")})`
            }
          }
        } else {
          if (valid_filters[key].case_insensitive) {
            filter_string += ` AND lower(${fullkey})=lower('${filter[key]}')`
          } else {
            filter_string += " AND " + fullkey + "='" + filter[key] + "'"
          }
        }
      } else if (valid_filters[key].type == "boolean") {
        var boolean = (/^[yYtTvVsS1]/.test(filter[key])) ? "true" : "false"
        filter_string += " AND " + fullkey + "=" + boolean + ""
      } else if (valid_filters[key].type == "boolean_only_true") {
        if (/^[yYtTvVsS1]/.test(filter[key])) {
          filter_string += " AND " + fullkey + "=true"
        }
      } else if (valid_filters[key].type == "boolean_only_false") {
        if (!/^[yYtTvVsS1]/.test(filter[key])) {
          filter_string += " AND " + fullkey + "=false"
        }
      } else if (valid_filters[key].type == "geometry") {
        if (!("archive" in filter[key] && typeof (filter[key] as any).toSQL === "function")) {
          errors.push("Invalid geometry object")
          console.error(errors[errors.length - 1])
        } else {
          filter_string += "  AND ST_Distance(st_transform(" + fullkey + ",4326),st_transform(" + filter[key].toSQL() + ",4326)) < 0.001"
        }
      } else if (valid_filters[key].type == "date" || valid_filters[key].type == "timestamp") {
        var d : Date
        if (filter[key] instanceof Date) {
          d = filter[key]
        } else {
          d = new Date(filter[key])
        }
        if (valid_filters[key].trunc != undefined) {
          assertValidDateTruncField(valid_filters[key].trunc)
          filter_string += ` AND date_trunc('${valid_filters[key].trunc}',${fullkey}) = date_trunc('${valid_filters[key].trunc}', '${d.toISOString()}'::timestamptz)`
        } else {
          filter_string += " AND " + fullkey + "='" + d.toISOString() + "'::timestamptz"
        }
      } else if (valid_filters[key].type == "timestart") {
        var offset = (new Date().getTimezoneOffset() * 60 * 1000) * -1
        if (filter[key] instanceof Date) {
          var ldate = new Date(filter[key].getTime() + offset).toISOString()
          filter_string += " AND " + fullkey + ">='" + ldate + "'"
        } else {
          var d = new Date(filter[key])
          var ldate = new Date(d.getTime() + offset).toISOString()
          filter_string += " AND " + fullkey + ">='" + ldate + "'"
        }
      } else if (valid_filters[key].type == "timeend") {
        var offset = (new Date().getTimezoneOffset() * 60 * 1000) * -1
        if (filter[key] instanceof Date) {
          var ldate = new Date(filter[key].getTime() + offset).toISOString()
          filter_string += " AND " + fullkey + "<='" + ldate + "'"
        } else {
          var d = new Date(filter[key])
          var ldate = new Date(d.getTime() + offset).toISOString()
          filter_string += " AND " + fullkey + "<='" + ldate + "'"
        }
      } else if (valid_filters[key].type == "greater_or_equal_date") {
        var ldate = new Date(filter[key]).toISOString()
        filter_string += ` AND ${fullkey} >= '${ldate}'::timestamptz`
      } else if (valid_filters[key].type == "smaller_or_equal_date") {
        var ldate = new Date(filter[key]).toISOString()
        filter_string += ` AND ${fullkey} <= '${ldate}'::timestamptz`
      } else if (valid_filters[key].type == "numeric_interval") {
        if (Array.isArray(filter[key])) {
          if (filter[key].length < 2) {
            errors.push("numeric_interval debe ser de al menos 2 valores")
            console.error(errors[errors.length - 1])
          } else {
            filter_string += " AND " + fullkey + ">=" + parseFloat(filter[key][0]) + " AND " + key + "<=" + parseFloat(filter[key][1])
          }
        } else {
          filter_string += " AND " + fullkey + "=" + parseFloat(filter[key])
        }
      } else if (valid_filters[key].type == "numeric_min") {
        filter_string += " AND " + fullkey + ">=" + parseFloat(filter[key])
      } else if (valid_filters[key].type == "numeric_max") {
        filter_string += " AND " + fullkey + "<=" + parseFloat(filter[key])
      } else if (valid_filters[key].type == "integer") {
        if (Array.isArray(filter[key])) {
          var values_int : number[] = filter[key].map(v => parseInt(v)).filter(v => v.toString() != "NaN")
          if (!values_int.length) {
            errors.push(`Invalid integer array : ${filter[key].toString()}`)
            console.error(errors[errors.length - 1])
          } else {
            filter_string += " AND " + fullkey + " IN (" + values_int.join(",") + ")"
          }
        } else {
          var value = parseInt(filter[key])
          if (value.toString() == "NaN") {
            errors.push(`Invalid integer: ${filter[key]}`)
            console.error(errors[errors.length - 1])
          } else {
            filter_string += " AND " + fullkey + "=" + value + ""
          }
        }
      } else if (valid_filters[key].type == "number" || valid_filters[key].type == "float") {
        if (Array.isArray(filter[key])) {
          var values_ = filter[key].map(v => parseFloat(v)).filter(v => v.toString() != "NaN")
          if (!values_.length) {
            errors.push(`Invalid float array: ${filter[key].toString()}`)
            console.error(errors[errors.length - 1])
          } else {
            filter_string += " AND " + fullkey + " IN (" + values_.join(",") + ")"
          }
        } else {
          var value = parseFloat(filter[key])
          if (value.toString() == "NaN") {
            errors.push(`Invalid float: ${filter[key]}`)
            console.error(errors[errors.length - 1])
          } else {
            filter_string += " AND " + fullkey + "=" + value + ""
          }
        }
      } else if (valid_filters[key].type == "interval") {
        var value_interval = createInterval(filter[key])
        if (!value_interval) {
          throw ("invalid interval filter: " + filter[key])
        }
        filter_string += ` AND ${fullkey}='${value_interval.toPostgres()}'::interval`
      } else if (valid_filters[key].type == "jsonpath") {
        if (!valid_filters[key].expression) {
          throw new Error("Missing expression for valid_filter " + key)
        }
        const jsonpath_expression = valid_filters[key].expression.replace("$0", filter[key])
        filter_string += ` AND jsonb_path_exists(${fullkey}, '${jsonpath_expression}')`
      } else {
        if (Array.isArray(filter[key])) {
          filter_string += " AND " + fullkey + " IN (" + filter[key].join(",") + ")"
        } else {
          filter_string += " AND " + fullkey + "=" + filter[key] + ""
        }
      }
    } else if (valid_filters[key].required) {
      errors.push("Falta valor para filtro obligatorio " + key)
      console.error(errors[errors.length - 1])
    }
  })
  if (errors.length > 0) {
    if (throw_on_error) {
      throw ("Invalid filter:\n" + errors.join("\n"))
    } else {
      return null
    }
  } else {
    return filter_string
  }
}


export function createInterval(value : any) {
  if(!value) {
    return //  parsePGinterval()
  }
  if(value.constructor && value.constructor.name == 'PostgresInterval') {
    var interval = parsePGinterval("0 seconds")
    Object.assign(interval,value)
    return interval
  }
  if(value instanceof Object) {
    var interval = parsePGinterval("0 seconds")
    Object.keys(value).map(k=>{
      switch(k) {
        case "milliseconds":
        case "millisecond":
          interval.milliseconds = value[k]
          break
        case "seconds":
        case "second":
          interval.seconds = value[k]
          break
        case "minutes":
        case "minute":
          interval.minutes = value[k]
          break
        case "hours":
        case "hour":
          interval.hours = value[k]
          break
        case "days":
        case "day":
          interval.days = value[k]
          break
        case "months":
        case "month":
        case "mon":
          interval.months = value[k]
          break
        case "years":
        case "year":
          interval.years = value[k]
          break
        default:
          break
      }
    })
    return interval
  }
  if(typeof value == 'string') {
    if(isJson(value)) {
      var interval = parsePGinterval("0 seconds")
      Object.assign(interval,JSON.parse(value))
      return interval
    } else {
      return intervalFromString(value)
      // return parsePGinterval(value)
    }
  } else {
    console.error("timeSteps.createInterval: Invalid value")
    return
  }
}

function isJson(str : string) {
    try {
        JSON.parse(str);
    } catch (e) {
        return false;
    }
    return true;
}

export function intervalFromString(interval_string : string) {
	const kvp = interval_string.split(/\s+/)
	if(kvp.length > 1) {
		var interval = parsePGinterval("0 seconds")
		for(var i=0;i<kvp.length-1;i=i+2) {
			var key = interval_key_map[kvp[i+1].toLowerCase()]
			if(!key) {
				throw("Invalid interval key " + kvp[i+1].toLowerCase())
			}
			interval[key] = parseInt(kvp[i])
		}
	} else {
		var interval = parsePGinterval(interval_string)
	}
	// Object.assign(interval,JSON.parse(value))
	return interval
}

export const interval_key_map = {
	milliseconds: "milliseconds",
	millisecond: "milliseconds",
	seconds: "seconds",
	second: "seconds",
	minutes: "minutes",
	minute: "minutes",
	hours: "hours",
	hour: "hours",
	days: "days",
	day:  "days",
	months: "months",
	month: "months",
	mon: "months",
	years: "years",
	year: "years"
}

export function pasteIntoSQLQuery(query : string, params : any[]) : string {
    for(var i=params.length-1;i>=0;i--) {
      var value
      switch(typeof params[i]) {
        case "string":
          value = escapeLiteral(params[i])
          break;
        case "number":
          value = parseFloat(params[i])
          if(value.toString() == "NaN") {
            throw(new Error("Invalid number"))
          }
          break
        case "object":
          if(params[i] instanceof Date) {
            value = "'" + params[i].toISOString() + "'::timestamptz::timestamp"
          } else if(params[i] instanceof Array) {
            // if(/';/.test(params[i].join(","))) {
            // 	throw("Invalid value: contains invalid characters")
            // }
            value = escapeLiteral(`{${params[i].join(",")}}`) // .map(v=> (typeof v == "number") ? v : "'" + v.toString() + "'")
          } else if(params[i] === null) {
            value = "NULL"
          } else if (params[i].constructor && params[i].constructor.name == 'PostgresInterval') {
              value = `${escapeLiteral(params[i].toPostgres())}::interval`
          } else {
            value = escapeLiteral(params[i].toString())
          }
          break;
        case "undefined": 
          value = "NULL"
          break;
        default:
          value = escapeLiteral(params[i].toString())
      }
      var I = parseInt(i)+1
      var placeholder = "\\$" + I.toString()
      // console.log({placeholder:placeholder,value:value})
      query = query.replace(new RegExp(placeholder,"g"), value)
    }
    return query
  }