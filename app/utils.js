'use strict'

const internal = {}
const timeSteps = require('./timeSteps')
var Validator = require('jsonschema').Validator;
const fs = require('fs')
const path = require('path')
var apidoc = JSON.parse(fs.readFileSync(path.resolve(__dirname,'../public/json/apidocs.json'),'utf-8'))
var schemas = apidoc.components.schemas
traverse(schemas,changeRef)
var g = new Validator();
for(var key in schemas) {
    g.addSchema(schemas[key],"/" + key)
}
const CSV = require('csv-string');
const PostgresInterval = require('postgres-interval');
const {Geometry: a5_geometry} = require('./geometry')

// var geom =  new Geometry("POINT(-53 -32)")
// console.log("util: geom instanceof Geometry: " + geom instanceof Geometry)

internal.validate_with_model = function (instance,model) {
	if(!schemas.hasOwnProperty(model)) {
		throw("model " + model + " no encontrado en schema")
	}
	var result = g.validate(instance,schemas[model])
	if(result.errors.length) {
		console.error(result.toString())
		return { "valid": false, "reason": result.toString() } 
	}
	return { "valid": true}
}

internal.control_filter = function (valid_filters, filter, tablename, crud) {
	var filter_string = " "
	var control_flag = 0
	Object.keys(valid_filters).forEach(key=>{
		var fullkey = (tablename) ? "\"" + tablename + "\".\"" + key + "\"" : "\"" + key + "\""
		if(filter[key]) {
			if(/[';]/.test(filter[key])) {
				console.error("Invalid filter value")
				control_flag++
			}
			if(valid_filters[key] == "regex_string") {
				var regex = filter[key].replace('\\','\\\\')
				filter_string += " AND " + fullkey  + " ~* '" + filter[key] + "'"
			} else if(valid_filters[key] == "string") {
				filter_string += " AND "+ fullkey + "='" + filter[key] + "'"
			} else if (valid_filters[key] == "boolean") {
				var boolean = (/^[yYtTvVsS1]/.test(filter[key])) ? "true" : "false"
				filter_string += " AND "+ fullkey + "=" + boolean + ""
			} else if (valid_filters[key] == "boolean_only_true") {
				if (/^[yYtTvVsS1]/.test(filter[key])) {
					filter_string += " AND "+ fullkey + "=true"
				} 
			} else if (valid_filters[key] == "boolean_only_false") {
				if (!/^[yYtTvVsS1]/.test(filter[key])) {
					filter_string += " AND "+ fullkey + "=false"
				} 
			} else if (valid_filters[key] == "geometry") {
				if(! filter[key] instanceof a5_geometry) {
					console.error("Invalid geometry object")
					control_flag++
				} else {
					filter_string += "  AND ST_Distance(st_transform(" + fullkey + ",4326),st_transform(" + filter[key].toSQL() + ",4326)) < 0.001" 
				}
			} else if (valid_filters[key] == "timestart") {
				var offset = (new Date().getTimezoneOffset() * 60 * 1000) * -1
				if(filter[key] instanceof Date) {
					var ldate = new Date(filter[key].getTime()  + offset).toISOString()
					filter_string += " AND " + fullkey + ">='" + ldate + "'"
				} else {
					var d = new Date(filter[key])
					var ldate = new Date(d.getTime()  + offset).toISOString()
					filter_string += " AND " + fullkey + ">='" + ldate + "'"
				}
			} else if (valid_filters[key] == "timeend") {
				var offset = (new Date().getTimezoneOffset() * 60 * 1000) * -1
				if(filter[key] instanceof Date) {
					var ldate = new Date(filter[key].getTime()  + offset).toISOString()
					filter_string += " AND " + fullkey + "<='" + ldate + "'"
				} else {
					var d = new Date(filter[key])
					var ldate = new Date(d.getTime()  + offset).toISOString()
					filter_string += " AND " + fullkey + "<='" + ldate + "'"
				}
			} else if (valid_filters[key] == "numeric_interval") {
				if(Array.isArray(filter[key])) {
					if(filter[key].length < 2) {
						console.error("numeric_interval debe ser de al menos 2 valores")
						control_flag++
					} else {
						filter_string += " AND " + fullkey + ">=" + parseFloat(filter[key][0]) + " AND " + key + "<=" + parseFloat(filter[key][1])
					}
				} else {
					filter_string += " AND " + fullkey + "=" + parseFloat(filter[key])
				}
			} else if (valid_filters[key].type == "numeric" || valid_filters[key].type == "number" || valid_filters[key].type == "float") {
				if(Array.isArray(filter[key])) {
					var values = filter[key].map(v=>parseFloat(v)).filter(v=>v.toString()!="NaN")
					if(!values.length) {
						console.error("Invalid float")
						control_flag++
					} else {
						filter_string += " AND "+ fullkey + " IN (" + values.join(",") + ")"
					}
				} else {
					var value = parseFloat(filter[key])
					if(value.toString() == "NaN") {
						console.error("Invalid number")
						control_flag++
					} else {
						filter_string += " AND "+ fullkey + "=" + value + ""
					}
				}
			} else {
				if(Array.isArray(filter[key])) {
					filter_string += " AND "+ fullkey + " IN (" + filter[key].join(",") + ")"
				} else {
					filter_string += " AND "+ fullkey + "=" + filter[key] + ""
				}
			}
		}
	})
	if(control_flag > 0) {
		return null
	} else {
		return filter_string
	}
}

/**
 * @typedef {Object} updateClauseResult
 * @property {string} string - update clause string 
 * @property {any[]} params - query parameters 
 * @returns 
 */

/**
 * 
 * @param {*} valid_fields 
 * @param {*} fields 
 * @returns {updateClauseResult} object containing string and params array
 */
internal.update_clause = function (valid_fields, fields) {
	var update_clause = []
	var params = []
	for(var key of Object.keys(valid_fields)) {
		if(!fields.hasOwnProperty(key) || typeof fields[key] == 'undefined') {
			// if value is null it will update to NULL, if undefined it will not update
			continue
		} else if (fields[key] == null) {
			params.push(fields[key])
			update_clause.push(`"${key}"=$${params.length}`)
		} else if(valid_fields[key].type.toLowerCase() == "string") {
			params.push(fields[key].toString())
			update_clause.push(`"${key}"=$${params.length}`) 
		} else if(valid_fields[key].type.toLowerCase() == "geometry") {
			params.push(JSON.stringify(fields[key]))
			update_clause.push(`"${key}"=ST_GeomFromGeoJSON($${params.length})`)
		} else if(["timestamp","timestamptz","date"].indexOf(valid_fields[key].type.toLowerCase()) >= 0) {
			params.push(fields[key].toISOString())
			update_clause.push(`"${key}"=$${params.length}::timestamptz`)
		} else if(["json","jsonb"].indexOf(valid_fields[key].type.toLowerCase()) >= 0) {
			params.push(JSON.stringify(fields[key]))
			update_clause.push(`"${key}"=$${params.length}`)
		} else {
			params.push(fields[key])
			update_clause.push(`"${key}"=$${params.length}`)
		}
	}
	// console.log({
	// 	string: update_clause.join(", "),
	// 	params: params
	// })
	return {
		string: update_clause.join(", "),
		params: params
	}
}

internal.isIterable = function(obj) {
	// checks for null and undefined
	if (obj == null) {
	  return false;
	}
	return typeof obj[Symbol.iterator] === 'function';
}

internal.not_null = class extends Object {
}

internal.control_filter2 = function (valid_filters, filter, default_table, crud, throw_on_error=false) {
	// valid_filters = { column1: { table: "table_name", type: "data_type", required: bool, column: "column_name"}, ... }  
	// filter = { column1: "value1", column2: "value2", ....}
	// default_table = "table"
	var filter_string = " "
	var errors  = []
	Object.keys(valid_filters).forEach(key=>{
		var table_prefix = (valid_filters[key].table) ? '"' + valid_filters[key].table + '".' :  (default_table) ? '"' + default_table + '".' : ""
		var column_name = (valid_filters[key].column) ? '"' + valid_filters[key].column + '"' : '"' + key + '"'
		var fullkey = table_prefix + column_name
		if(filter[key] instanceof internal.not_null) {
			filter_string += ` AND ` + fullkey + ` IS NOT NULL `
		} else if(typeof filter[key] != "undefined" && filter[key] !== null) {
			if(/[';]/.test(filter[key])) {
				errors.push("Invalid filter value")
				console.error(errors[errors.length-1])
			}
			if(valid_filters[key].type == "regex_string") {
				var regex = filter[key].replace('\\','\\\\')
				filter_string += " AND " + fullkey  + " ~* '" + filter[key] + "'"
			} else if(valid_filters[key].type == "string") {
				if(Array.isArray(filter[key])) {
					var values = filter[key].filter(v=>v != null).map(v=>v.toString()).filter(v=>v != "")
                    if(!values.length) {
						errors.push("Empty or invalid string array")
						console.error(errors[errors.length-1])
                    } else {
						if(valid_filters[key].case_insensitive) {
							filter_string += ` AND lower(${fullkey}) IN ( ${values.map(v=>`lower('${v}')`).join(",")})`
						} else {
							filter_string += ` AND ${fullkey} IN ( ${values.map(v=>`'${v}'`).join(",")})`
						}
                    }
				} else {
					if(valid_filters[key].case_insensitive) {
						filter_string += ` AND lower(${fullkey})=lower('${filter[key]}')`
					} else {
						filter_string += " AND "+ fullkey + "='" + filter[key] + "'"
					}
				}
			} else if (valid_filters[key].type == "boolean") {
				var boolean = (/^[yYtTvVsS1]/.test(filter[key])) ? "true" : "false"
				filter_string += " AND "+ fullkey + "=" + boolean + ""
			} else if (valid_filters[key].type == "boolean_only_true") {
				if (/^[yYtTvVsS1]/.test(filter[key])) {
					filter_string += " AND "+ fullkey + "=true"
				} 
			} else if (valid_filters[key].type == "boolean_only_false") {
				if (!/^[yYtTvVsS1]/.test(filter[key])) {
					filter_string += " AND "+ fullkey + "=false"
				} 
			} else if (valid_filters[key].type == "geometry") {
				if(! filter[key] instanceof a5_geometry) {
					errors.push("Invalid geometry object")
					console.error(errors[errors.length-1])
				} else {
					filter_string += "  AND ST_Distance(st_transform(" + fullkey + ",4326),st_transform(" + filter[key].toSQL() + ",4326)) < 0.001" 
				}
			} else if (valid_filters[key].type == "date" || valid_filters[key].type == "timestamp") {
                let d
				if(filter[key] instanceof Date) {
                    d = filter[key]
                } else {
                    d = new Date(filter[key])
                }
				if(valid_filters[key].trunc != undefined) {
					internal.assertValidDateTruncField(valid_filters[key].trunc)
					filter_string += ` AND date_trunc('${valid_filters[key].trunc}',${fullkey}) = date_trunc('${valid_filters[key].trunc}', '${d.toISOString()}'::timestamptz)`	
				} else {
					filter_string += " AND " + fullkey + "='" + d.toISOString() + "'::timestamptz"
				}
            } else if (valid_filters[key].type == "timestart") {
				var offset = (new Date().getTimezoneOffset() * 60 * 1000) * -1
				if(filter[key] instanceof Date) {
					var ldate = new Date(filter[key].getTime()  + offset).toISOString()
					filter_string += " AND " + fullkey + ">='" + ldate + "'"
				} else {
					var d = new Date(filter[key])
					var ldate = new Date(d.getTime()  + offset).toISOString()
					filter_string += " AND " + fullkey + ">='" + ldate + "'"
				}
			} else if (valid_filters[key].type == "timeend") {
				var offset = (new Date().getTimezoneOffset() * 60 * 1000) * -1
				if(filter[key] instanceof Date) {
					var ldate = new Date(filter[key].getTime()  + offset).toISOString()
					filter_string += " AND " + fullkey + "<='" + ldate + "'"
				} else {
					var d = new Date(filter[key])
					var ldate = new Date(d.getTime()  + offset).toISOString()
					filter_string += " AND " + fullkey + "<='" + ldate + "'"
				}
			} else if (valid_filters[key].type == "greater_or_equal_date") {
				var ldate = new Date(filter[key]).toISOString()
				filter_string += ` AND ${fullkey} >= '${ldate}'::timestamptz`
			} else if (valid_filters[key].type == "smaller_or_equal_date") {
				var ldate = new Date(filter[key]).toISOString()
				filter_string += ` AND ${fullkey} <= '${ldate}'::timestamptz`
			} else if (valid_filters[key].type == "numeric_interval") {
				if(Array.isArray(filter[key])) {
					if(filter[key].length < 2) {
						errors.push("numeric_interval debe ser de al menos 2 valores")
						console.error(errors[errors.length-1])
					} else {
						filter_string += " AND " + fullkey + ">=" + parseFloat(filter[key][0]) + " AND " + key + "<=" + parseFloat(filter[key][1])
					}
				} else {
					 filter_string += " AND " + fullkey + "=" + parseFloat(filter[key])
				}
			} else if(valid_filters[key].type == "numeric_min") {
				filter_string += " AND " + fullkey + ">=" + parseFloat(filter[key])
			} else if(valid_filters[key].type == "numeric_max") {
				filter_string += " AND " + fullkey + "<=" + parseFloat(filter[key])
            } else if (valid_filters[key].type == "integer") {
                if(Array.isArray(filter[key])) {
                    var values = filter[key].map(v=>parseInt(v)).filter(v=>v.toString()!="NaN")
                    if(!values.length) {
						errors.push(`Invalid integer array : ${filter[key].toString()}`)
						console.error(errors[errors.length-1])
                    } else {
    					filter_string += " AND "+ fullkey + " IN (" + values.join(",") + ")"
                    }
				} else {
                    var value = parseInt(filter[key])
                    if(value.toString() == "NaN") {
						errors.push(`Invalid integer: ${filter[key]}`)
						console.error(errors[errors.length-1])
                    } else {
                        filter_string += " AND "+ fullkey + "=" + value + ""
                    }
				}
            } else if (valid_filters[key].type == "number" || valid_filters[key].type == "float") {
                if(Array.isArray(filter[key])) {
                    var values = filter[key].map(v=>parseFloat(v)).filter(v=>v.toString()!="NaN")
                    if(!values.length) {
						errors.push(`Invalid float array: ${filter[key].toString()}`)
						console.error(errors[errors.length-1])
                    } else {
                        filter_string += " AND " + fullkey + " IN (" + values.join(",") + ")"
                    }
                } else {
                    var value = parseFloat(filter[key])
                    if(value.toString() == "NaN") {
						errors.push(`Invalid float: ${filter[key]}`)
						console.error(errors[errors.length-1])
                    } else {
                        filter_string += " AND " + fullkey + "=" + value + ""
                    }
                }
			} else if (valid_filters[key].type == "interval") {
				var value = timeSteps.createInterval(filter[key])
				if(!value) {
					throw("invalid interval filter: " + filter[key])
				}
				filter_string += ` AND ${fullkey}='${value.toPostgres()}'::interval` 
			} else if (valid_filters[key].type == "jsonpath") {
				if (!valid_filters[key].expression) {
					throw new Error("Missing expression for valid_filter " + key)
				}
				const jsonpath_expression = valid_filters[key].expression.replace("$0",filter[key])
				filter_string += ` AND jsonb_path_exists(${fullkey}, '${jsonpath_expression}')` 
			} else {
				if(Array.isArray(filter[key])) {
					filter_string += " AND "+ fullkey + " IN (" + filter[key].join(",") + ")"
				} else {
					filter_string += " AND "+ fullkey + "=" + filter[key] + ""
				}
			}
		} else if (valid_filters[key].required) {
			errors.push("Falta valor para filtro obligatorio " + key)
			console.error(errors[errors.length-1])
		}
	})
	if(errors.length > 0) {
		if(throw_on_error) {
			throw("Invalid filter:\n" + errors.join("\n"))
		} else {
			return null
		}
	} else {
		return filter_string
	}
}

internal.build_group_by_clause = function (valid_columns,columns=[],default_table) {
	// valid_columns = { "column_name_or_alias": {"column":"column_name", "table": "table_name" (optional), "extract":"date_part" (optional)},...}
	// columns = ["column_name_or_alias1","column_name_or_alias2",...]
	// default_table = "table_name"
	// RETURNS {select:array,group_by:array,order_by:array}

	var select_arr = []
	var group_by_arr = []
	var order_by_arr = []
	var control_flag = 0
	var columns
	for(var key of Object.keys(valid_columns)) {
		const c = valid_columns[key]
		if(!columns.includes(key)) {
			continue
		}
		var name = (c.column) ? c.column : key
		var g = (c.table) ? "\"" + c.table + "\".\"" + name + "\"" : (default_table) ? "\"" + default_table + "\".\"" + name + "\"" : "\"" + name + "\""
		if(c.date_part) {
			if(c.type) {
				g += "::" + c.type
			} else {
				g += "::timestamp"
			}
			g = "date_part('" + c.date_part + "'," + g + ")"
		} else if(c.date_trunc) {
			if(c.type) {
				g += "::" + c.type
			} else {
				g += "::timestamp"
			}
			g = "date_trunc('" + c.date_trunc + "'," + g + ")"
		} else {
			if(c.type) {
				g += "::" + c.type
			}
		}
		group_by_arr.push(g)
		var s =  g + ' AS "' + key + '"'
		select_arr.push(s)
		order_by_arr.push('"' + key + '"')
	}
	if(!select_arr.length) {
		return
	}
	return {select: select_arr, group_by: group_by_arr, order_by: order_by_arr}
}	

internal.pasteIntoSQLQuery = function (query,params) {
	for(var i=params.length-1;i>=0;i--) {
		var value
		switch(typeof params[i]) {
			case "string":
				value = "'" + params[i] + "'"
				break;
			case "number":
				value = params[i]
				break
			case "object":
				if(params[i] instanceof Date) {
					value = "'" + params[i].toISOString() + "'::timestamptz::timestamp"
				} else if(params[i] instanceof Array) {
					value = "'{" + params[i].join(",") + "}'" // .map(v=> (typeof v == "number") ? v : "'" + v.toString() + "'")
				} else if(params[i] === null) {
					value = "NULL"
				} else if (params[i].constructor && params[i].constructor.name == 'PostgresInterval') {
						value = "'" + params[i].toPostgres() + "'::interval"
				} else {
					value = params[i].toString()
				}
				break;
			case "undefined": 
				value = "NULL"
				break;
			default:
				value = "'" + params[i].toString() + "'"
		}
		var I = parseInt(i)+1
		var placeholder = "\\$" + I.toString()
		// console.log({placeholder:placeholder,value:value})
		query = query.replace(new RegExp(placeholder,"g"), value)
	}
	return query
}

internal.build_read_query = function(model_name,filter,table_name,options) {
	if(!schemas.hasOwnProperty(model_name)) {
		throw("model name not found")
	}
	var model = schemas[model_name]
	if(!table_name) {
		table_name = model.table_name
	}
	var child_tables = {}
	var meta_tables = {}
	var selected_columns = Object.keys(model.properties).filter(key=>{
		if(model.properties[key].type == 'array' && model.properties[key].hasOwnProperty("items") && model.properties[key].items.hasOwnProperty("$ref")) {
			child_tables[key] = model.properties[key].items.$ref.split("/").pop()
			return false
		} else if (model.properties[key].hasOwnProperty("$ref")) {
			meta_tables[key] = model.properties[key].$ref.split("/").pop()
		} else {
			return true
		}
	})
	var filter_string = internal.control_filter3(model,filter,table_name)
	if(!filter_string) {
		throw("Invalid filter")
	}
	const order_by_clause = ""
	if (options && options.order_by) {
		var order_by
		if(!Array.isArray(options.order_by)) {
			if(options.order_by.indexOf(",") >= 0) {
				order_by = options.order_by.split(",")
			} else {
				order_by = [options.order_by]
			}
		} else {
			order_by = options.order_by
		}
		for(var i in order_by) {
			if(selected_columns.indexOf(order_by[i]) == -1) {
				throw("invalid order_by option - invalid property")
			}
		}
		order_by_clause = " ORDER BY " + order_by.map(key=>getFullKey(model,key,table_name)).join(",")
	}
	return {
		query: "SELECT " + selected_columns.map(key=> getFullKey(model,key,table_name)).join(", ") + " FROM " + '"' + table_name + '" WHERE 1=1 ' + filter_string + order_by_clause,
		child_tables: child_tables,
		meta_tables: meta_tables,
		table_name: table_name
	}
}

function getFullKey(model,key,default_table) {
	return (model.table_name) ? "\"" + model.table_name + "\".\"" + key + "\"" : (default_table) ? "\"" + default_table + "\".\"" + key + "\"" : "\"" + key + "\""
}

internal.getFullKey = getFullKey

internal.control_filter3 = function (model, filter, default_table, crud) {
	var filter_string = " "
	var control_flag = 0
	for(const key of Object.keys(model.properties)) {
		const property = model.properties[key]
		if(property.type == 'array' && property.items && property.items["$href"]) {
			continue
		} else if (!property.type && property.$ref) {
			property.type = property.$ref.split("/").pop()
		}
		var fullkey = getFullKey(model,key,default_table)
		if(typeof filter[key] == "undefined" || filter[key] === null) {
			// if (model.required.indexOf(key) >= 0) {
			// 	console.error("Falta valor para filtro obligatorio " + key)
			// 	control_flag++
			// }
			continue
		}
		const value = filter[key]
		if(/[';]/.test(value)) {
			console.error("Invalid filter value for property " + key +": invalid characters")
			control_flag++
			continue
		}
		if(property.type) {
			if(property.type == "string") {
				if(property.format == "regexp") {
					var regexp = value.replace('\\','\\\\')
					filter_string += " AND " + fullkey  + " ~* '" + regexp + "'"
				} else if (property.format == "date" || property.format == "date-time") {
					let date
					if(value instanceof Date) {
						date = value
					} else {
						date = new Date(value)
					}
					if(date.toString() == "Invalid Date") {
						console.error("Invalid filter value for property " + key + ": invalid date")
						control_flag++
						continue
					} 
					if (property.interval) {
						if(property.interval == "begin") {
							filter_string += " AND " + fullkey + ">='" + date.toISOString() + "'::timestamptz"
						} else if (property.interval == "end") {
							filter_string += " AND " + fullkey + "<='" + date.toISOString() + "'::timestamptz"
						} else {
							filter_string += " AND " + fullkey + "='" + date.toISOString() + "'::timestamptz"		
						}
					} else {
						filter_string += " AND " + fullkey + "='" + date.toISOString() + "'::timestamptz"
					}
				} else if (property.format == "time-interval") {
					const interval = timeSteps.createInterval(value)
					if(!interval) {
						control_flag++
						continue
					}
					filter_string += " AND " + fullkey + "='" + timeSteps.interval2string(interval) + "'::interval"
				} else {
					filter_string += " AND " + fullkey + "='" + value + "'"
				}
			} else if (property.type == "TimeInterval") {
				const interval = timeSteps.createInterval(value)
				if(!interval) {
					control_flag++
					continue
				}
				filter_string += " AND " + fullkey + "='" + timeSteps.interval2string(interval) + "'::interval"
			} else if (property.type == "boolean") {
				const boolean = /^[yYtTvVsS1]/.test(value)
				if(property.format) {
					if(!boolean && property.format == "only-true") {
						continue
					} else if (boolean && property.format == "only-false") {
						continue
					} else {
						filter_string += " AND "+ fullkey + "=" + boolean.toString()	
					}
				} else {
					filter_string += " AND "+ fullkey + "=" + boolean.toString()
				}
			} 
			//   else if (valid_filters[key].type == "numeric_interval") {
			// 	if(Array.isArray(value)) {
			// 		if(value.length < 2) {
			// 			console.error("numeric_interval debe ser de al menos 2 valores")
			// 			control_flag++
			// 		} else {
			// 			filter_string += " AND " + fullkey + ">=" + parseFloat(value[0]) + " AND " + key + "<=" + parseFloat(value[1])
			// 		}
			// 	} else {
			// 		filter_string += " AND " + fullkey + "=" + parseFloat(value)
			// 	}
			// } 
			  else if (property.type == "integer") {
				if(Array.isArray(value)) {
					console.log("array of integers: " + value.join(","))
					var values = value.map(v=>parseInt(v)).filter(v=>v.toString()!="NaN")
					if(!values.length) {
						console.error("Invalid integer")
						control_flag++
						continue
					} 
					filter_string += " AND "+ fullkey + " IN (" + values.join(",") + ")"
				} else {
					var integer = parseInt(value)
					if(integer.toString() == "NaN") {
						console.error("Invalid integer")
						control_flag++
						continue
					}
					filter_string += " AND "+ fullkey + "=" + integer + ""
				}
			} else if (property.type == "number") {
				if(Array.isArray(value)) {
					var values = value.map(v=>parseFloat(v)).filter(v=>v.toString()!="NaN")
					if(!values.length) {
						console.error("Invalid float")
						control_flag++
						continue
					}
					filter_string += " AND "+ fullkey + " IN (" + values.join(",") + ")"
				} else {
					var number = parseFloat(value)
					if(number.toString() == "NaN") {
						console.error("Invalid integer")
						control_flag++
						continue
					}
					filter_string += " AND "+ fullkey + "=" + number + ""
				}
			} else {
				if(Array.isArray(value)) {
					filter_string += " AND "+ fullkey + " IN (" + value.join(",") + ")"
				} else {
					filter_string += " AND "+ fullkey + "=" + value + ""
				}
			}
		} else if (property["$ref"]) {
			if(property["$ref"] == "#/components/schemas/Geometry") {
				let geometry 
				if(!value instanceof a5_geometry) { // value.constructor && value.constructor.name == "geometry") {
					geometry = new a5_geometry(value)
				} else {
					geometry = value
				}
				if(!geometry) {
					control_flag++
					continue
				}
				filter_string += "  AND ST_Distance(st_transform(" + fullkey + ",4326),st_transform(" + geometry.toSQL() + ",4326)) < 0.001"
			}
		}
	}
	if(control_flag > 0) {
		return null
	} else {
		return filter_string
	}
}

internal.getDeepValue = function(obj, path){
    for (var i=0, path=path.split('.'), len=path.length; i<len; i++){
		if(obj === null) {
			return
		}
        obj = obj[path[i]];
        if(typeof obj == 'undefined') {
            break
        }
    };
    return obj;
};

internal.setDeepValue = function(obj, path, value){
    for (var i=0, path=path.split('.'), len=path.length; i<len-1; i++){
        if(typeof obj[path[i]] == 'undefined') {
            obj[path[i]] = {}
        }
        obj = obj[path[i]];
    }
    obj[path[i]] = value
    return
};

internal.delay = async function(t, val) {
    return new Promise(function(resolve) {
        setTimeout(function() {
            // console.log("waited " + t + " ms")
            resolve(val)
        },t)
    })
}

internal.gdalDatasetRasterBandsToArray = function(dataset) {
    var size = dataset.rasterSize
    var bands = []
    dataset.bands.forEach(band=>{
        var pixels = []
        for(var i=0;i<size.x;i++) {
            var row = []
            for(var j=0;j<size.y;j++) {
                row.push(band.pixels.get(i,j))
            }
            pixels.push(row)
        }
		const statistics = band.computeStatistics(false)
        bands.push({
            colorInterpretation: band.colorInterpretation,
            categoryNames: band.categoryNames,
            noDataValue: band.noDataValue,
            offset: band.offset,
            scale: band.scale,
            unitType: band.unitType,
            hasArbitraryOverviews: band.hasArbitraryOverviews,
            dataType: band.dataType,
            readOnly: band.readOnly,
            maximum: statistics.max,
            minimum: statistics.min,
            blockSize: band.blockSize,
            pixels: pixels,
            // overviews: band.overviews,
            size: band.size,
            description: band.description,
            id: band.id,
            // metadata: band.getMetadata(),
            statistics: statistics // band.getStatistics(false,false) // (true,true)
        })
    })
    return bands
}

internal.gdalDatasetToJSON = function(dataset) {
    return {
        geoTransform: dataset.geoTransform,
        GCPs: dataset.getGCPs(),
        metadata: dataset.getMetadata(),
        driver: dataset.driver.description,
        rasterSize: dataset.rasterSize,
        srs: (dataset.srs) ? dataset.srs.toPrettyWKT() : undefined,
        bands: internal.gdalDatasetRasterBandsToArray(dataset)
    }
}

internal.parseField = function(parameter,value) {
	if(parameter.type == "integer") {
		return parseInt(value)
	} else if(parameter.type == "float") {
		return parseFloat(value)
	} else if(parameter.type == "string") {
		return value.toString()
	} else if(parameter.type == "date") {
		return timeSteps.parseDateString(value)
	} else if (parameter.class) {
		return new parameter.class(value)
	} else {
		return value
	}
}

internal.getConfig = function(config,default_config) {
	const result = {...default_config}
	if(config) {
		for(var key of Object.keys(config)) {
			result[key] = config[key]
		}
	}
	return result
}

/**
 * Assert validity of query arguments list
 * @param {Array[Object]} valid_types. Item properties:
 *      type: oneOf: ["string", "integer", "float", "boolean"]
 *      allowNull: boolean default false 
 *      name: string, optional  
 * @param {Array} args 
 */
internal.control_query_args = function(valid_types, args) {
	for(const [index, value] of args.entries()) {
		if(index + 1 > valid_types.length ) {
			throw("Invalid query args: index " + index + " out of bounds")
		}
		try {
			internal.control_query_arg(valid_types[index], value)
		} catch(e) {
			throw("Bad query argument at index " + index + ": " + e.toString())
		}
	}
}

/**
 * Assert that value is a valid valid_type PG query argument
 * @param {Object} valid_type 
 *      type: oneOf: ["string", "integer", "float"]
 *      allowNull: boolean default false 
 *      name: string, optional  
 * @param {*} value 
 */
internal.control_query_arg = function(valid_type={}, value) {
	const name = valid_type.name ?? ""
	if(value == null) {
		if(!valid_type.allowNulls) {
			throw(`Invalid argument: ${name} can't be null`)
		}
		return
	}
	if(valid_type.type == "integer") {
		if(parseInt(value).toString() == "NaN") {
			throw(`Invalid integer argument ${name}: ${value}`)
		}
	} else if(valid_type.type == "float") {
		if(parseFloat(value).toString() == "NaN") {
			throw(`Invalid float argument ${name}: ${value}`)
		}
	} else if(valid_type.type == "boolean") {
		if(["true", "false"].indexOf(value.toString().toLowerCase()) < 0) {
			throw(`Invalid boolean argument ${name}: ${value}`)
		}
	} else { // if(valid_type.type == "string") 
		if(/[';]/.test(value)) {
			throw(`Invalid string argument ${name}: ${value}. Forbidden characters found`)
		}
	}


}

/**
 * Convert an array of objects to a csv string or file. 
 * 
 * Notes:
 * - The keys of the first item are used as table headers. If Other keys exist in other items they are not included in the result.
 * @param {Array<Object>} items - It is assumed that all items have the same keys and that the values are stringifiable
 * @param {string=} output_file 
 * @param {boolean=true} header - include table header
 * @returns {string|undefined} The csv string if output_file is not set, else undefined
 */
internal.arrayOfObjectsToCSV = function(items,output_file,header=true) {
	const header_fields = Object.keys(items[0])
	const rows = items.map(i=>{
		return header_fields.map(k=>i[k])
	})
	if(header) {
		var csv_string = CSV.stringify(
			[
				header_fields,
				...rows
			]
		)
	} else {
		var csv_string = CSV.stringify(rows)
	}
	if (output_file) {
		fs.writeFileSync(output_file,csv_string)
		return
	}
	return csv_string
}

internal. observacionArrayToObject = function(observacion) {
	return {
		timestart: observacion[0],
		timeend: observacion[1],
		valor: observacion[2],
		id: (observacion.length >= 4) ? observacion[3] : undefined
	}
}


/**
 * Efficiency indicators
 * @typedef {Object} CalStats
 * @property {number} nse - Nash-Sutcliffe model efficiency coefficient
 * @property {number} r - Pearson correlation coefficient
 * @property {number} r2 - determination coefficient
 * @property {number} kge - Kling-Gupta model efficiency coefficient
 * @property {number} speds - SPEDS phase coefficient
 * @property {number} n - number of data pairs
 * @property {string} timestart - Start time
 * @property {string} timeend - End time
 */

/**
 * Get efficiency indicators of pronosticos vs. observaciones (Rnash, Rpearson, Kling-Gupta efficiency (KGE), SPEDS)
 * @param {CRUD.observaciones} observaciones
 * 	  Observations (truth)
 * @param {array<CRUD.pronostico>} pronosticos
 *    Simulations 
 * @param {PostgresInterval} dt
 *    Intended time step of simulated series. Observations distanced up to dt / 2 both ways will be coupled with simulated values.
 * @return {CalStats}
 *    Efficiency indicators
 */
internal.getCalStats = function(
	observaciones,
	pronosticos,
	dt = PostgresInterval("1 days")
) {
	dt = (typeof dt.toPostgres === 'function') ? dt : PostgresInterval(dt)
	if(dt.toPostgres() == "0") {
		dt = PostgresInterval("1 days")
	}
	const points = [] // this will accumulate obs-sim pairs with conciding timestart (+/- dt / 2)
	for(const pronostico of pronosticos) {
		const p = (Array.isArray(pronostico)) ? internal.observacionArrayToObject(pronostico) : pronostico
		if(p.valor == undefined) { continue }
		const p_timestart_ms = p.timestart.getTime()
		const dt_ms = timeSteps.advanceInterval(p.timestart,dt).getTime() - p_timestart_ms
		for(const observacion of observaciones) {
			const o = (Array.isArray(observacion)) ? internal.observacionArrayToObject(observacion) : observacion
			if(o.valor == undefined) { continue }
			// if(o.timestart.getTime() == p.timestart.getTime()) {
			const o_timestart_ms = o.timestart.getTime()
			if(o_timestart_ms >= p_timestart_ms - dt_ms / 2 && o_timestart_ms < p_timestart_ms + dt_ms / 2 ) {
					points.push({
					date: p.timestart,
					valor_obs: o.valor,
					valor_sim: p.valor
				})
				break
			}
		}
	}
	if(!points.length) {
		console.error("No obs-sim pairs found for CalStats")
		return
	}
	points.sort((a,b) => a.date.getTime() - b.date.getTime())

	const results = {}

	var sum_xy = 0
	var sum_xx = 0
	var sum_yy = 0
	var sum_x = 0
	var sum_y = 0
	var sum_se = 0
	for(const a of points) {
		sum_x += a.valor_obs
		sum_y += a.valor_sim
		sum_xy += a.valor_obs * a.valor_sim
		sum_xx += a.valor_obs * a.valor_obs
		sum_yy += a.valor_sim * a.valor_sim
		sum_se += (a.valor_obs - a.valor_sim) ** 2
	}

	const mean_x = sum_x / points.length
	const mean_y = sum_y / points.length

	var sum_dev_x_2 = 0
	var sum_dev_y_2 = 0
	var sum_devx_devy = 0 
	for(const a of points) {
		sum_dev_x_2 += (a.valor_obs - mean_x) ** 2
		sum_dev_y_2 += (a.valor_sim - mean_y) ** 2
		sum_devx_devy += (a.valor_obs - mean_x) * (a.valor_sim - mean_y)
	}
	
	results.obs_mean = mean_x
	results.obs_var = sum_dev_x_2 / points.length
	results.sim_mse = sum_se / points.length
	results.nse = 1 - sum_se / sum_dev_x_2 //  results.sim_mse / results.obs_var

	results.sim_mean = mean_y
	results.sim_var = sum_dev_y_2 / points.length
	
	results.cov = sum_devx_devy / points.length
	results.pearson = results.cov / (results.obs_var) ** 0.5 / (results.sim_var) ** 0.5 
	results.r2 = results.pearson ** 2

	const alpha = (results.sim_var) ** 0.5 / (results.obs_var) ** 0.5
	const beta = results.sim_mean / results.obs_mean
	results.kge = 1 - ((results.pearson - 1) ** 2 + (alpha - 1) ** 2 + (beta -1) ** 2) ** 0.5
	const n = points.length
	results.n = n
	results.timestart = points[0].date.toISOString()
	results.timeend = points[points.length - 1].date.toISOString()
	// least squares linear regression
	const mean_xx = sum_xx / n
	const mean_yy = sum_yy / n
	const mean_xy = sum_xy / n
	const slope = sum_devx_devy / sum_dev_x_2
	const intercept = results.sim_mean - slope * results.obs_mean
	const r2 = Math.pow((mean_xy - mean_x * mean_y) / Math.sqrt(( mean_xx - mean_x ** 2 ) * ( mean_yy - mean_y**2 )),2)
	// const slope = (results.n * sum_xy - sum_obs * sum_sim) / (results.n * sum_xx + sum_obs * sum_obs)
	// results.points = points
	results.linear_regression = {
		slope: slope,
		intercept: intercept,
		r2: r2
	}

	if(points.length == 1) {
		console.error("Not enough obs-sim pairs for speds in CalStats")
		results.speds = undefined
	} else {
		results.speds = points.map((a,i) => {
			if(i == 0) {
				return 0
			}
			if(a.valor_obs >= points[i-1].valor_obs) {
				if(a.valor_sim >= points[i-1].valor_sim) {
					return 1
				} else {
					return 0
				}
			} else {
				if(a.valor_sim < points[i-1].valor_sim) {
					return 1
				} else {
					return 0
				}
			}
		}).slice(1,).reduce((sum, a) => sum + a) / (points.length - 1)
	}

	return results
}


function changeRef(object,key) {
    if(key == "$ref") {
        object[key] = "/" + object[key].split("/").pop()
    }
}

function traverse(o,func) {
    for (var i in o) {
        func.apply(this,[o,i]);  
        if (o[i] !== null && typeof(o[i])=="object") {
            //going one step down in the object tree!!
            traverse(o[i],func);
        }
    }
}

internal.assertValidDateTruncField = function(field) {
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

internal.sanitizeIdFilter = function (id, column) {
	if (Array.isArray(id)) {
		const sanitizedArray = id.map(Number);

		if (!sanitizedArray.every(id_ => Number.isInteger(id_) && id_ > 0)) {
			throw new Error("Invalid id: array must contain only positive integers");
		}

		if (sanitizedArray.length === 0) {
			throw new Error("Invalid id: array cannot be empty");
		}

		return ` AND ${column} IN (${sanitizedArray.join(",")})`;
	} else {
		const id_ = Number(id);
		if (!Number.isInteger(id_) || id_ <= 0) {
			throw new Error("Invalid id: must be a positive integer");
		}

		return ` AND ${column}=${id_}`;
	}
}


module.exports = internal