'use strict'

const fs = require('promise-fs')
const YAML = require('yaml');
const CSV = require('csv-string')
const utils = require('./utils')
const internal = {}
const {Interval} = require('./timeSteps')
const {Geometry} = require('./geometry')

internal.writeModelToFile = async (model,output_file,output_format) => {
	if(!model) {
		throw("missing model")
	}
	if(!output_file) {
		throw("Missing output_file")
	}
	output_format = (output_format) ? output_format : "json"
	if(output_format=="json") {
		await fs.writeFile(output_file,JSON.stringify(model))
	} else if(output_format=="csv") {
		if(!model.toCSV) {
			throw("toCSV() not defined for this class")
		}
		await fs.writeFile(output_file,model.toCSV())
	} else if(output_format=="raster") {
		await fs.writeFile(output_file,new Buffer.from(model.valor))
	} else {
		throw("Invalid format")
	}
	await delay(500) 
}

internal.readModelFromFile = (model_class,input_file,input_format,options={}) => {
	const separator = options.separator ?? ","
	if(!model_class) {
		throw("missing model_class")
	}
	if(!input_file) {
		throw("Missing input_file")
	}
	input_format = (input_format) ? input_format : "json"
	if(input_format=="json" || input_format=="yml") {
		var content = fs.readFileSync(input_file,'utf-8')
		var parsed_content = YAML.parse(content) // JSON.parse(content)
		if(options.property_name != null) {
			if(!parsed_content.hasOwnProperty(options.property_name)) {
				throw(`Property ${options.property_name} not found in json file`)
			}
			parsed_content = parsed_content[options.property_name]
		}
		if(model_class.prototype instanceof Array) {
			return new model_class(parsed_content)
		} else if(Array.isArray(parsed_content)) {
			return parsed_content.map(r=>new model_class(r))
		} else {
			return new model_class(parsed_content)
		}			
	} else if(input_format=="csv") {
		if(!model_class.fromCSV) {
			throw("fromCSV() not defined for this class")
		}
		var content = fs.readFileSync(input_file,'utf-8')
		if(model_class.prototype instanceof Array) {
			return model_class.fromCSV(content, separator)
		} else {
			// if(Array.isArray(parsed_content)) {
			var parsed_content = content.split("\n").filter(x => !/^\s*$/.test(x))
			if(options.header) {
				var columns = parsed_content.shift()
				columns = columns.split(separator)
				// console.debug("columns: " + columns.join(" - ") + ". rows: " + parsed_content.length)
				return parsed_content.map(r=>model_class.fromCSV(r,separator,columns))
			} else {
				return parsed_content.map(r=>model_class.fromCSV(r,separator))
			}
		} 
		// else {
		// 	return model_class.fromCSV(content)
		// }
	} else if(input_format=="raster") {
		if(!model_class.fromRaster) {
			throw("fromRaster() not defined for this class")
		}
		// var content = fs.readFileSync(input_file)
		return model_class.fromRaster(input_file) // content)
	} else if(input_format=="geojson") {
		if(!model_class.fromGeoJSON) {
			throw("fromGeoJSON() not defined for this class")
		}
		return model_class.fromGeoJSON(input_file,options.nombre_property,options.id_property)
	} else {
		throw("Invalid format")
	}
}

internal.baseModel = class {
	async writeFile(output_file,output_format) {
		return internal.writeModelToFile(this,output_file,output_format)
    }
	static readFile(input_file,input_format,options) {
		return internal.readModelFromFile(this,input_file,input_format,options)
	}
	static async createFromFile(input_file,input_format,options) {
		const read_result = this.readFile(input_file,input_format,options)
		if(typeof read_result.create === 'function') {
			return read_result.create()
		} else if(Array.isArray(read_result)) {
			const result = []
			for(var row of read_result) {
				result.push(await row.create())
			}
			return result
		} else {
			throw(new Error("Missing create() method for this class"))
		}
	}
	/**
	 * Reads list of tuples from yml or json or csv. IF csv, the header must contain the field names. Tuples must include all primary keys and at least one non-primary key field. If the primary keys match a database record, it is updated. Else the tuple is skipped (For record insertion use .createFromFile) 
	 * @param {string} input_file 
	 * @param {string} input_format 
	 * @param {*} options 
	 */
	static async updateFromFile(input_file,input_format="yml",options={}) {
		var parsed_content
		if(input_format=="csv") {
			var content = fs.readFileSync(input_file,'utf-8')
			parsed_content = CSV.parse(content,{output: "objects"})
		} else {
			var content = fs.readFileSync(input_file,'utf-8')
			parsed_content = YAML.parse(content)
			if(options.property_name) {
				if(!parsed_content[property_name]) {
					throw(new Error("property " + property_name + " not found in file " + input_file))
				}
				parsed_content = parsed_content[options.property_name]
			}
			if(!Array.isArray(parsed_content)) {
				throw(new Error("file content must be an array"))
			}	
		}
		return this.update(parsed_content)
	}
	constructor(fields={}) {
		for(var key of Object.keys(this.constructor._fields)) {
			this[key] = undefined
		}
		this.set(fields)
	}
	static sanitizeValue(value,definition={}) {
		if(value == null) {
			return value	
		} else {
			if(definition.type) {
				if(definition.type instanceof Function) {
					// prueba si es construible
					try {
						var obj_value = new definition.type(value)
						return obj_value
					} catch(e) {
						console.warn("type is not constructable, trying call")
						return definition.type(value)
					}
				} else if(definition.type == "string") {
					return value.toString()
				} else if (definition.type == "integer") {
					if(parseInt(value).toString() == "NaN") {
						throw(new Error("integer field sanitization error: value: '"+ value + "' can't be parsed as integer"))
					}
					return parseInt(value)
				} else if (definition.type == "numeric" || definition.type == "real" || definition.type == "number"  || definition.type == "float") {
					if(parseFloat(value).toString() == "NaN") {
						throw(new Error("integer field sanitization error: value: '"+ value + "' can't be parsed as float"))
					}
					return parseFloat(value)
				} else if (definition.type == "interval") {
					return new Interval(value)
				} else if (definition.type == "object") {
					if(typeof value == "string" && value.length) {
						try {
							return JSON.parse(value)
						} catch(e) {
							throw(new Error("json field sanitization error: string: '"+ value + "'. Error: " + e.toString()) )
						}
					} else {
						return new Object(value)
					}
				} else if (definition.type == "geometry") {
					var obj
					if(typeof value == "string" && value.length) {
						try {
							obj = JSON.parse(value)
						} catch(e) {
							throw(new Error("json field sanitization error: string: '"+ value + "'. Error: " + e.toString()) )
						}
					} else {
						obj = value
					}
					return new Geometry(obj)
					
				} else if (definition.type == "timestamp") {
					return new Date(value)
				} else if (definition.type == "array") {
					if(utils.isIterable(value)) {
						if(definition.items) {
							return value.map(item=>{
								return this.sanitizeValue(item,definition.items)
							})
						} else {
							return value.map(item=>item)
						}
					} else {
						throw("Invalid value, not iterable")
					}
				} else {
					return value
				}
			} else if (definition.class) {
				if(value instanceof definition.class) {
					// console.log("Value is instance of class, returning as is")
					return value
				} else if (typeof value == "string") {
					// console.log("value is string, parsing")
					try {
						var parsed = JSON.parse(value)
					} catch(e) {
						throw(new Error("json field sanitization error: string: '"+ value + "'. Error: " + e.toString() ))
					}
					return new definition.class(parsed)
				} else {
					// console.log("value is else, instantiating class")
					return new definition.class(value)
				}
			} else {
				// console.log("else, returning value as is")
				return value
			}
		}
	}

	static getForeignKeyFields() {
		const foreign_keys = {}
		for(var key of Object.keys(this._fields)) {
			if(this._fields[key].foreign_key) {
				foreign_keys[key] = this._fields[key]
			}
		}
		return foreign_keys
	}

	static getPrimaryKeyFields() {
		const primary_key_fields = {}
		for(var key of Object.keys(this._fields)) {
			if(this._fields[key].primary_key) {
				primary_key_fields[key] = this._fields[key]
			}
		}
		return primary_key_fields
	}

	getPrimaryKeyValues() {
		const primary_key_fields = this.constructor.getPrimaryKeyFields()
		const primary_key_values = {}
		for(var key of Object.keys(primary_key_fields)) {
			primary_key_values[key] = this[key]
		}
		return primary_key_values
	}

	async setParents() {
		const foreign_key_fields = this.constructor.getForeignKeyFields()
		for(var key of Object.keys(foreign_key_fields)) {
			const parent = await foreign_key_fields[key].class.read(this[key].getPrimaryKeyValues())
			if(!parent.length) {
				console.error("Couldn't find parent row in table " + foreign_key_fields[key].class._table_name)
				continue
			}
			this[key] = parent[0]
		}
	}

	getParentFields(field, key_value_pairs) {
		const parent_fields = {}
		for(var fk of Object.keys(field.foreign_key)) {
			if(key_value_pairs && fk == field.column) {
				parent_fields[field.foreign_key[fk]] = key_value_pairs[fk]
			} else {
				parent_fields[field.foreign_key[fk]] = (key_value_pairs && key_value_pairs[fk]) ? key_value_pairs[fk] : this[fk]
			}
		}
		return parent_fields
	}

	set(key_value_pairs={}) {
		const foreign_key_fields = this.constructor.getForeignKeyFields()
		const foreign_key_columns = Object.keys(foreign_key_fields).map(k=>foreign_key_fields[k].column)
		for(var key of Object.keys(key_value_pairs)) {
			// console.log("KEY:" + key)
			if(Object.keys(this.constructor._fields).indexOf(key) < 0) {
				if(foreign_key_columns.indexOf(key) < 0) {
					// console.log(key_value_pairs)
					throw(new Error("Invalid field key: " + key + ". table name: " + this.constructor._table_name))
				}
				for(var k of Object.keys(foreign_key_fields)) {
					if(foreign_key_fields[k].column == key) {
						const fk_fields = this.getParentFields(foreign_key_fields[k],key_value_pairs)
						this[k] = new foreign_key_fields[k].class(fk_fields)
						break
					}
				}
			} else {
				try { 
					this[key] = this.constructor.sanitizeValue(key_value_pairs[key],this.constructor._fields[key])
				} catch(e) {
					throw(new Error("Can't set property '" + key + "'. " + e.toString()))
				}
			}
		}
	}
	/**
	 * 
	 * @param {object[]} data - list of instances of this class (or objects parseable into it) 
	 * @param {*} options
	 * @param {boolean} options.header - include csv header 
	 * @param {string[]} options.columns - print only this columns
	 * @returns 
	 */
	static toCSV(data,options={}) {
		const rows = []
		if(options.header) {
            rows.push(this.getCSVHeader(options.columns))
        }
		for(var row of data) {
			if(!row instanceof this) {
				row = new this(row)
			}
			rows.push(Object.keys(this._fields).filter(key=>(options.columns) ? options.columns.indexOf(key) >= 0 : true).map(key=>{
				if(this._fields[key].type && (this._fields[key].type == "timestamp" || this._fields[key].type == "date")) {
					return (row[key]) ? row[key].toISOString() : ""
				} else {
					return row[key]
				}
			}))
		}
		return CSV.stringify(rows).replace(/\r\n$/,"")
	}
	/**
	 * 
	 * @param {object} options - options
	 * @param {boolean} options.header - add header line with column names 
	 * @param {string[]} options.columns - print only this columns
	 * @returns {string} csv encoded string
	 */
	toCSV(options={}) {
		const rows = []
		if(options.header) {
            rows.push(this.constructor.getCSVHeader(options.columns))
        } 
		rows.push(Object.keys(this.constructor._fields).filter(key=>(options.columns) ? options.columns.indexOf(key) >= 0 : true).map(key=>{
			if(this.constructor._fields[key].type && (this.constructor._fields[key].type == "timestamp" || this.constructor._fields[key].type == "date")) {
				return this[key].toISOString()
			} else {
				return this[key]
			}
		}))
		return CSV.stringify(rows).replace(/\r\n$/,"")
	}
	/**
	 * 
	 * @returns {string[]} column names
	 */
	static getCSVHeader(columns) {
		if(!this._fields) {
			return [] // ""
		}
		if(columns) {
			return Object.keys(this._fields).filter(key=>columns.indexOf(key)>=0)	
		}
		return Object.keys(this._fields) // CSV.stringify(Object.keys(this._fields)).replace(/\r\n$/,"")
	}
	/**
	 * 
	 * @param {string} row_csv_string - delimited string
	 * @param {string[]} [columns] - ordered field names to assign to parsed csv line 
	 * @returns {object} an instance of this class
	 */
	static fromCSV(row_csv_string,separator=",",columns) {
		if(!this._fields) {
			throw("Missing constructor._fields for class " + this.name)
		}
		columns = (columns) ? columns : Object.keys(this._fields)
		const row = CSV.parse(row_csv_string, separator)[0].map(c=> (!c.length) ? undefined : c)
		const result = {}
		for(var i in columns) {
			if(Object.keys(this._fields).indexOf(columns[i]) < 0) {
				console.error("Bad column name: " + columns[i] + ", skipping")
				continue
			}
			result[columns[i]] = row[i]
		}
		return new this(result)
	}
	build_insert_statement() {
		if(!this.constructor._table_name) {
			throw("Missing constructor._table_name")
		}
		var columns = [] 
		var values = []
		var on_conflict_columns = []
		var on_conflict_action = []
		var params = []
		var index=0
		for(var key of Object.keys(this.constructor._fields)) {
			index = index + 1
			columns.push(`"${key}"`)
			values.push((this.constructor._fields[key].type && this.constructor._fields[key].type == "geometry") ? `ST_GeomFromGeoJSON($${index})` : `$${index}`)
			if(this.constructor._fields[key].primary_key) {
				on_conflict_columns.push(`"${key}"`)
			} else {
				on_conflict_action.push(`"${key}"=COALESCE(excluded."${key}","${this.constructor._table_name}"."${key}")`)
			}
			if (this.constructor._fields[key].type) {
				if(["geometry","object"].indexOf(this.constructor._fields[key].type) >= 0) {
					params.push(JSON.stringify(this[key]))
				} else if (["interval"].indexOf(this.constructor._fields[key].type) >= 0) {
					params.push(interval2string(this[key]))
				} else {
					params.push(this[key])
				}
			} else if(this.constructor._fields[key].class) {
				params.push(JSON.stringify(this[key]))
			} else {
				params.push(this[key])
			}
		}
		var on_conflict_clause = (on_conflict_columns.length) ? (on_conflict_action) ? `ON CONFLICT (${on_conflict_columns.join(",")}) DO UPDATE SET ${on_conflict_action.join(",")}` : `ON CONFLICT (${on_conflict_columns.join(",")}) DO NOTHING` : `ON CONFLICT DO NOTHING`
		return {
			string: `INSERT INTO "${this.constructor._table_name}" (${columns.join(",")}) VALUES (${values.join(",")}) ${on_conflict_clause} RETURNING ${columns.join(",")}`,
			params: params
		}
	}
	static getColumns() {
		return Object.keys(this._fields).map(key=>{
			if(this._fields[key].child) {
				return
			} else if(this._fields[key].column) {
				return `"${this._fields[key].column}"`
			} else if(this._fields[key].type && this._fields[key].type == "geometry") {
				return `ST_AsGeoJSON("${key}") AS "${key}"`
			} else {
				return `"${key}"`
			}
		}).filter(c=>c)
	}
	// getJoins(filter) {
	// 	const joins = []
	// 	for(var key of Object.keys(this.constructor._fields).filter(k=>this.constructor._fields[key].class)) {
	// 		const join_table = this.constructor._fields[key].class._table_name
	// 		joins.push(`LEFT JOIN "${join_table}" ON ("${join_table}".)`
	// 			fk: (this.constructor._fields[key].fk) ? this.constructor._fields[key].fk : "id",
	// 			[]
	// 		)
	// 	}
	// }
	checkPK() {
		for(var key of Object.keys(this.constructor._fields).filter(key=>this.constructor._fields[key].primary_key)) {
			if(this[key] == null) {
				throw(new Error("Missing primary key field " + key + ". Insert attempt on table " + this.constructor._table_name))
			}
		} 
	}
	async create() {
		this.checkPK()
		const statement = this.build_insert_statement()
		// console.log(statement.string)
		// console.log(statement.params)
		const result = await global.pool.query(statement.string,statement.params)
		if(!result.rows.length) {
			throw("nothing inserted")
		}
		this.set(result.rows[0])
		return this
	}
	static build_read_statement(filter={}) {
		const columns = this.getColumns()
		// const joins = this.getJoins(filter)
		const filters = utils.control_filter2({...this._fields,...this._additional_filters},filter)
		const query_string = `SELECT ${columns.join(",")} FROM "${this._table_name}" WHERE 1=1 ${filters}`
		return query_string
	}
	/**
	 * 
	 * @param {Object} filter 
	 * @param {Object} options
	 * @param {boolean} options.mapped_only
	 * @returns 
	 */
	static async read(filter={},options={}) {
		var statement = this.build_read_statement(filter,options.mapped_only)
		const result = await global.pool.query(statement)
		return result.rows.map(r=>new this(r))
	}
	build_update_query(update_keys=[]) {
		if(!this.constructor._table_name) {
			throw("Missing constructor._table_name. Can't build update query")
		}
		const primary_keys = []
		const valid_update_fields = {}
		for(var key of Object.keys(this.constructor._fields)) {
			if(this.constructor._fields[key].primary_key) {
				primary_keys.push(key)
			} else {
				valid_update_fields[key] = this.constructor._fields[key]
			}
		}
		const filters = primary_keys.map((key,i)=>`"${key}"=$${i+1}`)
		const params = [...primary_keys.map(key=>this[key])]
		var update_clause = []
		for(var key of Object.keys(valid_update_fields)) {
			if(!update_keys.indexOf(key) < 0 || typeof this[key] == 'undefined') {
				// if value is null it will update to NULL, if undefined it will not update
				continue
			} else if (this[key] == null) {
				params.push(this[key])
				update_clause.push(`"${key}"=$${params.length}`)
			} else if (valid_update_fields[key].class) {
				params.push(this[key][valid_update_fields[key].foreign_key[valid_update_fields[key].column]])
				update_clause.push(`"${valid_update_fields[key].column}"=$${params.length}`)			
			} else if(valid_update_fields[key].type.toLowerCase() == "string") {
				params.push(this[key].toString())
				update_clause.push(`"${key}"=$${params.length}`) 
			} else if(valid_update_fields[key].type.toLowerCase() == "geometry") {
				params.push(JSON.stringify(this[key]))
				update_clause.push(`"${key}"=ST_GeomFromGeoJSON($${params.length})`)
			} else if(["timestamp","timestamptz","date"].indexOf(valid_update_fields[key].type.toLowerCase()) >= 0) {
				params.push(this[key].toISOString())
				update_clause.push(`"${key}"=$${params.length}::timestamptz`)
			} else if(["json","jsonb"].indexOf(valid_update_fields[key].type.toLowerCase()) >= 0) {
				params.push(JSON.stringify(this[key]))
				update_clause.push(`"${key}"=$${params.length}`)
			} else {
				params.push(this[key])
				update_clause.push(`"${key}"=$${params.length}`)
			}
		}
		if(!update_clause.length) {
			console.error("Nothing set to update")
			return
		}
		const returning = this.constructor.getColumns()
		return {
			string: `UPDATE "${this.constructor._table_name}" SET ${update_clause.join(", ")} WHERE ${filters.join(" AND ")} RETURNING ${returning.join(",")}`,
			params: params
		}
	}

	/**
	 * Updates each row matching given primary keys with given non-primary key field values
	 * @param {object[]} updates - List of tuples. Each tuple must contain table primary key values and at least one non-primary key field with a new value
	 */
	static async update(updates=[]) {
		if(!this._fields) {
			throw(new Error("Can't use update method on this class: constructor._fields not defined"))
		}
		const primary_key_fields = Object.keys(this._fields).filter(key=>this._fields[key].primary_key)
		if(!primary_key_fields.length) {
			throw(new Error("Can't use update method on this class: missing primary key fields on constructor._fields"))
		}
		const result = []
		tuple_loop:
		for(var update_tuple of updates) {
			const read_filter = {}
			const changes = {}
			for(var key of Object.keys(this._fields)) { 
				if(primary_key_fields.indexOf(key) >= 0) {
					if(!update_tuple[key]) {
						console.error("Missing primary key " + key + ". Skipping")
						continue tuple_loop
					}
					read_filter[key] = update_tuple[key]
				} else {
					if(typeof update_tuple[key] != 'undefined') {
						changes[key] = update_tuple[key]
					}
				}
			}
			if(!Object.keys(changes).length) {
				console.error("Nothing set to change")
				continue tuple_loop
			}
			const read_result = await this.read(read_filter)
			if(!read_result.length) {
				console.error("No rows matched for update. Consider using .create")
				continue tuple_loop
			}
			result.push(await read_result[0].update(changes))			
		}
		return result
	}

	async update(changes={}) {
		this.set(changes)
		const statement = this.build_update_query(Object.keys(changes))
		if(!statement) {
			// console.error("Nothing set to update")
			return this
		}
		try {
			var result = await global.pool.query(statement.string,statement.params)
		} catch(e) {
			throw(e)
		}
		if(!result.rows.length) {
			console.error("Nothing updated")
		}
		this.set(result.rows[0])
		await this.setParents()
		return this// new this.constructor(result.rows[0])
	}

	static async delete(filter={}) {
		if(!this._table_name) {
			throw("Missing constructor._table_name. Can't build update query")
		}
		var filter_string = utils.control_filter2(this._fields,filter)
		if(!filter_string.length) {
			throw("At least one filter required for delete action")
		}
		const returning = this.getColumns()
		var result = await global.pool.query(`DELETE FROM "${this._table_name}" WHERE 1=1 ${filter_string} RETURNING ${returning.join(",")}`)
		return result.rows.map(r=>new this(r))
	}

	partial(columns=[]) {
		const partial = {} // new internal.baseModel()
		for(var key of columns) {
			partial[key] = this[key]
		}
		return new this.constructor(partial)
	}

	static _table_name = undefined
	static _fields = {}
	static _additional_filters = {}
}

internal.BaseArray = class extends Array {
	async writeFile(output_file,output_format) {
		return internal.writeModelToFile(this,output_file,output_format)
    }
	static readFile(input_file,input_format,property_name,options) {
		return internal.readModelFromFile(this,input_file,input_format,property_name,options)
	}
}


module.exports = internal