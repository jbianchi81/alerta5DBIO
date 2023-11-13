'use strict'

const {baseModel} = require('./baseModel')
const { estacion, escena, serie, VariableName, unidades, "var": Variable, observacion } = require('./CRUD')
const utils = require('./utils')
const {isoDurationToHours, interval2string, advanceInterval, retreatInterval} = require('./timeSteps')
const { control_filter2 } = require('./utils')
const CSV = require('csv-string')
const internal = {}
const fs = require('fs')

internal.accessor_feature_of_interest = class extends baseModel {
	constructor(fields={}) {
		super(fields)
	}
	static _fields = {
		accessor_id: {type: "string", primary_key:true},
		feature_id: {type: "string", primary_key:true},
		name: {type: "string"},
		geometry: {type: "geometry"},
		result: {type: "object"},
		estacion_id: {type: "integer"},
		area_id: {type: "integer"},
		escena_id: {type: "integer"},
        network_id: {type: "string"}
	}
	static _table_name = "accessor_feature_of_interest"
	async create() {
		if(!this.accessor_id || !this.feature_id) {
			throw("Missing accessor_id and/or feature_id")
		}
		try {
			var result = await global.pool.query("INSERT INTO accessor_feature_of_interest (accessor_id, feature_id, name, geometry, result, estacion_id, area_id, escena_id, network_id) VALUES ($1,$2,$3,ST_GeomFromGeoJSON($4),$5,$6,$7,$8,$9) ON CONFLICT (accessor_id, feature_id) DO UPDATE SET name=coalesce(excluded.name,accessor_feature_of_interest.name), geometry=coalesce(excluded.geometry,accessor_feature_of_interest.geometry), result=coalesce(excluded.result,accessor_feature_of_interest.result), estacion_id=coalesce(excluded.estacion_id,accessor_feature_of_interest.estacion_id), area_id=coalesce(excluded.area_id,accessor_feature_of_interest.area_id), escena_id=coalesce(excluded.escena_id,accessor_feature_of_interest.escena_id), network_id=coalesce(excluded.network_id,accessor_feature_of_interest.network_id) RETURNING accessor_id, feature_id, name, st_asGeoJSON(geometry)::json AS geometry, result, estacion_id, area_id, escena_id, network_id",[this.accessor_id.toString(),this.feature_id.toString(),this.name, JSON.stringify(this.geometry),JSON.stringify(this.result),this.estacion_id,this.area_id,this.escena_id,this.network_id])
		} catch(e) {
			throw(e)
		}
		if(!result.rows.length) {
			throw("Nothing inserted")
		}
		this.set(result.rows[0])
		return this// new this.constructor(result.rows[0])
	}
	// static async read(filter={}) {
	// 	var statement = this.build_read_statement(filter)
	// 	// var filter_string = utils.control_filter2({accessor_id:{type:"string"},feature_id:{type:"string"},name:{type:"string"},estacion_id:{type:"integer"},area_id:{type:"integer"},escena_id:{type:"integer"}},filter)
	// 	const result = global.pool.query(statement)
	// 	// var result = await global.pool.query("SELECT * FROM accessor_feature_of_interest WHERE 1=1 " + filter_string)
	// 	return result.rows.map(r=>new this(r)) // internal.accessor_feature_of_interest(r))
	// }
	async update(changes={}) {
		if(!this.accessor_id || !this.feature_id) {
			throw("Missing accessor_id and/or feature_id")
		}
		var valid_fields = {name: {type:"string"}, geometry:{type:"geometry"}, result:{type:"json"}, estacion_id: {type:"integer"}, area_id: {type:"integer"}, escena_id: {type:"integer"}, network_id:{type:"integer"}}
		for(var key of Object.keys(valid_fields)) {
			if(changes.hasOwnProperty(key) && typeof changes[key] != 'undefined') {
				this[key] = changes[key]
			}
		}
		var update_clause = utils.update_clause(valid_fields, this)
		if(!update_clause.params.length) {
			throw("Nothing set to update")
		}
		const stmt = `UPDATE accessor_feature_of_interest SET ${update_clause.string} WHERE accessor_id=$${update_clause.params.length+1} AND feature_id=$${update_clause.params.length+2} RETURNING ${this.constructor.getColumns().join(",")}`
		const params = [...update_clause.params, this.accessor_id.toString(),this.feature_id.toString()]
		// console.log(stmt)
		// console.log(params)
		try {
			var result = await global.pool.query(stmt,params)
		} catch(e) {
			throw(e)
		}
		if(!result.rows.length) {
			throw("Nothing updated")
		}
		this.set(result.rows[0])
		return this// new this.constructor(result.rows[0])
	}
	static async delete(filter={}) {
		var filter_string = utils.control_filter2({accessor_id:{type:"string"},feature_id:{type:"string"},name:{type:"string"},estacion_id:{type:"integer"},area_id:{type:"integer"},escena_id:{type:"integer"},network_id:{type:"integer"}},filter)
		if(!filter_string.length) {
			throw("At least one filter required for delete action")
		}
		var result = await global.pool.query("DELETE FROM accessor_feature_of_interest WHERE 1=1 " + filter_string + ` RETURNING ${this.getColumns().join(",")}`)
		return result.rows.map(r=>new this(r))
	}
	toEstacion() { 
        return new estacion({
			nombre: this.name,
            tabla: this.network_id,
            id_externo: this.feature_id,
            geom: this.geometry,
			id: this.estacion_id
        })
    }
	async findEstacion() {
		const matches = await estacion.read({
			id_externo: this.feature_id,
			tabla: this.network_id
		})
		if(matches.length) {
			console.log("Found " + matches.length + " estaciones. Returning first.")
			return matches[0]
		} else {
			console.error("No estaciones found")
		}
		return 

	}
	toArea() {
		return new Area({
			nombre: `${this.network_id}:${this.feature_id}`,
			geom: this.geometry,
			id: this.area_id
		})
	}
	toEscena() {
		return new escena({
			nombre: `${this.network_id}:${this.feature_id}`,
			geom: this.geometry,
			id: this.escena_id
		})
	}
	async mapToEstacion(id) {
		const estacion = this.toEstacion()
		if(id) {
			estacion.id = parseInt(id)
		}
		await estacion.create()
		await this.update({estacion_id:estacion.id})
		return estacion
	}
	async mapToArea(id) {
		const area = this.toArea()
		if(id) {
			area.id = parseInt(id)
		}
		await area.create()
		await this.update({area_id:area.id})
		return area
	}
	async mapToEscena(id) {
		const escena = this.toEscena()
		if(id) {
			escena.id = parseInt(id)
		}
		await escena.create()
		await this.update({area_id:escena.id})
		return escena
	}
}

internal.accessor_observed_property = class extends baseModel {
	constructor(fields={}) {
		super(fields)
	}
	static _fields = {
		accessor_id: {type: "string", primary_key:true},
		observed_property_id: {type: "string", primary_key:true}, // URI or code of observed property at source
		name: {type: "string"},
		result: {type: "object"},
		variable_name: {type: "string"} // name of observed property in local database (references VariableName.VariableName)
	}
	static _table_name = "accessor_observed_property"
	async create() {
		if(!this.accessor_id || !this.observed_property_id) {
			throw("Missing accessor_id and/or observed_property_id")
		}
		try {
			var result = await global.pool.query("INSERT INTO accessor_observed_property (accessor_id, observed_property_id, name, result, variable_name) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (accessor_id, observed_property_id) DO UPDATE SET name=coalesce(excluded.name,accessor_observed_property.name), result=coalesce(excluded.result,accessor_observed_property.result), variable_name=coalesce(excluded.variable_name,accessor_observed_property.variable_name) RETURNING accessor_id, observed_property_id, name, result, variable_name",[this.accessor_id.toString(),this.observed_property_id.toString(),this.name,JSON.stringify(this.result),this.variable_name])
		} catch(e) {
			throw(e)
		}
		if(!result.rows.length) {
			throw("Nothing inserted")
		}
		this.set(result.rows[0])
		return this// new this.constructor(result.rows[0])
	}
	static async read(filter={}) {
		var filter_string = utils.control_filter2({accessor_id:{type:"string"},observed_property_id:{type:"string"},name:{type:"string"},variable_name:{type:"string"}},filter)
		var result = await global.pool.query("SELECT * FROM accessor_observed_property WHERE 1=1 " + filter_string)
		return result.rows.map(r=>new this(r))
	}
	/**
	 * 	Updates variable_name of observed_properties from CSV file
	 * @param {string} accessor_id
	 * @param {string} filename
	 * @returns {Promise<internal.accessor_observed_property[]>} Promise returning array of updated observed_properties
	*/
	static async updateFromCSV(accessor_id,filename) {
		const variableName_map = CSV.parse(fs.readFileSync(filename,'utf-8'),{output:"objects"})
		const result = []
		for(var vn of variableName_map) {
			var observed_properties = await this.read({accessor_id:accessor_id, observed_property_id:vn.observed_property_id})
			if(!observed_properties.length) {
				console.log("Didn't find matching observed_property: " + vn.observed_property_id + ". Creating.")
				const op = new this({accessor_id: accessor_id, observed_property_id: vn.observed_property_id, variable_name: vn.variable_name})
				await op.create()
				result.push(op)
			} else {
				for(var observed_property of observed_properties) {
					console.log("Found matching observed property: " + observed_property.observed_property_id + ". Updating.")
					await observed_property.update({variable_name: vn.variable_name})
					result.push(observed_property)
				}
			}
		}
		return result
	}
	async update(changes={}) {
		if(!this.accessor_id || !this.observed_property_id) {
			throw("Missing accessor_id and/or observed_property_id")
		}
		var valid_fields = {name:{type:"string"}, result:{type:"json"}, variable_name: {type:"string"}}
		for(var key of Object.keys(valid_fields)) {
			if(changes.hasOwnProperty(key) && typeof changes[key] != 'undefined') {
				this[key] = changes[key]
			}
		}
		var update_clause = utils.update_clause(valid_fields, this)
		if(!update_clause.params.length) {
			throw("Nothing set to update")
		}
		const stmt = `UPDATE accessor_observed_property SET ${update_clause.string} WHERE accessor_id=$${update_clause.params.length+1} AND observed_property_id=$${update_clause.params.length+2} RETURNING *`
		const params = [...update_clause.params, this.accessor_id.toString(),this.observed_property_id.toString()]
		// console.log(stmt)
		// console.log(params)
		try {
			var result = await global.pool.query(stmt,params)
		} catch(e) {
			throw(e)
		}
		if(!result.rows.length) {
			throw("Nothing updated")
		}
		this.set(result.rows[0])
		return this// new this.constructor(result.rows[0])
	}
	static async delete(filter={}) {
		var filter_string = utils.control_filter2({accessor_id:{type:"string"},observed_property_id:{type:"string"},name:{type:"string"},var_id:{type:"integer"}},filter)
		if(!filter_string.length) {
			throw("At least one filter required for delete action")
		}
		var result = await global.pool.query("DELETE FROM accessor_observed_property WHERE 1=1 " + filter_string + " RETURNING *")
		return result.rows.map(r=>new this(r))
	}
	toVariableName() {
		return new VariableName({
			VariableName: this.variable_name,
			href: this.observed_property_id
		})
	}
	async mapToVariableName(variable_name) {
		const variableName = this.toVariableName()
		if(variable_name) {
			variableName.VariableName = variable_name
		}
		await variableName.create()
		await this.update({variable_name:variableName.VariableName})
		return variableName
	}
}

internal.accessor_unit_of_measurement = class extends baseModel {
	constructor(fields={}) {
		super(fields)
	}
	static _fields = {
		accessor_id: {type: "string", primary_key: true},
		unit_of_measurement_id: {type: "string", primary_key: true},
		unit_id: {type: "integer"}
	}
	static _table_name = "accessor_unit_of_measurement"
	async create() {
		const result = await global.pool.query(`INSERT INTO accessor_unit_of_measurement (accessor_id,unit_of_measurement_id,unit_id) VALUES ($1,$2,$3) ON CONFLICT (accessor_id, unit_of_measurement_id) DO UPDATE SET unit_id=coalesce(excluded.unit_id,accessor_unit_of_measurement.unit_id) RETURNING *`,[this.accessor_id, this.unit_of_measurement_id,this.unit_id])
		if(!result.rows.length) {
			throw("Nothing inserted")
		}
		this.set(result.rows[0])
		return this// new this.constructor(result.rows[0])
	}
	static async read(filter={}) {
		var filter_string = control_filter2({accessor_id:{type:"string"}, unit_of_measurement_id:{type:"string"},unit_id:{type:"integer"}},filter)
		const result = await global.pool.query(`SELECT accessor_id, unit_of_measurement_id, unit_id FROM accessor_unit_of_measurement WHERE 1=1 ${filter_string} ORDER BY accessor_id,unit_of_measurement_id`)
		return result.rows.map(r=>new this(r))
	}
	static async updateFromCSV(accessor_id,filename) {
		const unit_map = CSV.parse(fs.readFileSync(filename,'utf-8'),{output:"objects"})
		const result = []
		for(var u of unit_map) {
			var units = await this.read({accessor_id:accessor_id, unit_of_measurement_id:u.unit_of_measurement_id})
			if(!units.length) {
				const unit_of_measurement = this({accessor_id: accessor_id, unit_of_measurement_id: u.unit_of_measurement_id, unit_id: u.unit_id})
				await unit_of_measurement.create()
				result.push(unit_of_measurement)
			} else {
				for(var unit of units) {
					await unit.update({unit_id: u.unit_id})
					result.push(unit)
				}
			}
		}
		return result
	}
	async update(changes={}) {
		if(!this.accessor_id || !this.unit_of_measurement_id) {
			throw("Missing accessor_id and/or unit_of_measurement_id")
		}
		var valid_fields = {unit_id:{type:"integer"}}
		for(var key of Object.keys(valid_fields)) {
			if(changes.hasOwnProperty(key) && typeof changes[key] != 'undefined') {
				this[key] = changes[key]
			}
		}
		var update_clause = utils.update_clause(valid_fields, this)
		if(!update_clause.params.length) {
			throw("Nothing set to update")
		}
		const stmt = `UPDATE accessor_unit_of_measurement SET ${update_clause.string} WHERE accessor_id=$${update_clause.params.length+1} AND unit_of_measurement_id=$${update_clause.params.length+2} RETURNING *`
		const params = [...update_clause.params, this.accessor_id.toString(),this.unit_of_measurement_id.toString()]
		// console.log(stmt)
		// console.log(params)
		try {
			var result = await global.pool.query(stmt,params)
		} catch(e) {
			throw(e)
		}
		if(!result.rows.length) {
			throw("Nothing updated")
		}
		this.set(result.rows[0])
		return this// new this.constructor(result.rows[0])
	}
	async delete() {
		const result = await global.pool.query(`DELETE FROM accessor_unit_of_measurement WHERE accessor_id=$1 AND unit_of_measurement=$2 RETURNING *`,[this.accessor_id,this.unit_of_measurement])
		if(!result.rows.length) {
			throw("Nothing deleted")
		}
		return new this.constructor(result.rows[0])
	}
	static async delete(filter={}) {
		var filter_string = control_filter2({accessor_id:{type:"string"}, unit_of_measurement_id:{type:"string"},unit_id:{type:"integer"}},filter)
		if(!filter_string.length) {
			throw("At least one filter required for delete action")
		}
		const result = await global.pool.query(`DELETE FROM accessor_unit_of_measurement WHERE 1=1 ${filter_string} RETURNING *`)
		return result.rows.map(r=>new this(r))
	}
	toUnidades() {
		return new unidades({
			id: this.unit_id,
			nombre: this.unit_of_measurement_id
		})
	}
	async findUnidades() {
		const this_unidades = this.toUnidades()
		if(!this_unidades.id) {
			console.error("No unit_id for unit_of_measurement_id " + this.unit_of_measurement_id)
			return this_unidades
		}
		const matched_unidades = await unidades.read({
			id: this_unidades.id
		})
		if(matched_unidades) {
			console.log("Found unidades")
			return matched_unidades
		} else {
			console.error("Didn't find matching unidades")
			return this_unidades
		}
	}
}

internal.accessor_time_value_pair = class extends baseModel {
	constructor(fields={}) {
		super(fields)
	}
	static _fields = {
		accessor_id: {type: "string", primary_key:true}, 
		timeseries_id: {type: "string", primary_key:true}, 
		timestamp: {type: "timestamp", primary_key: true},
		numeric_value: {type: "real"},
		json_value: {type: "object"},
		raster_value: {type: "raster"},
		result: {type: "object"}, 
		observaciones_puntual_id: {type: "integer"}, 
		observaciones_areal_id: {type: "integer"}, 
		observaciones_rast_id: {type: "integer"}
	}
	static _table_name = "accessor_time_value_pair"
	static _additional_filters = {
		timestart: {
			type: "timestart",
			column: "timestamp"
		},
		timeend: {
			type: "timeend",
			column: "timestamp"
		}
	}
	getTipo(tso) {
		if(this.observaciones_puntual_id) {
			return "puntual"
		} else if (tso && tso.series_puntual_id) {
			return "puntual" 
		} else if(this.observaciones_areal_id) {
			return "areal"
		} else if (tso && tso.series_areal_id) {
			return "areal"
		} else if(this.observaciones_rast_id) {
			return "rast"
		} else if (tso && tso.series_rast_id) {
			return "rast"
		} else {
			console.warn("Tipo not determined")
			return
		}
	}
	getSeriesId(tso) {
		const tipo = this.getTipo(tso)
		if(tipo == "puntual") {
			if(tso) {
				return tso.series_puntual_id
			}
			return this.observaciones_puntual_id
		} else if(tipo == "areal") {
			if(tso) {
				return tso.series_areal_id
			}
			return this.observaciones_areal_id 
		} else if(tipo == "rast") {
			if(tso) {
				return tso.series_rast_id
			}
			return this.observaciones_raster_id
		} else {
			console.warn("series_id not determined")
			return
		}
	}
	getValue() {
		return this.numeric_value ?? this.json_value ?? this.raster_value
	}
	toObservacion(tso) {
		const tipo = this.getTipo(tso)
		const series_id = this.getSeriesId(tso)
		const valor = this.getValue()
		var timestart = new Date(this.timestamp)
		if(timestart.toString() == "Invalid Date") {
			throw("accessor_time_value_pair.toObservacion: Invalid date")
		}
		if(tso && tso.time_support) {
			if(tso && /^Preceding/.test(tso.data_type)) {
				var timeend = new Date(timestart)
				timestart = retreatInterval(timestart,tso.time_support)
			} else {
				var timeend = advanceInterval(timestart,tso.time_support)	
			}
		} else {
			var timeend = new Date(timestart)
		}
		return new observacion({
			tipo: tipo,
			series_id: series_id,
			timestart: timestart,
			timeend: timeend,
			valor: valor
		})
	}
	// static async read(filter={}) {
	// 	const statement = this.build_read_statement(filter)
	// 	const result = await global.pool.query(statement)
	// 	return result.rows.map(r=>new this(r))
	// }
	// TODO async delete
}

internal.accessor_timeseries_observation = class extends baseModel {
	constructor(fields={}) {
		super(fields)
		// if(!this.observed_property) {
		// 	this.observed_property = new internal.accessor_observed_property({accessor_id: fields.accessor_id, observed_property_id: fields.observed_property_id})
		// }
		// if(!this.unit_of_measurement) {
		// 	this.unit_of_measurement = new internal.accessor_unit_of_measurement({accessor_id: fields.accessor_id, unit_of_measurement_id: fields.unit_of_measurement_id})
		// }
		// if(!this.feature_of_interest) {
		// 	this.feature_of_interest = new internal.accessor_feature_of_interest({accessor_id: fields.accessor_id, feature_id:fields.feature_of_interest_id})
		// }

		// this.data = (fields.data && fields.data.length) ? fields.data.map(d=>(d instanceof internal.accessor_time_value_pair) ? d : new internal.accessor_time_value_pair(d)) : undefined
	}
	static _fields = {
		accessor_id: {type: "string", primary_key: true},
		timeseries_id: {type: "string", primary_key: true},
		result: {type: "object"},
		series_puntual_id: {type: "integer"},
		series_areal_id: {type: "integer"},
		series_rast_id: {type: "integer"},
		time_support: {type: "interval"},
		data_type: {type: "string"},
		observed_property: {class: internal.accessor_observed_property, foreign_key:{accessor_id: "accessor_id",observed_property_id: "observed_property_id"}, column: "observed_property_id" },
		unit_of_measurement: {class: internal.accessor_unit_of_measurement, foreign_key:{accessor_id:"accessor_id",unit_of_measurement_id:"unit_of_measurement_id"}, column: "unit_of_measurement_id" },
		feature_of_interest: {class: internal.accessor_feature_of_interest, foreign_key:{accessor_id: "accessor_id",feature_of_interest_id: "feature_id"}, column: "feature_of_interest_id" },
		data: {type: "array", items: {class: internal.accessor_time_value_pair}, child: {accessor_id: "accessor_id", timeseries_id: "timeseries_id"}},
		begin_position: {type: "timestamp"},
		end_position: {type: "timestamp"}
	}
	static _table_name = "accessor_timeseries_observation"
	async create() {
		if(!this.accessor_id || !this.timeseries_id) {
			throw("Missing accessor_id and/or timeseries_id")
		}
		if(this.feature_of_interest) {
			await this.feature_of_interest.create()
		}
		if(this.unit_of_measurement) {
			await this.unit_of_measurement.create()
		}
		if(this.observed_property) {
			await this.observed_property.create()
		}
		try {
			var result = await global.pool.query("INSERT INTO accessor_timeseries_observation (accessor_id, timeseries_id, result, series_puntual_id, series_areal_id, series_rast_id, feature_of_interest_id, observed_property_id, unit_of_measurement_id, time_support, data_type,begin_position, end_position) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) ON CONFLICT (accessor_id, timeseries_id) DO UPDATE SET result=coalesce(excluded.result,accessor_timeseries_observation.result), series_puntual_id=coalesce(excluded.series_puntual_id,accessor_timeseries_observation.series_puntual_id), series_areal_id=coalesce(excluded.series_areal_id,accessor_timeseries_observation.series_areal_id), series_rast_id=coalesce(excluded.series_rast_id,accessor_timeseries_observation.series_rast_id), feature_of_interest_id=coalesce(excluded.feature_of_interest_id, accessor_timeseries_observation.feature_of_interest_id),  observed_property_id=coalesce(excluded.observed_property_id, accessor_timeseries_observation.observed_property_id), unit_of_measurement_id=coalesce(excluded.unit_of_measurement_id, accessor_timeseries_observation.unit_of_measurement_id), time_support=coalesce(excluded.time_support, accessor_timeseries_observation.time_support) , data_type=coalesce(excluded.data_type, accessor_timeseries_observation.data_type), begin_position=coalesce(excluded.begin_position,accessor_timeseries_observation.begin_position), end_position=coalesce(excluded.end_position,accessor_timeseries_observation.end_position) RETURNING accessor_id, timeseries_id, result, series_puntual_id, series_areal_id, series_rast_id, time_support, data_type, begin_position, end_position",[this.accessor_id.toString(),this.timeseries_id.toString(),JSON.stringify(this.result),this.series_puntual_id,this.series_areal_id,this.series_rast_id,(this.feature_of_interest) ? this.feature_of_interest.feature_id : undefined, (this.observed_property) ? this.observed_property.observed_property_id : undefined, (this.unit_of_measurement) ? this.unit_of_measurement.unit_of_measurement_id : undefined, interval2string(this.time_support), this.data_type, this.begin_position, this.end_position])
		} catch(e) {
			throw(e)
		}
		if(!result.rows.length) {
			throw("Nothing inserted")
		}
		if(this.data) {
			for(var tvp of this.data) {
				await tvp.create()
			}
		}
		// const new_tso = {
		// 	...result.rows[0],
		// 	feature_of_interest: this.feature_of_interest,
		// 	unit_of_measurement: this.unit_of_measurement,
		// 	observed_property: this.observed_property
		// }
		this.set(result.rows[0]) // new_tso)
		return this//new this.constructor(new_tso)
	}
	static async read(filter={}) {
		var filter_string = utils.control_filter2({
			accessor_id:{type:"string"},
			timeseries_id:{type:"string"},
			feature_of_interest_id:{type:"string"},
			feature_of_interest_name:{type:"string",table:"accessor_feature_of_interest","column":"name"},
			observed_property_id:{type:"string"},
			unit_of_measurement_id:{type:"string"},
			series_puntual_id:{type:"integer"},
			series_areal_id:{type:"integer"},
			series_rast_id:{type:"integer"},
			var_id:{type:"integer",table:"series_union_all"},
			proc_id:{type:"integer",table:"series_union_all"},
			unit_id:{type:"integer",table:"accessor_unit_of_measurement"},
			fuentes_id:{type:"integer",table:"series_union_all"},
			tipo:{type:"string",table:"series_union_all"},
			geometry:{type:"geometry",table:"accessor_feature_of_interest"},
			estacion_id:{type:"integer",table:"accessor_feature_of_interest"},
			area_id:{type:"integer",table:"accessor_feature_of_interest"},
			escena_id:{type:"integer",table:"accessor_feature_of_interest"},
			network_id:{type:"integer",table:"accessor_feature_of_interest"},
			variable_name:{type:"string",table:"accessor_observed_property"},
			data_type:{type:"string"},
			time_support:{type:"interval"},
			timestart:{type:"timestart",table:"accessor_time_value_pair",column:"timestamp"},
			timeend:{type:"timeend",table:"accessor_time_value_pair",column:"timestamp"},
			begin_position: {type: "smaller_or_equal_date", table:"accessor_timeseries_observation", column: "begin_position"},
			end_position: {type: "greater_or_equal_date", table:"accessor_timeseries_observation", column: "end_position"}
		},filter,"accessor_timeseries_observation")
		if(filter.timestart || filter.timeend) {
			var stmt = "SELECT \
				accessor_timeseries_observation.accessor_id,\
				accessor_timeseries_observation.timeseries_id,\
				accessor_timeseries_observation.data_type,\
				accessor_timeseries_observation.time_support,\
				accessor_timeseries_observation.result,\
				accessor_timeseries_observation.series_puntual_id,\
				accessor_timeseries_observation.series_areal_id,\
				accessor_timeseries_observation.series_rast_id,\
				json_build_object('accessor_id',accessor_feature_of_interest.accessor_id, 'feature_id', accessor_feature_of_interest.feature_id, 'geometry', st_asgeojson(accessor_feature_of_interest.geometry)::json,'result',accessor_feature_of_interest.result, 'estacion_id',accessor_feature_of_interest.estacion_id, 'area_id', accessor_feature_of_interest.area_id, 'escena_id', accessor_feature_of_interest.escena_id, 'network_id', accessor_feature_of_interest.network_id,'name',accessor_feature_of_interest.name) AS feature_of_interest,\
				row_to_json(accessor_observed_property.*) AS observed_property,\
				row_to_json(accessor_unit_of_measurement.*) AS unit_of_measurement,\
				json_agg(row_to_json(accessor_time_value_pair.*)) AS data,\
				accessor_timeseries_observation.begin_position,\
				accessor_timeseries_observation.end_position\
			FROM accessor_timeseries_observation \
			LEFT OUTER JOIN series_union_all ON ((series_union_all.tipo='puntual' AND series_union_all.id=accessor_timeseries_observation.series_puntual_id) OR (series_union_all.tipo='areal' AND series_union_all.id=accessor_timeseries_observation.series_areal_id) OR (series_union_all.tipo='raster' AND series_union_all.id=accessor_timeseries_observation.series_rast_id)) \
			LEFT OUTER JOIN accessor_feature_of_interest on (accessor_timeseries_observation.feature_of_interest_id=accessor_feature_of_interest.feature_id) \
			LEFT OUTER JOIN accessor_observed_property ON (accessor_timeseries_observation.observed_property_id=accessor_observed_property.observed_property_id) \
			LEFT OUTER JOIN accessor_unit_of_measurement ON (accessor_timeseries_observation.unit_of_measurement_id=accessor_unit_of_measurement.unit_of_measurement_id)\
			LEFT OUTER JOIN accessor_time_value_pair ON (accessor_timeseries_observation.accessor_id=accessor_time_value_pair.accessor_id AND accessor_timeseries_observation.timeseries_id=accessor_time_value_pair.timeseries_id)\
			WHERE 1=1 " + filter_string + "\
			GROUP BY accessor_timeseries_observation.accessor_id,\
			accessor_timeseries_observation.timeseries_id,\
			accessor_timeseries_observation.data_type,\
			accessor_timeseries_observation.time_support,\
			accessor_timeseries_observation.result,\
			accessor_timeseries_observation.series_puntual_id,\
			accessor_timeseries_observation.series_areal_id,\
			accessor_timeseries_observation.series_rast_id,\
			accessor_feature_of_interest.accessor_id,\
			accessor_feature_of_interest.feature_id,\
			accessor_feature_of_interest.geometry,\
			accessor_feature_of_interest.result,\
			accessor_observed_property.*,\
			accessor_unit_of_measurement.*,\
			accessor_timeseries_observation.begin_position,\
			accessor_timeseries_observation.end_position"
		} else {
			var stmt = "SELECT \
				accessor_timeseries_observation.accessor_id,\
				accessor_timeseries_observation.timeseries_id,\
				accessor_timeseries_observation.data_type,\
				accessor_timeseries_observation.time_support,\
				accessor_timeseries_observation.result,\
				accessor_timeseries_observation.series_puntual_id,\
				accessor_timeseries_observation.series_areal_id,\
				accessor_timeseries_observation.series_rast_id,\
				json_build_object('accessor_id',accessor_feature_of_interest.accessor_id, 'feature_id', accessor_feature_of_interest.feature_id, 'geometry', st_asgeojson(accessor_feature_of_interest.geometry)::json,'result',accessor_feature_of_interest.result, 'estacion_id',accessor_feature_of_interest.estacion_id, 'area_id', accessor_feature_of_interest.area_id, 'escena_id', accessor_feature_of_interest.escena_id, 'network_id', accessor_feature_of_interest.network_id,'name',accessor_feature_of_interest.name) AS feature_of_interest,\
				row_to_json(accessor_observed_property.*) AS observed_property,\
				row_to_json(accessor_unit_of_measurement.*) AS unit_of_measurement,\
				accessor_timeseries_observation.begin_position,\
				accessor_timeseries_observation.end_position\
			FROM accessor_timeseries_observation \
			LEFT OUTER JOIN series_union_all ON ((series_union_all.tipo='puntual' AND series_union_all.id=accessor_timeseries_observation.series_puntual_id) OR (series_union_all.tipo='areal' AND series_union_all.id=accessor_timeseries_observation.series_areal_id) OR (series_union_all.tipo='raster' AND series_union_all.id=accessor_timeseries_observation.series_rast_id)) \
			LEFT OUTER JOIN accessor_feature_of_interest on (accessor_timeseries_observation.feature_of_interest_id=accessor_feature_of_interest.feature_id) \
			LEFT OUTER JOIN accessor_observed_property ON (accessor_timeseries_observation.observed_property_id=accessor_observed_property.observed_property_id) \
			LEFT OUTER JOIN accessor_unit_of_measurement ON (accessor_timeseries_observation.unit_of_measurement_id=accessor_unit_of_measurement.unit_of_measurement_id)\
			WHERE 1=1 " + filter_string
		}
		var result = await global.pool.query(stmt)
		return result.rows.map(r=>new this(r))
	}
	// async update(changes={}) {
	// 	if(!this.accessor_id || !this.timeseries_id) {
	// 		throw("Missing accessor_id and/or timeseries_id")
	// 	}
	// 	var valid_fields = {result:{type:"json"}, series_puntual_id: {type:"integer"}, series_areal_id: {type:"integer"}, series_raster_id: {type:"integer"}}
	// 	for(var key of Object.keys(valid_fields)) {
	// 		if(changes.hasOwnProperty(key) && typeof changes[key] != 'undefined') {
	// 			this[key] = changes[key]
	// 		}
	// 	}
	// 	var update_clause = utils.update_clause(valid_fields, this)
	// 	if(!update_clause.params.length) {
	// 		throw("Nothing set to update")
	// 	}
	// 	const stmt = `UPDATE accessor_timeseries_observation SET ${update_clause.string} WHERE accessor_id=$${update_clause.params.length+1} AND timeseries_id=$${update_clause.params.length+2} RETURNING *`
	// 	const params = [...update_clause.params, this.accessor_id.toString(),this.timeseries_id.toString()]
	// 	// console.log(stmt)
	// 	// console.log(params)
	// 	try {
	// 		var result = await global.pool.query(stmt,params)
	// 	} catch(e) {
	// 		throw(e)
	// 	}
	// 	if(!result.rows.length) {
	// 		throw("Nothing updated")
	// 	}
	// 	this.set(result.rows[0])
	// 	await this.setParents()
	// 	return this// new this.constructor(result.rows[0])
	// }
	static async delete(filter={}) {
		var filter_string = utils.control_filter2({accessor_id:{type:"string"},timeseries_id:{type:"string"},series_puntual_id:{type:"integer"},series_areal_id:{type:"integer"},series_rast_id:{type:"integer"}},filter)
		if(!filter_string.length) {
			throw("At least one filter required for delete action")
		}
		var result = await global.pool.query("DELETE FROM accessor_timeseries_observation WHERE 1=1 " + filter_string + " RETURNING *")
		return result.rows.map(r=>new this(r))
	}
	toSerie() {
		return new serie({
			tipo: (this.series_puntual_id) ? "puntual" : (this.series_areal_id) ? "areal" : (this.series_rast_id) ? "raster" : undefined,
			id: (this.series_puntual_id) ? this.series_puntual_id : (this.series_areal_id) ? this.series_areal_id : (this.series_rast_id) ? this.series_rast_id : undefined,
			nombre: `${this.accesor_id}:${this.timeseries_id}`,
			estacion: this.feature_of_interest.toEstacion(),
			var: this.toVar(),
			unidades: this.unit_of_measurement.toUnidades(),
			procedimiento: {id: 1}
		})
	}
	async findSerie() {
		const this_estacion = await this.findEstacion() ?? this.feature_of_interest.toEstacion()
		const this_var = await this.findVariable()
		const this_unidades = await this.findUnidades()
		return new serie({
			tipo: (this.series_puntual_id) ? "puntual" : (this.series_areal_id) ? "areal" : (this.series_rast_id) ? "raster" : undefined,
			id: (this.series_puntual_id) ? this.series_puntual_id : (this.series_areal_id) ? this.series_areal_id : (this.series_rast_id) ? this.series_rast_id : undefined,
			nombre: `${this.accesor_id}:${this.timeseries_id}`,
			estacion: this_estacion,
			var: this_var,
			unidades: this_unidades,
			procedimiento: {id: 1}
		})
	}
	async mapToSerie(id) {
		const serie = this.toSerie()
		if(id) {
			serie.id = parseInt(id)
		}
		const found_var = await serie.var.find()
		if(!found_var) {
			await serie.var.create()
		}
		await serie.estacion.create()
		await serie.unidades.create()
		await serie.create()
		if(this.serie.tipo == "puntual") {
			await this.update({serie_puntual_id:serie.id})
		} else if(this.serie.tipo == "areal") {
			await this.update({serie_areal_id:serie.id})
		} else	if(this.serie.tipo == "raster") {
			await this.update({serie_rast_id:serie.id})
		}
		return serie
	}
	static datatype_cv = {
		Sporadic: {},
		Cumulative: {},
		Incremental: {},
		"Constant Over Interval": {},
		Categorical: {},
		Continuous: {
			aliases: ["Continuous/Instantaneous", "Instantaneous"]
		},
		"Average in Preceding Interval": {},
		"Average": {
			aliases: ["Average in Succeeding Interval", "Mean"]
		},
		"Constant in Preceding Interval": {},
		"Constant in Succeeding Interval": {
			aliases: ["Constant"]
		},
		Discontinuous: {},
		"Instantaneous Total": {},
		"Maximum in Preceding Interval": {},
		"Maximum": {
			aliases: ["Maximum in Succeeding Interval"]
		},
		"Minimum in Preceding Interval": {},
		"Minimum": {
			aliases: ["Minimum in Succeeding Interval"]
		},
		"Preceding Total": {
			aliases: ["Total"]
		},
		"Succeeding Total": {},
		"Mode in Preceding Interval": {},
		"Mode in Succeeding Interval": {
			aliases: ["Mode"]
		}
	}
	static mapToDatatypeCV(data_type) {
		var matches = Object.keys(this.datatype_cv).filter(key=> key.toLowerCase() == data_type.toLowerCase())
		if(matches.length) {
			return matches[0]
		} else {
			matches = Object.keys(this.datatype_cv).filter(key=> this.datatype_cv[key].aliases && this.datatype_cv[key].aliases.map(alias=>alias.toLowerCase()).indexOf(data_type.toLowerCase()) >= 0)
			if(!matches.length) {
				console.error("No match for " + data_type + " in datatype_cv")
				return
			} else {
				return matches[0]
			}
		}
	}
	toVar() {
		return new Variable({
			var: this.observed_property.name.substring(0,6),
			GeneralCategory: "Hydrology",
			nombre: this.observed_property.name,
			datatype: (this.data_type) ? this.constructor.mapToDatatypeCV(this.data_type) : "Continuous",
			VariableName: this.observed_property.variable_name,
			def_unit_id: this.unit_of_measurement.unit_id,
			timeSupport: this.time_support,
			valuetype: "Field Observation"
		})
	}
	/**
	 * Looks for matching variable in a5 schema. VariableName, datatype and timeSupport fields are used for the search. Note: 'Preceding' datatypes are matched to 'Succeding' equivalents 	 * 
	 * @returns {Variable} Variable
	 */
	async findVariable() {
		const this_variable = this.toVar()
		if(!this_variable.VariableName) {
			console.error("No VariableName for observedProperty " + this.observed_property.observed_property_id)
			return this_variable
		}
		if(!this_variable.datatype)  {
			console.error("No datatype for observedProperty " + this.observed_property.observed_property_id)
			return this_variable
		}
		var datatype_filter = this_variable.datatype
		if(this_variable.datatype == "Preceding Total") {
			datatype_filter = [
				"Preceding Total",
				"Succeeding Total"
			]
		}
		if(!this_variable.timeSupport) {
			console.error("No timeSupport for observedProperty " + this.observed_property.observed_property_id)
			return this_variable
		}
		const matching_variables = await Variable.read({
			datatype: datatype_filter,
			VariableName: this_variable.VariableName,
			timeSupport: this_variable.timeSupport
		})
		if(matching_variables.length) {
			console.log("Found " + matching_variables.length + " variables. Returning first match")
			return matching_variables[0]
		} else {
			console.error("Didn't find matching variables: \n" + JSON.stringify({
				datatype: this_variable.datatype,
				VariableName: this_variable.VariableName,
				timeSupport: this_variable.timeSupport
			}))
			return this_variable
		}
	}
	async findUnidades() {
		return this.unit_of_measurement.findUnidades()
	}
	async findEstacion() {
		return this.feature_of_interest.findEstacion()
	}
	static async getDistinctVariables(filter={},options={}) {
		const tso = await this.read(filter)
		const variables = tso.map(timeseries_observation=>timeseries_observation.toVar())
		const distinct_variables = []
		for(var v of variables) {
			var match = false
			for (var d of distinct_variables) {
				if(JSON.stringify(v.timeSupport) == JSON.stringify(d.timeSupport) && v.VariableName == d.VariableName && v.datatype == d.datatype) {
					match = true
					break
				}
			}
			if(!match) {
				if(options.map) {
					const found_var = await v.find()
					if(found_var) {
						// console.log(`timeSupport: ${JSON.stringify(v.timeSupport)} == ${JSON.stringify(found_var.timeSupport)} VariableName: ${v.VariableName} == ${found_var.VariableName} datatype: ${v.datatype} == ${found_var.datatype}`)
						distinct_variables.push(found_var)
					} else {
						distinct_variables.push(v)
					}
				} else {
					distinct_variables.push(v)
				}
			}
		}
		return distinct_variables
	}
}


module.exports = internal