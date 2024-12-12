'use strict'

require('./setGlobal')
const internal = {};
const Wkt = require('wicket')
var wkt = new Wkt.Wkt()
var parsePGinterval = require('postgres-interval')
//~ const Accessors = require('./accessors.js')
var sprintf = require('sprintf-js').sprintf, vsprintf = require('sprintf-js').vsprintf
var fs =require("promise-fs")
const { exec, spawn, execSync } = require('child_process');
const pexec = require('child-process-promise').exec;
const ogr2ogr = require("ogr2ogr")
var path = require('path');
const validFilename = require('valid-filename');
const printMap = require("./printMap")
const config = global.config // require('config');
const timeSteps = require('./timeSteps')
const QueryStream = require('pg-query-stream')
const JSONStream = require('JSONStream')
const CSV = require('csv-string')
// const gdal = require('gdal')
const tmp = require('tmp')
const { baseModel, BaseArray } = require('./baseModel')
const querystring = require('node:querystring'); 
var Validator = require('jsonschema').Validator;
// const { errorMonitor } = require('events');
var turfHelpers = require("@turf/helpers")
var pointsWithinPolygon = require("@turf/points-within-polygon")
// const { createCipheriv } = require('crypto');

// const series2waterml2 = require('./series2waterml2');
// const { relativeTimeThreshold } = require('moment-timezone');
const { pasteIntoSQLQuery, setDeepValue, delay, gdalDatasetToJSON, parseField, control_filter2, control_filter3, control_query_args, getCalStats, assertValidDateTruncField } = require('./utils');
const { DateFromDateOrInterval, Interval } = require('./timeSteps');
// const { isContext } = require('vm');
const logger = require('./logger');
const {serieToGmd} = require('./serieToGmd')
const {Geometry} = require('./geometry')

const { escapeIdentifier, escapeLiteral } = require('pg');
const { options } = require('marked');

const apidoc = JSON.parse(fs.readFileSync(path.resolve(__dirname,'../public/json/apidocs.json'),'utf-8'))
var schemas = apidoc.components.schemas
traverse(schemas,changeRef)
var v = new Validator();
for(var key in schemas) {
    v.addSchema(schemas[key],"/" + key)
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

if (!Promise.allSettled) {
  Promise.allSettled = promises =>
    Promise.all(
      promises.map((promise, i) =>
        promise
          .then(value => ({
            status: "fulfilled",
            value,
          }))
          .catch(reason => ({
            status: "rejected",
            reason,
          }))
      )
    );
}

function flatten(arr) {
  return arr.reduce(function (flat, toFlatten) {
    return flat.concat(Array.isArray(toFlatten) ? flatten(toFlatten) : toFlatten);
  }, []);
}

internal.geometry = class extends Geometry  {}

internal.red = class extends baseModel  {
	constructor() {
        super()
		this.id = undefined
		switch (arguments.length) {
			case 1:
				if(typeof(arguments[0]) == "string") {
					[this.id,this.tabla_id, this.nombre,this.is_public,this.public_his_plata] = arguments[0].split(",")
				} else {
					this.id = arguments[0].id
					this.tabla_id = arguments[0].tabla_id
					this.nombre = arguments[0].nombre
					this.public = arguments[0].public
					this.public_his_plata = arguments[0].public_his_plata 
				}
				break;
			default:
				[this.id,this.tabla_id, this.nombre,this.is_public,this.public_his_plata] = arguments
				break;
		}
		this.public = (!this.public) ? false : this.public
		this.public_his_plata = (!this.public_his_plata) ? false : this.public_his_plata
	}
	toString() {
		return "{tabla_id: " + this.tabla_id + ", nombre: " + this.nombre + ", public: " + this.public + ", public_his_plata: " + this.public_his_plata + ", id: " + this.id + "}"
	}
	toCSV() {
		return this.tabla_id + "," + this.nombre + "," + this.public + "," + this.public_his_plata + "," + this.id
	}
	toCSVless() {
		return this.tabla_id + "," + this.nombre + "," + this.id
	}
	getId(pool) {
		return pool.query("\
		SELECT id \
		FROM redes \
		WHERE tabla_id = $1\
		",[this.tabla_id])
		.then(res=>{
			if (res.rows.length>0) {
				this.id = res.rows[0].id
				return
			} else {
				return pool.query("\
				SELECT max(id)+1 AS id\
				FROM redes\
				")
				.then(res=>{
					this.id = res.rows[0].id
				})
			}
		})
		.catch(e=>{
			console.error(e)
		})
	}	
	async create() {
		const created = await internal.CRUD.upsertRed(this)
		if(!created) {
			return
		}
		Object.assign(this,created)
		return this
	}
	static async create(redes) {
		if(!Array.isArray(redes)) {
			const red = new this(redes)
			return [await red.create()]
		} else {
			const created = []
			for(var red of redes) {
				red = new this(red)
				created.append(await red.create())
			}
			return created
		}
	} 
	static async read(filter) {
		if(filter.id) {
			return internal.CRUD.getRed(filter.id)
		}
		return internal.CRUD.getRedes(filter)
	}

	async delete() {
		if(!this.id) {
			throw("Can't delete red. Missing id")
		}
		return internal.CRUD.deleteRed(this.id)
	}

	static async delete(filter={}) {
		if(!filter.id && !filter.nombre && !filter.tabla_id) {
			throw("Invalid filter. At least one of id, nombre must be defined")
		}
		const redes = await this.read(filter)
		const deleted = []
		for(const red of redes) {
			try {
				deleted.push(await red.delete())
			} catch(e) {
				throw(new Error(e))
			}
		}
		return deleted
	}
}

internal.red.build_read_query = function (filter) {
	//~ console.log(filter)
	const valid_filters = {nombre:"regex_string", tabla_id:"string", public:"boolean", public_his_plata:"boolean", id:"integer"}
	var filter_string=""
	var control_flag=0
	Object.keys(valid_filters).forEach(key=>{
		if(filter[key]) {
			if(/[';]/.test(filter[key])) {
				console.error("Invalid filter value")
				control_flag++
			}
			if(valid_filters[key] == "regex_string") {
				var regex = filter[key].replace('\\','\\\\')
				filter_string += " AND " + key  + " ~* '" + filter[key] + "'"
			} else if(valid_filters[key] == "string") {
				filter_string += " AND "+ key + "='" + filter[key] + "'"
			} else if (valid_filters[key] == "boolean") {
				var boolean = (/^[yYtTvVsS1]/.test(filter[key])) ? "true" : "false"
				filter_string += " AND "+ key + "=" + boolean + ""
			} else {
				filter_string += " AND "+ key + "=" + filter[key] + ""
			}
		}
	})
	if(control_flag > 0) {
		return Promise.reject(new Error("invalid filter value"))
	}
	//~ console.log("filter_string:" + filter_string)
	return "SELECT * from redes WHERE 1=1 " + filter_string
}

internal.estacion = class extends baseModel {
	static _table_name = "estaciones"
	static _foreign_key_fields = {
		tabla: {type: "string", table: "redes", column: "tabla_id"}
	}
	static _fields = {
		id: {type: "integer", primary_key: true},
		nombre: {type: "string"},
		id_externo: {type: "string"},
		geom: {type: "geometry"},
		provincia: {type: "string"},
		pais: {type: "string"},
		rio: {type: "string"},
		has_obs: {type: "boolean"},
		tipo: {type: "string"},
		automatica: {type: "boolean"},
		habilitar: {type: "boolean"},
		propietario: {type: "string"},
		abreviatura: {type: "string"},
		URL: {type: "string"},
		localidad: {type: "string"},
		real: {type: "boolean"},
		nivel_alerta: {type: "number"},
		nivel_evacuacion: {type: "number"},
		nivel_aguas_bajas: {type: "number"},
		altitud: {type: "number"},
		public: {type: "boolean"},
		cero_ign: {type: "number"},
		ubicacion: {type: "string"},
		drainage_basin: {type: "object"},
		tabla: {foreign_key: true, type: "string", table: "redes", column: "tabla_id"},
		red: {type: internal.red}
	}
	constructor() {
		switch(arguments.length) {
			case 1:
				if(typeof(arguments[0]) === "string") {
					super()
					var arg_arr =  arguments[0].split(",")
					this.id = arg_arr[0]
					this.nombre = arg_arr[1]
					this.id_externo = arg_arr[2]
					this.geom = new internal.geometry(arg_arr[3])
					this.tabla = arg_arr[4]
					this.provincia = arg_arr[5]
					this.pais = arg_arr[6]
					this.rio=arg_arr[7]
					this.has_obs= arg_arr[8]
					this.tipo =arg_arr[9]
					this.automatica =arg_arr[10]
					this.habilitar =arg_arr[11]
					this.propietario =arg_arr[12]
					this.abreviatura =arg_arr[13]
					this.URL =arg_arr[14]
					this.localidad =arg_arr[15]
					this.real =arg_arr[16]
					this.nivel_alerta = arg_arr[17]
					this.nivel_evacuacion = arg_arr[18]
					this.nivel_aguas_bajas = arg_arr[19]
					this.altitud = arg_arr[19]
					this.public = arg_arr[20]
					this.cero_ign = arg_arr[21]
					this.ubicacion = arg_arr[22]
					this.drainage_basin = arg_arr[23]
					this.red = (arg_arr[24]) ? new internal.red(arg_arr[24]) : undefined
				} else {
					arguments[0].provincia = (arguments[0].hasOwnProperty("provincia")) ? arguments[0].provincia : arguments[0].distrito
					delete arguments[0].distrito
					super(arguments[0])
					// this.id = arguments[0].id
					// this.nombre = arguments[0].nombre
					// this.id_externo = arguments[0].id_externo
					// this.geom = (arguments[0].hasOwnProperty("geom") && arguments[0].geom != null) ? new internal.geometry(arguments[0].geom) : undefined
					// this.tabla = arguments[0].tabla
					// this.provincia = (arguments[0].hasOwnProperty("provincia")) ? arguments[0].provincia : arguments[0].distrito
					// this.pais = arguments[0].pais
					// this.rio=arguments[0].rio
					// this.has_obs= arguments[0].has_obs
					// this.tipo =arguments[0].tipo
					// this.automatica =arguments[0].automatica
					// this.habilitar =arguments[0].habilitar
					// this.propietario =arguments[0].propietario
					// this.abreviatura =arguments[0].abreviatura
					// this.URL =arguments[0].URL
					// this.localidad =arguments[0].localidad
					// this.real =arguments[0].real
					// this.nivel_alerta = arguments[0].nivel_alerta
					// this.nivel_evacuacion = arguments[0].nivel_evacuacion
					// this.nivel_aguas_bajas = arguments[0].nivel_aguas_bajas
					// this.altitud = arguments[0].altitud
					// this.public = arguments[0].public
					// this.cero_ign = arguments[0].cero_ign
					// this.ubicacion = arguments[0].ubicacion
					// this.drainage_basin = arguments[0].drainage_basin
				}
				break;
			default:
				super()
				this.nombre = arguments[0]
				this.id_externo = arguments[1]
				this.geom = arguments[2]
				this.tabla = arguments[3]
				this.provincia = arguments[4]
				this.pais = arguments[5]
				this.rio=arguments[6]
				this.has_obs= arguments[7]
				this.tipo =arguments[8]
				this.automatica =arguments[9]
				this.habilitar =arguments[10]
				this.propietario =arguments[11]
				this.abreviatura =arguments[12]
				this.URL =arguments[13]
				this.localidad =arguments[14]
				this.real =arguments[15]
				this.nivel_alerta = arguments[16]
				this.nivel_evacuacion = arguments[17]
				this.nivel_aguas_bajas = arguments[18]
				this.altitud = arguments[19]
				this.public = arguments[20]
				this.cero_ign = arguments[21]
				this.ubicacion = arguments[22]
				this.drainage_basin = arguments[23]
				this.red = arguments[24]
				break;
		}
		//~ console.log({estacion:this})
	}
	async getId(pool=global.pool) {
		if(this.id) {
			return Promise.resolve()
		} // else
		return pool.query("\
		SELECT id \
		FROM estaciones \
		WHERE id_externo = $1\
		AND tabla = $2\
		",[this.id_externo,this.tabla])
		.then(res=>{
			if (res.rows.length>0) {
				this.id = res.rows[0].id
				return
			} 
		})
		.catch(e=>{
			console.error(e)
			return
		})
	}
	async getEstacionId() {
		if(!this.id_externo || !this.tabla) {
			console.warn("id_externo and/or tabla missing from estacion. Can't retrieve id")
			return this
		}
		return global.pool.query("\
		SELECT unid \
		FROM estaciones \
		WHERE id_externo = $1\
		AND tabla = $2\
		",[this.id_externo,this.tabla])
		.then(res=>{
			if (res.rows.length>0) {
				this.id = res.rows[0].unid
				//~ console.log({getEstacionId_id:res.rows[0].unid})
			} else {
				console.warn("Estacion not found. Not setting id")
			}
			return this
		})
	}
	toString() {
		return "{id:" + this.id + ", nombre: " + this.nombre + ", id_externo: " + this.id_externo + ", geom: " + ((this.geom instanceof internal.geometry) ? this.geom.toString() : "" )+ ", tabla: " + this.tabla + ", provincia: " + this.provincia + ", pais: " + this.pais + ", rio: " + this.rio + ", has_obs: " + this.has_obs + ", tipo: " + this.tipo + ", automatica: " + this.automatica + ", habilitar: " + this.habilitar + ", propietario: " + this.propietario + ", abreviatura: " + this.abreviatura + ", URL:" + this.URL + ", localidad: " + this.localidad + ", real: " + this.real + ", nivel_alerta: " + this.nivel_alerta + ", nivel_evacuacion: " + this.nivel_evacuacion + ", nivel_aguas_bajas: " + this.nivel_aguas_bajas + ",altitud:" + this.altitud + "}"
	}
	toCSV() {
		return this.id + "," + this.nombre + "," + this.id_externo + "," + ((this.geom instanceof internal.geometry) ? this.geom.toString() : "" ) + "," + this.tabla + "," + this.provincia + "," + this.pais + "," + this.rio + "," + this.has_obs + "," + this.tipo + "," + this.automatica + "," + this.habilitar + "," + this.propietario + "," + this.abreviatura + "," + this.URL + "," + this.localidad + "," + this.real + "," + this.nivel_alerta + "," + this.nivel_evacuacion + "," + this.nivel_aguas_bajas + "," + this.altitud
	}
	toCSVless() {
		return this.id + "," + this.nombre + "," + this.tabla + "," + ((this.geom instanceof internal.geometry) ? this.geom.toString() : "")
	}
	toJSON() {
		return {
			id: this.id,
			nombre: this.nombre,
			id_externo: this.id_externo,
			geom: this.geom,
			tabla: this.tabla,
			provincia: this.provincia,
			pais: this.pais,
			rio: this.rio,
			has_obs: this.has_obs,
			tipo: this.tipo,
			automatica: this.automatica,
			habilitar: this.habilitar,
			propietario: this.propietario,
			abreviatura: this.abreviatura,
			URL: this.URL,
			localidad: this.localidad,
			real: this.real,
			nivel_alerta: this.nivel_alerta,
			nivel_evacuacion: this.nivel_evacuacion,
			nivel_aguas_bajas: this.nivel_aguas_bajas,
			altitud: this.altitud,
			public: this.public,
			cero_ign: this.cero_ign,
			ubicacion: this.ubicacion,
			drainage_basin: this.drainage_basin,
			red: this.red
		}
	}
	toGeoJSON(includeProperties=true, includeDrainageBasin=true) {
		var geojson
		console.debug({"coordinates":this.geom.coordinates})
		geojson = turfHelpers.point(this.geom.coordinates)
		if(includeProperties) {
			geojson.properties = {}
			Object.keys(this).forEach(key=>{
				if(key == "geom" || key == "drainage_basin") {
					return
				}
				geojson.properties[key] = this[key]
			})
		}
		if(includeDrainageBasin && this.drainage_basin) {
			const point_geometry = {...geojson.geometry} 
			geojson.geometry = {
				type: "GeometryCollection",
				geometries: [
					point_geometry,
					this.drainage_basin.geometry
				]
			}
			geojson.properties = {...geojson.properties, ...this.drainage_basin.properties}
		}
		return geojson
	}
	static toGeoJSON(estaciones=[]) {
		const result = {
			"type": "FeatureCollection",
			"features": []
		}
		estaciones.forEach((estacion,i)=>{
			if(!estacion instanceof internal.estacion) {
				var estacion_o = new internal.estacion(estacion)
			} else {
				var estacion_o = estacion
			}
			result.features.push(estacion_o.toGeoJSON())
		})
		return result
	}
	isWithinPolygon(polygon) {
		if(polygon instanceof internal.geometry) {
			if(polygon.type.toLowerCase() != "polygon") {
				console.error("bad geometry for polygon")
				return false
			}
			polygon = polygon.toGeoJSON()
		}
		if(!this.geom || !this.geom.coordinates || this.geom.coordinates.length < 2) {
			console.error("isWithinPolygon: Missing geometry for estacion")
			return false
		}
		var point = this.toGeoJSON(false) // turfHelpers.point([this.geom.coordinates[0],this.geom.coordinates[1]])
		var within = pointsWithinPolygon(point,polygon)
		if(within.features.length) {
			return true
		} else {
			return false
		}
	}
	static async read(filter={},options) {
		if(filter.id && !Array.isArray(filter.id)) {
			return internal.CRUD.getEstacion(filter.id,filter.public, options)
		}
		return internal.CRUD.getEstaciones(filter,options)
	}
	async create(options) {
		const created = await internal.CRUD.upsertEstacion(this,options)
		this.id = created.id
		return created
	}
	static async create(data,options) {
		return internal.CRUD.upsertEstaciones(data,options)
	}
	async update(fields={}) {
		if(fields.id) {
			await this.updateId(fields.id)
		}
		if(Object.keys(fields).filter(k=> k != "id").length) {
			this.set(fields)
			return internal.CRUD.updateEstacion(this)
		} else {
			return this
		}
	}
	async updateId(id) {
		try {
			var result = await global.pool.query(`
				UPDATE estaciones
				SET unid=$1
				WHERE tabla=$2
				AND id_externo=$3
				RETURNING unid as id`, [id, this.tabla, this.id_externo])
		} catch(e) {
			throw("estacion.updateId failed: " + e.toString())
		}
		if(!result.rows.length) {
			throw("Nothing updated")
		}
		this.set({id:result.rows[0].id})
		return
	} 

	async delete() {
		return internal.CRUD.deleteEstacion(this.id)
	}
	static async delete(filter) {
		return internal.CRUD.deleteEstaciones(filter)
	}
	// retrieve drainage basins as a geoJSON FeatureCollection
	static async getDrainageBasins(filter={}) {
		const valid_filters = {
			"estacion_id": {
				"type": "integer",
				"table": "estaciones",
				"column": "unid"
			},
			"area_id": {
				"type": "integer",
				"table": "areas_pluvio",
				"column": "unid"
			}
		}
		const filter_string = control_filter2(valid_filters, filter, "estaciones")
		const result = await global.pool.query(`
		with a as ( 
			select 
				areas_pluvio.unid as area_id, 
				estaciones.unid as estacion_id, 
				areas_pluvio.area,
				row_number() over (
					partition by estaciones.unid
					order by area desc
				) as row_number
			from areas_pluvio 
			join estaciones 
				on estaciones.unid=areas_pluvio.exutorio_id
			WHERE 1=1 ${filter_string}
		)
		select 
			a.estacion_id,
			a.area_id,
			a.area,
			areas_pluvio.nombre as area_name,
			st_asgeojson(areas_pluvio.geom)::json as geom
		from a join areas_pluvio
			on a.area_id=areas_pluvio.unid
		where a.row_number = 1
		order by a.estacion_id;`)
		return {
			type: "FeatureCollection",
			features: result.rows.map(r=>{
				return {
					type: "Feature",
					geometry: r.geom,
					properties: {
						estacion_id: r.estacion_id,
						area_id: r.area_id,
						area_name: r.area_name,
						area: r.area,
						area_units: "m^2"
					}
				}
			})
		}
	}
	async getDrainageBasin() {
		if(!this.id) {
			console.warn("Estacion property id not defined: can't retrieve drainage basin")
			this.drainage_basin = null
			return
		}
		const collection = await this.constructor.getDrainageBasins({estacion_id:this.id})
		if(!collection.features.length) {
			this.drainage_basin = null
		} else {
			this.drainage_basin = collection.features[0]
		}
	}
}

internal.area = class extends baseModel  {
	static _table_name = "areas_pluvio"
	constructor() {
        super()
		switch(arguments.length) {
			case 1:
				if(typeof(arguments[0]) === "string") {
					var arg_arr = arguments[0].split(",")
					this.id = arg_arr[0]
					this.nombre = arg_arr[1]
					this.geom = new internal.geometry(arg_arr[2])
					this.exutorio = (arg_arr[3]) ? new internal.geometry(arg_arr[3]) : null
					this.exutorio_id = arg_arr[4]
				} else {
					this.id = arguments[0].id
					this.nombre = arguments[0].nombre
					this.geom = (arguments[0].geom) ? new internal.geometry(arguments[0].geom) : undefined
					this.exutorio = (arguments[0].exutorio) ? (arguments[0].exutorio.geom) ? new internal.geometry(arguments[0].exutorio.geom) : (arguments[0].exutorio.type && arguments[0].exutorio.coordinates) ? new internal.geometry(arguments[0].exutorio) : null : null
					this.exutorio_id = arguments[0].exutorio_id
					this.ae = arguments[0].ae
					this.rho = arguments[0].rho
					this.wp = arguments[0].wp
					this.activar = arguments[0].activar
					this.mostrar = arguments[0].mostrar
					this.area = arguments[0].area
					if(config.verbose) {
						// console.log({new_area: 
						// 	{
						// 		id: this.id,
						// 		nombre: this.nombre,
						// 		geom: this.geom,
						// 		exutorio: this.exutorio,
						// 		exutorio_id: this.exutorio_id
						// 	}
						// })
					}
				}
				break;
			default:
				this.nombre = arguments[0]
				this.geom = arguments[1]
				this.exutorio = (arguments[2]) ? arguments[2] : null
				this.exutorio_id = arguments[3]
				break;
		}
	}
	getId(pool) {
		return pool.query("\
		SELECT unid \
		FROM areas_pluvio \
		WHERE nombre = $1\
		AND geom = st_geomfromtext($2,4326)\
		",[this.nombre, this.geom.toString()])
		.then(res=>{
			if (res.rows.length>0) {
				this.id = res.rows[0].unid
				return
			} else {
				return pool.query("\
				SELECT max(unid)+1 AS id\
				FROM areas_pluvio\
				")
				.then(res=>{
					this.id = res.rows[0].id
				})
			}
		})
		.catch(e=>{
			console.error(e)
		})
	}
	toString() {
		return JSON.stringify({
			id: this.id,
			nombre: this.nombre,
			exutorio_id: this.exutorio_id,
			area: this.area, 
			ae: this.ae, 
			rho: this.rho, 
			wp: this.wp, 
			activar: this.activar, 
			mostrar: this.mostrar 
		})
	}
	toCSV() {
		return [this.id, `"${this.nombre}"`, this.exutorio_id, this.area, this.ae, this.rho, this.wp, this.activar, this.mostrar].join(",")
	}
	toCSVless() {
		return this.id + "," + `"${this.nombre}"`
	}
	static _fields = {
		id: {type: "integer", primary_key: true},
		geom: {type: "geometry"},
		nombre: {type: "string"},
		exutorio: {type: "geometry"},
		exutorio_id: {type: "integer"},
		area: {type: "number"},
		ae: {type: "number"},
		rho: {type: "number"},
		wp: {type: "number"},
		activar: {type: "boolean"},
		mostrar: {type: "boolean"}
	}
	async create() {
		const created = await internal.CRUD.upsertArea(this)
		if(created) {
			Object.assign(this,created)
			return this
		}
		return
	}
	static async read(filter={},options) {
		if(filter.id && !Array.isArray(filter.id)) {
			return internal.CRUD.getArea(filter.id,options)
		}
		return internal.CRUD.getAreas(filter,options)
	}
	async update(changes={}) {
		this.set(changes)
		return this.create()
	}
	static async update(filter={},update={},options) {
		if(filter.area_id && !filter.id) {
			filter.id = filter.area_id
		}
		const matches = await internal.area.read(filter)
		if(!matches) {
			console.warn("No matches to update")
			return []
		} else if (Array.isArray(matches)) {
			const results = []
			for(var area of matches) {
				try {
					console.debug("Try update area.id=" + area.id)
					var result = await area.update(update)
				} catch(e) {
					throw(e)
				}
				results.push(result)
			}
			return results
		} else {
			try {
				console.debug("Try update area.id=" + matches.id)
				var result = await matches.update(update)
			} catch(e) {
				throw(e)
			}
			return [ result ]
		}
	}
	async delete() {
		try {
			var result = await internal.CRUD.deleteArea(this.id)
		} catch(e) {
			throw(e)
		}
		return result
	}
	static async delete(filter) {
		if(filter.area_id && !filter.id) {
			filter.id = filter.area_id
		}
		const matches = await internal.area.read(filter)
		if(!matches) {
			console.warn("No matches to delete")
			return []
		} else if (Array.isArray(matches)) {
			const results = []
			for(var area of matches) {
				try {
					console.debug("Try delete area.id=" + area.id)
					var result = await area.delete()
				} catch(e) {
					throw(e)
				}
				results.push(result)
			}
			return results
		} else {
			try {
				console.debug("Try delete area.id=" + matches.id)
				var result = await matches.delete()
			} catch(e) {
				throw(e)
			}
			return [ result ]
		}
	}
	static fromGeoJSON(
		geojson_file,
		nombre_property = "nombre",
		id_property = "id"
		) {
		const geojson_data = JSON.parse(fs.readFileSync(geojson_file,'utf-8'))
		const areas = []
		if(geojson_data.type == "Feature") {
			areas.push(new this({
				nombre: geojson_data.properties[nombre_property],
				id: geojson_data.properties[id_property],
				geom: geojson_data.geometry
			}))
		} else {
			// assumes featureCollection type
			for(const item of geojson_data.features) {
				areas.push(new this({
					nombre: item.properties[nombre_property],
					id: item.properties[id_property],
					geom: item.geometry
				}))
			}
		}
		return areas 
	}
	toGeoJSON(includeProperties=true, includeOutlet=true) {
		var geojson
		geojson = turfHelpers.polygon(this.geom.coordinates)
		if(includeProperties) {
			geojson.properties = {}
			Object.keys(this).forEach(key=>{
				if(key == "geom" || key == "exutorio") {
					return
				}
				geojson.properties[key] = this[key]
			})
		}
		if(includeOutlet && this.exutorio) {
			const area_geometry = {...geojson.geometry} 
			geojson.geometry = {
				type: "GeometryCollection",
				geometries: [
					area_geometry,
					this.exutorio.geom
				]
			}
			geojson.properties = {...geojson.properties, ...this.exutorio.properties}
		}
		return geojson
	}
}

internal.escena = class extends baseModel  {
	constructor() {
        super()
		switch(arguments.length) {
			case 1:
				if(typeof(arguments[0]) === "string") {
					var arg_arr = arguments[0].split(",")
					this.id = arg_arr[0]
					this.nombre = arg_arr[1]
					this.geom = new internal.geometry(arg_arr[2])
				} else {
					this.id = arguments[0].id
					this.nombre = arguments[0].nombre
					this.geom = (arguments[0].hasOwnProperty("geom")) ? new internal.geometry(arguments[0].geom) : undefined
				}
				break;
			default:
				this.nombre = arguments[0]
				this.geom = (arguments[1]) ? new internal.geometry(arguments[1]) : undefined
				break;
		}
	}
	getId(pool) {
		return pool.query("\
		SELECT id \
		FROM escenas \
		WHERE nombre = $1\
		AND geom = st_geomfromtext($2,4326)\
		",[this.nombre, this.geom.toString()])
		.then(res=>{
			if (res.rows.length>0) {
				this.id = res.rows[0].id
				return
			} else {
				return pool.query("\
				SELECT max(unid)+1 AS id\
				FROM areas_pluvio\
				")
				.then(res=>{
					this.id = res.rows[0].id
				})
			}
		})
		.catch(e=>{
			console.error(e)
		})
	}
	toString() {
		return "{id:" + this.id + ",nombre: " + this.nombre + "}"
	}
	toCSV() {
		return this.id + "," + this.nombre
	}
	toCSVless() {
		return this.id + "," + this.nombre
	}
	static async read(filter={},options) {
		if(filter.id) {
			return internal.CRUD.getEscena(filter.id,options)
		}
		return internal.CRUD.getEscenas(filter,options)
	}
	async create() {
		const created = await internal.CRUD.upsertEscena(this)
		if(created) {
			Object.assign(this,created)
			return this
		} else {
			return
		}
	}

	static async create(escenas) {
		return internal.CRUD.upsertEscenas(escenas)
	}

	async delete() {
		return internal.CRUD.deleteEscena(this.id)
	}

	static async delete(filter={}) {
		var matches = await this.read(filter)
		if(!matches) {
			console.log("Nothing to delete")
			return []
		}
		if(!Array.isArray(matches)) {
			matches = [matches]
		}
		const deleted = []
		for(var escena of matches) {
			const deleted_ = await escena.delete()
			if(deleted_) {
				deleted.push(deleted_)
			}
		}
		return deleted
	}
}

internal.VariableName = class extends baseModel {
	constructor() {
		super()
		this.VariableName = arguments[0].VariableName
		this.href = arguments[0].href
	}
	async create() {
		const result = await global.pool.query(`INSERT INTO "VariableName" ("VariableName","href") VALUES ($1,$2) ON CONFLICT ("VariableName") DO UPDATE SET href=coalesce(excluded.href,href) RETURING *`,[this.VariableName,this.href])
		if(!result.rows.length) {
			throw("Nothing inserted")
		}
		return new this.constructor(result.rows[0])
	}
	static async read(filter={}) {
		const valid_filters = {VariableName:{type:"string"},href:{type:"string"}}
		var filter_string = control_filter2(valid_filters,filter)
		const result = await global.pool.query(`SELECT "VariableName",href from "VariableName" WHERE 1=1 ${filter_string} ORDER BY "VariableName"`)
		return result.rows.map(r=>new this(r))
	}
	async delete() {
		const result = await global.pool.query(`DELETE FROM "VariableName" WHERE "VariableName"=$1 RETURNING *`,[this.VariableName])
		if(!result.rows.length) {
			throw("Nothing deleted")
		}
		return new this.constructor(result.rows[0])
	}
	static async delete(filter={}) {
		const valid_filters = {VariableName:{type:"string"},href:{type:"string"}}
		var filter_string = control_filter2(valid_filters,filter)
		if(!filter_string.length) {
			throw("At least one filter needed for delete action")
		}
		const result = await global.pool.query(`DELETE FROM "VariableName" WHERE 1=1 ${filter_string} RETURNING *`)
		if(!result.rows.length) {
			throw("Nothing deleted")
		}
		return result.rows.map(r=>new this(r))
	}
}

internal["var"] = class extends baseModel  {
	constructor(args) {
        super(args)
		// console.log("time support: " + ((this.timeSupport) ? JSON.stringify(this.timeSupport) : "undefined"))
		// variable,nombre,abrev,type,datatype,valuetype,GeneralCategory,VariableName,SampleMedium,def_unit_id,timeSupport
		// switch(arguments.length) {
		// 	case 1:
		// 		if(typeof(arguments[0]) === "string") {
		// 			var arg_arr = arguments[0].split(",")
		// 			this.id =arg_arr[0]
		// 			this["var"] = arg_arr[1]
		// 			this.nombre = arg_arr[2]
		// 			this.abrev =arg_arr[3]
		// 			this.type= arg_arr[4]
		// 			this.datatype= arg_arr[5]
		// 			this.valuetype=arg_arr[6]
		// 			this.GeneralCategory = arg_arr[7]
		// 			this.VariableName =  arg_arr[8]
		// 			this.SampleMedium = arg_arr[9]
		// 			this.def_unit_id = arg_arr[10]
		// 			this.timeSupport = arg_arr[11]
		// 			this.def_hora_corte = arg_arr[12]
		// 		} else {
		// 			this.id =arguments[0].id
		// 			this["var"] = arguments[0]["var"]
		// 			this.nombre = arguments[0].nombre
		// 			this.abrev =arguments[0].abrev
		// 			this.type= arguments[0].type
		// 			this.datatype= arguments[0].datatype
		// 			this.valuetype=arguments[0].valuetype
		// 			this.GeneralCategory = arguments[0].GeneralCategory
		// 			this.VariableName =  arguments[0].VariableName
		// 			this.SampleMedium = arguments[0].SampleMedium
		// 			this.def_unit_id = arguments[0].def_unit_id
		// 			this.timeSupport = arguments[0].timeSupport
		// 			this.def_hora_corte = arguments[0].def_hora_corte
		// 		}
		// 		break;
		// 	default:
		// 		this["var"] = arguments[0]
		// 		this.nombre = arguments[1]
		// 		this.abrev =arguments[2]
		// 		this.type= arguments[3]
		// 		this.datatype= arguments[4]
		// 		this.valuetype=arguments[5]
		// 		this.GeneralCategory = arguments[6]
		// 		this.VariableName =  arguments[7]
		// 		this.SampleMedium = arguments[8]
		// 		this.def_unit_id = arguments[9]
		// 		this.timeSupport = arguments[10]
		// 		this.def_hora_corte = arguments[11]
		// 		break;
		// }
	}
	async getId() {
		var result = await global.pool.query("\
			SELECT id FROM var WHERE var=$1 AND \"GeneralCategory\"=$2\
		",[this["var"], this.GeneralCategory])
		if(result.rows.length) {
			if(this.id) {
				if(result.rows[0].id == this.id) {
					return
				} else {
					throw("var already exists with different id")
				}
			} else {
				this.id = result.rows[0].id
			}
		} else {
			if(this.id) {
				return
			} else {
				const new_id = await global.pool.query("\
				SELECT max(id)+1 AS id\
				FROM var\
				")
				this.id = new_id.rows[0].id
			}
		}
	}
	toString() {
		return "{id:" + this.id + ",var:" + this["var"]+ ", nombre:" + this.nombre + ",abrev:" + this.abrev + ",type:" + this.type + ",datatype: " + this.datatype + ",valuetype:" + this.valuetype + ",GeneralCategory:" + this.GeneralCategory + ",VariableName:" + this.VariableName + ",SampleMedium:" + this.SampleMedium + ",def_unit_id:" + this.def_unit_id + ",timeSupport:" + JSON.stringify(this.timeSupport) + "}"
	}
	toCSV() {
		return this.id + "," + this["var"]+ "," + this.nombre + "," + this.abrev + "," + this.type + "," + this.datatype + "," + this.valuetype + "," + this.GeneralCategory + "," + this.VariableName + "," + this.SampleMedium + "," + this.def_unit_id + "," + JSON.stringify(this.timeSupport)
	}
	static _fields = {
		id: {type: "integer", primary_key: true},
		var: {type: "string"},
		nombre: {type: "string"},
		abrev: {type: "string"},
		type: {type: "string"},
		datatype: {type: "string"},
		valuetype: {type: "string"},
		GeneralCategory: {type: "string"},
		VariableName: {type: "string"},
		SampleMedium: {type: "string"},
		def_unit_id: {type: "string"},
		timeSupport: {type: "interval"},
		def_hora_corte: {type: "interval"}
	}
	static _table_name = "var"
	toCSVless() {
		return this.id + "," + this["var"]+ "," + this.nombre
	}
	toJSON() {
		return {
			"id": this.id,
			"var": this["var"],
			"nombre": this.nombre,
			"abrev": this.abrev,
			"type": this.type,
			"datatype": this.datatype,
			"valuetype": this.valuetype,
			"GeneralCategory": this.GeneralCategory,
			"VariableName": this.VariableName,
			"SampleMedium": this.SampleMedium,
			"def_unit_id": this.def_unit_id,
			"timeSupport": this.timeSupport,
			"def_hora_corte": this.def_hora_corte
		}
	}
	// static settable_parameters = ["var","nombre","abrev","type","datatype","valuetype","GeneralCategory","VariableName","SampleMedium","def_unit_id","timeSupport","def_hora_corte"]
	// set(changes={}) {
	// 	for(var key of Object.keys(changes)) {
	// 		if(this.constructor.settable_parameters.indexOf(key) < 0) {
	// 			// console.error(`Can't update parameter ${key}`)
	// 			continue
	// 		} 
	// 		this[key] = changes[key]
	// 	}
	// }
	async create() {
		return internal.CRUD.upsertVar(this)
	}
	static async read(filter={},options) {
		if(filter.id && !Array.isArray(filter.id)) {
			return internal.CRUD.getVar(filter.id)
		}
		return internal.CRUD.getVars(filter)
	}
	async find() {
		if(this.id) {
			var result = await internal.var.read({id:this.id})
		} else if(this.VariableName) {
			var result = await internal.var.read({VariableName:this.VariableName,timeSupport:this.timeSupport ?? null,datatype:this.datatype ?? 'Continuous'})
			if(result != null && result.length) {
				result = result[0]
			} else {
				result = undefined
			}
		} else {
			console.error("id or VariableName required to find var")
			return
		}
		if(!result) {
			console.error("Var not found")
			return
		} else {
			return result
		}
	}
	async update(changes={}) {
		this.set(changes)
		return internal.CRUD.upsertVar(this)
	}
	static async delete(filter={},options={}) {
		var matches = await this.read(filter)
		if(!matches.length) {
			console.log("Nothing to delete")
			return []
		}
		const deleted = []
		for(var i in matches) {
			deleted.push(await matches[i].delete()) // internal.CRUD.deleteVar(matches[i].id))
		}
		return deleted.filter(r=>r)
	}
	async delete() {
		if(!this.id) {
			console.error("Can't delete this instance since it was not yet created")
			return
		}
		try {
			var result = await internal.CRUD.deleteVar(this.id)
		} catch(e) {
			console.error(e)
			return
		}
		return result

	}
}

internal.procedimiento = class extends baseModel  {
	constructor() {
        super() // nombre, abrev, descripcion
		switch(arguments.length) {
			case 1:
				if(typeof(arguments[0]) === "string") {
					var arg_arr = arguments[0].split(",")
					this.id = arg_arr[0]
					this.nombre = arg_arr[1]
					this.abrev = arg_arr[2]
					this.descripcion =arg_arr[3]
				} else {
					this.id = arguments[0].id
					this.nombre = arguments[0].nombre
					this.abrev = arguments[0].abrev
					this.descripcion =arguments[0].descripcion
				}
				break;
			default:
				this.nombre = arguments[0]
				this.abrev = arguments[1]
				this.descripcion =arguments[2]
				break;
		}
	}
	static _fields = {
		id: {type: "integer", primary_key:true},
		nombre: {type: "string"},
		abrev: {type: "string"},
		descripcion: {type: "string"}
	}
	static _table_name = "procedimiento"
	getId(pool) {
		return pool.query("\
			SELECT id FROM procedimiento WHERE nombre=$1 AND descripcion=$2\
		",[this.nombre, this.descripcion]
		).then(res=>{
			if (res.rows.length>0) {
				this.id = res.rows[0].id
				return
			} else {
				return pool.query("\
				SELECT max(id)+1 AS id\
				FROM procedimiento\
				")
				.then(res=>{
					this.id = res.rows[0].id
				})
			}
		})
	}
	toString() {
		return "{id:" + this.id + ",nombre:" + this.nombre + ", abrev:" + this.abrev + ",descripcion:"  + this.descripcion + "}"
	}
	toCSV() {
		return this.id + "," + this.nombre + "," + this.abrev + "," + this.descripcion
	}
	toCSVless() {
		return this.id + "," + this.nombre
	}
	static async read(filter={}) {
		if(filter.id) {
			return internal.CRUD.getProcedimiento(filter.id)
		}
		return internal.CRUD.getProcedimientos(filter)
	}
	toJSON() {
		return {
			id: this.id, // (this.id != null) ? parseInt(this.id) : null,
			nombre: this.nombre, // (this.nombre) ? this.nombre : null,
			abrev: this.abrev, // (this.abrev) ? this.abrev : null,
			descripcion: this.descripcion // (this.descripcion) ? this.descripcion : null
		}
	}
}

internal.unidades = class extends baseModel  {
	constructor() {
        super() // nombre, abrev, UnitsID, UnitsType
		switch(arguments.length) {
			case 1:
				if(typeof(arguments[0]) === "string") {
					var arg_arr = arguments[0].split(",")
					this.id = arg_arr[0]
					this.nombre = arg_arr[1]
					this.abrev = arg_arr[2]
					this.UnitsID = arg_arr[3]
					this.UnitsType = arg_arr[4]
				} else {
					this.id = arguments[0].id
					this.nombre = arguments[0].nombre
					this.abrev = arguments[0].abrev
					this.UnitsID = arguments[0].UnitsID
					this.UnitsType = arguments[0].UnitsType
				}
				break;
			default:
				//~ this.id = arguments[0]
				this.nombre = arguments[0]
				this.abrev = arguments[1]
				this.UnitsID = arguments[2]
				this.UnitsType = arguments[3]
				break;
		}
	}
	static _fields = {					
		id: {type: "integer", primary_key:true},
		nombre: {type: "string"},
		abrev: {type: "string"},
		UnitsID: {type: "integer"},
		UnitsType: {type: "string"}
	}
	static _table_name = "unidades"
	getId(pool) {
		return pool.query("\
			SELECT id FROM unidades WHERE nombre=$1 AND \"UnitsID\"=$2 AND \"UnitsType\"=$3\
		",[this.nombre, this.UnitsID, this.UnitsType]
		).then(res=>{
			if (res.rows.length>0) {
				this.id = res.rows[0].id
				return
			} else {
				return pool.query("\
				SELECT max(id)+1 AS id\
				FROM unidades\
				")
				.then(res=>{
					this.id = res.rows[0].id
				})
			}
		})
	}
	toString() {
		return "{id:" + this.id + ",nombre:" + this.nombre + ", abrev:" + this.abrev + ",UnitsID:" + this.UnitsID + ", UnitsType:" + this.UnitsType + "}"
	}
	toCSV() {
		return this.id + "," + this.nombre + "," + this.abrev + "," + this.UnitsID + "," + this.UnitsType
	}
	toCSVless() {
		return this.id + "," + this.nombre
	}
	toJSON() {
		return {
			id: this.id, // (this.id) ? parseInt(this.id) : null,
			nombre: this.nombre, // (this.nombre) ? this.nombre : null,
			abrev: this.abrev, // (this.abrev) ? this.abrev : null,
			UnitsID: this.UnitsID, // (this.UnitsID) ? this.UnitsID : null,
			UnitsType: this.UnitsType // (this.UnitsType) ? this.UnitsType : null
		}
	}
	static async read(filter={}) {
		if(filter.id && !Array.isArray(filter.id)) {
			return internal.CRUD.getUnidad(filter.id)
		}
		return internal.CRUD.getUnidades(filter)
	}

}

internal.fuente = class extends baseModel {
	constructor() {
        super()  // nombre, data_table, data_column, tipo, def_proc_id, def_dt, hora_corte, def_unit_id, def_var_id, fd_column, mad_table, scale_factor, data_offset, def_pixel_height, def_pixel_width, def_srid, def_extent, date_column, def_pixeltype, abstract, source
		// if(config.verbose) {
		// 	console.log({
		// 		new_fuente: arguments
		// 	})
		// }
		
		switch(arguments.length) {
			case 1:
				if(typeof(arguments[0]) === "string") {
					// console.log("new fuente argument length 1 type string")
					var arg_arr = arguments[0].split(",")
					this.id = arg_arr[0]
					this.nombre = arg_arr[1]
					this.data_table = arg_arr[2]
					this.data_column = arg_arr[3]
					this.tipo = arg_arr[4]
					this.def_proc_id= arg_arr[5]
					this.def_dt = timeSteps.createInterval(arg_arr[6])
					this.hora_corte = timeSteps.createInterval(arg_arr[7])
					this.def_unit_id = arg_arr[8]
					this.def_var_id = arg_arr[9]
					this.fd_column = arg_arr[10]
					this.mad_table = arg_arr[11]
					this.scale_factor = arg_arr[12]
					this.data_offset = arg_arr[13]
					this.def_pixel_height = arg_arr[14]
					this.def_pixel_width = arg_arr[15]
					this.def_srid = arg_arr[16]
					this.def_extent = (arg_arr[17]) ? new internal.geometry(arg_arr[17]) : undefined
					this.date_column = arg_arr[18]
					this.def_pixeltype = arg_arr[19]
					this.abstract = arg_arr[20]
					this.source = arg_arr[21]
					this.public = arg_arr[22]
				} else {
					// console.log("new fuente argument length 1 type not string")
					this.id = arguments[0].id
					this.nombre = arguments[0].nombre
					this.data_table = arguments[0].data_table
					this.data_column = arguments[0].data_column
					this.tipo = arguments[0].tipo
					this.def_proc_id= arguments[0].def_proc_id
					this.def_dt = timeSteps.createInterval(arguments[0].def_dt)
					this.hora_corte = timeSteps.createInterval(arguments[0].hora_corte)
					this.def_unit_id = arguments[0].def_unit_id
					this.def_var_id = arguments[0].def_var_id
					this.fd_column = arguments[0].fd_column
					this.mad_table = arguments[0].mad_table
					this.scale_factor = arguments[0].scale_factor
					this.data_offset = arguments[0].data_offset
					this.def_pixel_height = arguments[0].def_pixel_height
					this.def_pixel_width = arguments[0].def_pixel_width
					this.def_srid = arguments[0].def_srid
					this.def_extent = (arguments[0].def_extent) ? new internal.geometry(arguments[0].def_extent) : undefined
					this.date_column = arguments[0].date_column
					this.def_pixeltype = arguments[0].def_pixeltype
					this.abstract = arguments[0].abstract
					this.source = arguments[0].source
					this.public = arguments[0].public
					this.constraints = arguments[0].constraints
				}
				break;
			default:
				console.log("new fuente argument length > 1")
				this.nombre = arguments[0]
				this.data_table = arguments[1]
				this.data_column = arguments[2]
				this.tipo = arguments[3]
				this.def_proc_id= arguments[4]
				this.def_dt = timeSteps.createInterval(arguments[5])
				this.hora_corte = timeSteps.createInterval(arguments[6])
				this.def_unit_id = arguments[7]
				this.def_var_id = arguments[8]
				this.fd_column = arguments[9]
				this.mad_table = arguments[10]
				this.scale_factor = arguments[11]
				this.data_offset = arguments[12]
				this.def_pixel_height = arguments[13]
				this.def_pixel_width = arguments[14]
				this.def_srid = arguments[15]
				this.def_extent = (arguments[16]) ? new internal.geometry(arguments[16]) : undefined
				this.date_column = arguments[17]
				this.def_pixeltype = arguments[18]
				this.abstract = arguments[19]
				this.source = arguments[20]
				this.public = arguments[21]
				break;
		}
	}
	async getId() {
		const existing_fuente = await global.pool.query("\
			SELECT id FROM fuentes WHERE nombre=$1 and tipo=$2\
		",[this.nombre, this.tipo])
		if (existing_fuente.rows.length) {
			this.id = existing_fuente.rows[0].id
		} else {
			if(this.id) {
				const is_id_taken = await global.pool.query("\
					SELECT 1 FROM fuentes WHERE id=$1",[this.id]) 
				if(is_id_taken.rows.length) {
					throw("fuente id already taken")
				}
			} else {
				const new_id = await global.pool.query("\
					SELECT max(id)+1 AS id\
					FROM fuentes\
				")
				this.id = new_id.rows[0].id
			}
		}
		return
	}
	getConstraint(column_names) {
		const match = this.constraints.filter(c=>c.check(column_names))
		if(!match.length) {
			console.log("constraint not found")
			return
		}
		return match[0]
	}
	hasConstraint(column_names) {
		if(this.getConstraint(column_names)) {
			return true
		} else {
			return false
		}
	}
	hasDateConstraint() {
		return this.hasConstraint([this.date_column])
	}
	hasDateFdConstraint() {
		return this.hasConstraint([this.date_column,this.fd_column])
	}
	toString() {
		return "{id:" + this.id + ",nombre:" + this.nombre + ", data_table:" + this.data_table + ", data_column:" + this.data_column + ", tipo:" + this.tipo + ", def_proc_id:" + this.def_proc_id + ", def_dt:" + JSON.stringify(this.def_dt) + ", hora_corte:" + JSON.stringify(this.hora_corte) + ", def_unit_id:" + this.def_unit_id + ", def_var_id:"+ this.def_var_id  + ", fd_column:"+  this.fd_column + ", mad_table:" + this.mad_table + ", scale_factor:" + this.scale_factor + ", data_offset:" + this.data_offset + ", def_pixel_height:" + this.def_pixel_height + ", def_pixel_width:" + this.def_pixel_width + ", def_srid:" + this.def_srid + ", def_extent:" + this.def_extent + ", date_column:" + this.date_column + ", def_pixeltype:" + this.def_pixeltype + ", abstract:" + this.abstract + ", source:" + this.source + "}"
	}
	toCSV() {
		return this.id + "," + this.nombre + "," + this.data_table + "," + this.data_column + "," + this.tipo + "," + this.def_proc_id + "," + JSON.stringify(this.def_dt) + "," + JSON.stringify(this.hora_corte) + "," + this.def_unit_id + ","+ this.def_var_id  + "," + this.fd_column + "," + this.mad_table + "," + this.scale_factor + "," + this.data_offset + "," + this.def_pixel_height + "," + this.def_pixel_width + "," + this.def_srid + "," + this.def_extent + "," + this.date_column + "," + this.def_pixeltype + "," + this.abstract + "," + this.source
	}
	toCSVless() {
		return this.id + "," + this.nombre + "," + this.source
	}
	toJSON() {
		return {
			id: this.id, // (this.id) ? parseInt(this.id) : null,
			nombre: this.nombre, // (this.nombre) ? this.nombre : null,
			data_table: this.data_table, // (this.data_table) ? this.data_table : null,
			data_column: this.data_column, // (this.data_column) ? this.data_column : null,
			tipo: this.tipo, // (this.tipo) ? this.tipo : null,
			def_proc_id: this.def_proc_id, // (this.def_proc_id) ? parseInt(this.def_proc_id) : null,
			def_dt: this.def_dt, // (this.def_dt) ? this.def_dt : null,
			hora_corte: this.hora_corte, // (this.hora_corte) ? this.hora_corte : null,
			def_unit_id: this.def_unit_id, // (this.def_unit_id) ? parseInt(this.def_unit_id) : null,
			def_var_id: this.def_var_id, //(this.def_var_id) ? parseInt(this.def_var_id) : null,
			fd_column: this.fd_column, // (this.fd_column) ? this.fd_column : null,
			mad_table: this.mad_table, // (this.mad_table) ? this.mad_table : null,
			scale_factor: this.scale_factor, // (this.scale_factor) ? parseFloat(this.scale_factor) : null,
			data_offset: this.data_offset, // (this.data_offset) ? parseFloat(this.data_offset) : null,
			def_pixel_height: this.def_pixel_height, // (this.def_pixel_height) ? parseFloat(this.def_pixel_height) : null,
			def_pixel_width: this.def_pixel_width, // (this.def_pixel_width) ? parseFloat(this.def_pixel_width) : null,
			def_srid: this.def_srid, // (this.def_srid) ? parseInt(this.def_srid) : null,
			def_extent: this.def_extent, // (this.def_extent) ? this.def_extent : null,
			date_column: this.date_column, // (this.date_column) ? this.date_column : null,
			def_pixeltype: this.def_pixeltype, // (this.def_pixeltype) ? this.def_pixeltype : null,
			abstract: this.abstract, // (this.abstract) ? this.abstract : null,
			source: this.source, // (this.source) ? this.source : null,
			public: this.public, // (this.public != null) ? Boolean(this.public) : null,
			constraints: this.constraints // (this.constraints) ? this.constraints : null
		}
	}
	static async read(filter={},options) {
		if(filter.id) {
			return internal.CRUD.getFuente(filter.id)
		}
		return internal.CRUD.getFuentes(filter,options)
	}
	async create(options={}) {
		// console.log({options:options})
		const created = await internal.CRUD.upsertFuente(this)
		if(created) {
			Object.assign(this,created)
			if(options.create_cube_table && this.data_table) {
				// CHECK IF DATA TABLE EXISTS
				console.log("Check if data table exists")
				const cube_table_exists = await this.checkTableExists()
				if(!cube_table_exists) {
					// CREATE DATA TABLE
					console.log("create data table")
					await this.createCubeTable()
				}
			}
			return this
		} else {
			return
		}
	}

	async delete(options={}) {
		const deleted = await internal.CRUD.deleteFuente(this.id)
		if(options.drop_cube_table) {
			await this.dropCubeTable()
		}
		return deleted
	}

	static async delete(filter={},options={}) {
		var matches = await this.read(filter)
		if(!matches) {
			console.log("Nothing to delete")
			return []
		}
		if(!Array.isArray(matches)) {
			matches = [matches]
		}
		const deleted = []
		for(var fuente of matches) {
			const deleted_ = await fuente.delete(options)
			if(deleted_) {
				deleted.push(deleted_)
			}
		}
		return deleted
	} 
	async checkTableExists(table_schema='public') {
		const stmt = `SELECT EXISTS (
			SELECT 1 
			FROM information_schema.tables 
			WHERE table_schema = $1 AND table_name = $2)`
		try {
			var result = await global.pool.query(stmt,[table_schema,this.data_table])
		} catch(e) {
			throw(e)
		}
		return result.rows[0].exists
	}

	async createCubeTable(schema_name="public") {
		if(!this.data_table) {
			throw("Can't create cube table: data_table is undefined")
		}
		var data_table = escapeIdentifier(this.data_table)
		var date_column = escapeIdentifier(this.date_column ?? "date")
		var data_column = escapeIdentifier(this.data_column ?? "rast")
		schema_name = escapeIdentifier(schema_name)
		if(this.fd_column) {
			var fd_column = escapeIdentifier(this.fd_column)
			const query = `CREATE TABLE ${schema_name}.${data_table} (
				id serial PRIMARY KEY,
				${date_column} timestamptz NOT NULL,
				${fd_column} timestamptz NOT NULL,
				${data_column} raster NOT NULL,
				CONSTRAINT date_forecast_date_unique_key UNIQUE (${date_column},${fd_column})
			)`
			await global.pool.query(query)
		} else {
			const query = `CREATE TABLE ${schema_name}.${data_table} (
				id serial PRIMARY KEY,
				${date_column} timestamptz UNIQUE NOT NULL,
				${data_column} raster NOT NULL
			)`
			await global.pool.query(query)
		}
		return
	}

	async dropCubeTable(schema_name="public") {
		if(!this.data_table) {
			throw("Unable to drop cube table: data_table undefined")
		}
		var data_table = escapeIdentifier(this.data_table)
		schema_name = escapeIdentifier(schema_name)
		const stmt = `DROP TABLE IF EXISTS ${schema_name}.${data_table}`
		await global.pool.query(stmt)
		return
	}

}

internal.fuente.build_read_query = function(filter) {
	if(filter && filter.tipo && filter.tipo.toLowerCase() == "puntual") {
		return internal.red.build_read_query(filter)
	}
	if(filter && filter.geom) {
		filter.def_extent = filter.geom
	}
	const valid_filters = {id: "numeric", nombre: "regex_string", data_table: "string", data_column: "string", tipo: "string", def_proc_id: "numeric", def_dt: "string", hora_corte: "string", def_unit_id: "numeric", def_var_id: "numeric", fd_column: "string", mad_table: "string", scale_factor: "numeric", data_offset: "numeric", def_pixel_height: "numeric", def_pixel_width: "numeric", def_srid: "numeric", def_extent: "geometry", date_column: "string", def_pixeltype: "string", abstract: "regex_string", source: "regex_string",public:"boolean_only_true"}
	var filter_string = internal.utils.control_filter(valid_filters,filter)
	if(!filter_string) {
		return Promise.reject(new Error("invalid filter value"))
	}
	console.log("filter_string:" + filter_string)
	return "SELECT id, nombre, data_table, data_column, tipo, def_proc_id, def_dt, hora_corte, def_unit_id, def_var_id, fd_column, mad_table, scale_factor, data_offset, def_pixel_height, def_pixel_width, def_srid, st_asgeojson(def_extent)::json def_extent, date_column, def_pixeltype, abstract, source, public\
	 FROM fuentes \
	 WHERE 1=1 " + filter_string
}



internal.serie = class extends baseModel {
	constructor() {
		// console.log("New serie:")
		// console.log(JSON.stringify(arguments[0]))
		super(...arguments)
		switch(arguments.length) {
			case 1:
				if(typeof(arguments[0]) === "string") {
					var args_arr = arguments[0].split(",") // [this.id,this.estacion_id, this.var_id, this.proc_id, this.unit_id, this.tipo, this.fuentes_id] 
					this.id = args_arr[0]
					this.tipo = args_arr[5]
					this["var"]  = internal.CRUD.getVar(args_arr[2])
					this.procedimiento  = internal.CRUD.getProcedimiento(args_arr[3])
					this.unidades = internal.CRUD.getUnidad(args_arr[4])
					if(this.tipo == "areal") {
						this.fuente = internal.CRUD.getFuente(args_arr[6])
						this.estacion = internal.CRUD.getArea(args_arr[1])
					} else if(this.tipo == "rast" || this.tipo == "raster") {
						this.fuente = internal.CRUD.getFuente(args_arr[6])
						this.estacion = internal.CRUD.getEscena(args_arr[1])
					} else {
						this.estacion = internal.CRUD.getEstacion(args_arr[1])
					}
				} else {
					this.id = arguments[0].id
					this.tipo = arguments[0].tipo;
					if (arguments[0].estacion) {
						if(this.tipo == "areal") {
							this.estacion = new internal.area(arguments[0].estacion)
						} else if (this.tipo == "rast" || this.tipo == "raster") {
							this.estacion = new internal.escena(arguments[0].estacion)
						} else {
							this.estacion = new internal.estacion(arguments[0].estacion)
						}
					} else if (arguments[0].estacion_id) {
						this.estacion = {id:arguments[0].estacion_id}
					} else if (this.tipo == "areal") {
						if(arguments[0].area) {
							this.estacion = new internal.area(arguments[0].area)
						} else if(arguments[0].area_id) {
							this.estacion = {id:arguments[0].area_id}
						} else {
							this.estacion = {}
						}
					} else if (arguments[0].area_id) {
						this.tipo = "areal"
						this.estacion = {id: arguments[0].area_id}
					} else if (this.tipo == "rast" || this.tipo == "raster") {
						if(arguments[0].escena) {
							this.estacion = new internal.escena(arguments[0].escena)
						} else if(arguments[0].escena_id) {
							this.estacion = {id:arguments[0].escena_id}
						} else {
							this.estacion = {}
						}
					} else {
						this.estacion = {}
					}
					if (arguments[0]["var"]) {
						this["var"] =  new internal["var"](arguments[0]["var"])
					} else if (arguments[0].var_id) {
						this["var"] = {id:arguments[0].var_id}
					} else {
						this["var"] = {} 
					}
					if (arguments[0].procedimiento) {
						this.procedimiento = new internal.procedimiento(arguments[0].procedimiento)
					} else if (arguments[0].proc_id) {
						this.procedimiento = {id:arguments[0].proc_id}
					} else {
						this.procedimiento = {}
					}
					if (arguments[0].unidades) {
						this.unidades = new internal.unidades(arguments[0].unidades)
					} else if (arguments[0].unit_id) {
						this.unidades = {id:arguments[0].unit_id}
					} else {
						this.unidades = {}
					}
					if(this.tipo == "areal" || this.tipo == "rast" || this.tipo == "raster") {
						if (arguments[0].fuente) {
							this.fuente = new internal.fuente(arguments[0].fuente)
						} else if (arguments[0].fuentes_id) {
							this.fuente = {id:arguments[0].fuentes_id}
						} else {
							this.fuente = {}
						}
					} else {
						this.fuente = {}
					}
					if( (arguments[0].observaciones)) {
						this.observaciones = new internal.observaciones(arguments[0].observaciones)
					} else {
						this.observaciones = null
					}
					this.date_range = arguments[0].date_range
					this.monthlyStats = arguments[0].monthlyStats
					for(var key of ["beginTime","endTime","count","minValor","maxValor"]) {
						if(arguments[0].hasOwnProperty(key)) {
							this[key] = arguments[0][key]
						}
					}
					this.pronosticos = arguments[0].pronosticos
				}
				break;
			default:
				[this.estacion, this["var"], this.procedimiento, this.unidades, this.tipo, this.fuente] = arguments
				break;
		}
		// console.log({serie:this})
	}
	toJSON() {
		return {
			tipo: (this.tipo) ? this.tipo : "puntual",
			id: (this.id) ? parseInt(this.id) : null, 
			estacion: this.estacion,
			var: this["var"],
			procedimiento: this.procedimiento,
			unidades: this.unidades,
			fuente: this.fuente,
			date_range: this.date_range,
			monthlyStats: this.monthlyStats,
			beginTime: this.beginTime,
			endTime : this.endTime,
			count: this.count,
			minValor: this.minValor,
			maxValor: this.maxValor,
			observaciones: this.observaciones,
			pronosticos: this.pronosticos,
			percentiles: this.percentiles
		}
	}
	toString() {
		if (this.tipo == "areal") {
			return "{id:" + this.id + ", area:" + this.estacion.toString() + ", var:" + this["var"].toString() + ", procedimiento:" + this.procedimiento.toString() + ", unidades:" + this.unidades.toString() + ", tipo:" + this.tipo + ", fuente:" + this.fuente.toString() + "}" 
		} else {
			return "{id:" + this.id + ", estacion:" + this.estacion.toString() + ", var:" + this["var"].toString() + ", procedimiento:" + this.procedimiento.toString() + ", unidades:" + this.unidades.toString() + ", tipo:" + this.tipo
		}
	}

	/**
	 * Return key-value pair representation of this object
	 * @param {*} options 
	 * @param {string} [options.delimiter="="] - key-value delimiter
	 * @param {Boolean} [options.no_comment=false] - if false, inserts '# ' at the beginning of each line
	 * @param {Boolean} [options.single_line=false] - if false prints one key value pair per line. Else, it uses a whitespace as separator
	 * @returns {string} 
	 */
	toKVP(options={}) {
		var sep= (options.delimiter) ? options.delimiter : "=" 
		var line_start = (options.no_comment) ? "" : "# "
		var kvp_sep = (options.single_line) ? " " : "\n"
		var kvp_string = `${line_start}id${sep}${this.id}${kvp_sep}${line_start}estacion_id${sep}${this.estacion.id}${kvp_sep}${line_start}name${sep}${this.estacion.nombre}${kvp_sep}${line_start}longitude${sep}${this.estacion.geom.coordinates[0]}${kvp_sep}${line_start}latitude${sep}${this.estacion.geom.coordinates[1]}${kvp_sep}${line_start}var_id${sep}${this["var"].id}${kvp_sep}${line_start}proc_id${sep}${this.procedimiento.id}${kvp_sep}${line_start}unit_id${sep}${this.unidades.id}${kvp_sep}${line_start}tipo${sep}${this.tipo}`
			for(var key in ["beginTime","endTime"]) {
				if(this[key]) {
					kvp_string = `${kvp_string}${kvp_sep}${line_start}${key}${sep}${this[key].toISOString()}`
				}
			}
			for(var key in ["count","minValor","maxValor"]) {
				if(this[key]) {
					kvp_string = `${kvp_string}${kvp_sep}${line_start}${key}${sep}${this[key].toString()}`
				}
			}
			if (this.tipo == "areal" || this.tipo == "raster") {
				kvp_string = `${kvp_string}${kvp_sep}${line_start}fuentes_id${sep}${this.fuente.id}`
			} else {
				kvp_string = `${kvp_string}${kvp_sep}${line_start}tabla_id${sep}${this.estacion.tabla}${kvp_sep}${line_start}id_externo${sep}${this.estacion.id_externo}`
			}
			return kvp_string
	}

	toGmd() {
		return serieToGmd(this)
	}

	/**
	 * Returns csv header for this object
	 * @param {*} options 
	 * @param {string} [options.delimiter=,]
	 * @param {Boolean} [options.print_observaciones=false] 
	 * @returns {string}
	 */
	getCSVHeader(options={}) {
		var sep = (options.delimiter) ? options.delimiter : ","
		if(options.print_observaciones) {
			return ""
		}
		return ["id","estacion.id","estacion.nombre","estacion.geom.coordinates[0]","estacion.geom.coordinates[1]","var.id","procedimiento.id","unidades.id","tipo","beginTime","endTime","count","minValor","maxValor","fuente.id","estacion.tabla","estacion.id_externo"].join(sep)
	}
	/**
	 * Returns csv string for this object
	 * @param {*} options
	 * @param {string} [options.delimiter=,]
	 * @param {Boolean} [options.print_observaciones=false] 
	 * @returns {string}
	 */
	toCSV(options={}) {
		var sep = (options.delimiter) ? options.delimiter : ","
		if(!options.print_observaciones) {
			const row = [this.id,this.estacion.id,this.estacion.nombre,this.estacion.geom.coordinates[0],this.estacion.geom.coordinates[1],this.var.id,this.procedimiento.id,this.unidades.id,this.tipo,this.beginTime,this.endTime,this.count,this.minValor,this.maxValor,this.fuente.id,this.estacion.tabla,this.estacion.id_externo].map(c=>(c!= null) ? (c instanceof Date) ? c.toISOString() : c.toString() : "").join(sep)
			return row
		}
		var csv_string = this.toKVP() + "\n"		
		if (this.observaciones) {
			if(this.monthlyStats) {
				var obs = this.observaciones.toCSV({delimiter:sep,hasMonthlyStats:true})
			} else {
				var obs = this.observaciones.toCSV({delimiter:sep,hasMonthlyStats:false})
			}
			return csv_string + `${obs}`
		} else {
			return csv_string
		}
	}
	toCSVcat(options={}) {
		var sep = (options.delimiter) ? options.delimiter : ","
		if (this.observaciones) {
			return this.observaciones.toCSVcat({delimiter:sep}) // ,hasMonthlyStats:true})
		} else {
			return ""
		}
	}
	toCSVless() {
		if (this.tipo == "areal") {
			return this.id + "," + this.estacion.id +"," + this["var"].id + "," + this.procedimiento.id + "," + this.unidades.id + "," + this.tipo + "," + this.fuente.id
		} else {
			return this.id + "," + this.estacion.id +"," + this["var"].id + "," + this.procedimiento.id + "," + this.unidades.id + "," + this.tipo
		}
	}
	toMnemos() {
		var var_matches = Object.keys(config.snih.variable_map).filter(key=>{
			return (config.snih.variable_map[key].var_id == this.var.id)
		})
		var codigo_de_variable
		if(var_matches.length <= 0) {
			console.error("Variable id " + this.var.id + " no encontrado en config.snih.variable_map")
			codigo_de_variable = null
		} else {
			codigo_de_variable = var_matches[0]
		}
		var observaciones = this.observaciones.map(o=>{
			return {
				codigo_de_estacion: this.estacion.id,
				codigo_de_variable: codigo_de_variable,
				dia: sprintf("%02d",o.timestart.getDate()), 
				mes: sprintf("%02d", o.timestart.getMonth()+1),
				anio: sprintf("%04d", o.timestart.getFullYear()),
				hora: sprintf("%02d", o.timestart.getHours()),
				minuto: sprintf("%02d", o.timestart.getMinutes()),
				valor: o.valor
			}
		})
		return this.arr2csv(observaciones)
	}
	arr2csv(arr) {
		if(! Array.isArray(arr)) {
			throw "arr2csv: Array incorrecto" 
		}
		var lines = arr.map(line=> {
			// console.log(line)
			if(Array.isArray(line.valor)) {
				return [line.codigo_de_estacion, line.codigo_de_variable, line.dia, line.mes, line.anio, line.hora, line.minuto, ...line.valor].join(",")
			} else {
				return [line.codigo_de_estacion, line.codigo_de_variable, line.dia, line.mes, line.anio, line.hora, line.minuto, line.valor].join(",")
			}
		})
		return lines.join("\n")
	}
	
	/**
	Retrieve id of series instance from database and set. If not found and default_to_next=true, sets next value in id sequence
	*
	* @param default_to_next boolean default true
	* @returns Promise<integer|undefined> - series id
	*/
	async getId(default_to_next=true) {
		if(this.tipo == "areal") {
			//~ console.log([this.estacion.id, this["var"].id, this.procedimiento.id, this.unidades.id, this.fuente.id])
			var res = await global.pool.query("\
				SELECT id FROM series_areal WHERE area_id=$1 AND var_id=$2 AND proc_id=$3 AND unit_id=$4 AND fuentes_id=$5\
			",[this.estacion.id, this["var"].id, this.procedimiento.id, this.unidades.id, this.fuente.id]
			)
			if (res.rows.length>0) {
				this.id = res.rows[0].id
				return this.id
			} else if (default_to_next) {
				res = await global.pool.query("\
				SELECT max(id)+1 AS id\
				FROM series_areal\
				")
				this.id = res.rows[0].id
				return this.id
			} else {
				return
			}
		} else {
			if(!this.estacion.id || !this["var"].id || !this.procedimiento.id || !this.unidades.id) {
				console.error("Can't retrieve series.id: missing one or more of estacion.id, var.id, procedimiento.id or unidades.id")
				return
			}
			var res = await global.pool.query("\
				SELECT id FROM series WHERE estacion_id=$1 AND var_id=$2 AND proc_id=$3 AND unit_id=$4\
			",[this.estacion.id, this["var"].id, this.procedimiento.id, this.unidades.id]
			)
			if (res.rows.length>0) {
				this.id = res.rows[0].id
				return
			} else if (default_to_next) {
				res = await global.pool.query("\
				SELECT max(id)+1 AS id\
				FROM series\
				")
				this.id = res.rows[0].id
			} else {
				return
			}
		}
	}
	
	async getStats() {
		if(!this.id) {
			console.error("Se necesita el id de serie para obtener las estadísticas")
			return this
		}
		if(this.tipo == "areal") {
			const rows = await executeQueryReturnRows("\
				SELECT min(timestart) begintime,max(timestart) endtime, count(timestart) count, min(valor) minValor, max(valor) maxValor FROM observaciones_areal,valores_num_areal WHERE series_id=$1  AND observaciones_areal.id=valores_num_areal.obs_id\
			",[this.id])
			if (rows.length>0) {
				this.beginTime = rows[0].begintime
				this.endTime = rows[0].endtime
				this.count = rows[0].count
				this.minValor = rows[0].minvalor
				this.maxValor = rows[0].maxvalor
			} else {
				this.count = 0
			}
		} else if (this.tipo == "rast" || this.tipo == "raster") {
			const rows = executeQueryReturnRows("\
				WITH stats as (\
					SELECT min(timestart) begintime,\
					max(timestart) endtime, \
					count(timestart) count, \
					ST_SummaryStatsAgg(valor,1,true) stats \
				FROM observaciones_rast WHERE series_id=$1\
				)\
				SELECT begintime, \
					   endtime, \
					   count, \
					   round((stats).sum::numeric, 3) sum,\
						round((stats).mean::numeric, 3) mean,\
						round((stats).stddev::numeric, 3) stddev,\
						round((stats).min::numeric, 3) min,\
						round((stats).max::numeric, 3) max\
				FROM stats\
				",[this.id])
			if (rows.length>0) {
				this.beginTime = rows[0].begintime
				this.endTime = rows[0].endtime
				this.count = rows[0].count
				this.rastStats = {sum: rows[0].sum, mean: rows[0].mean, stddev: rows[0].stddev, min: rows[0].min, max: rows[0].max} 
			} else {
				this.count = 0
			}
		} else {
			const rows = await executeQueryReturnRows("\
				SELECT min(timestart) begintime,max(timestart) endtime, count(timestart) count, min(valor) minValor, max(valor) maxValor FROM observaciones,valores_num WHERE series_id=$1 AND observaciones.id=valores_num.obs_id\
			",[this.id])
			if (rows.length>0) {
				this.beginTime = rows[0].begintime
				this.endTime = rows[0].endtime
				this.count = rows[0].count
				this.minValor = rows[0].minvalor
				this.maxValor = rows[0].maxvalor
			} else {
				this.count = 0
			}
		}
		return this
	}
	
		
	getDateRange(pool) {
		if(!this.id) {
			return Promise.reject("falta serie.id")
		} 
		var table = (this.tipo) ? (this.tipo == "puntual") ? "series_date_range" : (this.tipo == "areal") ? "series_areal_date_range" : (this.tipo == "rast" || this.tipo == "raster") ? "series_rast_date_range" : "series_date_range"  : "series_date_range"
		return pool.query("SELECT * FROM " + table + " WHERE series_id=$1",[this.id]) 
		.then(result=>{
			if(result.rows.length == 0) {
				console.error("no se encontró rango de fechas de la serie " + this.tipo + " id:"+ this.id)
				return {
					timestart: undefined,
					timeend: undefined,
					count: 0
				}
			}
			this.date_range = {
				timestart: result.rows[0].timestart,
				timeend: result.rows[0].timeend,
				count: result.rows[0].count
			}
			return true
		})
	}

	tipo_guess() {
		if(!this.observaciones || this.observaciones.length == 0) {
			return
		}
		if(!this.tipo && this.observaciones[0].tipo) {
			var tipo_guess = this.observaciones[0].tipo
			var count = 0
			for(var i in this.observaciones) {
				if (this.observaciones[i].tipo != tipo_guess) {
					break
				}
				count++
			}
			if(count == this.observaciones.length) {
				this.tipo = tipo_guess
			}
		}
	}

	idIntoObs() {
		if(this.id && this.observaciones) {
			for(var i in this.observaciones) {
				this.observaciones[i].series_id = this.id
			}
		}
	}

	getWeibullPercentiles(reference_timestart=new Date("1991-01-01"),reference_timeend=new Date("2021-01-01"),percentage_complete_threshold=60,as_array=true) {
		// groups observaciones by month within the reference period and computes mean
		var obs_grouped_by_month = {}
		for(var m=0;m<=11;m++) {
			var observaciones = this.observaciones.filter(o=>o.valor != null && o.timestart.getUTCMonth() == m && o.timestart >= reference_timestart && o.timestart <= reference_timeend).sort((a,b)=> a.valor - b.valor)
			if(!observaciones.length) {
				console.error(`No observaciones found for series_id=${this.id} month=${m}`)
				// obs_grouped_by_month[m] = {	observaciones: []}
				continue				
			}
			var count = observaciones.length
			var mean = observaciones.map(o=>o.valor).reduce((sum=0,v)=>sum+v) / count
			for(var i in observaciones) {
				observaciones[i].rank = parseFloat(i) + 1
				observaciones[i].percentage_of_average = observaciones[i].valor / mean * 100
				observaciones[i].weibull_percentile = observaciones[i].rank / (count + 1)
			}
			var begin_date = new Date(Math.min.apply(null,observaciones.map(o=>o.timestart.getTime())))
			var end_date = new Date(Math.max.apply(null,observaciones.map(o=>o.timeend.getTime())))
			var period_length = reference_timeend.getUTCFullYear() - reference_timestart.getUTCFullYear()
			var percentage_complete = count / period_length * 100
			if(percentage_complete < percentage_complete_threshold) {
				console.error(`Percentage complete for series_id=${this.id} month=${m} (${percentage_complete}) below threshold (${percentage_complete_threshold})`)
				continue
			}
			obs_grouped_by_month[m] = {
				tipo: this.tipo,
				series_id: this.id,
				valores: observaciones.map(o=>o.valor),
				count: count,
				mean: mean,
				mon: m,
				min: observaciones.map(o=>o.valor)[0],
				max: observaciones.map(o=>o.valor)[observaciones.length-1],
				timestart: begin_date,
				timeend: end_date,
				percentage_complete: percentage_complete
			}
			for(var p of [1,10,13,28,50,72,87,90,99]) {
				var p_key = sprintf("p%02d", p)
				obs_grouped_by_month[m][p_key] = getPercentile(obs_grouped_by_month[m].valores,p/100)
			}
		}
		// Computes value as percentage of mean and assigns rank to each element in this.observaciones
		for(var i in this.observaciones) {
			const obs = this.observaciones[i]
			if(obs.valor == null) {
				continue
			}
			var m = obs.timestart.getUTCMonth()
			if(!obs_grouped_by_month.hasOwnProperty(m)) {
				console.error(`Missing historical data for series_id=${this.id} month=${m}`)
				continue
			}
			const stats = { percentage_of_average: obs.valor / obs_grouped_by_month[m].mean * 100 }
			stats.rank = 1
			stats.count = obs_grouped_by_month[m].count
			stats.month = m
			stats.historical_monthly_mean = obs_grouped_by_month[m].mean
			for(var j in obs_grouped_by_month[m].valores) {
				if(obs.valor < obs_grouped_by_month[m].valores[j]) {
					break
				}
				stats.rank = stats.rank + 1
			}
			stats.weibull_percentile = stats.rank / (obs_grouped_by_month[m].count + 1)	
			stats.percentile_category = assignPercentileCategory(stats.weibull_percentile)
			obs.setStats(stats)
		}
		if(as_array) {
			obs_grouped_by_month = Object.keys(obs_grouped_by_month).map(key=>{
				return obs_grouped_by_month[key] 
			})	
		}
		this.monthlyStats = obs_grouped_by_month
		
		return obs_grouped_by_month
	}
	// Object.keys(wp).map(k=> { return {month: k , mean:wp[k].mean}})

	set(changes={}) {
		const settable_parameters = ["var","procedimiento","unidades","estacion","fuente"]
		for(var key of Object.keys(changes)) {
			if(settable_parameters.indexOf(key) < 0) {
				// console.error(`Can't update parameter ${key}`)
				continue
			} 
			this[key] = changes[key]
		}
	}

	getDateRangeTable(options={}) {
		return (this.tipo == "areal") ? (options.guardadas) ? "series_areal_guardadas_date_range" : "series_areal_date_range" : (this.tipo == "rast" || this.tipo == "raster") ? (options.guardadas) ? "series_rast_guardadas_date_range" : "series_rast_date_range" : (options.guardadas) ? "series_guardadas_date_range" : "series_date_range"
	}

	static getDateRangeTable(tipo="puntual",options={}) {
		return (tipo == "areal") ? (options.guardadas) ? "series_areal_guardadas_date_range" : "series_areal_date_range" : (tipo == "rast" || tipo == "raster") ? (options.guardadas) ? "series_rast_guardadas_date_range" : "series_rast_date_range" : (options.guardadas) ? "series_guardadas_date_range" : "series_date_range"
	}

	static async refreshDateRange(tipo="puntual",options={}) {
		const date_range_table = this.getDateRangeTable(tipo,options)
		return global.pool.query(`REFRESH MATERIALIZED VIEW CONCURRENTLY ${date_range_table}`)
	}

	async refreshDateRange(options={}) {
		const date_range_table = this.getDateRangeTable(options)
		return global.pool.query(`REFRESH MATERIALIZED VIEW CONCURRENTLY ${date_range_table}`)
	}

	getSeriesTable() {
		return (this.tipo.toUpperCase() == "AREAL") ? "series_areal" : (this.tipo.toUpperCase() == "RAST" || this.tipo.toUpperCase() == "RASTER") ? "series_rast" : "series"
	}

	static 	getSeriesTable(tipo) {
		return (tipo.toUpperCase() == "AREAL") ? "series_areal" : (tipo.toUpperCase() == "RAST" || tipo.toUpperCase() == "RASTER") ? "series_rast" : "series"
	}

	static getFeatureIdColumn(tipo) {
		return (tipo.toUpperCase() == "AREAL") ? "area_id" : (tipo.toUpperCase() == "RAST" || tipo.toUpperCase() == "RASTER") ? "escena_id" : "estacion_id"
	}

	async create(options) {
		const result = await internal.CRUD.upsertSerie(this,options)
		if(options && options.refresh_date_range) {
			await this.refreshDateRange(options) // global.pool.query(`REFRESH MATERIALIZED VIEW ${this.getDateRangeTable(options)}`)
		}
		internal.serie.refreshJsonView()
		return result
	}
	static async refreshJsonView() {
		return internal.CRUD.refreshSeriesJson()
	}
	static async create(series,options={}) {
		const results = await internal.CRUD.upsertSeries(series,options.all,options.upsert_estacion,options.generate_id)
		if(options && options.refresh_date_range) {
			if(results.length) {
				const types = [new Set(results.map(r=>r.tipo))]
				for(var tipo of types) {
					await this.refreshDateRange(tipo,options) // global.pool.query(`REFRESH MATERIALIZED VIEW ${this.getDateRangeTable(results[0].tipo,options)}`)
				}
			}
		}
		this.refreshJsonView()
		return results
	}

	/**
	 * Reads series from database
	 * @param {Object} filter 
	 * valid parameters:
	 * - tipo : str        - either "puntual", "areal" or "raster"
	 * - id : int          - series identifier
	 * - timestart : date  - begin date of observations
	 * - timeend : date    - end date of observations
	 * - public : boolean  - if true, retrieves only public series
	 * - timeupdate        - update date of observations
	 * @param {Object} options 
	 * valid parameters:
	 * - no_metadata bool
	 * - obs_type string
	 * - asArray bool
	 * - getStats bool
	 * - getMonthlyStats bool
	 * - getWeibullPercentiles bool
	 * - format string
	 * - regular bool
	 * - dt interval
	 * @returns {Promise<array<internal.serie>>|Promise<serie>} Instance of Serie if filter.id is set, else array of series 
	 */
	static async read(filter={},options={}) {
		if(filter.id && !Array.isArray(filter.id) ) {
			return internal.CRUD.getSerie(filter.tipo,filter.id,filter.timestart,filter.timeend,options,filter.public,filter.timeupdate)
		}
		filter.tipo = (filter.tipo) ? filter.tipo : "puntual"
		return internal.CRUD.getSeries(filter.tipo,filter,options)
	}
	async update(changes={}) {
		if(this.id == null) {
			console.error("Can't update serie without id")
			return
		}
		const stmt = this.updateQuery(changes)
		// console.log(stmt)
		const result = await global.pool.query(stmt)
		if(!result.rows.length) {
			console.error("Serie with specified id not found. Nothing updated")
			return
		}
		this.constructor.refreshJsonView()
		const updated = await internal.serie.read({tipo:this.tipo,id:result.rows[0].id})
		if(!updated) {
			console.error("Updated serie not found")
			return
		} else {
			Object.assign(this,updated)
			return updated
		}
	}
	updateQuery(changes={}) {
		const params = [this.id]
		const series_table = this.getSeriesTable()
		const object_properties = {
			"var_id": "var",
			"proc_id": "procedimiento",
			"unit_id": "unidades",
			"estacion_id": "estacion",
			"area_id": "estacion",
			"escena_id": "estacion",
			"fuentes_id": "fuente"
		}
		const updateable_fields = (series_table == "series_areal") ? ["var_id","proc_id","unit_id","area_id","fuentes_id"] : (series_table == "series_rast") ? ["var_id","proc_id","unit_id","escena_id","fuentes_id"] : ["var_id","proc_id","unit_id","estacion_id"]
		var set_clause = []
		var set_clause_index = 1
		for(var key of updateable_fields) {
			if(changes.hasOwnProperty(key) && changes[key] != null) {
				set_clause_index = set_clause_index + 1
				set_clause.push(`${key}=\$${set_clause_index}`)
				params.push(changes[key])
			} else if(changes.hasOwnProperty(object_properties[key]) && changes[object_properties[key]].id) {
				set_clause_index = set_clause_index + 1
				set_clause.push(`${key}=\$${set_clause_index}`)
				params.push(changes[object_properties[key]].id)
			}
		}
		if(!set_clause.length) {
			console.error("No changes to update")
			return pasteIntoSQLQuery(`SELECT * FROM ${series_table} WHERE id=$1`,params) 
		}
		return pasteIntoSQLQuery(`UPDATE ${series_table} SET ${set_clause.join(", ")} WHERE id=$1 RETURNING *`,params)
	}

	static async delete(filter={},options={}) {
		var matches = await this.read(filter,{fromView:false})
		if(matches != null && !Array.isArray(matches)) {
			matches = [matches]
		}
		if(matches == undefined || !matches.length) {
			console.error("No series matched to delete")
			return []
		}
		const deleted = []
		for(var serie of matches) {
			// console.log(serie instanceof this)
			const deleted_ = await serie.delete()
			if(deleted_) { // internal.CRUD.deleteVar(matches[i].id))
				deleted.push(deleted_)
			}
		}
		this.refreshJsonView()
		return deleted // matches.filter(m=>{
			// filter out instances that could not be deleted
		// 	return (deleted.map(d=>d.id).indexOf(m.id) >= 0)
		// })
	}
	async delete() {
		if(!this.id) {
			console.error("Can't delete this instance since it was not yet created")
			return
		}
		try {
			var result = await internal.CRUD.deleteSerie(this.tipo,this.id)
		} catch(e) {
			throw(e)
		}
		return result
	}

	aggregateMonthly(timestart,timeend,agg_function="acum",precision=2,time_support,expression,min_obs=15,inst,date_offset=0,utc=false) {
		const observaciones = this.observaciones.filter(o=>o.valor!=null)
		if(!observaciones || !observaciones.length) {
			console.error("Can't aggregate. Missing observaciones")
			return
		}
		observaciones.sort((a,b)=>a.timestart.getTime() - b.timestart.getTime())
		timestart = (timestart) ? timestart : observaciones[0].timestart
		timeend = (timeend) ? timeend : observaciones[observaciones.length-1].timeend
		if (!time_support) {
			time_support = this.var.timeSupport
		} else {
			if(/[';]/.test(time_support)) {
				throw("Invalid timeSupport")
			} 
		}
		if(!time_support || timeSteps.interval2epochSync(time_support) ==0) {
			inst = true
		}
		if(agg_function.toLowerCase() == "expression") {
			var aggFunction = expression
		} else if(inst) {
			if(!aggFuncInst.hasOwnProperty(agg_function.toLowerCase())) {
				throw("Invalid aggFunction")
			} else {
				var aggFunction = aggFuncInst[agg_function.toLowerCase()]
			}
		} else if(!aggFunc.hasOwnProperty(agg_function.toLowerCase())) {
			throw("Invalid aggFunction")
		} else {
			var aggFunction = aggFunc[agg_function.toLowerCase()]
		}
		var monthly_timeseries = timeSteps.getMonthlyTimeseries(timestart,timeend, date_offset, utc)
		var obs_index = 0
		for(var timeStep of monthly_timeseries) {
			timeStep.count = 0
			const matches = []
			if(observaciones[obs_index].timestart.getTime() >= timeStep.timeend.getTime()) {
				// el periodo observado comienza después
				timeStep.valor = null
				continue
			}
			var i = obs_index
			for(i=obs_index;i<observaciones.length;i++) {
				if(observaciones[i].timestart.getTime() >= timeStep.timeend.getTime()) {
					// se pasó
					break
				}
				if(inst) {
					if(agg_function.toLowerCase() == "nearest") {
						var shifted_timestep = {
							timestart: new Date(timeStep.timestart),
							timeend: new Date(timeStep.timeend)
						}
						shifted_timestep.timestart.setDate(shifted_timestep.timestart.getDate() - 15)
						shifted_timestep.timeend.setDate(shifted_timestep.timeend.getDate() - 15)
						if(observaciones[i].timestart.getTime() < shifted_timestep.timestart.getTime() || observaciones[i].timestart.getTime() >= shifted_timestep.timeend.getTime()) {
							// le falta
							continue
						}	
					} else if(observaciones[i].timestart.getTime() < timeStep.timestart.getTime() || observaciones[i].timestart.getTime() >= timeStep.timeend.getTime()) {
						// le falta
						continue
					}
				} else {
					var duration = getDuration(observaciones[i],timeStep)
					if(!duration) {
						// le falta
						// obs_index = obs_index + 1
						continue
					}
				}
				matches.push(observaciones[i])
				timeStep.count = timeStep.count + 1
				obs_index = i
			}
			if(!matches.length) {
				// el periodo observado termina antes
				timeStep.valor = null
				continue
			}
			if(timeStep.count < min_obs) {
				console.log("Matched observations are less than min_obs for time step " + timeStep.timestart.toString())
				timeStep.valor = null
				continue
			}
			timeStep.valor = aggFunction(matches,timeStep)
			timeStep.valor = (timeStep.valor != null) ? parseFloat(timeStep.valor.toFixed(precision)) : null
		}
		return monthly_timeseries
	}

	aggregateTimeStep(timestart,timeend,time_step,agg_function="acum",precision=2,time_support,expression,min_obs=15,inst,dest_series_id,offset,utc) {
		if(!time_step) {
			throw("missing time_step")
		}
		const valid_time_step = {
			month: {
				name: "month",
				generate_time_series_function: timeSteps.getMonthlyTimeseries,
				half_time_step: {
					unit: "days",
					value: 15
				},
				time_step: {
					month: 1
				},
				time_step_string: "1 mon"
			},
			day: {
				name: "day",
				generate_time_series_function: timeSteps.getDailyTimeseries,
				half_time_step: {
					unit: "hours",
					value: 12
				},
				time_step: {
					day: 1
				},
				time_step_string: "1 day"

			}
		}
		if(!valid_time_step.hasOwnProperty(time_step)) {
			throw("invalid time_step: valid values: 'month','day'")
		}
		time_step = valid_time_step[time_step]
		// filter and sort observaciones, set time period, inst and agg_function
		const observaciones = this.observaciones.filter(o=>o.valor!=null)
		if(!observaciones || !observaciones.length) {
			console.error("Can't aggregate. Missing observaciones")
			return
		}
		observaciones.sort((a,b)=>a.timestart.getTime() - b.timestart.getTime())
		timestart = (timestart) ? timestart : observaciones[0].timestart
		timeend = (timeend) ? timeend : observaciones[observaciones.length-1].timeend
		if (!time_support) {
			time_support = this.var.timeSupport
		} else {
			if(/[';]/.test(time_support)) {
				throw("Invalid timeSupport")
			} 
		}
		if(!time_support || timeSteps.interval2epochSync(time_support) ==0) {
			inst = true
		}
		if(agg_function.toLowerCase() == "expression") {
			var aggFunction = expression
		} else if(inst) {
			if(!aggFuncInst.hasOwnProperty(agg_function.toLowerCase())) {
				throw("Invalid aggFunction")
			} else {
				var aggFunction = aggFuncInst[agg_function.toLowerCase()]
			}
		} else if(!aggFunc.hasOwnProperty(agg_function.toLowerCase())) {
			throw("Invalid aggFunction")
		} else {
			var aggFunction = aggFunc[agg_function.toLowerCase()]
		}
		// generate timeseries
		var aggregated_timeseries = time_step.generate_time_series_function(timestart,timeend,offset,utc) // getMonthlyTimeseries(timestart,timeend)
		// iterate timeseries and calculate values
		var obs_index = 0
		for(var timeStep of aggregated_timeseries) {
			timeStep.count = 0
			const matches = []
			if(observaciones[obs_index].timestart.getTime() >= timeStep.timeend.getTime()) {
				// el periodo observado comienza después
				timeStep.valor = null
				continue
			}
			if(dest_series_id) {
				timeStep.series_id = dest_series_id
				timeStep.tipo = this.tipo
			}
			var i = obs_index
			for(i=obs_index;i<observaciones.length;i++) {
				if(observaciones[i].timestart.getTime() >= timeStep.timeend.getTime()) {
					// se pasó
					break
				}
				if(inst) {
					if(agg_function.toLowerCase() == "nearest") {
						var shifted_timestep = {
							timestart: new Date(timeStep.timestart),
							timeend: new Date(timeStep.timeend)
						}
						// shift timestamps half timestep to the left (to center it around the timestart)
						if(time_step.half_time_step.unit == "days") {
							shifted_timestep.timestart.setDate(shifted_timestep.timestart.getDate() - time_step.half_time.value)
							shifted_timestep.timeend.setDate(shifted_timestep.timeend.getDate() - time_step.half_time.value)
						} else if (time_step.half_time_step.unit == "hours") {
							shifted_timestep.timestart.setHours(shifted_timestep.timestart.getHours() - time_step.half_time.value)
							shifted_timestep.timeend.setHours(shifted_timestep.timeend.getHours() - time_step.half_time.value)
						}
						if(observaciones[i].timestart.getTime() < shifted_timestep.timestart.getTime() || observaciones[i].timestart.getTime() >= shifted_timestep.timeend.getTime()) {
							// le falta
							continue
						}	
					} else if(observaciones[i].timestart.getTime() < timeStep.timestart.getTime() || observaciones[i].timestart.getTime() >= timeStep.timeend.getTime()) {
						// le falta
						continue
					}
				} else {
					var duration = getDuration(observaciones[i],timeStep)
					if(!duration) {
						// le falta
						// obs_index = obs_index + 1
						continue
					}
				}
				matches.push(observaciones[i])
				timeStep.count = timeStep.count + 1
				obs_index = i
			}
			if(!matches.length) {
				// el periodo observado termina antes
				timeStep.valor = null
				continue
			}
			if(timeStep.count < min_obs) {
				console.log("Matched observations are less than min_obs for time step " + timeStep.timestart.toString())
				timeStep.valor = null
				continue
			}
			timeStep.valor = aggFunction(matches,timeStep)
			timeStep.valor = (timeStep.valor != null) ? parseFloat(timeStep.valor.toFixed(precision)) : null
		}
		return new internal.observaciones(aggregated_timeseries)
	}

	async createObservaciones() {
		if(this.observaciones && this.observaciones.length) {
			this.observaciones.setTipo(this.tipo)
			this.observaciones.setSeriesId(this.id)
			this.observaciones = await this.observaciones.create()
		} else {
			console.warn("serie: no observaciones to create")
		}
		return this.observaciones
	}

	createObservacionesQuery(options={}) {
		return this.observaciones.createQuery(this.tipo,options)
	}

	setObservaciones(observaciones) {
		if(observaciones != null && observaciones.length) {
			this.observaciones = new internal.observaciones(observaciones)
		} else {
			this.observaciones = new internal.observaciones([])
		}
	}

	async getObservaciones(timestart,timeend,inline=true) {
		if(!this.id) {
			console.warn("Missing series id. Can't get observaciones")
			return
		}
		const observaciones = await internal.observaciones.read({tipo: this.tipo, series_id: this.id, timestart:timestart,timeend:timeend})
		if(inline) {
			this.setObservaciones(observaciones)
		} else {
			return observaciones
		}
	}

	/**
     * Return true if serie matches the filter parameters
     * @param {Serie} serie 
     * @param {Object} filter
     * @param {integer|integer[]} filter.var_id
     * @param {integer|integer[]} filter.proc_id
     * @param {integer|integer[]} filter.unit_id
     * @param {integer|integer[]} filter.estacion_id
     * @param {integer|integer[]} filter.fuentes_id
     * @param {string|string[]} filter.id_externo
     * @param {string|string[]} filter.tabla
     * @returns {boolean}
     */
	filterSerie(filter={}) {
		if(filter.var_id) {
			if(Array.isArray(filter.var_id)) {
				if(filter.var_id.indexOf(this.var.id) < 0) {
					return false
				}
			} else {
				if(filter.var_id != this.var.id) {
					return false
				}
			}
		}
		if(filter.unit_id) {
			if(Array.isArray(filter.unit_id)) {
				if(filter.unit_id.indexOf(this.unidades.id) < 0) {
					return false
				}
			} else {
				if(filter.unit_id != this.unidades.id) {
					return false
				}
			}
		}
		if(filter.proc_id) {
			if(Array.isArray(filter.proc_id)) {
				if(filter.proc_id.indexOf(this.procedimiento.id) < 0) {
					return false
				}
			} else {
				if(filter.proc_id != this.procedimiento.id) {
					return false
				}
			}
		}
		if(filter.estacion_id) {
			if(Array.isArray(filter.estacion_id)) {
				if(filter.estacion_id.indexOf(this.estacion.id) < 0) {
					return false
				}
			} else {
				if(filter.estacion_id != this.estacion.id) {
					return false
				}
			}
		}
		if(filter.id_externo) {
			if(Array.isArray(filter.id_externo)) {
				if(filter.id_externo.indexOf(this.estacion.id_externo) < 0) {
					return false
				}
			} else {
				if(filter.id_externo != this.estacion.id_externo) {
					return false
				}
			}
		}
		if(filter.tabla) {
			if(Array.isArray(filter.tabla)) {
				if(filter.tabla.indexOf(this.estacion.tabla) < 0) {
					return false
				}
			} else {
				if(filter.tabla != this.estacion.tabla) {
					return false
				}
			}
		}
		if(filter.fuentes_id) {
			if(!this.fuente) {
				return false
			}
			if(Array.isArray(filter.fuentes_id)) {
				if(filter.fuentes_id.indexOf(this.fuentes.id) < 0) {
					return false
				}
			} else {
				if(filter.fuentes_id != this.fuentes.id) {
					return false
				}
			}
		}
		return true
	}

	static async getDerivedSerie(
		tipo="puntual",
		series_id,
		timestart,
		timeend,
		method="expression",
		expression="${valor_0}",
		join_type="left",
		output_series_id = undefined,
		create_observaciones = false,
		unit_id = undefined
	) {
		const series = []
		for(var id of series_id) {
			series.push(await internal.CRUD.getSerie(tipo, id, timestart, timeend))
		}
		const result_serie = await this.computeExpression(series, method, expression, join_type, output_series_id, unit_id)
		if(create_observaciones) {
			await result_serie.createObservaciones()
		}
		return result_serie
	}
	// series are joined by exact timestart match
	// expression must be a valid js expression and may use variable names timestart, timeend and valor_0 [valor_1, valor_2, ...]
	static computeExpression(
		series=[],
		method="expression",
		expression="${valor_0}",
		join_type="left",
		output_series_id=undefined,
		unit_id=undefined
	) {
		if(!series.length) {
			throw("Series is of length 0")
		}
		var serie_0 = series[0]
		if(!serie_0) {
			throw("Series[0] is undefined")
		}
		if(!serie_0.observaciones || !serie_0.observaciones.length) {
			throw("Missing observaciones for serie id: " + serie_0.id + " i: 0")
		}
		serie_0.id = output_series_id
		var value_keys = series.map((el, i) => `valor_${i}`)
		expression = expression.replace("timestart", `o.timestart`)
		expression = expression.replace("timeend", `o.timeend`)
		for(var value_key of value_keys) {
			var re = new RegExp(value_key, 'g')
			expression = expression.replace(re, `o.${value_key}`)
		}
		// console.debug("series.length: " + series.length + ", value_keys: " + value_keys, "expression: " + expression)

		for(var o of serie_0.observaciones) {
			o.valor_0 = o.valor
			o.valor = undefined
			o.unit_id = unit_id ?? o.unit_id
			o.nombre = undefined
			o.timeupdate = undefined
			o.id = undefined
			o.series_id = output_series_id
			for(var i=1; i<series.length; i++) {
				var serie = series[i]
				var key = value_keys[i]
				if(!serie.observaciones || !serie.observaciones.length) {
					throw("Missing observaciones for serie id: " + serie.id + " i: " + i)
				}
				const matches = serie.observaciones.filter(oj=> oj.timestart.getTime() == o.timestart.getTime())
				if(!matches.length) {
					if(join_type=="inner") {
						throw("Missing timestart " + o.timestart)
					}
					o[key] = null
				} else {
					o[key] = matches[0].valor
				}
			}	
			o.valor = eval(expression)
		}
		return serie_0
	}
}

const asc = arr => arr.sort((a, b) => a - b)

const quantile = (arr, q) => {
    const sorted = asc(arr);
    const pos = (sorted.length - 1) * q;
    const base = Math.floor(pos);
    const rest = pos - base;
    if (sorted[base + 1] !== undefined) {
        return sorted[base] + rest * (sorted[base + 1] - sorted[base]);
    } else {
        return sorted[base];
    }
}

const aggFunc = {
	avg: function(observaciones,timeStep) {
		var sum_valor = 0
		var sum_time = 0
		for(var o of observaciones) {
			var duration = getDuration(o,timeStep)
			sum_valor = sum_valor + duration * o.valor 
			sum_time = sum_time + duration
		}
		var mean = (sum_time) ? sum_valor / sum_time : null
		return mean // (mean != null) ? parseFloat(mean.toFixed(precision)) : null
	},
	acum: function(observaciones,timeStep) {
		var sum = 0
		for(var o of observaciones) {
			var duration = getDuration(o,timeStep)
			sum = sum + (duration / (timeStep.timeend.getTime() - timeStep.timestart.getTime())) * o.valor
		}
		return sum // parseFloat(sum.toFixed(precision))
	},
	mean: function(observaciones,timeStep) {
		var sum_valor = 0
		var sum_time = 0
		for(var o of observaciones) {
			var duration = getDuration(o,timeStep)
			sum_valor = sum_valor + duration * o.valor 
			sum_time = sum_time + duration
		}
		var mean = (sum_time) ? sum_valor / sum_time : null
		return mean // (mean != null) ? parseFloat(mean.toFixed(precision)) : null
	},
	sum: function (observaciones,timeStep) {
		var sum = 0
		for(var o of observaciones) {
			sum = sum + o.valor
		}
		return sum // parseFloat(sum.toFixed(precision))
	},
	min: function (observaciones,timeStep) {
		var min = [...observaciones].sort((a,b)=>a.valor - b.valor)[0].valor
		return min // parseFloat(min.toFixed(precision))
	},
	max: function (observaciones,timeStep) {
		var max = [...observaciones].sort((a,b)=>a.valor - b.valor)[observaciones.length-1].valor
		return max // parseFloat(max.toFixed(precision))
	},
	count: function (observaciones,timeStep) {
		var count = observaciones.length
		return count // parseFloat(count.toFixed(precision))
	},
	diff: function (observaciones,timeStep) {
		var sorted_obs = [...observaciones].sort((a,b)=>a.valor - b.valor)
		var diff = sorted_obs[sorted_obs.length-1].valor - sorted_obs[0].valor
		return diff // parseFloat(diff.toFixed(precision))
	}, 
	increment: function(observaciones,timeStep) {
		var max = [...observaciones].sort((a,b)=>a.valor - b.valor)[observaciones.length-1].valor
		var increment = max - observaciones[0].valor
		return increment // parseFloat(increment.toFixed(precision))
	},
	math: function(observaciones,timeStep) {
		var result = expression(observaciones,timeStep)
		return result // parseFloat(result.toFixed(precision))
	}
}

const aggFuncInst = {
	nearest: function(observaciones,timeStep) {
		// var mid_step = new Date((timeStep.timestart.getTime() + timeStep.timeend.getTime()) / 2)
		var diff = observaciones.map((o,i)=>{
			return {
				diff: Math.abs(o.timestart.getTime() - timeStep.timestart.getTime()),
				index: i
			} 
		})
		diff.sort((a,b)=>a.diff - b.diff)
		var index_of_closest = diff[0].index
		return observaciones[index_of_closest].valor
	},
	acum: function(observaciones,timeStep) {
		var sum = 0
		for(var o of observaciones) {
			sum = sum + o.valor
		}
		return sum // parseFloat(sum.toFixed(precision))
	},
	avg: function(observaciones,timeStep) {
		var sum = 0
		var count = 0
		for(var o of observaciones) {
			sum = sum + o.valor 
			count = count + 1
		}
		var mean = (count) ? sum / count : null
		return mean // (mean != null) ? parseFloat(mean.toFixed(precision)) : null
	},
	mean: function(observaciones,timeStep) {
		var sum = 0
		var count = 0
		for(var o of observaciones) {
			sum = sum + o.valor 
			count = count + 1
		}
		var mean = (count) ? sum / count : null
		return mean // (mean != null) ? parseFloat(mean.toFixed(precision)) : null
	},
	sum: function (observaciones,timeStep) {
		var sum = 0
		for(var o of observaciones) {
			sum = sum + o.valor
		}
		return sum // parseFloat(sum.toFixed(precision))
	},
	min: function (observaciones,timeStep) {
		var min = [...observaciones].sort((a,b)=>a.valor - b.valor)[0].valor
		return min // parseFloat(min.toFixed(precision))
	},
	max: function (observaciones,timeStep) {
		var max = [...observaciones].sort((a,b)=>a.valor - b.valor)[observaciones.length-1].valor
		return max // parseFloat(max.toFixed(precision))
	},
	count: function (observaciones,timeStep) {
		var count = observaciones.length
		return count // parseFloat(count.toFixed(precision))
	},
	diff: function (observaciones,timeStep) {
		var sorted_obs = [...observaciones].sort((a,b)=>a.valor - b.valor)
		var diff = sorted_obs[sorted_obs.length-1].valor - sorted_obs[0].valor
		return diff // parseFloat(diff.toFixed(precision))
	}, 
	increment: function(observaciones,timeStep) {
		var max = [...observaciones].sort((a,b)=>a.valor - b.valor)[observaciones.length-1].valor
		var increment = max - observaciones[0].valor
		return increment // parseFloat(increment.toFixed(precision))
	},
	math: function(observaciones,timeStep) {
		var result = expression(observaciones,timeStep)
		return result // parseFloat(result.toFixed(precision))
	}
}

function getDuration(o,timeStep) {
	return (o.timestart >= timeStep.timeend) ? 0 : (o.timestart >= timeStep.timestart) ? (o.timeend >= timeStep.timeend) ? timeStep.timeend.getTime() - o.timestart.getTime() : o.timeend.getTime() - o.timestart.getTime() : (o.timeend <= timeStep.timestart) ? 0 : (o.timeend <= timeStep.timeend) ? o.timeend.getTime() - timeStep.timestart.getTime() : timeStep.timeend.getTime() - timeStep.timestart.getTime()
}

function assignPercentileCategory(value, categories) {
	const flow_percentile_categories = (categories) ? categories : [
		{
			"name": "low flow",
			"range": [ -Infinity, 0.13],
			"number": 1
		},
		{
			"name": "below normal",
			"range": [ 0.13, 0.28],
			"number": 2 
		},
		{
			"name": "normal range",
			"range": [ 0.28, 0.72] ,
			"number": 3
		},
		{
			"name": "above normal",
			"range": [ 0.72, 0.87] ,
			"number": 4
		},
		{
			"name": "high flow",
			"range": [ 0.87, Infinity] ,
			"number": 5
		},
	]
	for(var i in flow_percentile_categories) {
		if(value < flow_percentile_categories[i].range[1]) {
			return flow_percentile_categories[i]
		}
	}
	console.error("Percentile value doesn't belong to any of the categories")
	return
}

internal.serie.build_read_query = function(filter={},options={}) {
	// var model = apidoc.serie
	var valid_filters
	var table
	var tipo
	var order_string
	// 					FILTERS
	if(!filter.tipo) {
		tipo = "puntual"
	} else {
		tipo = filter.tipo
	}
	if(!filter.series_id && filter.id) {
		filter.series_id = filter.id
	}
	var valid_filters = {
		id:{
			type: "integer",
			table: "series"
		},var_id:{
			type:"integer",
			table: "series"
		},proc_id:{
			type:"integer",
			table: "series"
		},unit_id:{
			type: "integer",
			table: "series"
		},public: {
			type:"boolean_only_true"
		},date_range_before: {
			column: "timestart",
			type: "timeend",
			table: "date_range"
		},date_range_after: {
			column: "timeend",
			type: "timestart",
			table: "date_range"
		},
		count: {
			type: "numeric_min",
			table: "date_range"
		},
		data_availability: {
			type: "data_availability",
			table: "date_range"
		},
		GeneralCategory: {
			type: "string",
			table: "var"
		}
	}
	// 					DATE RANGE / DATA AVAILABILITY FILTER
	var timestart = (filter.timestart) ? new Date(filter.timestart) : undefined
	if (timestart && timestart.toString() == "Invalid Date") {
		throw("Invalid timestart)")
	}
	var timeend = (filter.timeend) ? new Date(filter.timeend) : undefined
	if (timeend && timeend.toString() == "Invalid Date") {
		throw("Invalid timeend)")
	}	
	var date_range_table = internal.serie.getDateRangeTable(tipo,options)
	var series_range_join = (filter.has_obs || (filter.data_availability && ["h","c","n","r"].indexOf(filter.data_availability.toLowerCase()) >= 0)) ? "INNER" : "LEFT OUTER"
	var date_range_query = `
		${series_range_join} JOIN (
			SELECT 
				"${date_range_table}".series_id::int,
				"${date_range_table}".timestart::timestamptz,
				"${date_range_table}".timeend::timestamptz,
				"${date_range_table}".count::int,
				CASE WHEN "${date_range_table}".timeend IS NOT NULL
					THEN 
						CASE WHEN now() - "${date_range_table}".timeend < '1 days'::interval
							THEN 'RT'
						WHEN now() - "${date_range_table}".timeend < '3 days'::interval
							THEN 'NRT'
						WHEN ("${date_range_table}".timestart <= coalesce(${(timeend) ? ("'" + timeend.toISOString() + "'") : "NULL"},now())) AND ("${date_range_table}".timeend >= coalesce(${(timestart) ? ("'" + timestart.toISOString() + "'") : "NULL"},now()-'90 days'::interval))
							THEN 'C'
						ELSE 'H'
						END
					ELSE NULL
				END AS data_availability
			FROM "${date_range_table}"
		) AS date_range ON (
			series.id=date_range.series_id
		)`
	//					PRONOS FILTERS
	var pronos_filter_string = internal.utils.control_filter2(
		{
			public: {type: "boolean"},
			cal_id: {type: "integer"},
			cal_grupo_id: {type: "integer"}		
		},
		filter,
		"series_prono_date_range_last"
	)
	if(filter.has_prono || filter.cal_id || filter.cal_grupo_id) {
		var pronos_join = "JOIN"
	} else {
		var pronos_join = "LEFT OUTER JOIN"
	}
	var pronos_grouped_query = `${pronos_join} (SELECT
		series_table,
		estacion_id,
		var_id,
		max(forecast_date)::timestamptz AS forecast_date,
		json_agg(
			json_build_object(
				'series_id', series_id,
				'series_table', series_table,
				'begin_date',begin_date,
				'end_date',end_date,
				'count',count,
				'cal_id',cal_id,
				'forecast_date',forecast_date,
				'public',public,
				'cal_grupo_id',cal_grupo_id
			   )
		) AS pronosticos
		FROM series_prono_date_range_last 
		WHERE 1=1
		${pronos_filter_string}
		GROUP BY
			series_table,
			estacion_id,
			var_id
	) AS pronos ON (
		pronos.estacion_id=series.${internal.serie.getFeatureIdColumn(tipo)}
		AND pronos.var_id=series.var_id
		AND pronos.series_table='${internal.serie.getSeriesTable(tipo)}'
	)`
	//					SORT
	var sort_fields = {
		id:{column: "id",
			table: "series"},
		estacion_id: {
			table: "series",
			column: internal.serie.getFeatureIdColumn(tipo),
		},
		var_id:{
			table: "series"},
		var_name:{table: "var", column: "nombre"},
		proc_id:{
			table: "series"},
		unit_id:{
			table: "series"},
		timestart:{
			table: "date_range"},
		timeend:{
			table: "date_range"},
		count:{
			table: "date_range"},
		forecast_date:{
			table: "pronos"},
		data_availability:{
			table: null}
	}
	sort_fields[internal.serie.getFeatureIdColumn(tipo)] = {
		table: "series",
		column: internal.serie.getFeatureIdColumn(tipo)
	}
	//						PAGINATION
	var [limit,pagination,page_offset,limit_string] = internal.utils.getLimitString(filter.limit,filter.offset)
	var properties
	//				JOIN CLAUSES
	var join_clauses = [
		`JOIN var 
		ON (var.id=series.var_id)`,
		`JOIN procedimiento 
		ON (procedimiento.id=series.proc_id)`,
		`JOIN unidades
		ON (unidades.id=series.unit_id)`,
		pronos_grouped_query,
		date_range_query
	]
	// 				SELECT FIELDS
	if(options.no_metadata) {
		var select_fields = [
			`'${tipo}' AS tipo`,
			"series.id AS id",
			"series.id AS series_id",
			"procedimiento.id AS proc_id",
			"procedimiento.nombre AS proc_nombre",
			"var.id AS var_id",
			"var.nombre AS var_nombre",
			`var."GeneralCategory" AS "GeneralCategory"`,
			"unidades.id AS unit_id",
			"unidades.nombre AS unit_nombre",
			`date_range.timestart AS timestart`,
			`date_range.timeend AS timeend`,
			`date_range.count AS count`,
			`CASE WHEN date_range.data_availability IS NOT NULL
			THEN CASE WHEN pronos.forecast_date IS NOT NULL
				THEN date_range.data_availability || '+S'::text
				ELSE date_range.data_availability
				END
			ELSE CASE WHEN pronos.forecast_date IS NOT NULL
				THEN 'S'
				ELSE 'N'
				END
			END
			AS data_availability`,
			`pronos.forecast_date AS forecast_date`
		]
	} else {
		var select_fields = [
			`'${tipo}' AS tipo`,
			"series.id AS id",
			`json_build_object(
				'id', procedimiento.id, 
				'nombre', procedimiento.nombre, 
				'abrev', procedimiento.abrev,
				'descripcion', procedimiento.descripcion
			) AS procedimiento`,
			`json_build_object(
				'id', var.id, 
				'var', var.var, 
				'nombre', var.nombre, 
				'abrev', var.abrev, 
				'type', var.type, 
				'datatype', var.datatype, 
				'valuetype', var.valuetype,
				'GeneralCategory', var."GeneralCategory",
				'VariableName', var."VariableName", 
				'SampleMedium', var."SampleMedium", 
				'def_unit_id', var.def_unit_id, 
				'timeSupport', var."timeSupport", 
				'def_hora_corte', var.def_hora_corte
			) AS var`,
			`json_build_object(
				'id', unidades.id, 
				'nombre', unidades.nombre, 
				'abrev', unidades.abrev, 
				'UnitsID', unidades."UnitsID", 
				'UnitsType', unidades."UnitsType"
			) AS unidades`,
			`json_build_object(
				'timestart', date_range.timestart, 
				'timeend', date_range.timeend, 
				'count', date_range.count,
				'data_availability', CASE WHEN date_range.data_availability IS NOT NULL
					THEN CASE WHEN pronos.forecast_date IS NOT NULL
						THEN date_range.data_availability || '+S'::text
						ELSE date_range.data_availability
						END
					ELSE CASE WHEN pronos.forecast_date IS NOT NULL
						THEN 'S'
						ELSE 'N'
						END
					END
			) AS date_range`,
			`pronos.forecast_date AS forecast_date`,
			`pronos.pronosticos AS pronosticos`
		]
	}
	// 						TYPE SPECIFIC PARAMETERS
	if(tipo.toUpperCase() == "AREAL" ) {
		valid_filters = {...valid_filters,...{
			estacion_id: {
				type: "integer",
				table: "areas_pluvio",
				column: "unid"
			},
			fuentes_id:{
				type:"integer"
			},public: {
				type:"boolean_only_true",
				table: "fuentes"
			},
			tabla_id:{
				type:"string",
				table: "estaciones",
				column: "tabla"
			},
			red_id: {
				type: "integer",
				table: "redes",
				column: "id"
			},
			geom: {
				type: "geometry", 
				table: "areas_pluvio"
			},
			search: {
				type: "search", 
				table: "areas_pluvio", 
				columns: [
					{name: "id", table: "series"},
					{name: "tabla", table: "estaciones"},
					{name: "nombre"},
					{name: "unid"},
					{name: "id_externo", table: "estaciones"},
					{name: "rio", table: "estaciones"},
					{name: "nombre", table: "var"},
					{name: "nombre", table: "fuentes"}
				],
				case_insensitive: true
			},
			exutorio_id: {
				type: "integer",
				table: "areas_pluvio"
			}
		}}
		sort_fields = {...sort_fields,...{
			nombre:{table: "areas_pluvio"},
			geom:{function: "st_xmin(areas_pluvio.geom)"},
			longitud:{function: "st_xmin(areas_pluvio.geom)"},
			latitud:{function: "st_ymin(areas_pluvio.geom)"},
			rio:{table: "estaciones"},
			tabla:{table: "estaciones", column: "tabla"},
			fuentes_id:{table: "series"},
			id_externo:{table: "estaciones"}
		}}
		table = "series_areal"
		join_clauses = [...join_clauses,...[
			`JOIN fuentes 
				ON (fuentes.id=series.fuentes_id)`,
			`JOIN areas_pluvio 
				ON (areas_pluvio.unid=series.area_id)`,
			`LEFT JOIN estaciones 
				ON (estaciones.unid = areas_pluvio.exutorio_id)`,
			`LEFT JOIN redes
				ON (estaciones.tabla = redes.tabla_id)`
		]]
		if(options.no_metadata) {
			select_fields = [...select_fields,...[
				"series.area_id AS estacion_id",
				"areas_pluvio.nombre AS estacion_nombre",
				"fuentes.id AS fuentes_id",
				"fuentes.nombre AS fuentes_nombre"
			]]
		} else {
			select_fields.push(
				`json_build_object(
					'id', fuentes.id, 
					'nombre', fuentes.nombre, 
					'data_table', fuentes.data_table,
					'data_column', fuentes.data_column, 
					'tipo', fuentes.tipo, 
					'def_proc_id', fuentes.def_proc_id, 
					'def_dt', fuentes.def_dt, 
					'hora_corte', fuentes.hora_corte, 
					'def_unit_id', fuentes.def_unit_id,
					'def_var_id', fuentes.def_var_id, 
					'fd_column', fuentes.fd_column, 
					'mad_table', fuentes.mad_table, 
					'scale_factor', fuentes.scale_factor, 
					'data_offset', fuentes.data_offset, 
				 	'def_pixel_height', fuentes.def_pixel_height,
					'def_pixel_width', fuentes.def_pixel_width,
				 	'def_extent', st_asgeojson(fuentes.def_extent)::json, 
					'date_column', fuentes.date_column, 
					'def_pixeltype', fuentes.def_pixeltype, 
					'abstract', fuentes.abstract, 
					'source', fuentes.source, 
					'public', fuentes.public
				) AS fuente`
			)
			if(options.include_geom && !options.no_geom) {
				select_fields.push(
					`json_build_object(
						'id', areas_pluvio.unid, 
						'nombre', areas_pluvio.nombre, 
						'geom', st_asgeojson(areas_pluvio.geom)::json, 
						'exutorio', json_build_object(
							'id', estaciones.unid,
							'geom', st_asgeojson(estaciones.geom)::json,
							'tabla', estaciones.tabla
						)
					) AS estacion`
				)
			} else {
				select_fields.push(`json_build_object(
					'id', areas_pluvio.unid, 
					'nombre', areas_pluvio.nombre, 
					'exutorio', json_build_object(
						'id', estaciones.unid,
						'geom', st_asgeojson(estaciones.geom)::json,
						'tabla', estaciones.tabla
					)
				) AS estacion`)
			}
		}
	} else if (tipo.toUpperCase() == "RASTER" || tipo.toUpperCase() == "RAST") {
		valid_filters = {...valid_filters,...{
			estacion_id:{
				type: "integer",
				table: "escenas",
				column: "id"
			},
			fuentes_id:{
				type:"integer"
			},
			public: {
				type:"boolean_only_true",
				table: "fuentes"
			},
			geom: {
				type: "geometry", 
				table: "escenas"
			},
			search: {
				type: "search", 
				table: "escenas", 
				columns: [
					{name: "id", table: "series"},
					{name: "nombre"},
					{name: "id"},
					{name: "nombre", table: "var"},
					{name: "nombre", table: "fuentes"}
				],
				case_insensitive: true
			}
		}}
		sort_fields = {...sort_fields,...{
			nombre:{table: "escenas"},
			geom:{function: "st_xmin(escenas.geom)"},
			longitud:{function: "st_xmin(escenas.geom)"},
			latitud:{function: "st_ymin(escenas.geom)"},
			fuentes_id:{table: "series"}
		}}
		table = "series_rast"
		join_clauses = [...join_clauses,...[
			`JOIN fuentes 
				ON (fuentes.id=series.fuentes_id)`,
			`JOIN escenas 
				ON (escenas.id=series.escena_id)`
		]]
		if(options.no_metadata) {
			select_fields = [...select_fields,...[
				"escenas.id AS estacion_id",
				"series.nombre",
				"escenas.nombre AS estacion_nombre",
				"fuentes.id AS fuentes_id",
				"fuentes.nombre AS fuentes_nombre"
			]]
		} else {
			select_fields.push(
				`json_build_object(
					'id', fuentes.id, 
					'nombre', fuentes.nombre, 
					'data_table', fuentes.data_table,
					'data_column', fuentes.data_column, 
					'tipo', fuentes.tipo, 
					'def_proc_id', fuentes.def_proc_id, 
					'def_dt', fuentes.def_dt, 
					'hora_corte', fuentes.hora_corte, 
					'def_unit_id', fuentes.def_unit_id,
					'def_var_id', fuentes.def_var_id, 
					'fd_column', fuentes.fd_column, 
					'mad_table', fuentes.mad_table, 
					'scale_factor', fuentes.scale_factor, 
					'data_offset', fuentes.data_offset, 
					'def_pixel_height', fuentes.def_pixel_height,
					'def_pixel_width', fuentes.def_pixel_width,
				 	'def_extent', st_asgeojson(fuentes.def_extent)::json, 
					'date_column', fuentes.date_column, 
					'def_pixeltype', fuentes.def_pixeltype, 
					'abstract', fuentes.abstract, 
					'source', fuentes.source, 
					'public', fuentes.public
				) AS fuente`
			)
			if(options.include_geom && !options.no_geom) {
				select_fields.push(
					`json_build_object(
						'id', escenas.id, 
						'nombre', escenas.nombre, 
						'geom', st_asgeojson(escenas.geom)::json
					) AS estacion`
				)
			} else {
				select_fields.push(`json_build_object(
					'id', escenas.id, 
					'nombre', escenas.nombre
				) AS estacion`)
			}
		}		
		table = "series_rast"
	} else if (tipo.toUpperCase() == "PUNTUAL") {
		filter.red_id = (filter.red_id) ? filter.red_id : (filter.fuentes_id) ? filter.fuentes_id : undefined
		valid_filters = {...valid_filters,...{
			estacion_id:{
				type:"integer",
				table: "estaciones",
				column: "unid"
			},
			id_externo: {
				type: "string",
				table: "estaciones"
			},
			tabla_id:{
				type:"string",
				table: "estaciones",
				column: "tabla",
				alias: "tabla"
			},
			red_id: {
				type: "integer",
				table: "redes",
				column: "id"
			},
			pais: {
				type: "string",
				table: "estaciones",
				column: "pais"
			},
			geom: {
				type: "geometry", 
				table: "estaciones"
			},
			public: {
				type: "boolean_only_true",
				table: "redes"
			},
			search: {
				type: "search", 
				table: "estaciones", 
				columns: [
					{name: "id", table: "series"},
					{name: "tabla"},
					{name: "nombre"},
					{name: "unid"},
					{name: "id_externo"},
					{name: "rio"},
					{name: "distrito"},
					{name: "pais"},
					{name: "propietario"},
					{name: "ubicacion"},
					{name: "localidad"},
					{name: "nombre", table: "var"}
				],
				case_insensitive: true
			}
		}}
		sort_fields = {...sort_fields,...{
			nombre:{table: "estaciones"},
			rio:{table: "estaciones"},
			propietario:{table: "estaciones"},
			localidad:{table: "estaciones"},
			ubicacion:{table: "estaciones"},
			geom:{function: "st_x(estaciones.geom)"},
			longitud:{function: "st_x(estaciones.geom)"},
			latitud:{function: "st_y(estaciones.geom)"},
			tabla:{table: "estaciones", column: "tabla"},
			id_externo:{table: "estaciones"}
		}}
		table = "series"
		join_clauses = [...join_clauses,...[
			`JOIN estaciones 
				ON (estaciones.unid=series.estacion_id)`,
			`JOIN redes
				ON (redes.tabla_id = estaciones.tabla)`,
			`LEFT OUTER JOIN alturas_alerta AS nivel_alerta 
				ON (estaciones.unid = nivel_alerta.unid AND nivel_alerta.estado='a')`,
			`LEFT OUTER JOIN alturas_alerta AS nivel_evacuacion 
				ON (estaciones.unid = nivel_evacuacion.unid AND nivel_evacuacion.estado='e')`,
			`LEFT OUTER JOIN alturas_alerta AS nivel_aguas_bajas 
				ON (estaciones.unid = nivel_aguas_bajas.unid AND nivel_aguas_bajas.estado='b')`
		]]
		if(options.no_metadata) {
			select_fields = [...select_fields,...[
				"series.estacion_id AS estacion_id",
				"estaciones.nombre AS estacion_nombre",
				"estaciones.id_externo AS id_externo",
				"estaciones.tabla AS tabla",
				"st_asgeojson(estaciones.geom)::json AS geom",
				"redes.nombre AS fuentes_nombre",
				"redes.public AS public"
			]]
		} else {
			// if(options.include_geom && !options.no_geom) {
			// 	select_fields.push(
			// 		`json_build_object(
			// 			'id', estaciones.unid, 
			// 			'nombre', estaciones.nombre, 
			// 			'id_externo', estaciones.id_externo, 
			// 			'geom', st_asgeojson(estaciones.geom::geometry)::json, 
			// 			'tabla', estaciones.tabla, 
			// 			'pais', estaciones.pais, 
			// 			'rio', estaciones.rio, 
			// 			'has_obs', estaciones.has_obs, 
			// 			'tipo', estaciones.tipo, 
			// 			'automatica', estaciones.automatica, 
			// 			'habilitar', estaciones.habilitar, 
			// 			'propietario', estaciones.propietario, 
			// 			'abreviatura', estaciones.abrev, 
			// 			'localidad', estaciones.localidad, 
			// 			'real', estaciones."real", 
			// 			'nivel_alerta', nivel_alerta.valor, 
			// 			'nivel_evacuacion', nivel_evacuacion.valor, 
			// 			'nivel_aguas_bajas', nivel_aguas_bajas.valor, 
			// 			'altitud', estaciones.altitud, 
			// 			'public', redes.public, 
			// 			'cero_ign', estaciones.cero_ign, 
			// 			'red_id', redes.id, 
			// 			'red_nombre', redes.nombre
			// 		) AS estacion`
			// 	)
			// } else {
				select_fields.push(`json_build_object(
					'id', estaciones.unid, 
					'nombre', estaciones.nombre, 
					'id_externo', estaciones.id_externo, 
					'geom', st_asgeojson(estaciones.geom::geometry)::json,
					'longitude', st_x(estaciones.geom::geometry),
					'latitude', st_y(estaciones.geom::geometry),
					'tabla', estaciones.tabla, 
					'pais', estaciones.pais, 
					'rio', estaciones.rio, 
					'has_obs', estaciones.has_obs, 
					'tipo', estaciones.tipo, 
					'automatica', estaciones.automatica, 
					'habilitar', estaciones.habilitar, 
					'propietario', estaciones.propietario, 
					'abreviatura', estaciones.abrev, 
					'localidad', estaciones.localidad, 
					'real', estaciones."real", 
					'nivel_alerta', nivel_alerta.valor, 
					'nivel_evacuacion', nivel_evacuacion.valor, 
					'nivel_aguas_bajas', nivel_aguas_bajas.valor, 
					'altitud', estaciones.altitud, 
					'public', redes.public, 
					'cero_ign', estaciones.cero_ign, 
					'red_id', redes.id, 
					'red_nombre', redes.nombre,
					'red', json_build_object('nombre', redes.nombre, 'id', redes.id, 'tabla_id', redes.tabla_id, 'public', redes.public, 'public_his_plata', redes.public_his_plata)
				) AS estacion`)
			// }
		}		
	} else {
		console.error("invalid tipo")
		throw "invalid tipo"
	}
	var filter_string=internal.utils.control_filter2(valid_filters,filter,undefined,true)
	// console.debug({filter:filter,filter_string:filter_string})
	var order_string = internal.utils.build_order_by_clause(sort_fields,options.sort,"series",["estacion_id","var_id","proc_id"],options.order)
	// console.log({order_string:order_string,sort:options.sort,order:options.order})
	select_fields.push(`count(*) OVER() AS total`)
	return `SELECT  ${select_fields.join(", \n")}  
		FROM ${table} AS series 
		${join_clauses.join("\n")}
		WHERE 1=1
		${filter_string}
		${order_string}
		${limit_string}`	
}


internal.serieRegular = class extends baseModel {
	constructor() {
        super()
		switch(arguments.length) {
			case 1:
				this.fromSeries = arguments[0].fromSeries  // internal.serie
				this.timestart= arguments[0].timestart  // date
				this.timeend = arguments[0].timeend     // date
				this.dt = arguments[0].dt               // interval
				this.t_offset = arguments[0].t_offset   // interval
				this.funcion = arguments[0].funcion // string
				this.data = arguments[0].data           //  array [[timestart,timeend,value],...]
				break;
			default:
				this.fromSeries = arguments[0]  // internal.serie
				this.timestart= arguments[1]  // date
				this.timeend = arguments[2]     // date
				this.dt = arguments[3]               // interval
				this.t_offset = arguments[4]   // interval
				this.funcion = arguments[5] // string
				this.data = arguments[6]           //  array [[timestart,timeend,value],...]
				break;
		}
	}
	toString() {
		return 
	}
	toCSV() {
		return
	}
	toCSVless() {
		return
	}
	
}

internal.campo2 = class extends baseModel {
	constructor() {
        super()
		switch(arguments.length) {
			case 1: 
				this.var_id= arguments[0].var_id
				this.proc_id= arguments[0].proc_id
				this.unit_id= arguments[0].unit_id
				this.timestart= new Date(arguments[0].timestart)
				this.timeend= new Date(arguments[0].timeend)
				this.geom= (arguments[0].geom) ? new internal.geometry(arguments[0].geom) : null
				this.estacion_id= arguments[0].estacion_id
				this.red_id= arguments[0].red_id
				this.options= arguments[0].options
				this.seriesRegulares= arguments[0].seriesRegulares //(arguments[0].seriesRegulares) ? arguments[0].seriesRegulares.map(s=> new internal.serieRegular(s)) : []
				break;
			default:
				this.var_id= arguments[0]
				this.proc_id= arguments[1]
				this.unit_id= arguments[2]
				this.timestart= new Date(arguments[3])
				this.timeend= new Date(arguments[4])
				this.geom= (arguments[5]) ? new internal.geometry(arguments[5]) : null
				this.estacion_id= arguments[6]
				this.red_id= arguments[7]
				this.options= arguments[8]
				this.seriesRegulares= arguments[9] // (arguments[9]) ? arguments[9].map(s=> new internal.serieRegular(s)) : []
				break;
		}
	}
	toString() {
		return  "var_id:" + this.var_id + ", proc_id:" + this.proc_id + ", unit_id:" + this.unit_id + ", timestart:" + this.timestart.toISOString() + ", timeend:" + this.timeend.toISOString() + ", geom:" + this.geom.toString() + ", estacion_id:" + this.estacion_id + ", red_id:" + this.red_id + ", options:" + this.options.toString() + ", seriesRegulares:" + (this.seriesRegulares) ? ("[" + this.seriesRegulares.map(s=>s.toString()).join(",") + "]") : "[]"
	}
	toCSV() {
		return
	}
	toCSVless() {
		return
	}
}

internal.campo = class extends baseModel {
	constructor() {
        super()
		switch(arguments.length) {
			case 1: 
				this.filter = arguments[0].filter
				this.options = arguments[0].options
				this.proc_id = arguments[0].proc_id
				this.procedimiento = arguments[0].procedimiento
				this.series = arguments[0].series
				this.timestart = arguments[0].timestart
				this.timeend = arguments[0].timeend
	            this.unidades=arguments[0].unidades
	            this.unit_id=arguments[0].unit_id
				this.var_id=arguments[0].var_id
				this.variable=arguments[0].variable
				break;
			default:
				this.filter = arguments[0]
				this.options = arguments[1]
				this.proc_id = arguments[2]
				this.procedimiento = arguments[3]
				this.series = arguments[4]
				this.timestart = arguments[5]
				this.timeend = arguments[6]
	            this.unidades=arguments[7]
	            this.unit_id=arguments[8]
				this.var_id=arguments[9]
				this.variable=arguments[10]
				break;
		}
	}
	toString() {
		return JSON.stringify(this)
	}
	toCSV() {
		var csv = [
			"# VARIABLE=" + this.variable.abrev,
			"# TIMESTART=" + this.timestart.toISOString(),
			"# TIMEEND=" + this.timeend.toISOString(),
			"# PROCEDIMIENTO=" + this.procedimiento.abrev,
			"# UNIDADES=" + this.unidades.abrev,
			"",
			"lon,lat,estacion_id,nombre,tabla,red_id,valor,count"
		]
		return csv.concat(this.series.map(s=> sprintf("%.5f,%.5f,%d,%-40s,%-24s,%d,%f,%d", s.estacion.geom.coordinates[0],s.estacion.geom.coordinates[1],s.estacion.id,s.estacion.nombre,s.estacion.tabla,s.estacion.red_id,parseFloat(s.valor),s.count))).join("\n")
	}
	toCSVless() {
		return
	}
	toGeoJSON() {
		var features = this.series.map(s=> {
			return  {
				type: "Feature",
				geometry: s.estacion.geom,
				properties: {
					estacion_id: s.estacion.id,
					nombre: s.estacion.nombre,
					tabla: s.estacion.tabla,
					red_id: s.estacion.red_id,
					series_id: s.id,
					variable: this.variable.abrev,
					procedimiento: this.procedimiento.abrev,
					unidades: this.unidades.abrev,
					timestart: this.timestart,
					timeend: this.timeend,
					valor: parseFloat(s.valor),
					count: s.count
				}
		    }
		})
		return  {
			type: "FeatureCollection", 
			features: features, 
			"@properties": {
				filter: this.filter,
				options: this.options,
				proc_id: this.proc_id,
				procedimiento: this.procedimiento,
				timestart: this.timestart,
				timeend: this.timeend,
	            unidades: this.unidades,
	            unit_id: this.unit_id,
				var_id: this.var_id
			}
		}
	}
	toGrid(params={}) {
		var rand = Math.random().toString().substring(2,8)
		var geojson_file = (params.geojson_file) ? params.geojson_file : "/tmp/campo_" + rand + ".geojson" 
		var grid_file = (params.grid_file) ? params.grid_file : "/tmp/campo_" + rand + ".tif" 
		return fs.writeFile(geojson_file,JSON.stringify(this.toGeoJSON()))
		.then(()=> {
			var sys_call
			sys_call = "gdal_grid -zfield valor -l campo -outsize 300 300 -txe -70.0 -40.0 -tye -10.0 -40.0 -a nearest:radius1=2.0:radius2=2.0:angle=0.0:nodata=9999.0 -of GTiff " + geojson_file + " " + grid_file
			return new Promise( (resolve,reject) =>{
				exec(sys_call, (err, stdout, stderr)=>{
					if(err) {
						reject(err)
					}
					resolve(stdout)
				})
			})
			.then(result=>{
				console.log(result)
				return fs.readFile(grid_file,{encoding:'hex'})
			}).then(data=>{
				if (params.series_id) {
					return new internal.observacion({tipo:"raster",series_id:params.series_id,timestart:this.timestart,timeend:this.timeend,valor: '\\x' + data})
				}					
				return new internal.observacion({tipo:"raster",timestart:this.timestart,timeend:this.timeend,valor:'\\x' + data})
			})
		})
	}
}

const toFixedFloat = function(value,decimals) {
	if(value != null) {
		return parseFloat(parseFloat(value).toFixed(decimals))
	}
	return null
}

internal.observacionStats = class extends baseModel {
	constructor() {
        super()
		// %.2f,%d,%d,%d,%.2f,%.2f,%s
		if(arguments[0]) {
			this.percentage_of_average = toFixedFloat(arguments[0].percentage_of_average,2)
			this.rank = (arguments[0].rank != null) ? parseInt(arguments[0].rank) : null
			this.count = (arguments[0].count != null) ? parseInt(arguments[0].count) : null
			this.month = (arguments[0].month != null) ? parseInt(arguments[0].month) : null
			this.historical_monthly_mean = toFixedFloat(arguments[0].historical_monthly_mean,2)
			this.weibull_percentile = toFixedFloat(arguments[0].weibull_percentile,2)
			this.percentile_category = (arguments[0].percentile_category) ? {
				name: arguments[0].percentile_category.name, 
				range: (arguments[0].percentile_category.range  != null) ? [toFixedFloat(arguments[0].percentile_category.range[0],2), toFixedFloat(arguments[0].percentile_category.range[1],2)] : undefined,
				number: arguments[0].percentile_category.number,
			} : null
		}
	}
	toJSON() {
		return {
			"percentage_of_average": toFixedFloat(this.percentage_of_average,2),
			"rank": this.rank,
			"count": this.count,
			"month": this.month,
			"historical_monthly_mean": toFixedFloat(this.historical_monthly_mean,2),
			"weibull_percentile": toFixedFloat(this.weibull_percentile,2),
			"percentile_category": (this.percentile_category) ? {name: this.percentile_category.name, range: (this.percentile_category.range  != null) ? [toFixedFloat(this.percentile_category.range[0],2), toFixedFloat(this.percentile_category.range[1],2)] : undefined} : null
		}
	}
}

internal.observacion = class extends baseModel {
	constructor() {
		super()
		var tipo, series_id, timestart, timeend, nombre, descripcion, unit_id, timeupdate, valor, scale, offset, options, stats
		var opt_fields = ['id','descripcion','nombre','unit_id','timeupdate']
		switch(arguments.length) {
			case 1:
				if(typeof(arguments[0]) == "string") {
					[this.id,this.tipo, this.series_id, this.timestart, this.timeend, this.nombre, this.descripcion, this.unit_id, this.timeupdate, this.valor, this.scale, this.offset, this.count, this.options, this.stats] = arguments[0].split(",")
					opt_fields.forEach(key=>{
						if(this[key]) {
							if(this[key] == "undefined") {
								this[key] = undefined
							}
						}
					})
				} else {
					if(arguments[0].valor != null) {
						if(arguments[0].valor instanceof Buffer) {
							this.valor = arguments[0].valor
						} else if(arguments[0].valor instanceof Object && arguments[0].valor.type == "Buffer") {
							this.valor = Buffer.from(arguments[0].valor.data)
						} else {
							this.valor = arguments[0].valor
						}
					} else if(arguments[0].filename) {
						this.valor = fs.readFileSync(arguments[0].filename)
					}
					this.id = arguments[0].id
					this.tipo = (arguments[0].tipo) ? (arguments[0].tipo == "rast") ? "raster" : arguments[0].tipo : undefined
					this.series_id=arguments[0].series_id
					this.timestart=(arguments[0].timestart) ? timeSteps.parseDateString(arguments[0].timestart) : undefined
					this.timeend=(arguments[0].timeend) ? timeSteps.parseDateString(arguments[0].timeend) : undefined
					this.nombre=arguments[0].nombre
					this.descripcion=arguments[0].descripcion
					this.unit_id=arguments[0].unit_id
					this.timeupdate= (arguments[0].timeupdate) ? timeSteps.parseDateString(arguments[0].timeupdate) : new Date()
					
					this.scale=arguments[0].scale
					this.offset=arguments[0].offset
					this.count=arguments[0].count
					this.options=arguments[0].options
					this.stats = (arguments[0].stats != null && Object.keys(arguments[0].stats).length) ? new internal.observacionStats(arguments[0].stats) : undefined
				}
				break;
			default:
				[this.tipo, this.series_id, this.timestart, this.timeend, this.nombre, this.descripcion, this.unit_id, this.timeupdate, this.valor, this.scale, this.offset, this.count, this.options, this.stats] = arguments
				break;
		}
		this.timeupdate = (this.timeupdate) ? this.timeupdate : new Date()
		//~ console.log(this.timestart instanceof Date)
		//~ if(! this.timestart instanceof Date) {
			//~ this.timestart = new Date(this.timestart)
		//~ }
		//~ console.log(this.timeend instanceof Date)
		//~ if(! this.timeend instanceof Date) {
			//~ this.timeend = new Date(this.timeend)
		//~ }
		this.timestart = (typeof this.timestart == 'string') ? timeSteps.parseDateString(this.timestart) : this.timestart
		this.timeend = (typeof this.timeend == 'string') ? timeSteps.parseDateString(this.timeend) : this.timeend
		//~ console.log({timestart:this.timestart,timeend:this.timeend})
	}
	isValid() {
		var tipos = ["puntual","areal","raster"]
		return ( this.timestart.toString() == 'Invalid Date' || this.timeend.toString() == 'Invalid Date' || parseInt(this.series_id).toString() == "NaN" || !this.isValidValue() || !tipos.includes(this.tipo)) ? false : true
	}
	isValidValue() {
		if(this.valor === null) {
			return true
		}
		this.setValType()
		switch(this.val_type) {
			case "num":
				return (parseFloat(this.valor).toString() != "NaN")
				break;
			case "numarr":
				var numarr = this.valor.filter(v=>parseFloat(v).toString() != "NaN")
				if(numarr.length == this.valor.length) {
					return true
				} else {
					return false
				}
				break;
			case "rast":
				return (this.value instanceof Buffer)
				break;
			default:
				return false
		}
	}
				
	setValType(valType) {  // si valType es nulo y this.valor es no nulo se determina val_type de acuerdo al tipo de this.valor
		if(valType) {
			const valid = ["num","numarr","rast"]
			if(valType == "num") {
				this.val_type = "num"
			} else if(valType == "numarr") {
				this.val_type = "numarr"
			} else if (valType == "rast") {
				this.val_type
			} else {
				console.error("invalid value type")
				
			}
		} else if (!this.val_type && this.valor) {
			if(Array.isArray(this.valor)) {
				var numarr = this.valor.filter(v=>parseFloat(v).toString() != "NaN")
				if(numarr.length > 0) {
					this.val_type = "numarr"
				} else {
					console.error("value type no detectado: array no numérico")
				}
			} else if (parseFloat(this.valor).toString() != "NaN") {
				this.val_type = "num"
			} else if (this.valor instanceof Buffer) {
				this.val_type = "rast"
			} else {
				console.error("value type no detectado")
			}
		}
		return this.val_type
	}
	async getId(pool,client) {
		var release_client = false
		if(!client) {
			release_client = true
			if(pool) {
				client = await pool.connect()
			} else {
				client = await global.pool.connect()
			}
		}
		if(this.tipo == "areal") {
			try {
				var res = await client.query("\
					SELECT id FROM observaciones_areal WHERE series_id=$1 AND timestart=$2 AND timeend=$3\
				",[this.series_id, this.timestart, this.timeend])
				if (res.rows.length>0) {
					this.id = res.rows[0].id
					return this.id
				} else {
					res = await client.query("\
						SELECT max(id)+1 AS id\
						FROM observaciones_areal\
						")
					this.id = res.rows[0].id
					return this.id
				}
			}
			catch(e) {
				throw(e)
			}
			finally {
				if(release_client) {
					client.release()
				}
			}
		} else if (this.tipo == "rast") {
			try {
				var res = await client.query("\
					SELECT id FROM observaciones_rast WHERE series_id=$1 AND timestart=$2 AND timeend=$3\
				",[this.series_id, this.timestart, this.timeend])
				if (res.rows.length>0) {
					this.id = res.rows[0].id
					return this.id
				} else {
					res = await client.query("\
						SELECT max(id)+1 AS id\
						FROM observaciones_rast\
						")
					this.id = res.rows[0].id
					return this.id
				}
			}
			catch(e) {
					throw(e)
				}
			finally {
				if(release_client) {
					client.release()
				}
			}
		} else {
			try {
				var res = await client.query("\
					SELECT id FROM observaciones WHERE series_id=$1 AND timestart=$2 AND timeend=$3\
				",[this.series_id, this.timestart, this.timeend])
				if (res.rows.length>0) {
					this.id = res.rows[0].id
					return
				} else {
					res = await client.query("\
					SELECT max(id)+1 AS id\
					FROM observaciones\
					")
					this.id = res.rows[0].id
					return
				}
			}
			catch(e) {
				throw(e)
			}
			finally {
				if(release_client) {
					client.release()
				}
			}
		}
	}
	
	// rasterToJSON() {
	// 	const tmpFile = tmp.fileSync({mode: "0644", prefix: 'RasterObs',postfix:'.tif'})
	// 	this.toRaster(tmpFile.name)
	// 	const dataset = gdal.open(tmpFile.name)
	// 	const as_json = gdalDatasetToJSON(dataset)
	// 	fs.rmSync(tmpFile.name)
	// 	return as_json

	// }

	toJSON() {
		return {
			id: (this.id) ? parseInt(this.id) : null, 
			tipo: (this.tipo) ? this.tipo : "puntual",
			series_id: (this.series_id) ? parseInt(this.series_id) : null,
			timestart: (this.timestart) ? this.timestart.toISOString() : null, 
			timeend: (this.timeend) ? this.timeend.toISOString() : null,
			nombre: (this.nombre) ? this.nombre : null,
			descripcion: (this.descripcion) ? this.descripcion : null,
			unit_id: (this.unit_id) ? parseInt(this.unit_id) : null,
			timeupdate: (this.timeupdate) ? this.timeupdate.toISOString() : null,
			valor: (this.valor !== undefined) ? this.valor : null, // (this.valor != null) ? (this.tipo == "rast" || this.tipo == "raster") ? this.rasterToJSON() : parseFloat(this.valor) : null,
			stats: (this.stats) ? this.stats : null
		}
	}
	toString() {
		var valor = (this.valor) ? (this.tipo == "rast" || this.tipo == "raster") ? "rasterFile" : this.valor.toString() : "null"
		return "{" + "id:" + this.id + ", tipo:" + this.tipo + ", series_id:" + this.series_id + ", timestart:" + this.timestart.toISOString() + ", timeend:" + this.timeend.toISOString() + ", nombre:" + this.nombre + ", descrpcion:" + this.descripcion + ", unit_id:" + this.unit_id + ", timeupdate:" + this.timeupdate.toISOString() + ", valor:" + valor + "}"
	}
	// toJSON() {
	// 	return JSON.stringify({
	// 		id: this.id,
	// 		tipo: this.tipo,
	// 		series_id: this.series_id,
	// 		timestart: (this.timestart) ? this.timestart.toISOString() : undefined,
	// 		timeend: (this.timeend) ? this.timeend.toISOString() : undefined,
	// 		nombre: this.nombre,
	// 		descripcion: this.descripcion,
	// 		unit_id: this.unit_id,
	// 		timeupdate: (this.timeupdate) ? this.timeupdate.toISOString() : undefined,
	// 		valor: this.valor
	// 	})
	// }
	/**
	 * parses csv string into new observacion. Takes only first line of csv_string
	 */
	static fromCSV(csv_string,sep=",",header=["id","tipo","series_id","timestart","timeend","nombre","descripcion","unit_id","timeupdate","valor","stats.percentage_of_average","stats.rank","stats.count","stats.month","stats.historical_monthly_mean","stats.weibull_percentile","stats.percentile_category.name","stats.percentile_category.range.0","stats.percentile_category.range.1","stats.percentile_category.number"]) {
		var data = CSV.parse(csv_string,sep)[0]
		const observacion = {}
		for(var i in header) {
			// console.log(header[i])
			if(data[i] != null) {
				if(typeof data[i] == 'string') {
				    if(/^\s*$/.test(data[i])) {
						// console.log("skips empty string")
						continue
					} else if(/^\s*null\s*$/.test(data[i])) {
						// console.log("skips string='null'")
						continue
					} else {
						if(header[i].split(".").length > 1) {
							// console.log("set deep value")
							setDeepValue(observacion,header[i],data[i])
						} else if(header[i] == "valor" && isNumeric(data[i])) {
							observacion[header[i]] = parseFloat(data[i])
						} else {
							// console.log("set string value")
							observacion[header[i]] = data[i]		
						}
					}
				} else {
					// console.log("set non string value")
					observacion[header[i]] = data[i]
				}
			} else {
				// console.log("skip null")
			}
		}
		// console.log(observacion)
		return new this(observacion)
	}
	toCSV(options={}) {
		var sep = options.sep ?? ","
		var format_string_1 = ",%.2f,%d,%d,%d,%.2f,%.2f,%s,%.2f,%.2f,%d".replace(/\,/g,sep)
		var stats_string = (this.stats) ? sprintf(
			format_string_1, 
			this.stats.percentage_of_average,
			this.stats.rank,
			this.stats.count,
			this.stats.month,
			this.stats.historical_monthly_mean,
			this.stats.weibull_percentile, 
			this.stats.percentile_category.name, 
			this.stats.percentile_category.range[0], 
			this.stats.percentile_category.range[1],
			this.stats.percentile_category.number) : ""
		const result = [this.id,((this.tipo) ? this.tipo : "puntual"),this.series_id,((this.timestart) ? this.timestart.toISOString() : ""),((this.timeend) ? this.timeend.toISOString() : ""),this.nombre,((this.descripcion) ? this.descripcion : ""),((this.unit_id) ? this.unit_id : ""),((this.timeupdate) ? this.timeupdate.toISOString() : ""),((parseFloat(this.valor).toString() !== 'NaN') ? this.valor.toString() : "")].join(sep) + stats_string
		return result 
	}
	toCSVcat(options={}) {
		var sep = options.sep ?? ","
		return [
			(this.timestart) ? this.timestart.toISOString().substring(0,10) : "",
			(this.stats) ? this.stats.percentile_category.number : ""
		].join(sep)
	}
	toCSVless(include_id=true) {
		//~ return this.series_id + "," + ((this.timestart) ? this.timestart.toISOString() : "null") + "," +  ((parseFloat(this.valor)) ? this.valor.toString() : "null")
		return ((include_id) ? (this.series_id) ? `${this.series_id},` : "," : "") + ((this.timestart) ? this.timestart.toISOString() : "") + "," + ((this.timeend) ? this.timeend.toISOString() : "") + "," +  ((parseFloat(this.valor).toString() !== 'NaN') ? this.valor.toString() : "")
		
	}
	toMnemos(codigo_de_estacion,codigo_de_variable) {
		return [codigo_de_estacion,codigo_de_variable,sprintf("%02d",this.timestart.getDate()),sprintf("%02d", this.timestart.getMonth()+1),sprintf("%04d", this.timestart.getFullYear()),sprintf("%02d", this.timestart.getHours()),sprintf("%02d", this.timestart.getMinutes()),this.valor].join(",")
	}
	toRaster(output_file) {
		if(this.valor != null) {
			fs.writeFileSync(output_file,new Buffer.from(this.valor))
			// await delay(100)
			// var opt_md = (this.id) ? ` -mo "id=${this.id}"` : ""
			// execSync(`gdal_edit.py -mo "series_id=${this.series_id}" -mo "timestart=${this.timestart.toISOString()}" -mo "timeend=${this.timeend.toISOString()}" -mo "timeupdate=${this.timeupdate.toISOString()}" ${opt_md} ${output_file}`) 
		} else {
			logger.error(`No raster data found for series_id:${this.series_id}, timestart:${this.timestart.toISOString()}. Skipping`)
		}
	}
	static fromRaster(input_file) {
		if(!fs.existsSync(input_file)) {
			throw("Input raster file not found")
		}
		var file_info = execSync(`gdalinfo -json ${input_file}`,{encoding:'utf-8'})
		file_info = JSON.parse(file_info)
		const buffer = fs.readFileSync(input_file)
		return new internal.observacion({
			tipo: "raster",
			valor: buffer,
			id: file_info["metadata"][""]["id"],
			series_id: file_info["metadata"][""]["series_id"],
			timestart: new Date(file_info["metadata"][""]["timestart"]),
			timeend: new Date(file_info["metadata"][""]["timeend"]),
			timeupdate: new Date(file_info["metadata"][""]["timeupdate"])
		})
	}

	static settable_parameters = {
		"valor": {
			type: "any"
		}, 
		"timeupdate": {
			type: "date"
		}, 
		"nombre": {
			type: "string"
		}, 
		"descripcion": {
			type: "string"
		}, 
		"unit_id": {
			type: "integer"
		}, 
		"timestart": {
			type: "date"
		}, 
		"timeend": {
			type: "date"
		}, 
		"series_id": {
			type: "integer"
		},
		"tipo": {
			"type": "string"
		}
	}

	set(changes={}) {
		for(var key of Object.keys(changes)) {
			if(Object.keys(this.constructor.settable_parameters).indexOf(key) < 0) {
				// console.error(`Can't update parameter ${key}`)
				continue
			} 
			this[key] = parseField(this.constructor.settable_parameters[key],changes[key])
		}
	}

	setStats(stats) {
		if(stats != null && Object.keys(stats).length) {
			this.stats = new internal.observacionStats(stats)
		} else {
			this.stats = undefined
		}
	}

	async create(options={}) {
		return internal.CRUD.upsertObservacion(this,options.no_update)
	}

	static async read() {
		const observaciones = await internal.observaciones.read(...arguments)
		delete observaciones.metadata
		return observaciones
	}

	static async update() {
		const observaciones = await internal.observaciones.update(...arguments)
		delete observaciones.metadata
		return observaciones
	}

	static async delete() {
		const observaciones = await internal.observaciones.delete(...arguments)
		if(observaciones != undefined) {
			delete observaciones.metadata
		}
		return observaciones
	}

}

internal.observaciones_metadata = class extends baseModel {
	constructor() {
        super()
		if(!arguments.length) {
			return
		}
		var valid_md_keys = ["dt","t_offset","timestart","timeend","var_id","proc_id","estacion_id","fuentes_id","pivot","skip_nulls"]
		for(var key of valid_md_keys) {
			if(arguments[0][key]) {
				this[key] = arguments[0][key]
			}
		}
	}
	toCSV() {
		return Object.keys(this).map(key=>`${key},${this[key]}`).join("\n")
	}
}

internal.observacionPivot = class extends baseModel {
	constructor(observacion, options={}) {
		super()
		this.timestart = observacion.timestart
		this.timeend = observacion.timeend
		// all other keys must be series identifiers
		if(options.string_keys) {
			this._string_keys = true
			for(const key of Object.keys(observacion).filter(k=> (k != "timestart" && k != "timeend")).map(k=>k.toString()).sort()) {
				this[key] = observacion[key]
			}
		} else {
			this._string_keys = false
			for(const key of Object.keys(observacion).filter(k=> (k != "timestart" && k != "timeend")).map(k=>parseInt(k)).sort()) {
				if (key.toString() == "NaN") {
					throw(new Error("Invalid series identifier %s" % key.toString()))
				}
				this[key.toString()] = observacion[key]
			}
		}
	}

	toJSON() {
		const o = {}
		for (const key of Object.keys(this).filter(k =>
			(!/^_/.test(k))
		)) {
			o[key] = this[key]
		}
		return o
	}

	getSeriesHeaders() {
		return Object.keys(this).filter(k => 
			(k != "timestart" && k != "timeend" && !/^_/.test(k))
		).map(k => 
			(this._string_keys) ? k.toString() : parseInt(k)
		).sort()
	}

	getCSVHeader(options={}) {
		return ["timestart", "timeend", ...this.getSeriesHeaders()].join(options.sep ?? ",")
	}

	toCSV(options={}) {
		var sep = options.sep ?? ","
		if(options.headers) {
			var headers = options.headers
		} else {
			headers = this.getSeriesHeaders()
		}
		var values = headers.map(key=>{
			if(this[key]) {
				return this[key]
			} else {
				return "NULL"
			}
		}).join(sep)
		return `${new Date(this.timestart).toISOString()}${sep}${new Date(this.timeend).toISOString()}${sep}${values}`
	}
}


internal.observaciones = class extends BaseArray {
	constructor(arr,options) {
		if(arr) {
			super(...[])
			for (const x of arr) this.push(new internal.observacion(x))
		} else {
			super(arr)
		}
		this.getObsClass()
		if(options) {
			this.metadata = new internal.observaciones_metadata(options)
			if(options && options.pivot) {
				if(options.skip_nulls) {
					this.pivot()
				} else {
					if(this.metadata.timestart && this.metadata.timeend && this.metadata.dt && this.metadata.t_offset) {
						this.pivotRegular()
					} else {
						this.pivot()
					}
				}
			}
		} else {
			this.metadata = new internal.observaciones_metadata()
		}
		this.getTipo()
	}
	getObsClass() {
		this.forEach(o=>{
			if(!o instanceof internal.observacion) {
				o = new internal.observacion(o)
			}
		})
	}
	toJSON() {
		return this.map(o=>o.toJSON())
	}
    toString() {
        return this.map(o=>o.toString()).join("\n")
    }
	static fromCSV(csv_string,sep=",",has_header=true) {
		const data = []
		const metadata = {}
		var lines = csv_string.split("\n")
		var header_flag = (has_header) ? false : true
		var header
		for(var i in lines) {
			if(/^\s*#/.test(lines[i])) {
				// line is comment, check if it contains metadata
				var content = lines[i].replace(/^\s*#+\s*/,"").split("=")
				if(content.length>=2) {
					// save metadata key value pair
					content = content.map(s=>s.replace(/^\s+/,"").replace(/\s+$/,""))
					metadata[content[0]] = content[1]
				}
			} else if(/^\s*$/.test(lines[i])) {
				// empty line, skip
				continue
			} else {
				// first non-comment, non-empty line must be the header
				if(header_flag === false) {
					header_flag = true
					header = CSV.parse(lines[i],sep)[0]
					continue
				}
				// rest is data
				data.push(internal.observacion.fromCSV(lines[i],sep,header))

			}
		}
		metadata.header = header
		return new this(data,{metadata:metadata})

	}
    /**
	 * Returns csv string representing this object
	 * @param {*} options 
	 * @param {string} [options.delimiter=,]
	 * @param {Boolean} [options.hasMonthlyStats=false]
	 * @returns {string}
	 */
	toCSV(options={}) {
		var sep = (options.delimiter) ? options.delimiter : ","
		if(this.metadata && this.metadata.pivot) {
			var headers = Array.from(this.headers)
			var h1 = headers.shift()
			var h2 = headers.shift()
			headers.sort((a,b)=>a-b)
			var header = `${h1}${sep}${h2}${sep}${headers.join(sep)}\n`
			var body = this.map(o=>{
				var values = headers.map(key=>{
					if(o[key] != undefined) {
						return o[key]
					} else {
						return "NULL"
					}
				}).join(sep)
				return `${o.timestart.toISOString()}${sep}${o.timeend.toISOString()}${sep}${values}`
			}).join("\n")
			return `${header}${body}`
		} else {
			var header = `id${sep}tipo${sep}series_id${sep}timestart${sep}timeend${sep}nombre${sep}descripcion${sep}unit_id${sep}timeupdate${sep}valor`
			if(options.hasMonthlyStats) {
				header = header + `${sep}stats.percentage_of_average${sep}stats.rank${sep}stats.count${sep}stats.month${sep}stats.historical_monthly_mean${sep}stats.weibull_percentile${sep}stats.percentile_category.name${sep}stats.percentile_category.range.0${sep}stats.percentile_category.range.1`
			}
			return `${header}\n${this.map(o=>o.toCSV(sep)).join("\n")}`
		}
	}
	toCSVcat(options = {}) {
		var sep = (options.delimiter) ? options.delimiter : ","
		return `date${sep}flowcat\n${this.map(o=>o.toCSVcat(sep)).join("\n")}`
	}
    toCSVless(include_id=true) {
		if (include_id) {
	        return this.map(o=>o.toCSVless(True)).join("\n")
		} else {
			return this.map(o=>o.toCSVless(False)).join("\n")
		}
    }
    toMnemos(estacion_id,var_id) {
		var var_matches = Object.keys(config.snih.variable_map).filter(key=>{
			return (config.snih.variable_map[key].var_id == var_id)
		})
		var codigo_de_variable
		if(var_matches.length <= 0) {
			console.error("Variable id " + var_id + " no encontrado en config.snih.variable_map")
			codigo_de_variable = null
		} else {
			codigo_de_variable = var_matches[0]
		}
		return this.map(o=>o.toMnemos(estacion_id,codigo_de_variable)).join("\n")
    }
    removeDuplicates() {   // elimina observaciones con timestart duplicado
		var timestarts = []
		const filtered = []
		for(var i=0;i<this.length;i++) { 
			if(timestarts.indexOf(this[i].timestart.toISOString()) >= 0) {
				console.info("removing duplicate observacion, timestart: "+ this[i].timestart.toISOString())
				// this.splice(i,1)
				// i--
				
			} else {
				timestarts.push(this[i].timestart.toISOString())
				filtered.push(this[i])
			} 
		}
		return new this.constructor(filtered)
	}
	map(fn) {
		var result = []
		var i = -1
		this.forEach(o=>{
			i = i + 1
			if(!fn) {
				result.push(o)
			} else {
				if(typeof fn !== 'function') {
					throw("map argument must be a function")
				}
				result.push(fn(o,i,this))
			}
		})
		return result
	}
	pivot(inline=true) {
		var pivoted = {}
		var headers = new Set(["timestart","timeend"])
		this.forEach(o=>{
			if(!o.series_id) {
				throw("can't pivot: missing series_id")
			}
			const key = o.timestart.toISOString()
			if(!pivoted[key]) {
				pivoted[key] = {
					timestart: o.timestart, 
					timeend: o.timeend
				}
			}
			pivoted[key][o.series_id] = o.valor
			headers.add(o.series_id)
		})
		var result = Object.keys(pivoted).sort().map(key=>{
			return new internal.observacionPivot(pivoted[key])
		})
		if(inline) {
			this.length = 0
			this.push(...result)
			this.headers = headers
		}
		return result
	}
	pivotRegular(timestart,timeend,dt,t_offset,inline=true) {
		if(!timestart) {
			if(!this.metadata || !this.metadata.timestart) {
				throw("missing timestart")
			}
			timestart = this.metadata.timestart
		}
		if(!timeend) {
			if(!this.metadata || !this.metadata.timeend) {
				throw("missing timeend")
			}
			timeend = this.metadata.timeend
		}
		if(!dt) {
			if(!this.metadata.dt) {
				dt = {days:1}
			}
			dt = this.metadata.dt
		}
		if(!t_offset) {
			if(!this.metadata.t_offset) {
				t_offset = {hours:0}
			}
			t_offset = this.metadata.t_offset
		}
		timestart = timeSteps.setTOffset(timestart,t_offset)
		var dates = timeSteps.dateSeq(timestart,timeend,dt)
		this.sort((a,b)=>(a.timestart.getTime() - b.timestart.getTime()))
		var start = 0
		var end = response.length - 1
		var headers = new Set(["timestart","timeend"])
		var pivoted = dates.map(date=>{
			var row = {
				timestart: date,
				timeend: timeSteps.advanceInterval(date,dt)
			}
			for(var i=start;i<=end;i++) {
				var o = this[i]
				if(o.timestart.getTime() == date.getTime()) {
					row[o.series_id] = o.valor
					headers.add(o.series_id)
				} else if (o.timestart.getTime() > date.getTime()) {
					start = i
					break
				}
			}
			return row
		})
		if(inline) {
			this.length = 0
			this.push(...pivoted)
			this.headers = headers // Array.from(headers)
		}
		return pivoted
	}
	getTipo() {
		for(var o of this) {
			if(o.tipo) {
				this.metadata.tipo = o.tipo.toString()
				return o.tipo.toString()
			}
		}
		// console.error("observaciones tipo not defined")
		return
	}
	setSeriesId(series_id) {
		for(var o of this) {
			o.series_id = series_id
		}
	}
	setTipo(tipo) {
		for(var o of this) {
			o.tipo = tipo
		}
	}
	async create(options={}) {
		var tipo = this.getTipo()
		if(!this.length) {
			console.warn("No observaciones to create")
			return []
		}
		if(tipo == "raster") {
			const results = []
			for(var observacion of this) {
				results.push(await internal.CRUD.upsertObservacion(observacion,options.no_update))
			}
			return new internal.observaciones(results)
		}
		var query = this.createQuery(tipo,options)
		// console.debug(query)
		return new internal.observaciones(await executeQueryReturnRows(query,undefined))
	}
	static async create(observaciones, options={}) {
		const observaciones_instance = new internal.observaciones(observaciones)
		return observaciones_instance.create(options)
	}
	createQuery(tipo,options={}) {
		tipo = (tipo) ? tipo : "puntual"
		var val_on_conflict_clause = (options.no_update) ? "DO NOTHING" : "DO UPDATE"
		var obs_on_conflict_clause = (options.update_obs_metadata) ? "DO UPDATE SET timeupdate=excluded.timeupdate, nombre=excluded.nombre, descripcion=excluded.descripcion,unit_id=excluded.unit_id" : "DO NOTHING"
		var obs_table = (tipo == "areal") ? "observaciones_areal" : "observaciones"
		var val_table = (tipo == "areal") ? "valores_num_areal" : "valores_num"
		if(!this.length) {
			throw("Nothing to create (no observaciones)")
		}
		var union_excluded = (options.update_obs_metadata) ? "" : `union all
		select ${obs_table}.id,${obs_table}.series_id,${obs_table}.timestart,${obs_table}.timeend,${obs_table}.timeupdate,${obs_table}.unit_id,${obs_table}.nombre,${obs_table}.descripcion,candidates.valor
			from ${obs_table} join candidates ON (${obs_table}.series_id=candidates.series_id and ${obs_table}.timestart=candidates.timestart and ${obs_table}.timeend=candidates.timeend)`
		const filtered_obs = this.filter(o=>o.valor!=null)
		if(this.map(o=>(o.series_id) ? true : false).indexOf(false) >= 0) {
			throw(new Error("Missing series_id"))
		}
		if(this.map(o=>(o.timestart) ? true : false).indexOf(false) >= 0) {
			throw(new Error("Missing timestart"))
		}
		var params = [JSON.stringify(filtered_obs),tipo] // [JSON.stringify(this),tipo]
		return pasteIntoSQLQuery(`with candidates as (
			select * from json_populate_recordset(null::observacion_num,$1)
			),
		inserted_obs as (insert into ${obs_table} (series_id,timestart,timeend,timeupdate,nombre,descripcion,unit_id)
			select series_id,
				timestart,
				timeend,
				coalesce(timeupdate,now()),
				coalesce(nombre,'CRUD.serie.createObservacionesQuery'),
				descripcion,
				unit_id
			from candidates
			on conflict(series_id,timestart,timeend)
			${obs_on_conflict_clause}
			returning *),
		inserted_union_excluded as (
			select inserted_obs.id,inserted_obs.series_id,inserted_obs.timestart,inserted_obs.timeend,inserted_obs.timeupdate,inserted_obs.unit_id,inserted_obs.nombre,inserted_obs.descripcion,candidates.valor 
				from inserted_obs join candidates ON (inserted_obs.series_id=candidates.series_id and inserted_obs.timestart=candidates.timestart and inserted_obs.timeend=candidates.timeend)
			${union_excluded}
		),
		inserted_val as (
			insert into ${val_table} (obs_id,valor)
				select id,valor
				from inserted_union_excluded
			on conflict (obs_id)
			${val_on_conflict_clause}
			set valor=excluded.valor
			returning *)
		select inserted_union_excluded.id,$2 AS tipo,inserted_union_excluded.series_id,inserted_union_excluded.timestart,inserted_union_excluded.timeend,inserted_union_excluded.descripcion,inserted_union_excluded.timeupdate,inserted_union_excluded.nombre,inserted_union_excluded.unit_id,inserted_val.valor
		from inserted_union_excluded join inserted_val on (inserted_union_excluded.id=inserted_val.obs_id)
		order by inserted_union_excluded.series_id, inserted_union_excluded.timestart;
		`,params)
	}
	static async read(filter={},options) {
		return internal.CRUD.getObservaciones(filter.tipo,filter,options)
	}
	async update(changes={}) {
		for(var o of this) {
			o.set(changes)
			// console.log(o)
		}		
		const options = (["timeupdate","nombre","descripcion","unit_id"].some(k=> Object.keys(changes).includes(k))) ? {update_obs_metadata: true} : undefined
		return this.create(options)
	}
	static async update(filter,changes={}) {
		const observaciones = await this.read(filter,undefined)
		return observaciones.update(changes)
	}
	async delete(options={}) {
		var ids = this.map(o=>o.id).filter(id=>id)
		if(!ids.length) {
			console.error("Observaciones id not found for deletion")
			return
		}
		console.log("Found " + ids.length + " observaciones. Deleting")
		return internal.CRUD.deleteObservacionesById(this.getTipo(),ids,options.no_send_data)
	}
	static async delete(filter={},options={}) {
		var tipo = (filter.tipo) ? filter.tipo : "puntual"
		return internal.CRUD.deleteObservaciones(tipo,filter,options)
	}
	/**
	 * Exports observaciones as json files (one for each serie)
	 * @param {*} filter - one or more series or observaciones filter
	 * @param {*} options 
	 * @param {string} options.output - output file name
	 * @returns 
	 */
	static async backup(filter={},options={}) {
		return internal.CRUD.backupObservaciones(filter.tipo,filter,options)
	}
	/**
	 * Saves observaciones into archive tables
	 * @param {*} filter - one or more series or observaciones filter
	 * @param {*} options
	 * @param {Boolean} options.no_send_data - if true, return count of saved observaciones instead of the array of observaciones
	 * @param {Boolean} options.no_update - if true, conflicting rows in archive table will not update
	 * @param {Boolean} options.delete - if true, it deletes records from operational tables (they may be restored with .restore())
	 * @returns {internal.observaciones}
	 */
	static async archive(filter={},options={}) {
		if(options.delete) {
			const delete_options = {
				save: true,
				no_send_data: options.no_send_data,
				no_update: options.no_update
			}
			return internal.CRUD.deleteObservaciones2(filter.tipo,filter,delete_options)
		}
		return internal.CRUD.guardarObservaciones(filter.tipo,filter,options={})
	}
	// restore
	/**
	 * Reads archived observations and restores them into the operational tables. Deletes records from the archive
	 * @param {object} filter 
	 * @param {string} filter.tipo
	 * @param {integer} filter.id
	 * @param {integer} filter.series_id
	 * @param {Date} filter.timestart
	 * @param {Date} filter.timeend
	 * @param {integer} filter.unit_id
	 * @param {Date} filter.timeupdate
	 * @param {integer} filter.var_id
	 * @param {integer} filter.proc_id
	 * @param {integer} filter.red_id
	 * @param {string} filter.tabla_id
	 * @param {integer} filter.estacion_id
	 * @param {integer} filter.fuentes_id
	 * @param {integer} filter.area_id
	 * @param {integer} filter.escena_id
	 * @param {*} options 
	 * @param {Boolean} options.no_send_data - if true, return number of restored rows instead of array of restored rows
	 * @returns {internal.observaciones|integer} - Array of restored observaciones (or number or restored observaciones if options.no_send_data=true)
	 */
	 static async restore(filter={},options={}) {
		return internal.CRUD.restoreObservaciones(filter.tipo,filter,options)
	}
}

internal.observacionesGuardadas = class extends internal.observaciones {
	// create
	// read
	static async read(filter={},options={}) {
		return internal.CRUD.getObservacionesGuardadas(filter.tipo,filter,options)
	}
	// update
	// delete

}


internal.dailyStats = class extends baseModel {
	constructor() {
        super()
		switch(arguments.length) {
			case 1:
				if(typeof(arguments[0]) == "string") {
					[this.tipo,this.series_id,this.doy, this.count, this.min, this.max, this.mean, this.p01, this.p10, this.p50, this.p90, this.p99, this.window_size,this.timestart, this.timeend] = arguments[0].split(",")
				} else {
					this.tipo = arguments[0].tipo
					this.series_id = arguments[0].series_id
					this.doy = arguments[0].doy
					this.count=arguments[0].count
					this.min=arguments[0].min
					this.max=arguments[0].max
					this.mean=arguments[0].mean
					this.p01=arguments[0].p01
					this.p10=arguments[0].p10
					this.p50=arguments[0].p50
					this.p90=arguments[0].p90
					this.p99=arguments[0].p99
					this.window_size=arguments[0].window_size
					this.timestart=arguments[0].timestart
					this.timeend=arguments[0].timeend
				}
				break;
			default:
				[this.tipo, this.series_id, this.doy, this.count, this.min, this.max, this.mean, this.p01, this.p10, this.p50, this.p90, this.p99, this.window_size, this.timestart, this.timeend] = arguments
				break;
		}
	}
	toString() {
		return JSON.stringify({tipo:this.tipo,series_id:this.series_id,doy:this.doy,count:this.count, min:this.min, max:this.max, mean:this.mean, p01:this.p01, p10:this.p10, p50:this.p50, p90:this.p90, p99:this.p99, window_size:this.window_size, timestart:this.timestart, timeend:this.timeend})
	}
	toCSV() {
		return this.tipo + "," + this.series_id + "," + this.doy+ "," + this.count+ "," + this.min+ "," + this.max+ "," + this.mean+ "," + this.p01 + "," + this.p10+ "," + this.p50+ "," + this.p90+ "," + this.p99 + "," + this.window_size + "," + this.timestart.toISOString() + "," + this.timeend.toISOString()
	}
	toCSVless() {
		return this.tipo + "," + this.series_id + "," + this.doy+ "," + this.count+ "," + this.min+ "," + this.max+ "," + this.mean+ "," + this.p01 + "," + this.p10+ "," + this.p50+ "," + this.p90+ "," + this.p99 + "," + this.window_size + "," + this.timestart.toISOString() + "," + this.timeend.toISOString()
	}
}

internal.monthlyStats = class extends baseModel {
	constructor() {
        super()
		switch(arguments.length) {
			case 1:
				if(typeof(arguments[0]) == "string") {
					[this.tipo,this.series_id,this.mon, this.count, this.min, this.max, this.mean, this.p01, this.p10, this.p50, this.p90, this.p99, this.timestart, this.timeend] = arguments[0].split(",")
				} else {
					this.tipo = arguments[0].tipo
					this.series_id = arguments[0].series_id
					this.mon = arguments[0].mon
					this.count=arguments[0].count
					this.min= arguments[0].min
					this.max= arguments[0].max
					this.mean=arguments[0].mean
					this.p01=arguments[0].p01
					this.p10=arguments[0].p10
					this.p50=arguments[0].p50
					this.p90=arguments[0].p90
					this.p99=arguments[0].p99
					this.timestart=new Date(arguments[0].timestart)
					this.timeend=new Date(arguments[0].timeend)
				}
				break;
			default:
				[this.tipo, this.series_id, this.mon, this.count, this.min, this.max, this.mean, this.p01, this.p10, this.p50, this.p90, this.p99, this.timestart, this.timeend] = arguments
				break;
		}
	}
	toString() {
		return JSON.stringify({tipo:this.tipo,series_id:this.series_id,mon:this.mon,count:this.count, min:this.min, max:this.max, mean:this.mean, p01:this.p01, p10:this.p10, p50:this.p50, p90:this.p90, p99:this.p99, timestart:this.timestart, timeend:this.timeend})
	}
	toCSV() {
		return this.tipo + "," + this.series_id + "," + this.mon + "," + this.count+ "," + this.min+ "," + this.max+ "," + this.mean+ "," + this.p01 + "," + this.p10+ "," + this.p50+ "," + this.p90+ "," + this.p99 + "," + this.timestart.toISOString() + "," + this.timeend.toISOString()
	}
	toCSVless() {
		return this.tipo + "," + this.series_id + "," + this.doy+ "," + this.count+ "," + this.min+ "," + this.max+ "," + this.mean+ "," + this.p01 + "," + this.p10+ "," + this.p50+ "," + this.p90+ "," + this.p99 + "," + this.timestart.toISOString() + "," + this.timeend.toISOString()
	}
	static async read(filter,options) {
		return internal.CRUD.getMonthlyStats(filter.tipo,filter.series_id,filter.public,true)
	}
	static async create(monthly_stats,options={}) {
		// monthly_stats.forEach(m=>{
		// 	if(options.series_id) {
		// 		m.series_id = options.series_id
		// 	}
		// 	if(options.tipo) {
		// 		m.tipo = options.tipo
		// 	}
		// 	m = (m instanceof internal.monthlyStats) ? m : new internal.monthlyStats(m)
		// })
		return internal.CRUD.insertMonthlyStats(monthly_stats)
	}

	async create() {
		return executeQueryReturnRows(`INSERT INTO series_mon_stats (tipo, series_id, mon,count ,min ,max ,mean ,p01 ,p10 ,p50 ,p90 ,p99 ,timestart ,timeend) \
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) \
		ON CONFLICT (tipo,series_id,mon) DO UPDATE SET count=excluded.count ,min=excluded.min ,max=excluded.max ,mean=excluded.mean ,p01=excluded.p01 ,p10=excluded.p10 ,p50=excluded.p50 ,p90=excluded.p90 ,p99=excluded.p99 ,timestart=excluded.timestart ,timeend=excluded.timeend RETURNING *`,[this.tipo,this.series_id,this.mon,this.count,this.min,this.max,this.mean,this.p01,this.p10,this.p50,this.p90,this.p99,this.timestart,this.timeend])
		.then(result=>{
			if(!result.length) {
				console.error("Nothing insterted/updated")
				return
			}
			return new internal.monthlyStats(result[0])
		})
	}

	set(changes={}) {
		var settable_parameters = ["count", "min", "max", "mean", "p01", "p10", "p50", "p90", "p99", "p99", "timestart", "timeend"]
		for(var key of Object.keys(changes)) {
			if(settable_parameters.indexOf(key) < 0) {
				// console.error(`Can't update parameter ${key}`)
				continue
			} 
			this[key] = changes[key]
		}
	}
	async update(changes) {
		this.set(changes)
		return this.create()
	}

	async delete() {
		return executeQueryReturnRows(`DELETE FROM series_mon_stats WHERE tipo=$1 AND series_id=$2 AND mon=$3`,[this.tipo,this.series_id,this.mon])
		.then(result=>{
			return result.map(r=> new internal.monthlyStats(r))
		})
	}

	static async delete(filter={},options) {
		filter.tipo = (filter.tipo) ? filter.tipo : "puntual"
		var valid_filters = {
			"series_id": {
				type: "integer"
			},
			"mon": {
				type: "integer"
			},
			"count": {
				type: "integer"
			}, 
			"min": {
				type: "number"
			},
			"max": {
				type: "number"
			},
			"mean": {
				type: "number"
			},
			"p01": {
				type: "number"
			},
			"p10": {
				type: "number"
			},
			"p50": {
				type: "number"
			},
			"p90": {
				type: "number"
			},
			"p99": {
				type: "number"
			},
			"timestart": {
				type: "date"
			},"timeend": {
				type: "date"
			}
		}
		
		if(tipo == "puntual") {
			valid_filters = {...valid_filters, 
				"estacion_id": {
					type: "integer",
					table: "series"
				},
				"var_id": {
					type: "integer",
					table: "series"
				},
				"proc_id": {
					type: "integer",
					table: "series"
				},
				"unit_id": {
					type: "integer",
					table: "series"
				},
				"tabla": {
					type: "string",
					table: "estaciones"
				}
			}
			var filter_string = internal.utils.control_filter2(valid_filters,filter,"series_mon_stats")
			if(filter_string.length) {
				filter_string = `AND ${filter_string}`
			}
			var query_string = `DELETE FROM series_mon_stats USING series, estaciones WHERE series_mon_stats.tipo='puntual' AND series.id=series_mon_stats.series_id AND estaciones.unid = series.estacion_id ${filter_string} ON RETURNING *`
		} else if (tipo=="areal") {
			valid_filters = {...valid_filters, 
				"area_id": {
					type: "integer",
					table: "series_areal"
				},
				"var_id": {
					type: "integer",
					table: "series_areal"
				},
				"proc_id": {
					type: "integer",
					table: "series_areal"
				},
				"unit_id": {
					type: "integer",
					table: "series_areal"
				},
				"fuentes_id": {
					type: "integer",
					table: "series_areal"
				}
			}
			var filter_string = internal.utils.control_filter2(valid_filters,filter,"series_mon_stats")
			if(filter_string.length) {
				filter_string = `AND ${filter_string}`
			}
			var query_string = `DELETE FROM series_mon_stats USING series_areal,areas_pluvio WHERE series_mon_stats.tipo='areal' AND series_areal.id=series_mon_stats.series_id AND areas_pluvio.unid=series_areal.area_id ${filter_string}  ON RETURNING *`
		} else if (tipo=="raster") {
			valid_filters = {...valid_filters, 
				"escena_id": {
					type: "integer",
					table: "series_rast"
				},
				"var_id": {
					type: "integer",
					table: "series_rast"
				},
				"proc_id": {
					type: "integer",
					table: "series_rast"
				},
				"unit_id": {
					type: "integer",
					table: "series_rast"
				},
				"fuentes_id": {
					type: "integer",
					table: "series_rast"
				}
			}
			var filter_string = internal.utils.control_filter2(valid_filters,filter,"series_mon_stats")
			if(filter_string.length) {
				filter_string = `AND ${filter_string}`
			}
			var query_string = `DELETE FROM series_mon_stats USING series_rast,escenas WHERE series_mon_stats.tipo='raster' AND series_rast.id=series_mon_stats.series_id AND escenas.id=series_rast.escena_id ${filter_string}  ON RETURNING *`
		} else {
			throw("Bad tipo")
		}
		return await executeQueryReturnRows(query_string)
		.then(result=>{
			return result.map(r=>{
				return new internal.monthlyStats(r)
			})
		})
	}
	static async compute(filter={},options={}) {
		// defaults to 30 year period ending at the start of calendar 2 years ago
		filter.tipo = (filter.tipo) ? filter.tipo : "puntual"
		var default_timestart = new Date(new Date().getUTCFullYear() - 32,0,1)
		var default_timeend = new Date(new Date().getUTCFullYear() - 2,0,1)
		// defaults to percentage_complete_threshold = 60
		options.percentage_complete_threshold = (options.percentage_complete_threshold) ? options.percentage_complete_threshold : 60
		filter.timestart = (filter.timestart) ? filter.timestart : default_timestart
		filter.timeend = (filter.timeend) ? filter.timeend : default_timeend
		const series = await internal.serie.read(filter)
		if(!series.length) {
			console.error("series not found")
			return
		}
		const results = []
		for(var i in series) {
			const serie = series[i]
			serie.getWeibullPercentiles(filter.timestart,filter.timeend,options.percentage_complete_threshold, true) 
			results.push(...serie.monthlyStats)
		}
		return results
	}
}

internal.monthlyStatsList = class extends baseModel {
	constructor() {
        super()
		this.varNames = ["tipo", "series_id", "mon", "count", "min", "max", "mean", "p01", "p10", "p50", "p90", "p99", "timestart", "timeend"]
		this.values = arguments[0].map(v=>{
			if(v instanceof internal.monthlyStats) {
				return v
			} else {
				return new internal.monthlyStats(v)
			}
		})
	}
	toString() {
		return JSON.stringify(this.values)
	}
	toCSV() {
		return "# " + this.varNames.join(",") + "\n" + this.values.map(v=>v.toCSV()).join("\n")
	}
	toCSVless() {
		return "# " + this.varNames.join(",") + "\n" + this.values.map(v=>v.toCSVless()).join("\n")
	}
	filter(predicate, thisArg) {
		this.values = this.values.filter(predicate, thisArg)
		return new this.constructor(this.values)
	}
}

internal.dailyStatsList = class extends baseModel {
	constructor() {
        super()
		this.varNames = ["tipo", "series_id", "doy", "count", "min", "max", "mean", "p01", "p10", "p50", "p90", "p99", "window_size", "timestart", "timeend"]
		this.values = arguments[0].map(v=>{
			if(v instanceof internal.dailyStats) {
				return v
			} else {
				return new internal.dailyStats(v)
			}
		})
	}
	toString() {
		return JSON.stringify(this.values)
	}
	toCSV() {
		return "# " + this.varNames.join(",") + "\n" + this.values.map(v=>v.toCSV()).join("\n")
	}
	toCSVless() {
		return "# " + this.varNames.join(",") + "\n" + this.values.map(v=>v.toCSVless()).join("\n")
	}

	getMonthlyValues2() {
		var monthly = {}
		this.values.filter(v=>(timeSteps.doy2date(v.doy) == 15)).forEach(v=>{
			var mon = timeSteps.doy2month(v.doy)
			monthly[mon] = v
			delete monthly[mon].doy
			delete monthly[mon].window_size
		})
		return monthly
	}
	
	getMonthlyValues() {
		var monthly = {}
		this.values.forEach(v=>{
			var month = timeSteps.doy2month(v.doy)
			if(!monthly[month]) {
				monthly[month] = {
					values: [v]
				}
			} else {
				monthly[month].values.push(v)
			}
		})
		Object.keys(monthly).forEach(key=>{
			monthly[key].min = monthly[key].values.reduce((a,b)=>(a<b.min)?a:b.min,monthly[key].values[0].min)
			monthly[key].max = monthly[key].values.reduce((a,b)=>(a>b.max)?a:b.max,monthly[key].values[0].max)
			monthly[key].mean = monthly[key].values.reduce((a,b)=>a+b.mean,0) / monthly[key].values.length
			var p01 = monthly[key].values.map(v=>v.p01).sort((a, b) => a - b)
			monthly[key].p01 = p01[parseInt(p01.length/2)]
			var p10 = monthly[key].values.map(v=>v.p10).sort((a, b) => a - b)
			monthly[key].p10 = p10[parseInt(p10.length/2)]
			var p50 = monthly[key].values.map(v=>v.p50).sort((a, b) => a - b)
			monthly[key].p50 = p50[parseInt(p50.length/2)]
			var p90 = monthly[key].values.map(v=>v.p90).sort((a, b) => a - b)
			monthly[key].p90 = p90[parseInt(p90.length/2)]
			var p99 = monthly[key].values.map(v=>v.p99).sort((a, b) => a - b)
			monthly[key].p99 = p99[parseInt(p99.length/2)]
			monthly[key].count = monthly[key].values.reduce((a,b)=>a+b.count,0)
			monthly[key].timestart = monthly[key].values.reduce((a,b)=>(a.getTime()<b.timestart.getTime())?a:b.timestart,monthly[key].values[0].timestart)
			monthly[key].timeend = monthly[key].values.reduce((a,b)=>(a.getTime()>b.timeend.getTime())?a:b.timeend,monthly[key].values[0].timeend)
			delete monthly[key].values
		})
		return monthly
	}
	
}

internal.doy_percentil = class extends baseModel {
	constructor() {
        super()
		for (let [key, value] of Object.entries(arguments[0])) {
			this[key] = value
		}
	}
	toString() {
		return JSON.stringify(this)
	}
	toCSV() {
		return this.tipo + "," + this.series_id+ "," + this.doy+ "," + this.percentil+ "," + this.valor+ "," + this.count+ "," + this.timestart.toISOString().substring(0,10)+ "," + this.timeend.toISOString().substring(0,10) + "," + this.window_size
	} 
	toCSVless() {
		return this.tipo + "," + this.series_id+ "," + this.doy+ "," + this.percentil+ "," + this.valor
	}
} 

internal.percentiles = class extends baseModel {
	constructor() {
        super()
		this.tipo = arguments[0].tipo
		this.series_id = arguments[0].series_id
		this.percentiles = arguments[0].percentiles.map(p=>new internal.percentil(p))
	}
	toString() {
		return JSON.stringify(this)
	}
	toCSV() {
		return this.percentiles.map(p=>p.toCSV(this.tipo,this.series_id)).join("\n")
	} 
	toCSVless() {
		return this.percentiles.map(p=>p.toCSVless(this.tipo,this.series_id)).join("\n")
	}
}

internal.percentil = class extends baseModel {
	constructor() {
        super()
		// for (let [key, value] of Object.entries(arguments[0])) {
		// 	this[key] = value
		// }
		this.tipo = arguments[0].tipo
		this.series_id = arguments[0].series_id
		this.percentile = arguments[0].percentile
		this.valor = arguments[0].valor
		this.timestart = arguments[0].timestart
		this.timeend = arguments[0].timeend
		this.count = arguments[0].count
		
	}
	toString() {
		return JSON.stringify(this)
	}
	toCSV(tipo,series_id) {
		var tipo = (tipo) ? tipo : this.tipo
		var series_id = (series_id) ? series_id : this.series_id
		return tipo + "," + series_id + "," + this.percentile+ "," + this.valor+ "," + this.count+ "," + this.timestart.toISOString().substring(0,10)+ "," + this.timeend.toISOString().substring(0,10)
	} 
	toCSVless(tipo,series_id) {
		var tipo = (tipo) ? tipo : this.tipo
		var series_id = (series_id) ? series_id : this.series_id
		return tipo + "," + series_id+ "," + this.percentile + "," + this.valor
	}
} 

internal.observacionDia = class extends baseModel {
	constructor() {
        super()
		this.date = arguments[0].date
		this.series_id = arguments[0].series_id
		this.var_id = arguments[0].var_id
		this.proc_id = arguments[0].proc_id
		this.unit_id = arguments[0].unit_id
		this.estacion_id = arguments[0].estacion_id
		this.valor = arguments[0].valor
		this.fuentes_id = arguments[0].fuentes_id
		this.area_id = arguments[0].area_id
	}
	toString() {
		return JSON.stringify(this)
	}
	toCSV() {
		return this.series_id + "," + this.date + "," + this.var_id + "," + this.proc_id  + "," + this.unit_id  + "," + this.estacion_id  + "," + this.valor
	}
	toCSVless() {
		return this.series_id + "," + this.date + "," + this.valor
	}
}

// sim

internal.modelo = class extends baseModel {
	constructor() {
        super()
		var m = arguments[0]
		this.id = (m.id) ? parseInt(m.id) : undefined
		this.nombre = m.nombre
		this.tipo = m.tipo
		this.def_var_id = (m.def_var_id) ? m.def_var_id : undefined
		this.def_unit_id = (m.def_unit_id) ? m.def_unit_id : undefined
		this.parametros = (m.parametros) ? m.parametros.map(p=>new internal.modelo_parametro(p)) : undefined
		this.forzantes = (m.forzantes) ? m.forzantes.map(f=>new internal.modelo_forzante(f)) : undefined
		this.estados = (m.estados) ? m.estados.map(e=>new internal.modelo_estado(e)) : undefined
		this.outputs = (m.outputs) ? m.outputs.map(o=>new internal.modelo_output(o)) : undefined
		this.sortArrays()
	}
	sortArrays() {
		sortByOrden(this.parametros)
		sortByOrden(this.forzantes)
		sortByOrden(this.estados)
		sortByOrden(this.outputs)
	}
	toString() {
		return JSON.stringify(this)
	} 
	toCSV() {
		return this.id + "," + this.nombre + "," + this.tipo
	} 
	toCSVless() {
		return this.id + "," + this.nombre + "," + this.tipo
	} 
	async create() {
		const required_fields = ["nombre", "tipo", "def_var_id", "def_unit_id"]
		required_fields.forEach(key=>{
			if(typeof this[key] === undefined) {
				throw("Invalid modelo. Missing " + key)
			}
			if(this[key] == null) {
				throw("Invalid modelo. " + key + " is null")
			}
		})
		const client = await global.pool.connect()
		await client.query("BEGIN")
		if(this.id) {
			var result = await client.query(`
				INSERT INTO modelos (
					id,
					nombre,
					tipo,
					def_var_id,
					def_unit_id
				)
				VALUES (
					$1,
					$2,
					$3,
					$4,
					$5
			    )
				ON CONFLICT (id) 
				DO UPDATE SET 
					nombre=excluded.nombre, 
					tipo=excluded.tipo, 
					def_var_id=excluded.def_var_id, 
					def_unit_id=excluded.def_unit_id 
				RETURNING *
			`, [this.id, this.nombre, this.tipo, this.def_var_id, this.def_unit_id])
		} else {
			var result = await client.query(`
				INSERT INTO modelos (
					nombre,
					tipo,
					def_var_id,
					def_unit_id
				)
				VALUES (
					$1,
					$2,
					$3,
					$4
			    )
				ON CONFLICT (name) 
				DO UPDATE SET 
					tipo=excluded.tipo, 
					def_var_id=excluded.def_var_id, 
					def_unit_id=excluded.def_unit_id 
				RETURNING *
			`, [this.nombre, this.tipo, this.def_var_id, this.def_unit_id])
		}
		if(!result.rows.length) {
			throw("Nothing created")
		}
		Object.assign(this,result.rows[0])
		if(this.parametros) {
			for(var j=0;j<this.parametros.length;j++) {
				// console.log(this.parametros[j])
				this.parametros[j].model_id = this.id 
				const parametro = await this.upsertParametroDeModelo(client,this.parametros[j])
				this.parametros[j].id = parametro.id
			}
		}
		if(this.estados) {
			for(var j=0;j<this.estados.length;j++) {
				// console.log(this.estados[j])
				this.estados[j].model_id = this.id 
				const estado = await this.upsertEstadoDeModelo(client,this.estados[j])
				this.estados[j].id = estado.id
			}
		}
		if(this.forzantes) {
			for(var j=0;j<this.forzantes.length;j++) {
				this.forzantes[j].model_id = this.id 
				const forzante = await this.upsertForzanteDeModelo(client,this.forzantes[j])
				this.forzantes[j].id = forzante.id
			}
		}
		if(this.outputs) {
			for(var j=0;j<this.outputs.length;j++) {
				this.outputs[j].model_id = this.id 
				const output = await this.upsertOutputDeModelo(client,this.outputs[j])
				this.outputs[j].id = output.id
			}
		}
		await client.query("COMMIT")		
		return this		
	}
	static async read(filter={}) {
		return internal.CRUD.getModelos(filter.id ?? filter.model_id, filter.tipo,filter.name)
	}

	static async delete(filter={}) {
		return internal.CRUD.deleteModelos(filter.id ?? filter.model_id,filter.tipo)
	}
}

internal.modelo_parametro = class extends baseModel {
	constructor() {
        super()
		var m = arguments[0]
		this.orden = parseInt(m.orden)
		this.nombre = (m.nombre) ? m.nombre.toString() : undefined
		this.lim_inf = (m.lim_inf) ? parseFloat(m.lim_inf) : -Infinity 
		this.lim_sup= (m.lim_sup) ? parseFloat(m.lim_sup) : Infinity
		this.range_min = (m.range_min) ? parseFloat(m.range_min) : undefined
		this.range_max = (m.range_max) ? parseFloat(m.range_max) : undefined
	}
	toString() {
		return JSON.stringify(this)
	} 
	toCSV() {
		return this.orden + "," + this.nombre + "," + this.lim_inf + "," + this.lim_sup + "," + this.range_min + "," + this.range_max
	} 
	toCSVless() {
		return this.orden + "," + this.nombre
	} 
}

internal.modelo_forzante = class extends baseModel {
	constructor() {
        super()
		var m = arguments[0]
		this.orden = parseInt(m.orden)
		this.nombre = (m.nombre) ? m.nombre.toString() : undefined
		this.var_id = (m.var_id) ? parseInt(m.var_id) : undefined
		this.unit_id = (m.unit_id) ? parseInt(m.unit_id) : undefined
		this.inst = (m.hasOwnProperty("inst")) ? m.inst : undefined
		this.tipo = (m.tipo) ? (m.tipo == 'puntual') ? "puntual" : "areal" : "areal"
	}
	toString() {
		return JSON.stringify(this)
	} 
	toCSV() {
		return this.orden + "," + this.nombre + "," + this.var_id + "," + this.unit_id + "," + this.inst + "," + this.tipo
	} 
	toCSVless() {
		return this.orden + "," + this.nombre
	} 
}
internal.modelo_estado = class extends baseModel {
	constructor() {
        super()
		var m = arguments[0]
		this.orden = parseInt(m.orden)
		this.nombre = (m.nombre) ? m.nombre.toString() : undefined
		this.range_min = m.range_min
		this.range_max = m.range_max
		this.def_val = m.def_val
	}
	toString() {
		return JSON.stringify(this)
	} 
	toCSV() {
		return this.orden + "," + this.nombre + "," + this.range_min + "," + this.range_max + "," + this.def_val
	} 
	toCSVless() {
		return this.orden + "," + this.nombre
	} 
}

internal.modelo_output = class extends baseModel {
	constructor() {
        super()
		var m = arguments[0]
		this.orden = parseInt(m.orden)
		this.nombre = (m.nombre) ? m.nombre.toString() : undefined
		this.var_id = m.var_id
		this.unit_id = m.unit_id
		this.inst = (m.hasOwnProperty("inst")) ? m.inst : undefined
		this.series_table = (m.series_table) ? (m.series_table == 'series_areal') ? "series_areal" : "series" : "series"
	}
	toString() {
		return JSON.stringify(this)
	} 
	toCSV() {
		return this.orden + "," + this.nombre + "," + this.var_id + "," + this.unit_id + "," + this.inst + "," + this.series_table
	} 
	toCSVless() {
		return this.orden + "," + this.nombre
	}
}

const classes = {
	"Calibrado": internal.Calibrado
}

internal.genericModel = class extends baseModel {
	constructor(model_name) {
		super()
		this.schema = model_name
	}
	// getModelName() {
		// var model_name = this.constructor.name
		// return model_name.charAt(0).toUpperCase() + model_name.slice(1)
	// 	return this.schema
	// }
	remove_null_properties() {
		for(var key in this) {
			if(this[key] === null) {
				delete this[key]
			}
		}
	}
	validate() {
		this.remove_null_properties()
		return internal.utils.validate_with_model(this,this.schema)
	}
	
}

internal.calibrado = class extends internal.genericModel {
	constructor() {
		super("Calibrado")
		var m = arguments[0]
		if(m.estados_iniciales && ! m.estados) {
			m.estados = m.estados_iniciales
		}
		this.id = (m.id) ? parseInt(m.id) : undefined
		this.nombre = (m.nombre) ? m.nombre.toString() : undefined
		this.model_id = m.model_id
		this.modelo = m.modelo
		this.activar = (m.hasOwnProperty("activar")) ? m.activar : true
		this.parametros = this.getParametros(m.parametros)
		this.forzantes = this.getForzantes(m.forzantes)
		this.estados = this.getParametros(m.estados)
		this.outputs = this.getOutputs(m.outputs)
		this.selected = (m.hasOwnProperty("selected")) ? m.selected : false
		this.out_id = m.out_id
		this.area_id = m.area_id
		this.tramo_id = m.tramo_id
		this.dt = (m.dt) ? timeSteps.createInterval(m.dt) : undefined
		this.t_offset = (m.t_offset) ? timeSteps.createInterval(m.t_offset) : undefined
		this.stats = m.stats
		this.corrida = m.corrida

		this.sortArrays()
	}
	toString() {
		return JSON.stringify(this)
	} 
	toCSV() {
		return this.id + "," + this.nombre + "," + this.model_id + "," + this.activar + "," + this.selected + "," + this.out_id + "," + this.area_id + "," + this.tramo_id + "," + this.dt + "," + this.t_offset
	} 
	toCSVless() {
		return this.orden + "," + this.nombre
	}
	setArrayProperties() {
		this.parametros = getValoresAndOrder(this.parametros)
		this.estados = getValoresAndOrder(this.estados)
		return
	}
	sortArrays() {
		sortByOrden(this.parametros)
		sortByOrden(this.estados)
		sortByOrden(this.forzantes)
		sortByOrden(this.outputs)
	}
	getParametros(parametros) {
		if(!parametros) {
			return
		}
		var orden = 0
		return parametros.map(p=>{
			orden = orden + 1
			var parametro = {}
			if(typeof p == "number") {
				parametro.valor = p
				parametro.orden = orden
			} else {
				parametro.valor = p.valor
				parametro.orden = (p.orden) ? p.orden : orden
			}
			if(parseFloat(parametro.valor).toString() == "NaN") {
				throw(`invalid parametro: ${parametro.valor}`)
			}
			return new internal.parametro(parametro)
		})
	}
	getForzantes(forzantes) {
		if(!forzantes) {
			return
		}
		var orden = 0 
		return forzantes.map(f=>{
			orden = orden + 1
			var forzante = {}
			if(typeof f == "number") {
				forzante.series_id = f 
				forzante.series_table = "series"
				forzante.orden = orden
			} else {
				forzante.series_id = f.series_id
				forzante.series_table = f.series_table
				forzante.orden = (f.orden) ? f.orden : orden
			}
			if(parseInt(forzante.series_id).toString() == "NaN") {
				throw(new Error(`invalid forzante: ${forzante.series_id}`))
			}
			return new internal.forzante(forzante)
		})
	}

	getOutputs(outputs) {
		if(!outputs) {
			return
		}
		var orden = 0 
		return outputs.map(f=>{
			orden = orden + 1
			var output = {}
			if(typeof f == "number") {
				output.series_id = f 
				output.series_table = "series"
				output.orden = orden
			} else {
				output.series_id = f.series_id
				output.series_table = f.series_table
				output.orden = (f.orden) ? f.orden : orden
			}
			if(parseInt(output.series_id).toString() == "NaN") {
				throw(new Error(`invalid output: ${output.series_id}`))
			}
			return new internal.forzante(output)
		})
	}

	async create() {
		const created = await internal.CRUD.upsertCalibrado(this)
		if(created) {
			Object.assign(this,created)
			return this
		}
		return
	}
	static async read(filter={},options={}) {
		return internal.CRUD.getCalibrados(filter.estacion_id,filter.var_id,filter.includeCorr,filter.timestart,filter.timeend,filter.cal_id,filter.model_id,filter.qualifier,filter.public,filter.grupo_id,options.no_metadata,options.group_by_cal,filter.forecast_date,options.includeInactive,filter.series_id,filter.nombre)
	}
	static async delete(filter={}) {
		return internal.CRUD.deleteCalibrados(filter)
	}
}

function getValoresAndOrder(array) {
	var uniques = []
	var ordenes = []
	if(!array || !Array.isArray(array) || array.length == 0) {
		return array
	}
	array.forEach(p=>{
		if(!ordenes.includes(p.orden)) {
			ordenes.push(p.orden)
			uniques.push(p)
		}
	})
	uniques.sort((a,b)=>{
		return a.orden - b.orden
	})
	return uniques.map(p=>p.valor)
}

function sortByOrden(array) {
	if(!array || !Array.isArray(array) || array.length == 0) {
		return
	}
	array.sort((a,b)=>{
		if (a.orden && b.orden) {
			return a.orden - b.orden
		} else {
			return -1
		}
	})
}

internal.parametro = class extends baseModel {
	constructor() {
        super()
		var m = arguments[0]
		this.orden = parseInt(m.orden)
		this.valor = parseFloat(m.valor)
		this.cal_id = m.cal_id
		this.id = m.id
	}
	toString() {
		return JSON.stringify(this)
	} 
	toCSV() {
		return this.orden + "," + this.valor
	} 
	toCSVless() {
		return this.orden + "," + this.valor
	} 
}

internal.parametroDeModelo = class extends baseModel {
	constructor() {
        super()
		var m = arguments[0]
		this.id = m.id
		this.model_id = m.model_id
		this.nombre = m.nombre
		this.lim_inf = m.lim_inf
		this.range_min = m.range_min
		this.range_max = m.range_max
		this.lim_sup = m.lim_sup
		this.orden = m.orden
	}
	toString() {
		return JSON.stringify(this)
	} 
	toCSV() {
		return [this.id,this.model_id,this.nombre,this.lim_inf,this.range_max,this.range_min,this.lim_sup,this.orden].join(",")
	} 
	toCSVless() {
		return this.orden + "," + this.nombre
	} 
}


internal.forzante = class extends baseModel {
	constructor() {
        super()
		var m = arguments[0]
		this.orden = parseInt(m.orden)
		this.series_id = parseInt(m.series_id)
		this.series_table = (m.series_table) ? (m.series_table.toLowerCase() == "series_areal") ? "series_areal" : (m.series_table.toLowerCase() == "rast" || m.series_table.toLowerCase() == "raster") ?  "series_rast" : "series" : "series"
		this.cal_id = m.cal_id
		this.id = m.id
		this.model_id = m.model_id
		this.cal = m.cal
	}
	toString() {
		return JSON.stringify(this)
	} 
	toCSV() {
		return this.orden + "," + this.series_id + "," + this.series_table
	} 
	toCSVless() {
		return this.orden + "," + this.series_id + "," + this.series_table
	} 
	async getSerie() {
		var tipo = (this.series_table == "series") ? "puntual" : (this.series_table == "series_areal") ? "areal" : (this.series_table == "series_rast") ? "raster" : "puntual"
		// try {
			this.serie = await internal.CRUD.getSerie(tipo,this.series_id,undefined,undefined,{no_metadata:true})
		// } catch(e) {
		// 	throw(e)
		if(!this.serie) {
			console.error("crud: forzante: no se encontró serie")
		}
	}
}

internal.forzanteDeModelo = class extends baseModel {
	constructor() {
        super()
		var m = arguments[0]
		this.id = m.id
		this.model_id = m.model_id
		this.orden = parseInt(m.orden)
		this.var_id = parseInt(m.var_id)
		this.unit_id = parseInt(m.unit_id)
		this.nombre = m.nombre
		this.inst = m.inst
		this.tipo = m.tipo
		this.required = m.required
	}
	toString() {
		return JSON.stringify(this)
	} 
	toCSV() {
		return [this.id,this.model_id,this.orden,this.var_id,this.unit_id,this.nombre,this.inst,this.tipo,this.required].join(",")
	} 
	toCSVless() {
		return [this.model_id,this.orden,this.nombre].join(",")
	} 
}

internal.estado = class extends baseModel {
	constructor() {
        super()
		var m = arguments[0]
		this.orden = parseInt(m.orden)
		this.valor = parseFloat(m.valor)
		this.cal_id = m.cal_id
		this.id = m.id
	}
	toString() {
		return JSON.stringify(this)
	} 
	toCSV() {
		return this.orden + "," + this.valor
	} 
	toCSVless() {
		return this.orden + "," + this.valor
	} 
}

internal.estadoDeModelo = class extends baseModel {
	constructor() {
        super()
		var m = arguments[0]
		this.id = m.id
		this.model_id = parseInt(m.model_id)
		this.orden = parseInt(m.orden)
		this.nombre = m.nombre
		this.range_min = m.range_min
		this.range_max = m.range_max
		this.def_val = m.def_val
	}
	toString() {
		return JSON.stringify(this)
	} 
	toCSV() {
		return [this.id,this.model_id,this.orden,this.nombre,this.range_min,this.range_max,this.def_val].join(",")
	} 
	toCSVless() {
		return this.orden + "," + this.nombre
	} 
}


internal.output = class extends baseModel {
	constructor() {
        super()
		var m = arguments[0]
		this.orden = parseInt(m.orden)
		this.series_id = parseInt(m.series_id)
		this.series_table = (m.tipo) ? (m.tipo == 'series_areal') ? "series_areal" : "series" : "series"
		this.cal_id = m.cal_id
		this.id = m.id
	}
	toString() {
		return JSON.stringify(this)
	} 
	toCSV() {
		return this.orden + "," + this.series_id + "," + this.series_table
	} 
	toCSVless() {
		return this.orden + "," + this.series_id + "," + this.series_table
	}
	async getSerie(pool) {
		var tipo = (this.series_table == "series") ? "puntual" : (this.series_table == "series_areal") ? "areal" : "puntual"
		try {
			const crud = new internal.CRUD(pool)
			this.serie = await crud.getSerie(tipo,this.series_id,undefined,undefined,{no_metadata:true})
		} catch(e) {
			console.error("crud: forzante: no se encontró serie")
		}
	}
}

internal.corrida = class extends baseModel {
	constructor() {
        super()
		var m = arguments[0]
		this.id = m.id
		this.forecast_date = new Date(m.forecast_date)
		this.series = (m.series) ? m.series.map(s=>new internal.SerieTemporalSim(s)) : []
		this.cal_id = m.cal_id
	}
	toString() {
		return JSON.stringify(this)
	} 
	toCSV() {
		return "# cor_id=" + this.id + "\n# forecast_date=" + this.forecast_date + "\n\n\t" + this.series.map(s=>s.toCSV()).join("\n").replace(/\n/g,"\n\t")
	} 
	toCSVless() {
		return this.id + "," + this.forecast_date
	}

	/**
	 * 
	 * @param {internal.corrida[]|internal.corrida} corridas 
	 * @returns {Promise<internal.corrida[]|internal.corrida>}
	 */
	static async create(corridas) {
		if(Array.isArray(corridas)) {
			const created = []
			for(const corrida of corridas) {
				if(corrida instanceof internal.corrida) {
					const c = new internal.corrida(corrida)
					created.push(await c.create())
				} else {
					created.push(await corrida.create())
				}
			}
			return created
		} else {
			// console.debug({is_corrida: (corridas instanceof internal.corrida)})
			if(corridas instanceof internal.corrida) {
				return corridas.create()
			} else {
				// console.debug("Instanciando corrida")
				const corrida = new this(corridas)
				return corrida.create()
			}
		}
	}

	async create() {
		const created = await internal.CRUD.upsertCorrida(this)
		if(created) {
			Object.assign(this,created)
			await this.updateSeriesDateRangeByCorId()
			return created
		}
		return
	}
	static async read(filter={},options={}) {
		const corridas = await internal.CRUD.getPronosticos(filter.cor_id ?? filter.id,filter.cal_id,filter.forecast_timestart,filter.forecast_timeend,filter.forecast_date,filter.timestart,filter.timeend,filter.qualifier,filter.estacion_id,filter.var_id,options.includeProno,filter.isPublic,filter.series_id,options.series_metadata,filter.cal_grupo_id,options.group_by_qualifier,filter.model_id,filter.tipo)
		return corridas
	}
	static async delete(filter={}) {
		return internal.CRUD.deleteCorridas(filter)
	}
	async getSeries(filter={},options={}) {
		filter.cor_id = this.id
		return internal.CRUD.getCorridaSeries(filter,options.series_metadata,options.group_by_qualifier)
	}
	async setSeries(series,filter={},options={}) {
		if(series) {
			this.series = series
		} else {
			this.series = await this.getSeries(filter,options)
		}
	}
	async setPronosticos(filter={},options={}) {
		for(var serie of this.series) {
			if((filter.series_id && filter.series_table) && (serie.series_table != filter.series_table || serie.series_id != filter.series_id)) {
				continue
			} 
			await serie.setPronosticos(filter,options)
		}
	}
	async getQuantileSeries(filter={},quantiles=[0,0.25,0.5,0.75,1],labels=["min","1st_quartile","median","3rd_quartile","max"],create) {
		for(var serie of this.series) {
			const old_pronosticos = serie.pronosticos
			await serie.setPronosticos(filter,undefined)
			// console.log("get quantiles. series_id:" + serie.series_id)
			await serie.getQuantileSeries(quantiles,labels,create)
			// console.log({series_id: serie.id, quantile_series: result})
			if(create) {
				serie.pronosticos = old_pronosticos
			}
		}
		await this.updateSeriesDateRange(filter)
		return
	}

	async getAggregateSeries(tipo,source_series_id,dest_series_id,estacion_id,dest_fuentes_id,source_var_id,dest_var_id,dest_tipo,timestart,timeend,time_step="day",agg_function="mean",precision=2,offset,utc,proc_id=4,create=true,client,from_view) {
		var release_client = false
		if(!client) {
			release_client = true
			client =  await global.pool.connect()
		}
		// estacion_id,source_tipo,source_series_id,dest_series_id,source_var_id=20,dest_var_id=90,qualifier,timestart,timeend,dest_fuentes_id
		if(!source_var_id) {
			throw new Error("Missing source_var_id")
		}
		if(!dest_var_id) {
			throw new Error("Missing dest_var_id")
		}
		if(!dest_tipo) {
			throw new Error("Missing dest_tipo")
		}
		var source_tipo = tipo ?? ((source_var_id == 20) ? "areal" : "puntual")
		var source_series_table = (source_tipo == "areal") ? "series_areal" : "series"
		// console.log({tipo:tipo, source_tipo: source_tipo, source_series_table: source_series_table})
		var series_dest = await internal.serie.read({tipo:dest_tipo, estacion_id:estacion_id,var_id:dest_var_id,series_id:dest_series_id,fuentes_id:dest_fuentes_id,proc_id:proc_id},{no_metadata:true,fromView:from_view})
		// console.log("series_dest: " + series_dest.length)
		const series_agg = []
		var pronos_to_create = []
		for(var serie of this.series) {
			// console.log("serie " + serie.series_id + ", qualifier " + serie.qualifier)
			if(source_series_id && source_series_id!=serie.series_id) {
				// console.log("series_id skipped")
				continue
			}
			if(source_var_id && source_var_id!=serie.var_id) {
				// console.log("var_id skipped")
				continue
			}
			if(estacion_id && estacion_id!=serie.estacion_id) {
				// console.log("estacion_id skipped")
				continue
			}
			if(source_series_table!=serie.series_table) {
				// console.log("series_table skipped")
				continue
			}
			if(serie.series_table == "series_areal") {
				var serie_dest = series_dest.filter(s=>s.area_id == serie.estacion_id)
			} else {
				var serie_dest = series_dest.filter(s=>s.estacion_id == serie.estacion_id)
			}
			if(!serie_dest.length) {
				console.error("destination series not found for daily mean, estacion_id:" + serie.estacion_id + ", var_id:" + serie.var_id + ", series_table: " + serie.series_table)
				continue
			}
			serie_dest = serie_dest[0]
			await serie.setPronosticos({timestart:timestart,timeend:timeend},undefined,client)
			// console.log("got " + serie.pronosticos.length  + " pronosticos")
			if(!serie.pronosticos.length) {
				continue
			}
			const s  = new internal.serie({
				series_table: serie.series_table,
				series_id: serie.series_id
			})
			s.setObservaciones(serie.pronosticos)
			const agg_timeseries = await s.aggregateTimeStep(undefined,undefined,time_step,agg_function,precision,undefined,undefined,1,undefined,undefined,offset,utc) // s.aggregate(undefined,undefined,"mean",2,"00:00:00",undefined,15,true,date_offset,true) 
			// console.log("Got " + monthly_timeseries.length + " monthly averages")
			const result = {
				series_table: serie.series_table,
				series_id: serie_dest.id,
				estacion_id: serie.estacion_id,
				var_id: serie_dest.var_id,
				qualifier: serie.qualifier,
				pronosticos: agg_timeseries
			}
			if(create) {
				pronos_to_create.push(...result.pronosticos.map(p=>{
					p.series_id = result.series_id
					p.series_table = result.series_table
					p.qualifier = result.qualifier
					p.cor_id = this.id
					return p
				}))
			} else {
				series_agg.push(result)
			}
			if(pronos_to_create.length >= 5000) {
				await internal.CRUD.upsertPronosticos(client,pronos_to_create)
				pronos_to_create.length = 0
			}
			serie.pronosticos.length = 0
		}
		if(pronos_to_create.length) {
			await internal.CRUD.upsertPronosticos(client,pronos_to_create)
		}
		if(release_client) {
			client.release()
		}
		if(create) {
			await this.updateSeriesDateRange({var_id: dest_var_id, estacion_id: estacion_id, tipo: dest_tipo, series_id: dest_series_id})
			return
		} else {
			return series_agg
		}	
	}

	async getMonthlyMean(filter={},date_offset=0,create=true,client) {
		var release_client = false
		if(!client) {
			release_client = true
			client =  await global.pool.connect()
		}
		// estacion_id,source_tipo,source_series_id,dest_series_id,source_var_id=20,dest_var_id=90,qualifier,timestart,timeend,dest_fuentes_id
		var source_var_id = filter.source_var_id ?? 20
		var dest_var_id = filter.dest_var_id ?? 90
		var source_tipo = filter.source_tipo ?? ((source_var_id == 20) ? "areal" : "puntual")
		var source_series_table = (source_tipo == "areal") ? "series_areal" : "series"
		var dest_tipo = filter.dest_tipo ?? ((dest_var_id == 90) ? "areal" : "puntual")
		var series_dest = await internal.serie.read({tipo:dest_tipo, estacion_id:filter.estacion_id,var_id:dest_var_id,series_id:filter.dest_series_id,fuentes_id:filter.dest_fuentes_id,proc_id:4},{no_metadata:true})
		const series_mensuales = []
		var pronos_to_create = []
		for(var serie of this.series) {
			console.log("serie " + serie.series_id + ", qualifier " + serie.qualifier)
			if(filter.source_series_id && filter.source_series_id!=serie.series_id) {
				continue
			}
			if(source_var_id && source_var_id!=serie.var_id) {
				continue
			}
			if(filter.estacion_id && filter.estacion_id!=serie.estacion_id) {
				continue
			}
			if(source_series_table!=serie.series_table) {
				continue
			}
			if(serie.series_table == "series_areal") {
				var serie_dest = series_dest.filter(s=>s.area_id == serie.estacion_id)
			} else {
				var serie_dest = series_dest.filter(s=>s.estacion_id == serie.estacion_id)
			}
			if(!serie_dest.length) {
				console.error("destination series not found for monthly mean, estacion_id:" + serie.estacion_id + " series_table: " + serie.series_table)
				continue
			}
			serie_dest = serie_dest[0]
			await serie.setPronosticos({timestart:filter.timestart,timeend:filter.timeend},undefined,client)
				// const c = await CRUD.corrida.read({cor_id:corrida.id, tipo: (dest_var_id == 90) ? "areal" : "puntual",series_id:serie.series_id, timestart: timestart, timeend:timeend, qualifier: serie.qualifier},{group_by_qualifier:true, includeProno:true})
				// console.log("got " + c[0].series[0].pronosticos.length + " pronosticos from cor_id: " + corrida.id + ", series_id: " + serie.series_id + ", qualifier: " + serie.qualifier)
			console.log("got " + serie.pronosticos.length  + " pronosticos")
			if(!serie.pronosticos.length) {
				continue
			}
			const s  = new internal.serie({
				series_table: serie.series_table,
				series_id: serie.series_id
			})
			s.setObservaciones(serie.pronosticos)
			const monthly_timeseries = s.aggregateMonthly(undefined,undefined,"mean",2,"00:00:00",undefined,15,true,date_offset,true) 
			// console.log("Got " + monthly_timeseries.length + " monthly averages")
			const result = {
				series_table: serie.series_table,
				series_id: serie_dest.id,
				estacion_id: serie.estacion_id,
				var_id: serie_dest.var_id,
				qualifier: serie.qualifier,
				pronosticos: monthly_timeseries
			}
			if(create) {
				pronos_to_create.push(...result.pronosticos.map(p=>{
					p.series_id = result.series_id
					p.series_table = result.series_table
					p.qualifier = result.qualifier
					p.cor_id = this.id
					return p
				}))
			} else {
				series_mensuales.push(result)
			}
			if(pronos_to_create.length >= 5000) {
				await internal.CRUD.upsertPronosticos(client,pronos_to_create)
				pronos_to_create.length = 0
			}
			serie.pronosticos.length = 0
		}
		if(pronos_to_create.length) {
			await internal.CRUD.upsertPronosticos(client,pronos_to_create)
		}
		if(release_client) {
			client.release()
		}
		if(create) {
			await this.updateSeriesDateRange({var_id: dest_var_id, estacion_id: filter.estacion_id, tipo: dest_tipo, series_id: filter.dest_series_id})
			return
		} else {
			return series_mensuales
		}
	}

	async updateSeriesDateRangeByCorId() {
		await internal.CRUD.updateSeriesPronoDateRangeByCorId(this.id)
	}

	async updateSeriesDateRange(filter={}) {
		await internal.CRUD.updateSeriesPronoDateRange(
			{
				cor_id: this.id,
				tipo: filter.tipo,
				estacion_id: filter.estacion_id,
				var_id: filter.var_id,
				series_id: filter.series_id,
				tabla: filter.tabla,
				qualifier: filter.qualifier
			}
		)
		await internal.CRUD.updateSeriesPronoDateRangeByQualifier(
			{
				cor_id: this.id,
				tipo: filter.tipo,
				estacion_id: filter.estacion_id,
				var_id: filter.var_id,
				series_id: filter.series_id,
				tabla: filter.tabla,
				qualifier: filter.qualifier
			}
		)
		return 
	}
}

internal.SerieTemporalSim = class extends baseModel {
	constructor() {
        super()
		var m = arguments[0]
		this.series_table = m.series_table
		this.series_id = m.series_id
		this.cor_id = m.cor_id
		this.qualifier = m.qualifier
		this.pronosticos = (m.pronosticos) ? m.pronosticos.map(p=>new internal.pronostico(p)) : []
		this.var_id = m.var_id
		this.begin_date = m.begin_date
		this.end_date = m.end_date
		this.qualifiers = m.qualifiers
		this.count = m.count
		this.estacion_id = m.estacion_id
	}
	toString() {
		return JSON.stringify(this)
	} 
	toCSV() {
		return "# series_table=" + this.series_table + "\n# series_id=" + this.series_id + "\n\n" + this.pronosticos.map(p=>p.toCSV()).join("\n")
	} 
	toCSVless() {
		return this.id + "," + this.forecast_date
	}

	async getPronosticos(filter={},options={},client) {
		filter.series_id = this.series_id
		filter.tipo = (this.series_table) ? (this.series_table == "series_areal") ? "areal" : (this.series_table == "series") ? "puntual" : undefined : undefined 
		filter.cor_id = this.cor_id
		filter.qualifier = filter.qualifier ?? this.qualifier
		return internal.CRUD.getPronosticosArray(filter,options,client)

	}

	async setPronosticos(filter={},options={},client) {
		this.pronosticos = await this.getPronosticos(filter,options,client)

	}

	async getQuantileSeries(quantiles=[0,0.25,0.5,0.75,1],labels=["min","1st_quartile","median","3rd_quartile","max"],create) {
		const pivot_table = {}
		const dates = {}
		const pronosticos_ensemble = []
		for(var p of this.pronosticos) {
			// filter by qualifier. Keep only ensemble members (qualifier parseable to Int)
			if (!/^\d+$/.test(p.qualifier)) {
				continue
			}
			const date_index = p.timestart.toISOString()
			if(!pivot_table.hasOwnProperty(date_index)) {
				pivot_table[date_index] = [
					p.valor
				]
				dates[date_index] = {
					timestart: p.timestart,
					timeend: p.timeend
				}
			} else {
				pivot_table[date_index].push(p.valor)
			}
			pronosticos_ensemble.push(p)
		}
		const quantile_series = {}
		for(var date_index of Object.keys(pivot_table)) {
			var q_index = -1
			for(var q of quantiles) {
				q_index = q_index + 1
				var value = quantile(pivot_table[date_index],q)
				var label = (labels && labels[q_index]) ? labels[q_index] : `q${q.toString()}`
				const pronostico = new internal.pronostico({
					cor_id: this.cor_id,
					series_id: this.series_id,
					series_table: this.series_table,
					timestart: dates[date_index].timestart,
					timeend: dates[date_index].timeend,
					valor: value,
					count: pivot_table[date_index].length,
					qualifier: label
				})
				if(!quantile_series.hasOwnProperty(label)) {
					quantile_series[label] = new internal.SerieTemporalSim({
						cor_id: this.cor_id,
						series_table: this.series_table,
						series_id: this.series_id,
						var_id: this.var_id,
						begin_date: this.begin_date,
						end_date: this.end_date,
						qualifier: label,
						qualifiers: this.qualifiers,
						count: this.count,
						estacion_id: this.estacion_id,				
						pronosticos: []
					})
					
				}
				quantile_series[label].pronosticos.push(pronostico)
			}
		}
		const result_series = Object.keys(quantile_series).map(key=> quantile_series[key])
		// console.log("got " + result_series.length + " quantile series")
		this.pronosticos = pronosticos_ensemble
		const client = await global.pool.connect()
		const pronosticos_to_create = []
		for(var serie of result_series) {
			this.pronosticos.push(...serie.pronosticos)
			if(create) {
				pronosticos_to_create.push(...serie.pronosticos)
				if(pronosticos_to_create.length>=5000) {
					await internal.CRUD.upsertPronosticos(client,pronosticos_to_create) // serie.create(client)
					pronosticos_to_create.length = 0
				}
			}
		}
		if(create) {
			if(pronosticos_to_create.length) {
				await internal.CRUD.upsertPronosticos(client,pronosticos_to_create)
				pronosticos_to_create.length = 0
			}
			client.release()
			return
		} else {
			client.release()
			return result_series
		}
	}

	async create(client) {
		await internal.CRUD.upsertPronosticos(client,this.pronosticos)
		return
	}
}

internal.pronostico = class extends baseModel {
	constructor() {
        super()
		var m = arguments[0]
		this.id = m.id
		this.timestart = m.timestart
		this.timeend = (m.timeend) ? m.timeend : m.timestart
		if(m.valor != null) {
			if(m.valor instanceof Buffer) {
				this.valor = m.valor
			} else if(m.valor instanceof Object && m.valor.type == "Buffer") {
				this.valor = Buffer.from(m.valor.data)
			} else {
				this.valor = m.valor
			}
		} else if(m.filename) {
			this.valor = fs.readFileSync(m.filename)
		}
		this.qualifier = m.qualifier
		this.cor_id = m.cor_id
		this.series_id = m.series_id
		this.series_table = m.series_table
	}
	toString() {
		return JSON.stringify(this)
	} 
	toCSV() {
		return this.id + "," + this.timestart.toISOString() + "," + this.timeend.toISOString() + "," + this.valor + "," + this.qualifier
	} 
	toCSVless() {
		return this.id + "," + this.timestart.toISOString() + "," + this.timeend.toISOString() + "," + this.valor + "," + this.qualifier
	}
	toRaster(output_file) {
		if(this.valor != null) {
			fs.writeFileSync(output_file,new Buffer.from(this.valor))
		} else {
			logger.error(`No raster data found for series_id:${this.series_id}, timestart:${this.timestart.toISOString()}. Skipping`)
		}
	}
	static fromRaster(input_file) {
		const observacion = internal.observacion.fromRaster(input_file)
		return new internal.pronostico({
			series_table: "series_rast",
			valor: observacion.valor,
			id: observacion.id, 
			series_id: observacion.series_id,
			timestart: observacion.timestart,
			timeend: observacion.timeend
		})
	}

	static async delete(filter={},options={}) {
		const deleted = await internal.CRUD.deletePronosticos(
			filter.cor_id,
			filter.cal_id,
			filter.forecast_date,
			filter.timestart,
			filter.timeend,
			options.only_sim,
			filter.series_id,
			filter.estacion_id,
			filter.var_id,
			filter.tipo,
			filter.fuentes_id,
			filter.tabla,
			filter.qualifier
		)
		if(options.no_send_data) {
			return
		} else {
			return deleted
		}
	}
	static async read(filter={},options={}) {
		// console.debug({filter:filter})
		const results = await internal.CRUD.getPronosticos(
			filter.cor_id,
			filter.cal_id,
			filter.forecast_timestart,
			filter.forecast_timeend,
			filter.forecast_date,
			filter.timestart,
			filter.timeend,
			filter.qualifier,
			filter.estacion_id,
			filter.var_id,
			options.includeProno,
			filter.isPublic,
			filter.series_id,
			options.series_metadata,
			filter.grupo_id,
			options.group_by_qualifier,
			filter.model_id,
			filter.tipo,
			filter.tabla
		)
		return results
	}
	static async create(pronosticos) {
		const results = await internal.CRUD.upsertPronosticos(await global.pool.connect(),pronosticos)
		if(options.no_send_data) {
			return
		} else {
			return results
		}
	}
}	

/**
 * Pronostico class represents a single timestamp - value pair that may be part of a forecast timeseries (class SerieTemporalSim)
 */
internal.Pronostico = class extends internal.pronostico {
	/**
	 * Retrieves pronosticos that match the provided filter
	 * @param {Object} filter - the query filter.
	 * @param{integer} filter.id
	 * @param{integer} filter.cal_id
	 * @param{integer} filter.cor_id
	 * @param{integer} filter.series_id
	 * @param{String}  filter.tipo - one of: puntual, areal, rast
	 * @param{Date}    filter.timestart
	 * @param{Date}    filter.timeend
	 * @param{Date}    filter.forecast_date
	 * @param{integer} filter.estacion_id
	 * @param{integer} filter.var_id
	 * @param{String}  filter.qualifier
     * @param{String}  filter.tabla
	 * @param {Object} options 
	 * @returns Array[Pronostico]
	 */
	static async read(filter={}, options={}) {
		return (await internal.CRUD.getPronosticosArray(filter, options)).map(p=>{
			return new internal.Pronostico(p)
		})
	}
}

// accessors

internal.Accessor = class extends baseModel {
	constructor(type, parameters) {
		switch (type.toLowerCase()) {
			case "gfs":
				this.type = "gfs"
				this.object = new Accessors.gfs(parameters)
				this.get = this.object.getAndReadGFS
				this.testConnect = this.object.testConnect
				break
			default:
				this.type = null
				this.object = null
				break
		}
	}
	printObs(format="object") {
		if(!this.object) {
			console.error("gfs object not instantiated")
			return
		}
		if(!this.object.observaciones) {
			console.error("no observations found")
			return
		}
		switch(format.toLowerCase()) {
			case "csv":
				return this.object.observaciones.map(o=>o.toCSV()).join("\n")
				break
			case "txt":
				return this.object.observaciones.map(o=>o.toString()).join("\n")
				break
			case "json":
				return JSON.stringify(this.object.observaciones)
				break
			case "pretty":
			case "pretty_json":
			case "json_pretty":
				return  JSON.stringify(this.object.observaciones,null,2)
				break
			default:
				return this.object.observaciones
		}
	}
}

internal.accessor = class extends baseModel {
	constructor() {
		super()
		this.class = arguments[0].class
		this.url = arguments[0].url
		this.series_tipo = arguments[0].series_tipo
		this.series_source_id = arguments[0].series_source_id
		this.name = arguments[0].name
		this.config = arguments[0].config
		this.series_id = arguments[0].series_id
		this.upload_fields = arguments[0].upload_fields
		this.title = arguments[0].title
	}
	static async create(data,options) {
		if(Array.isArray(data)) {
			var created = []
			for(var element of data) {
				created.push(await element.create())
			}
			return created
		} else {
			return [await data.create()]
		}
	}

	async create() { //~ upsertAccessor(accessor) {
		var result = await global.pool.query("INSERT INTO accessors (class,url,series_tipo,series_source_id,name,config,series_id,upload_fields,title) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (name) DO UPDATE SET class=excluded.class, url=excluded.url, series_tipo=excluded.series_tipo, series_source_id=excluded.series_source_id, config=excluded.config, series_id=excluded.series_id,upload_fields=excluded.upload_fields,title=excluded.title RETURNING *", [this.class, this.url, this.series_tipo, this.series_source_id, this.name, this.config, this.series_id, this.upload_fields, this.title])
		if(!result.rows.length) {
			throw("Nothing upserted")
		}
		const created = new internal.accessor(result.rows[0]) 
		Object.assign(this,created)
		return this
	}
	
	static async read(filter={}) {
		if(filter.name) {
			var result = await global.pool.query("SELECT * from accessors where name=$1",[filter.name])
			if(!result.rows.length) {
				console.error("Accessor not found")
				return []
			}
			return new internal.accessor(result.rows[0])
		} else {
			var filter_string = internal.utils.control_filter2({class:{type:"string"},url:{type:"string"},series_tipo:{type:"string"},series_source_id:{type:"integer"},series_id:{type:"integer"},title:{type:"string"}},filter)
			var result = await global.pool.query("SELECT * FROM accessors WHERE 1=1 " + filter_string + " ORDER BY name")
			return result.rows.map(r=>new internal.accessor(r))
		}
	}
	static async delete(filter={}) {
		if(filter.name) {
			var result = await global.pool.query("DELETE FROM accessors WHERE name=$1 RETURNING *",[filter.name])
			if(!result.rows.length) {
				console.error("Accessor not found")
				return []
			}
			return new internal.accessor(result.rows[0])
		} else {
			var filter_string = internal.utils.control_filter2({class:{type:"string"},url:{type:"string"},series_tipo:{type:"string"},series_source_id:{type:"integer"},series_id:{type:"integer"},title:{type:"string"}},filter)
			var result = await global.pool.query("DELETE FROM accessors WHERE 1=1 " + filter_string + " RETURNING *")
			return result.rows.map(r=>new internal.accessor(r))
		}
	}
	async delete() {
		var result = await global.pool.query("DELETE FROM accessors WHERE name=$1 RETURNING *",[this.name])
		if(!result.rows.length) {
			throw("Accessor not found: nothing deleted")
		}
		return new internal.accessor(result.rows[0])
	}	
}

internal.engine = class {
	constructor(pool,config_,schemas_){
        global.pool = pool
		if(config_) {
			this.config = config_
			if(this.config.database) {
				this.dbConnectionString = "host=" + this.config.database.host + " user=" + this.config.database.user + " dbname=" + this.config.database.database + " password=" + this.config.database.password + " port=" + this.config.database.port
			}
		} else {
			this.config = config
		}
		this.models = {
			"Calibrado": internal.calibrado,
			"Output": internal.output,
			"Forzante": internal.forzante,
			"Parametro": internal.parametro,
			"Estado": internal.estado,
			"Serie": internal.serie,
			"Fuente": internal.fuente,
			"Unidad": internal.unidades,
			"Procedimiento": internal.procedimiento,
			"Estacion": internal.estacion,
			"Variable": internal.var
		}
		this.foreign_keys_alias = {
			"calibrados_id": "cal_id",
			"variables_id": "var_id"
		}
		this.schemas = (schemas_) ? schemas_ : schemas		
	}    

	
	build_read_query(model_name,filter,table_name,options) {
		if(!schemas.hasOwnProperty(model_name)) {
			throw("model name not found")
		}
		var model = schemas[model_name]
		if(!model) {
			throw("model not found")
		}
		var model_class = this.models[model_name]
		// console.log({model_name: model_name, model:model, model_class: model_class})
		if(model_class.build_read_query) {
			return model_class.build_read_query(model_name,filter,table_name,options)
		}
		if(!table_name) {
			table_name = model.table_name
		}
		var child_tables = {}
		var meta_tables = {}
		var selected_columns = Object.keys(model.properties).filter(key=>{
			if(model.properties[key].type == 'array' && model.properties[key].hasOwnProperty("items") && model.properties[key].items.hasOwnProperty("$ref")) {
				child_tables[key] = model.properties[key].items.$ref.split("/").pop()
				return false
			} else if (model.properties[key].hasOwnProperty("$ref") && model.properties[key].hasOwnProperty("foreign_key")) {
				meta_tables[key] = model.properties[key].$ref.split("/").pop()
				return false
			} else {
				return true
			}
		})
		var filter_string = internal.utils.control_filter3(model,filter,table_name)
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
			order_by_clause = ` ORDER BY ${order_by.map(key => internal.utils.getFullKey(model, key, table_name)).join(",")}`
		}
		return {
			query: `SELECT ${selected_columns.map(key => internal.utils.getFullKey(model, key, table_name)).join(", ")} FROM "${table_name}" WHERE 1=1 ${filter_string}${order_by_clause}`,
			child_tables: child_tables,
			meta_tables: meta_tables,
			table_name: table_name
		}
	}

	read(model_name,filter) {
		if(!this.models.hasOwnProperty(model_name)) {
			return Promise.reject("Invalid model_name")
		}
		try {
			var parent_query = this.build_read_query(model_name,filter)
		} catch(e) {
			return Promise.reject(e)
		}
		// console.log(parent_query.query)
		return global.pool.query(parent_query.query)
		.then(result=>{
			// console.log("model_name: " + model_name)
			// console.log("typeof:" + typeof this.models[model_name])
			return result.rows.map(i=>new this.models[model_name](i))
		})
		.then(async parents=>{
			// console.log({child_tables:parent_query.child_tables})
			if(Object.keys(parent_query.child_tables).length) {
				for (var parent of parents) {
					for(var key in parent_query.child_tables) {
						var foreign_key = parent_query.table_name + "_id"
						foreign_key = (this.foreign_keys_alias[foreign_key]) ? this.foreign_keys_alias[foreign_key] : foreign_key
						var child_filter = {}
						child_filter[foreign_key] = parent.id
						var child_query  = this.build_read_query(parent_query.child_tables[key],child_filter)
						var query_result = await global.pool.query(child_query.query)
						// console.log({query_result:query_result.rows})
						parent[key] = query_result.rows.map(j=> new this.models[parent_query.child_tables[key]](j))
					}
				}
			}
			if(Object.keys(parent_query.meta_tables).length) {
				for (var parent of parents) {
					for(var key in parent_query.meta_tables) {
						var foreign_key = key + "s_id" // parent_query.table_name + "_id"
						foreign_key = (this.foreign_keys_alias[foreign_key]) ? this.foreign_keys_alias[foreign_key] : foreign_key
						var meta_filter = {
							"id": parent[foreign_key]
						}
						var meta_table_name = (parent.hasOwnProperty("series_table")) ? parent.series_table : undefined
						var meta_query  = this.build_read_query(parent_query.meta_tables[key],meta_filter,meta_table_name)
						// console.log(meta_query.query)
						var query_result = await global.pool.query(meta_query.query)
						// console.log({query_result:query_result.rows})
						if(!query_result.rows.length) {
							console.error("meta element not found")
						} else {
							parent[key] = query_result.rows.map(j=> new this.models[parent_query.meta_tables[key]](j))
						}
					}
				}
			}
			return parents
		})
	}

	
}

internal.tableConstraint = class {
	constructor() {
		this.table_name = arguments[0].table_name
		this.constraint_name = arguments[0].constraint_name
		this.constraint_type = arguments[0].constraint_type
		this.column_names = arguments[0].column_names
	}
	/**
	 * Check if passed array with column names matches exactly those of this.column_names
	 * @param {Array<string>} columns_names 
	 * @returns {Boolean}
	 */	
	check(column_names) {
		this.column_names.sort()
		column_names.sort()
		return this.column_names.length === column_names.length && this.column_names.every((value, index) => value === column_names[index])
	}
	// toString() {
	// 	return 
	// }
}

internal.asociacion = class extends baseModel {
	constructor() {
		super()
		this.id = arguments[0].id
		this.source_tipo = arguments[0].source_tipo
		this.source_series_id = arguments[0].source_series_id
		this.dest_tipo = arguments[0].dest_tipo
		this.dest_series_id = arguments[0].dest_series_id
		this.agg_func = arguments[0].agg_func
		this.dt = new Interval(arguments[0].dt)
		this.t_offset = new Interval(arguments[0].t_offset)
		this.precision = arguments[0].precision
		this.source_time_support = new Interval(arguments[0].source_time_support)
		this.source_is_inst = arguments[0].source_is_inst
		this.habilitar = arguments[0].habilitar
		this.expresion = arguments[0].expresion
		this.cal_id = arguments[0].cal_id
	}
	async create() {
		const created = new internal.asociacion(await internal.CRUD.upsertAsociacion(this))
		Object.assign(this,created)
		return created
	}
	static async create(data) {
		var asociaciones = await internal.CRUD.upsertAsociaciones(data)
		return asociaciones.map(a=>new internal.asociacion(a))
	}
	static async read(filter={},options) {
		if(filter.id) {
			var asociacion = await internal.CRUD.getAsociacion(filter.id)
			if(asociacion) {
				return [new this(asociacion)]
			} else {
				return []
			}
		}
		var asociaciones = await internal.CRUD.getAsociaciones(filter,options)
		return asociaciones.map(a=>new internal.asociacion(a))
	}
	async delete() {
		if(!this.id) {
			throw("Can't delete asociacion: missing id")
		}
		return new internal.asociacion(await internal.CRUD.deleteAsociacion(this.id))
	}
	static async delete(filter) {
		var asociaciones = await internal.asociacion.read(filter)
		var deleted = []
		for(var a of asociaciones) {
			deleted.push(await a.delete())
		}
		return deleted
	}
	async run(filter,options) {
		return internal.CRUD.runAsociacion(this.id,filter,options)
	}
}


internal.CRUD = class {
	static dbConnectionString = (global.config.database) ? "host=" + global.config.database.host + " user=" + global.config.database.user + " dbname=" + global.config.database.database + " password=" + global.config.database.password + " port=" + global.config.database.port : undefined
    // red //
    
	static async upsertRed(red) {
		return red.getId(global.pool)
		.then(()=>{
			return global.pool.query("\
			INSERT INTO redes (id,tabla_id,nombre,public,public_his_plata)\
			VALUES ($1,$2,$3,$4,$5)\
			ON CONFLICT (id)\
			DO UPDATE SET nombre=$3, public=$4, public_his_plata=$5\
			RETURNING *",[red.id,red.tabla_id,red.nombre,red.public,red.public_his_plata])
		}).then(result=>{
			if(result.rows.length<=0) {
				console.error("Upsert failed")
				return
			}
			console.log("Upserted redes.id=" + result.rows[0].id)
			return new internal.red(result.rows[0])
		}).catch(e=>{
			console.error(e)
		})
	}
	
	static async upsertRedes(redes) {
		var promises = []
		for(var i = 0; i < redes.length; i++) {
			promises.push(this.upsertRed(new internal.red(redes[i])))
		}
		return Promise.all(promises)
	}
	
	static async deleteRed(id) {
		return global.pool.query("\
			DELETE FROM redes\
			WHERE id=$1\
			RETURNING *",[id]
		).then(result=>{
			if(result.rows.length<=0) {
				console.log("id not found")
				return 
			}
			console.log("Deleted redes.id=" + result.rows[0].id)
			return new internal.red(result.rows[0])
		}).catch(e=>{
			console.error(e)
		})
	}
	static async getRed(id) {
		return global.pool.query("\
		SELECT id,tabla_id,nombre,public,public_his_plata from redes \
		WHERE id=$1",[id])
		.then(result=>{
			if(result.rows.length<=0) {
				console.log("Red no encontrada")
				return
			}
			return result.rows[0]
			//~ const red = new internal.red(result.rows[0].tabla_id, result.rows[0].nombre, result.rows[0].public, result.rows[0].public_his_plata)
			//~ red.getId(global.pool)
			//~ .then(()=>{
				//~ return red
			//~ })
		})
	}
	static async getRedes(filter) {
		//~ console.log(filter)
		const valid_filters = {nombre:"regex_string", tabla_id:"string", public:"boolean", public_his_plata:"boolean", id:"integer"}
		var filter_string=""
		var control_flag=0
		Object.keys(valid_filters).forEach(key=>{
			if(filter[key]) {
				if(/[';]/.test(filter[key])) {
					console.error("Invalid filter value")
					control_flag++
				}
				if(valid_filters[key] == "regex_string") {
					var regex = filter[key].replace('\\','\\\\')
					filter_string += " AND " + key  + " ~* '" + filter[key] + "'"
				} else if(valid_filters[key] == "string") {
					filter_string += " AND "+ key + "='" + filter[key] + "'"
				} else if (valid_filters[key] == "boolean") {
					var boolean = (/^[yYtTvVsS1]/.test(filter[key])) ? "true" : "false"
					filter_string += " AND "+ key + "=" + boolean + ""
				} else {
					filter_string += " AND "+ key + "=" + filter[key] + ""
				}
			}
		})
		if(control_flag > 0) {
			return Promise.reject(new Error("invalid filter value"))
		}
		//~ console.log("filter_string:" + filter_string)
		return global.pool.query("SELECT * from redes WHERE 1=1 " + filter_string)
		.then(res=>{
			var redes = res.rows.map(red=>{
				return new internal.red(red)
			})
			return redes
		})
		.catch(e=>{
			console.error(e)
			return null
		})
	}
	
	
	// estacion //
	
	/**
	 * Creates or updates estacion depending if the new instance conflicts with the (tabla, id_externo) unique key
	 * @param {internal.estacion} estacion - instance to create or update.
	 * @param {Object} options
	 * @param {Boolean} options.no_update
	 * @param {Boolean} options.no_update_id
	 * @param {pg.Client=} client - pg client to include in a transactional block.
	 * @returns {Promise<internal.estacion>} - the new or updated estacion instance
	 */
	static async upsertEstacion(estacion,options={},client) {
		if(!estacion.tabla || !estacion.id_externo) {
			throw("Missing estacion.tabla and/or estacion.id_externo")
		}
		var release_client = false
		if(!client) {
			client = await global.pool.connect()
			release_client = true
		}
		const estaciones = await this.getEstaciones({tabla:estacion.tabla, id_externo: estacion.id_externo},undefined,client)
		if(estaciones.length) {
			// console.log("instance found in database, updating non-key fields")
			estacion.id = undefined
			estacion = await this.updateEstacion(estacion,options,client)
		} else {
			// instance not found, inserting new row
			// console.log("estacion instance not found, inserting new row")
			var result = await client.query(this.upsertEstacionQuery(estacion,options))
			if(!result.rows.length) {
				console.error("Estacion not inserted")
			} else {
				estacion = new internal.estacion(result.rows[0])
			}
		}
		if(release_client) {
			await client.release()
		}
		return estacion
	}
	
	static async upsertEstacion_(estacion,options={}) {
		return estacion.getEstacionId()
		.then(()=>{
			console.log({id:estacion.id})
			var query = this.upsertEstacionQuery(estacion,options)
			return global.pool.query(query)
		}).then(result=>{
			if(result.rows.length<=0) {
				console.error("Upsert failed")
				return
			}
			console.log("Upserted estacion: " + JSON.stringify(result.rows[0],null,2))
			Object.keys(result.rows[0]).forEach(key=>{
				estacion[key] = result.rows[0][key]
			})
			if(estacion.nivel_alerta || estacion.nivel_evacuacion || estacion.nivel_aguas_bajas) {
				return this.upsertNivelesAlerta(estacion)
				.then(estacion=>{
					return new internal.estacion(estacion)
				})
			} else {
				return new internal.estacion(estacion)
			}
		}).catch(e=>{
			console.error(e)
			return
		})
	}

	static upsertEstacionQuery(estacion,options={}) {
		var onconflictaction = (options.no_update) ? "DO NOTHING" : (options.no_update_id || !estacion.id) ? "DO UPDATE SET \
				nombre=excluded.nombre,\
				geom=excluded.geom,\
				distrito=excluded.distrito,\
				pais=excluded.pais,\
				rio=excluded.rio,\
				has_obs=excluded.has_obs,\
				tipo=excluded.tipo,\
				automatica=excluded.automatica,\
				habilitar=excluded.habilitar,\
				propietario=excluded.propietario,\
				abrev=excluded.abrev,\
				URL=excluded.URL,\
				localidad=excluded.localidad,\
				real=excluded.real"  : "DO UPDATE SET \
				nombre=excluded.nombre,\
				geom=excluded.geom,\
				distrito=excluded.distrito,\
				pais=excluded.pais,\
				rio=excluded.rio,\
				has_obs=excluded.has_obs,\
				tipo=excluded.tipo,\
				automatica=excluded.automatica,\
				habilitar=excluded.habilitar,\
				propietario=excluded.propietario,\
				abrev=excluded.abrev,\
				URL=excluded.URL,\
				localidad=excluded.localidad,\
				real=excluded.real,\
				cero_ign=excluded.cero_ign,\
				altitud=excluded.altitud,\
				unid=excluded.unid,\
				ubicacion=excluded.ubicacion"
		var ins_query = (estacion.id) ? "\
		INSERT INTO estaciones (nombre, id_externo, geom, tabla,  distrito, pais, rio, has_obs, tipo, automatica, habilitar, propietario, abrev, URL, localidad, real, altitud, cero_ign, ubicacion, unid) \
		VALUES ($1, $2, st_setsrid(st_point($3, $4),4326), $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)" : "\
		INSERT INTO estaciones (nombre, id_externo, geom, tabla,  distrito, pais, rio, has_obs, tipo, automatica, habilitar, propietario, abrev, URL, localidad, real, altitud, cero_ign, ubicacion) \
		VALUES ($1, $2, st_setsrid(st_point($3, $4),4326), $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)" 
		var query = `${ins_query} ON CONFLICT (tabla,id_externo) ${onconflictaction}
			RETURNING unid id, nombre, id_externo, st_asGeoJSON(geom)::json geom, tabla, distrito, pais, rio, has_obs, tipo, automatica, habilitar, propietario, abrev AS abreviatura, URL as "URL", localidad, real, altitud, cero_ign, ubicacion`
		var params = [estacion.nombre, estacion.id_externo, estacion.geom.coordinates[0], estacion.geom.coordinates[1], estacion.tabla, estacion.provincia, estacion.pais, estacion.rio, estacion.has_obs, estacion.tipo, estacion.automatica, estacion.habilitar, estacion.propietario, estacion.abreviatura, estacion.URL, estacion.localidad, estacion.real, estacion.altitud, estacion.cero_ign, estacion.ubicacion]
		if(estacion.id) {
			params.push(estacion.id)
		}
		var querystring = internal.utils.pasteIntoSQLQuery(query,params)
		// console.log(querystring) 
		return querystring
	}

	/** 
	 * updates estacion where unid=estacion.id if estacion.id is defined, else updates estacion where tabla=estacion.tabla and id_externo=estacion.id_externo. Only updates fields that are defined in the provided instance.
	 * @param {internal.estacion} estacion - the instance to update
	 * @param {Object=} options - not used
	 * @param {pg.Client=} client - pg client to include in a transactional block.
	 * @returns {Promise<internal.estacion>} A promise to the updated estacion instance
	 */
	static async updateEstacion(estacion,options={},client) {
		var release_client = false
		if(!client) {
			client = await global.pool.connect()
			release_client = true
		}
		var query
		var query_params
		if(!estacion.id) {
			if(!estacion.tabla || !estacion.id_externo) {
				return Promise.reject("Falta id o tabla + id_externo")
			}
			query = `
			UPDATE estaciones 
			SET nombre=coalesce($1,nombre), 
			geom=coalesce(st_setsrid(st_point($3, $4),4326),geom), 
			distrito=coalesce($6,distrito), 
			pais=coalesce($7,pais), 
			rio=coalesce($8, rio), 
			has_obs=coalesce($9,has_obs),
			tipo=coalesce($10,tipo),
			automatica=coalesce($11,automatica), 
			habilitar=coalesce($12, habilitar), 
			propietario=coalesce($13, propietario), 
			abrev=coalesce($14, abrev),
			url=coalesce($15, url), 
			localidad=coalesce($16, localidad), 
			real=coalesce($17, real), 
			cero_ign=coalesce($18,cero_ign), 
			altitud=coalesce($19,altitud), 
			ubicacion=coalesce($20, ubicacion)
			WHERE tabla=$5 and id_externo=$2
			RETURNING unid id, nombre, st_asGeoJSON(geom)::json geom, distrito, pais, rio, has_obs, tipo, automatica, habilitar, propietario, abrev AS abreviatura, URL as "URL", localidad, real, cero_ign, altitud, tabla, id_externo, ubicacion`
			query_params = [estacion.nombre, estacion.id_externo, (estacion.geom) ? estacion.geom.coordinates[0] : undefined, (estacion.geom) ? estacion.geom.coordinates[1] : undefined, estacion.tabla, estacion.provincia, estacion.pais, estacion.rio, estacion.has_obs, estacion.tipo, estacion.automatica, estacion.habilitar, estacion.propietario, estacion.abreviatura, estacion.URL, estacion.localidad, estacion.real, estacion.cero_ign, estacion.altitud, estacion.ubicacion]
		} else {
			query = `UPDATE estaciones 
			SET nombre=coalesce($1,nombre),
			id_externo=coalesce($2,id_externo), 
			geom=coalesce(st_setsrid(st_point($3, $4),4326),geom), 
			tabla=coalesce($5,tabla),
			distrito=coalesce($6,distrito), 
			pais=coalesce($7,pais), 
			rio=coalesce($8, rio), 
			has_obs=coalesce($9,has_obs),
			tipo=coalesce($10,tipo),
			automatica=coalesce($11,automatica), 
			habilitar=coalesce($12, habilitar), 
			propietario=coalesce($13, propietario), 
			abrev=coalesce($14, abrev),
			url=coalesce($15, url), 
			localidad=coalesce($16, localidad), 
			real=coalesce($17, real), 
			cero_ign=coalesce($19,cero_ign), 
			altitud=coalesce($20,altitud),
			ubicacion=coalesce($21,ubicacion)
			WHERE unid = $18
			RETURNING unid id, nombre, st_asGeoJSON(geom)::json geom, distrito, pais, rio, has_obs, tipo, automatica, habilitar, propietario, abrev AS abreviatura, URL as "URL", localidad, real, cero_ign, altitud, ubicacion`
			query_params = [estacion.nombre, estacion.id_externo, (estacion.geom) ? estacion.geom.coordinates[0] : undefined, (estacion.geom) ? estacion.geom.coordinates[1] : undefined, estacion.tabla, estacion.provincia, estacion.pais, estacion.rio, estacion.has_obs, estacion.tipo, estacion.automatica, estacion.habilitar, estacion.propietario, estacion.abreviatura, estacion.URL, estacion.localidad, estacion.real, estacion.id, estacion.cero_ign, estacion.altitud, estacion.ubicacion]
		}
		// console.debug(pasteIntoSQLQuery(query, query_params))
		const result = await client.query(query,query_params) 
		if(result.rows.length<=0) {
			console.error("No se encontró la estación")
			throw("No se encontró la estación")
		}
		// console.log("Updated estaciones.unid=" + result.rows[0].id)
		Object.keys(result.rows[0]).forEach(key=>{
			estacion[key] = result.rows[0][key]
		})
		if(estacion.nivel_alerta || estacion.nivel_evacuacion || estacion.nivel_aguas_bajas) {
			estacion = await this.upsertNivelesAlerta(estacion,client)
		}
		if(release_client) {
			client.release()
		}
		return new internal.estacion(estacion)
	}
	
	/**
	 * Inserts or updates into table alturas_alerta
	 * @param {internal.estacion} estacion - instance of estacion containing new values for alturas_alerta
	 * @param {Number=} estacion.nivel_alerta
	 * @param {Number=} estacion.nivel_evacuacion
	 * @param {Number=} estacion.nivel_aguas_bajas
	 * @param {pg.Client=} client - pg client to include in a transactional block.
	 * @returns {Promise<internal.estacion>} A promise to the estacion instance containing the updated alturas_alerta
	 */
	static async upsertNivelesAlerta(estacion,client) {
		var release_client = false
		if(!client) {
			client = await global.pool.connect()
			release_client = true
		}
		var querystring = "INSERT INTO alturas_alerta (unid,nombre,valor,estado) \
		VALUES ($1,$2,$3,$4) \
		ON CONFLICT(unid,estado) DO UPDATE SET nombre=excluded.nombre, valor=excluded.valor \
		RETURNING *"
		if(parseFloat(estacion.nivel_alerta).toString() != "NaN") {
			var result = await client.query(querystring,[estacion.id,'alerta',estacion.nivel_alerta,'a'])
			if(result && result.rows && result.rows[0]) {
				estacion.nivel_alerta = result.rows[0].valor
			}
		}
		if(parseFloat(estacion.nivel_evacuacion).toString() != "NaN") {
			var result = await client.query(querystring,[estacion.id,'evacuación',estacion.nivel_evacuacion,'e'])
			if(result && result.rows && result.rows[0]) {
				estacion.nivel_evacuacion = result.rows[0].valor
			}
		}
		if(parseFloat(estacion.nivel_aguas_bajas).toString() != "NaN") {
			var result = await client.query(querystring,[estacion.id,'aguas_bajas',estacion.nivel_aguas_bajas,'b'])
			if(result && result.rows && result.rows[0]) {
				estacion.nivel_aguas_bajas = result.rows[0].valor
			}
		}
		if(release_client) {
			client.release()
		}
		return estacion
	}
	
	static async upsertEstaciones(estaciones,options)  {
		// var upserted=[]
		var queries = []
		for(var i = 0; i < estaciones.length; i++) {
			// var estacion
			// try {
			// 	estacion = await this.upsertEstacion(new internal.estacion(estaciones[i]),options) //,options)
			// } catch (e) {
			// 	console.error(e)
			// }
			// if(estacion) {
			// 	upserted.push(estacion)
			// }
			if(estaciones[i].id) {
				const existing_estacion = await internal.estacion.read({id:estaciones[i].id})
				if(existing_estacion) {
					if(existing_estacion.tabla == estaciones[i].tabla && existing_estacion.id_externo == estaciones[i].id_externo) {
						console.error("Estación " + estaciones[i].id + " already exists. Updating")
						estaciones[i].id = undefined
					} else {
						throw("estacion id already taken")
					}
				}
			} 
			queries.push(this.upsertEstacionQuery(estaciones[i],options))
		}
		// return upserted // Promise.all(promises)
		return this.executeQueryArray(queries,internal.estacion)
	}
			
	static async deleteEstacion(unid) {
		return global.pool.query("\
			DELETE FROM estaciones\
			WHERE unid=$1\
			RETURNING *",[unid]
		).then(result=>{
			if(result.rows.length<=0) {
				console.log("unid not found")
				return
			}
			console.log("Deleted estaciones.unid=" + result.rows[0].unid)
			return result.rows[0]
		}).catch(e=>{
			//~ console.error(e)
			throw(e)
		})
	}
	
	static async deleteEstaciones(filter) {
		if(filter.id) {
			filter.estacion_id = filter.id
			delete filter.id
		}
		return this.getEstaciones(filter)
		.then(estaciones=>{
			if(estaciones.length == 0) {
				return []
			}
			var ids = estaciones.map(e=>e.id)
			return global.pool.query("\
				DELETE FROM estaciones \
				WHERE unid IN (" + ids.join(",") + ")\
				RETURNING *,st_x(geom) geom_x,st_y(geom) geom_y")
			.then(result=>{
				if(result.rows.length == 0) {
					return []
				} 
				return result.rows.map(row=>{
					const geometry = new internal.geometry("Point", [row.geom_x, row.geom_y])
					const estacion = new internal.estacion(row.nombre,row.id_externo,geometry,row.tabla,row.distrito,row.pais,row.rio,row.has_obs,row.tipo,row.automatica,row.habilitar,row.propietario,row.abrev,row.URL,row.localidad,row.real,undefined,undefined,undefined,row.altitud,row.public,row.cero_ign)
					estacion.id = row.unid
					return estacion
				})
			})
		})
	}

	static async getEstacion(id,isPublic,options={}) {
		const stmt = "\
		SELECT estaciones.nombre, estaciones.id_externo, st_x(estaciones.geom) geom_x, st_y(estaciones.geom) geom_y, estaciones.tabla,  estaciones.distrito, estaciones.pais, estaciones.rio, estaciones.has_obs, estaciones.tipo, estaciones.automatica, estaciones.habilitar, estaciones.propietario, estaciones.abrev, estaciones.URL, estaciones.localidad, estaciones.real, estaciones.id, estaciones.unid, nivel_alerta.valor nivel_alerta, nivel_evacuacion.valor nivel_evacuacion, nivel_aguas_bajas.valor nivel_aguas_bajas, estaciones.cero_ign, estaciones.altitud, redes.public, redes.nombre as red_nombre, redes.id as red_id\
		FROM estaciones\
		LEFT OUTER JOIN redes ON (estaciones.tabla = redes.tabla_id) \
		LEFT OUTER JOIN alturas_alerta nivel_alerta ON (estaciones.unid = nivel_alerta.unid AND nivel_alerta.estado='a') \
		LEFT OUTER JOIN alturas_alerta nivel_evacuacion ON (estaciones.unid = nivel_evacuacion.unid AND nivel_evacuacion.estado='e') \
		LEFT OUTER JOIN alturas_alerta nivel_aguas_bajas ON (estaciones.unid = nivel_aguas_bajas.unid AND nivel_aguas_bajas.estado='b') \
		WHERE estaciones.unid=$1 \
		AND estaciones.geom IS NOT NULL"
		// console.debug(pasteIntoSQLQuery(stmt, [id]))
		const result = await global.pool.query(stmt, [id])
		if(result.rows.length<=0) {
			console.log("estacion no encontrada")
			return
		}
		if(isPublic) {
			if(!result.rows[0].public) {
				console.log("estacion no es public")
				throw("el usuario no posee autorización para acceder a esta estación")
			}
		}
		const geometry = new internal.geometry("Point", [result.rows[0].geom_x, result.rows[0].geom_y])
		const estacion = new internal.estacion(result.rows[0].nombre,result.rows[0].id_externo,geometry,result.rows[0].tabla,result.rows[0].distrito,result.rows[0].pais,result.rows[0].rio,result.rows[0].has_obs,result.rows[0].tipo,result.rows[0].automatica,result.rows[0].habilitar,result.rows[0].propietario,result.rows[0].abrev,result.rows[0].URL,result.rows[0].localidad,result.rows[0].real,result.rows[0].nivel_alerta,result.rows[0].nivel_evacuacion,result.rows[0].nivel_aguas_bajas,result.rows[0].altitud,result.rows[0].public,result.rows[0].cero_ign,undefined,undefined,{id:result.rows[0].red_id, nombre:result.rows[0].red_nombre,tabla_id: result.rows[0].tabla})
		estacion.id =  result.rows[0].unid
		if(options.get_drainage_basin) {
			await estacion.getDrainageBasin()
		}
		return estacion
	}

	static async getEstacionesWithPagination(filter={},options={},req) {
		filter.limit = filter.limit ?? config.pagination.default_limit
		filter.limit = parseInt(filter.limit)
		if (filter.limit > config.pagination.max_limit) {
			throw(new Error("limit exceeds maximum records per page (" + config.pagination.max_limit) + ")")
		}
		filter.offset = filter.offset ?? 0
		filter.offset = parseInt(filter.offset)
		const result = await internal.CRUD.getEstaciones(filter,options)
		var is_last_page = (result.length < filter.limit)
		if(is_last_page) {
			return {
				estaciones: result,
				is_last_page: true
			}
		} else {
			var query_arguments = {...filter,...options}
			query_arguments.offset = filter.offset + filter.limit 
			var next_page_url = (req) ? `${req.protocol}://${req.get('host')}${req.path}?${querystring.stringify(query_arguments)}` : `obs/puntual/estaciones?${querystring.stringify(query_arguments)}`
			return {
				estaciones: result,
				is_last_page: false,
				next_page: next_page_url
			}
		}
	}
	
	static async getEstaciones(filter={},options={},client) {
		var release_client = false
		if(!client) {
			client = await global.pool.connect()
			release_client = true
		}
		if(filter.estacion_id) {
			filter.unid = filter.estacion_id
		}
		if(filter.id) {
			filter.unid = filter.id
		}
		const estaciones_filter = internal.utils.control_filter2(
			{
				nombre: {
					type: "regex_string"
				},
				unid: {
					type: "numeric"
				},
				// id: {
				// 	type: "numeric"
				// },
				id_externo: {
					type: "string"
				},
				distrito: {
					type: "regex_string"
				},
				pais: {
					type: "regex_string"
				},
				has_obs: {
					type: "boolean"
				},
				real: {
					type: "boolean"
				},
				habilitar: {
					type: "boolean"
				},
				tipo: {
					type: "string"
				},
				has_prono: {
					type: "boolean",
				},
				rio: {
					type: "regex_string"
				},
				geom: {
					type: "geometry"
				}, 
				propietario: {
					type: "regex_string"
				},
				automatica: {
					type: "boolean"
				}, 
				ubicacion: {
					type: "regex_string"
				},
				localidad: {
					type: "regex_string"
				}, 
				tipo_2: {
					type: "string"
				},
				tabla: {
					type: "string"
				}, 
				abrev: {
					type: "regex_string"
				}
			}, 
			filter, 
			"estaciones"
		)
		if(!estaciones_filter) {
			return Promise.reject("invalid filter")
		}
		const redes_filter = internal.utils.control_filter({fuentes_id: "integer", tabla_id: "string", public: "boolean_only_true", public_his_plata: "boolean"},filter, "redes")
		if(!redes_filter) {
			return Promise.reject("invalid filter")
		}
		var filter_string= estaciones_filter + " " + redes_filter
		//~ console.log("filter_string:" + filter_string)
		var pagination_clause = ""
		if(options.pagination) {
			pagination_clause = (filter.limit) ? `LIMIT ${filter.limit}` : ""
			pagination_clause += (filter.offset) ? ` OFFSET ${filter.offset}`: ""
		}
		var res = await client.query(`
			SELECT
				estaciones.nombre, 
				estaciones.id_externo, 
				st_asGeoJSON(estaciones.geom) AS geom,
				estaciones.tabla,  
				estaciones.distrito, 
				estaciones.pais, 
				estaciones.rio, 
				estaciones.has_obs, 
				estaciones.tipo, 
				estaciones.automatica, 
				estaciones.habilitar, 
				estaciones.propietario, 
				estaciones.abrev AS abreviatura, 
				estaciones.URL as "URL", 
				estaciones.localidad, 
				estaciones.real, 
				estaciones.id,
				estaciones.unid, 
				nivel_alerta.valor nivel_alerta, 
				nivel_evacuacion.valor nivel_evacuacion, 
				nivel_aguas_bajas.valor nivel_aguas_bajas, 
				cero_ign, 
				redes.public, 
				altitud,
				json_build_object(
					'nombre', redes.red_nombre,
					'id', redes.fuentes_id,
					'tabla_id', redes.tabla_id,
					'public', redes.public,
					'public_his_plata', redes.public_his_plata
				) as red
		FROM estaciones
		JOIN (SELECT id AS fuentes_id, tabla_id, public, public_his_plata, nombre AS red_nombre FROM redes) redes ON (estaciones.tabla=redes.tabla_id)
		LEFT OUTER JOIN alturas_alerta nivel_alerta ON (estaciones.unid = nivel_alerta.unid AND nivel_alerta.estado='a') 
		LEFT OUTER JOIN alturas_alerta nivel_evacuacion ON (estaciones.unid = nivel_evacuacion.unid AND nivel_evacuacion.estado='e') 
		LEFT OUTER JOIN alturas_alerta nivel_aguas_bajas ON (estaciones.unid = nivel_aguas_bajas.unid AND nivel_aguas_bajas.estado='b') 
		WHERE estaciones.geom IS NOT NULL ${filter_string} ORDER BY unid
		${pagination_clause}`)
		var estaciones = []
		for(var row of res.rows) {
			// row.geom = new internal.geometry("Point", [row.geom_x, row.geom_y])
			row.id = row.unid
			delete row.unid
			const estacion = new internal.estacion(row)
			// estacion.id = row.unid
			if(options && options.get_drainage_basin) {
				await estacion.getDrainageBasin()
			}
			estaciones.push(estacion)
		}
		if(release_client) {
			await client.release()
		}
		return estaciones
		// .catch(e=>{
		// 	console.error(e)
		// 	return null
		// })
	}
			
			
	// AREA //
	
	static async upsertArea(area) {
		return new Promise((resolve,reject)=>{
			if(area.id) {
				resolve(area)
			} else {
				resolve(area.getId(global.pool))
			}
		})
		.then(()=>{
			if(area.geom && area.geom.type && area.geom.type == "MultiPolygon") {
				area.geom = new internal.geometry({
					type: "Polygon",
					coordinates: area.geom.coordinates[0]
				})
			}
			return global.pool.query(this.upsertAreaQuery(area))
		}).then(result=>{
			if(result.rows.length<=0) {
				console.error("Upsert failed")
				return
			}
			console.log("Upserted areas_pluvio.unid=" + result.rows[0].id)
			//~ console.log(result.rows[0])
			return new internal.area(result.rows[0])
		}).catch(e=>{
			console.error(e)
			return
		})
	}
	
	static upsertAreaQuery (area)  {
		var query = ""
		var params = []
		if(area.exutorio) {
			if(area.id) {
				query = "\
				INSERT INTO areas_pluvio (unid, nombre, geom, exutorio, exutorio_id, ae, rho, wp, activar, mostrar) \
				VALUES ($1, $2, ST_GeomFromText($3,4326), ST_GeomFromText($4,4326), $5, $6, $7, $8, $9, $10)\
				ON CONFLICT (unid) DO UPDATE SET \
					nombre=excluded.nombre, \
					geom=excluded.geom, \
					exutorio=excluded.exutorio, \
					exutorio_id=excluded.exutorio_id, \
					area = excluded.area, \
					ae = excluded.ae, \
					rho = excluded.rho, \
					wp = excluded.wp, \
					activar = excluded.activar, \
					mostrar = excluded.mostrar \
				RETURNING \
					unid AS id, \
					nombre, \
					st_astext(geom) AS geom, \
					st_astext(exutorio) AS exutorio, \
					exutorio_id, \
					area, \
					ae, \
					rho, \
					wp, \
					activar, \
					mostrar"
				params = [area.id,area.nombre,area.geom.toString(),area.exutorio.toString(),area.exutorio_id, area.ae, area.rho, area.wp, area.activar, area.mostrar]
			} else {
				query = "\
				INSERT INTO areas_pluvio (nombre, geom, exutorio, exutorio_id, ae, rho, wp, activar, mostrar) \
				VALUES ($1, ST_GeomFromText($2,4326), ST_GeomFromText($3,4326), $4, $5, $6, $7, $8, $9)\
				RETURNING \
					unid AS id, \
					nombre, \
					st_astext(geom) AS geom, \
					st_astext(exutorio) AS exutorio, \
					exutorio_id, \
					area, \
					ae, \
					rho, \
					wp, \
					activar, \
					mostrar"
				params = [area.nombre, area.geom.toString(), area.exutorio.toString(), area.exutorio_id, area.ae, area.rho, area.wp, area.activar, area.mostrar]
			}
		} else {
			if(area.id) {
				query = "\
				INSERT INTO areas_pluvio (unid, nombre, geom, exutorio_id, ae, rho, wp, activar, mostrar) \
				VALUES ($1, $2, ST_GeomFromText($3,4326), $4, $5, $6, $7, $8, $9)\
				ON CONFLICT (unid) DO UPDATE SET \
					nombre=excluded.nombre,\
					geom=excluded.geom,\
					exutorio_id=excluded.exutorio_id, \
					area = excluded.area, \
					ae = excluded.ae, \
					rho = excluded.rho, \
					wp = excluded.wp, \
					activar = excluded.activar, \
					mostrar = excluded.mostrar \
				RETURNING \
					unid AS id, \
					nombre, \
					st_astext(geom) AS geom, \
					st_astext(exutorio) AS exutorio, \
					exutorio_id, \
					area, \
					ae, \
					rho, \
					wp, \
					activar, \
					mostrar"
				params = [area.id,area.nombre,area.geom.toString(),area.exutorio_id, area.ae, area.rho, area.wp, area.activar, area.mostrar]
			} else {
				query = "\
				INSERT INTO areas_pluvio (nombre, geom, exutorio_id, ae, rho, wp, activar, mostrar) \
				VALUES ($1, ST_GeomFromText($2,4326), $3, $4, $5, $6, $7, $8)\
				RETURNING \
					unid AS id, \
					nombre, \
					st_astext(geom) AS geom, \
					st_astext(exutorio) AS exutorio, \
					exutorio_id, \
					area, \
					ae, \
					rho, \
					wp, \
					activar, \
					mostrar"
				params = [area.nombre, area.geom.toString(), area.exutorio_id, area.ae, area.rho, area.wp, area.activar, area.mostrar]
			}
		}
		return internal.utils.pasteIntoSQLQuery(query,params)
	}
	//~ upsertAreas(areas) {
		//~ var promises=[]
		//~ for(var i = 0; i < areas.length; i++) {
			//~ promises.push(this.upsertArea(new internal.area(areas[i])))
		//~ }
		//~ return Promise.all(promises)
	//~ }
	
	static async upsertAreas(areas) {
		const created_areas = []
		for(const area of areas) {
			const created_area = await this.upsertArea(area)
			if(created_area) {
				created_areas.push(created_area)
			}
		}
		return created_areas
	}
			
	static async deleteArea(unid) {
		return global.pool.query("\
			DELETE FROM areas_pluvio\
			WHERE unid=$1\
			RETURNING areas_pluvio.unid id, \
			areas_pluvio.nombre, \
			st_astext(ST_ForcePolygonCCW(areas_pluvio.geom)) AS geom, \
			st_astext(areas_pluvio.exutorio) AS exutorio,\
			areas_pluvio.exutorio_id, \
			areas_pluvio.area, \
			areas_pluvio.ae, \
			areas_pluvio.rho, \
			areas_pluvio.wp, \
			areas_pluvio.activar, \
			areas_pluvio.mostrar", [unid]
		).then(result=>{
			if(result.rows.length<=0) {
				console.log("unid not found")
				return
			}
			console.log("Deleted areas_pluvio.unid=" + result.rows[0].id)
			return new internal.area(result.rows[0])
		}).catch(e=>{
			console.error(e)
		})
	}

	static async getArea(id,options={}) {
		var query = (options.no_geom) ? "\
		SELECT \
			areas_pluvio.unid AS id, \
			areas_pluvio.nombre, \
			st_astext(areas_pluvio.exutorio) AS exutorio,\
			areas_pluvio.area, \
			areas_pluvio.ae, \
			areas_pluvio.rho, \
			areas_pluvio.wp, \
			areas_pluvio.activar, \
			areas_pluvio.mostrar \
		FROM areas_pluvio\
		WHERE unid=$1" : "\
		SELECT \
			areas_pluvio.unid AS id, \
			areas_pluvio.nombre, \
			st_astext(ST_ForcePolygonCCW(areas_pluvio.geom)) AS geom, \
			st_astext(areas_pluvio.exutorio) AS exutorio,\
			areas_pluvio.area, \
			areas_pluvio.ae, \
			areas_pluvio.rho, \
			areas_pluvio.wp, \
			areas_pluvio.activar, \
			areas_pluvio.mostrar \
		FROM areas_pluvio\
		WHERE unid=$1"
		return global.pool.query(query,[id])
		.then(result=>{
			if(result.rows.length<=0) {
				console.error("area no encontrada")
				return
			}
			//~ var geom_parsed = JSON.parse(result.rows[0].geom)
			//~ const geom = new internal.geometry(geom_parsed.type, geom_parsed.coordinates)
			//~ var exut_parsed = JSON.parse(result.rows[0].exutorio)
			//~ const exutorio = new internal.geometry(exut_parsed.type, exut_parsed.coordinates)
			//~ const area = new internal.area(result.rows[0].nombre,geom, exutorio)
			//~ area.id = result.rows[0].unid
			//~ return area
			if(options.no_geom) {
				if(result.rows[0].exutorio) {
					result.rows[0].exutorio = new internal.geometry(result.rows[0].exutorio)
				}
				return result.rows[0]
			} else {
				return new internal.area(result.rows[0])
			}
		})
	}

	static async getAreasWithPagination(filter={},options={},req) {
		filter.limit = filter.limit ?? config.pagination.default_limit
		filter.limit = parseInt(filter.limit)
		if (filter.limit > config.pagination.max_limit) {
			throw(new Error("limit exceeds maximum records per page (" + config.pagination.max_limit) + ")")
		}
		filter.offset = filter.offset ?? 0
		filter.offset = parseInt(filter.offset)
		const result = await this.getAreas(filter,options)
		var is_last_page = (result.length < filter.limit)
		if(is_last_page) {
			return {
				areas: result,
				is_last_page: true
			}
		} else {
			var query_arguments = {...filter,...options}
			query_arguments.offset = filter.offset + filter.limit 
			var next_page_url = (req) ? `${req.protocol}://${req.get('host')}${req.path}?${querystring.stringify(query_arguments)}` : `obs/areal/areas?${querystring.stringify(query_arguments)}`
			return {
				areas: result,
				is_last_page: false,
				next_page: next_page_url
			}
		}

	}
	
	static async getAreas(filter,options) {
		if(filter.id) {
			filter.unid = filter.id
			delete filter.id
		}
		const valid_filters = {
			nombre: {
				type: "regex_string"
			},
			unid: {
				type: "integer"
			}, 
			geom: {
				type: "geometry",
			},
			exutorio: {
				type: "geometry"
			},
			exutorio_id: {
				type: "integer"
			},
			activar: {
				type: "boolean"
			},
			mostrar: {
				type: "boolean"
			}
		}
		var filter_string = internal.utils.control_filter2(valid_filters,filter,"areas_pluvio")
		if(!filter_string) {
			throw("Invalid filters")
		}
		var join_type = "LEFT"
		var tabla_id_filter = ""
		if(filter.tabla_id) {
			if(/[';]/.test(filter.tabla_id)) {
				throw("Invalid filter value")
			}
			join_type = "RIGHT"
			tabla_id_filter +=  ` AND estaciones.tabla='${filter.tabla_id}'`
		}
		var pagination_clause = (filter.limit) ? `LIMIT ${filter.limit}` : ""
		pagination_clause += (filter.offset) ? ` OFFSET ${filter.offset}`: ""
		//~ console.log("filter_string:" + filter_string)
		if(options && options.no_geom) {
			const stmt = "SELECT \
				areas_pluvio.unid id, \
				areas_pluvio.nombre, \
				st_astext(areas_pluvio.exutorio) exutorio, \
				areas_pluvio.exutorio_id, \
				areas_pluvio.area, \
				areas_pluvio.ae, \
				areas_pluvio.rho, \
				areas_pluvio.wp, \
				areas_pluvio.activar, \
				areas_pluvio.mostrar \
			FROM areas_pluvio \
			" + join_type + " JOIN estaciones ON (estaciones.unid=areas_pluvio.exutorio_id" + tabla_id_filter + ") \
			WHERE areas_pluvio.geom IS NOT NULL " + filter_string + " ORDER BY areas_pluvio.id\
			" + pagination_clause
			// console.debug(stmt)
			return global.pool.query(stmt)
			.then(res=>{
				return res.rows.map(r=>{
					if(r.exutorio) {
						r.exutorio = new internal.geometry(r.exutorio)
					}
					return r
				})
			})
		} else {
			const stmt = "SELECT \
				areas_pluvio.unid id, \
				areas_pluvio.nombre, \
				st_astext(areas_pluvio.geom) geom, \
				st_astext(areas_pluvio.exutorio) exutorio, \
				areas_pluvio.exutorio_id, \
				areas_pluvio.area, \
				areas_pluvio.ae, \
				areas_pluvio.rho, \
				areas_pluvio.wp, \
				areas_pluvio.activar, \
				areas_pluvio.mostrar \
			FROM areas_pluvio \
			" + join_type + " JOIN estaciones ON (estaciones.unid=areas_pluvio.exutorio_id" + tabla_id_filter + ") \
			WHERE areas_pluvio.geom IS NOT NULL " + filter_string + " ORDER BY id\
			" + pagination_clause
			// console.debug(stmt)
			return global.pool.query(stmt)
			.then(res=>{
				//~ console.log(res)
				var areas = res.rows.map(row=>{
					//~ const geom = new internal.geometry(row.geom)
					//~ const exutorio = new internal.geometry(row.exutorio)
					//~ const area = new internal.area(row.nombre,geom, exutorio)
					//~ area.id = row.unid
					return new internal.area(row) 
				})
				return areas
			})
			.catch(e=>{
				console.error(e)
				return null
			})
		}
	}
	
	// ESCENA //
	
	static async getEscena(id,options) {
		if(options && options.no_geom) {
			return global.pool.query("\
			SELECT escenas.id, escenas.nombre\
			FROM escenas\
			WHERE id=$1",[id])
			.then(result=>{
				if(result.rows.length<=0) {
					console.log("escena no encontrada")
					return
				}
				return result.rows[0]
			})
		} else {
			return global.pool.query("\
			SELECT escenas.id, escenas.nombre, st_astext(escenas.geom) AS geom\
			FROM escenas\
			WHERE id=$1",[id])
			.then(result=>{
				if(result.rows.length<=0) {
					console.log("escena no encontrada")
					return
				}
				//~ var geom_parsed = JSON.parse(result.rows[0].geom)
				//~ const geom = new internal.geometry(geom_parsed.type, geom_parsed.coordinates)
				//~ var exut_parsed = JSON.parse(result.rows[0].exutorio)
				//~ const exutorio = new internal.geometry(exut_parsed.type, exut_parsed.coordinates)
				//~ const area = new internal.area(result.rows[0].nombre,geom, exutorio)
				//~ area.id = result.rows[0].unid
				//~ return area
				return new internal.escena(result.rows[0])
			})
		}
	}
	
	static async getEscenas(filter,options) {
		const escenas_filter = internal.utils.control_filter({nombre:"regex_string", id:"numeric", geom: "geometry"}, filter, "escenas")
		if(!escenas_filter) {
			return Promise.reject("invalid filter")
		}
		var filter_string= escenas_filter
		console.log({filter_string:filter_string})
		if(options && options.no_geom) {
			return global.pool.query("SELECT escenas.id, escenas.nombre\
			FROM escenas\
			WHERE 1=1 " + filter_string + " ORDER BY id")
			.then(res=>{
				return res.rows
			})
		} else {
			return global.pool.query("SELECT escenas.id, escenas.nombre, st_asgeojson(escenas.geom)::json AS geom\
			FROM escenas\
			WHERE 1=1 " + filter_string + " ORDER BY id")
			.then(res=>{
				//~ console.log(res)
				var escenas = res.rows.map(row=>{
					//~ console.log({row:row})
					//~ const geometry = new internal.geometry("Polygon", row.geom.coordinates)
					const escena = new internal.escena({id:row.id,nombre:row.nombre,geom:row.geom})
					return escena
				})
				return escenas
			})
		}
		//~ .catch(e=>{
			//~ console.error(e)
			//~ return null
		//~ })
	}
	
	static async upsertEscena(escena, client) {
		//~ console.log("upsertEscena")
		if(!escena.id) {
			await escena.getId(global.pool)
		}
		var query = "\
			INSERT INTO escenas (id, nombre,geom) \
			VALUES ($1, $2, st_geomfromtext($3,4326))\
			ON CONFLICT (id) DO UPDATE SET \
				nombre=excluded.nombre,\
				geom=excluded.geom\
			RETURNING \
				escenas.id, \
				escenas.nombre, \
				st_astext(escenas.geom) AS geom"
		var query_arguments = [escena.id,escena.nombre,escena.geom.toString()]
		if(!escena.id) {
			query = "\
			INSERT INTO escenas (nombre,geom) \
			VALUES ($1, st_geomfromtext($2,4326))\
			ON CONFLICT (id) DO UPDATE SET \
				nombre=excluded.nombre,\
				geom=excluded.geom\
			RETURNING \
				escenas.id, \
				escenas.nombre, \
				st_astext(escenas.geom) AS geom"
			query_arguments = [escena.nombre,escena.geom.toString()]
		}
			//~ console.log(internal.utils.pasteIntoSQLQuery(query,[escena.id,escena.nombre,escena.geom.toString()]))
		try {
			if(client) {
				var result = await client.query(query,query_arguments)
			} else {
				var result = await global.pool.query(query,query_arguments)
			}
			if(!result.rows.length) {
				throw new Error("Upsert failed")
			}
		} catch(e) {
			throw new Error(e)
		}
		console.debug("Upserted escena.id=" + result.rows[0].id)
		return result.rows[0]
	}
	
	static async upsertEscenas(escenas) {
		const upserted = []
		for(const escena of escenas) {
			try {
				var e = await this.upsertEscena(escena)
			} catch(e) {
				throw e
			}
			upserted.push(e)
		}
		return upserted
	}
	
	static async deleteEscena(id) {
		return global.pool.query("\
			DELETE FROM escenas\
			WHERE id=$1\
			RETURNING *",[id]
		).then(result=>{
			if(result.rows.length<=0) {
				console.log("id not found")
				return
			}
			console.log("Deleted escenas.id=" + result.rows[0].id)
			return result.rows[0]
		})
	}

			
	static async deleteEscenas(id) {
		return global.pool.query("\
			DELETE FROM escenas\
			WHERE id=$1\
			RETURNING *",[id]
		).then(result=>{
			if(result.rows.length<=0) {
				console.log("id not found")
				return
			}
			console.log("Deleted escenas.id=" + result.rows[0].id)
			return result.rows[0]
		}).catch(e=>{
			console.error(e)
		})
	}


	// VAR //
	
	static async upsertVar(variable) {
		return variable.getId()
		.then(()=>{
			return this.interval2epoch(variable.timeSupport)
		}).then(timeSupport=>{
			//~ var timeSupport = (variable.timeSupport) ? (typeof variable.timeSupport == 'object') ? this.interval2epoch(variable.timeSupport) : variable.timeSupport : variable.timeSupport
			return global.pool.query(this.upsertVarQuery(variable))
		}).then(result=>{
			if(result.rows.length<=0) {
				throw("Upsert failed")
			}
			// console.log("Upserted var.id=" + result.rows[0].id)
			variable.set(result.rows[0])
			return new internal["var"](result.rows[0])
		}).catch(e=>{
			throw(e)
		})
	}

	static upsertVarQuery(variable) {
		var query = "\
		INSERT INTO var (id, var,nombre,abrev,type,datatype,valuetype,\"GeneralCategory\",\"VariableName\",\"SampleMedium\",def_unit_id,\"timeSupport\") \
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)\
		ON CONFLICT (var,\"GeneralCategory\") DO UPDATE SET \
			var=excluded.var,\
			nombre=excluded.nombre,\
			abrev=excluded.abrev,\
			type=excluded.type,\
			datatype=excluded.datatype,\
			valuetype=excluded.valuetype,\
			\"GeneralCategory\"=excluded.\"GeneralCategory\",\
			\"VariableName\"=excluded.\"VariableName\",\
			\"SampleMedium\"=excluded.\"SampleMedium\",\
			def_unit_id=excluded.def_unit_id,\
			\"timeSupport\"=excluded.\"timeSupport\"\
		RETURNING id,var,nombre,abrev,type,datatype,valuetype,\"GeneralCategory\",\"VariableName\",\"SampleMedium\",def_unit_id,\"timeSupport\""
		var params = [variable.id, variable["var"],variable.nombre, variable.abrev, variable.type, variable.datatype, variable.valuetype, variable.GeneralCategory, variable.VariableName, variable.SampleMedium, variable.def_unit_id, timeSteps.interval2string(variable.timeSupport)]
		return internal.utils.pasteIntoSQLQuery(query,params)
	}
	
	static async upsertVars(variables) {
		return Promise.all(variables.map(variable=>{
			return this.upsertVar(variable)
		}))
	}
			
	static async deleteVar(id) {
		return global.pool.query("\
			DELETE FROM var\
			WHERE id=$1\
			RETURNING id, var,nombre,abrev,type,datatype,valuetype,\"GeneralCategory\",\"VariableName\",\"SampleMedium\",def_unit_id,\"timeSupport\"",[id]
		).then(result=>{
			if(result.rows.length<=0) {
				console.log("id not found")
				return
			}
			console.log("Deleted var.id=" + result.rows[0].id)
			return new internal["var"](result.rows[0])
		}).catch(e=>{
			console.error(e)
		})
	}

	static async getVar(id) {
		id = parseInt(id)
		if(id.toString() == "NaN") {
			throw(new Error("Invalid variable id. Must be integer"))
		}
		return executeQueryReturnRows("\
		SELECT id, \
		       var,\
		       nombre,\
		       abrev,\
		       type,\
		       datatype,\
		       valuetype,\
		       \"GeneralCategory\",\
		       \"VariableName\",\
		       \"SampleMedium\",\
		       def_unit_id,\
		       \"timeSupport\",\
		       def_hora_corte\
		FROM var\
		WHERE id=$1",[id])
		.then(rows=>{
			if(rows.length<=0) {
				console.log("variable no encontrada")
				return
			}
			const variable = new internal["var"](rows[0]) // rows[0]["var"],rows[0].nombre,rows[0].abrev,rows[0].type,rows[0].datatype,rows[0].valuetype,rows[0].GeneralCategory,rows[0].VariableName,rows[0].SampleMedium,rows[0].def_unit_id,rows[0].timeSupport,rows[0].def_hora_corte)
			variable.id = rows[0].id
			return variable
		}).catch(e => {
			throw(new Error(e))
		})
	}
	
	static async getVars(filter) {
		const valid_filters = {
			id: {type: "numeric"}, 
			"var": {type: "string"},
			nombre: {type: "regex_string"},
			abrev: {type: "regex_string"},
			type: {type: "string"}, 
			datatype: {type: "string"}, 
			valuetype: {type: "string"}, 
			GeneralCategory: {type: "string"}, 
			VariableName: {type: "string"},
			SampleMedium: {type: "string"},
			def_unit_id: {type: "numeric"},
			timeSupport: {type: "interval"}, 
			def_hora_corte: {type: "interval"}
		}
		// console.log(filter)
		var filter_string = internal.utils.control_filter2(valid_filters,filter, undefined, true)
		// console.log(filter_string)
		if(!filter_string) {
			return Promise.reject(new Error("invalid filter value"))
		}
		const stmt = "SELECT id, var,nombre,abrev,type,datatype,valuetype,\"GeneralCategory\",\"VariableName\",\"SampleMedium\",def_unit_id,\"timeSupport\"\
		FROM var \
		WHERE 1=1 " + filter_string + " ORDER BY id"
		// console.debug("stmt: " + stmt)
		return global.pool.query(stmt)
		.then(res=>{
			//~ console.log(res)
			var variables = res.rows.map(row=>{
				const variable = new internal["var"](row) // row["var"],row.nombre,row.abrev,row.type,row.datatype,row.valuetype,row.GeneralCategory,row.VariableName,row.SampleMedium,row.def_unit_id,row.timeSupport,row.def_hora_corte)
				variable.id = row.id
				return variable
			})
			return variables
		})
		.catch(e=>{
			console.error(new Error(e))
			return null
		})
	}
	
	// PROCEDIMIENTO //
	
	static async upsertProcedimiento(procedimiento) {
		return new Promise((resolve, reject) => {
			if(!procedimiento.id) {
				resolve(procedimiento.getId(global.pool))
			} else {
				resolve(procedimiento)
			}
		})
		.then(()=>{
			return global.pool.query(this.upsertProcedimientoQuery(procedimiento))
		}).then(result=>{
			if(result.rows.length<=0) {
				console.error("Upsert failed")
				return
			}
			console.log("Upserted procedimiento.id=" + result.rows[0].id)
			return result.rows[0]
		}).catch(e=>{
			console.error(e)
			return
		})
	}
	
	static upsertProcedimientoQuery(procedimiento) {
		var query = "\
			INSERT INTO procedimiento (id,nombre,abrev,descripcion) \
			VALUES ($1, $2, $3, $4)\
			ON CONFLICT (id) DO UPDATE SET \
				nombre=excluded.nombre,\
				abrev=excluded.abrev,\
				descripcion=excluded.descripcion\
			RETURNING *"
		var params = [procedimiento.id, procedimiento.nombre, procedimiento.abrev, procedimiento.descripcion]
		return internal.utils.pasteIntoSQLQuery(query,params)
	}

	static async upsertProcedimientos(procedimientos) {
		return Promise.all(procedimientos.map(proc=>{
			return this.upsertProcedimiento(proc)
		}))
	}
			
	static async deleteProcedimiento(id) {
		return global.pool.query("\
			DELETE FROM procedimiento\
			WHERE id=$1\
			RETURNING *",[id]
		).then(result=>{
			if(result.rows.length<=0) {
				console.log("id not found")
				return
			}
			console.log("Deleted procedimiento.id=" + result.rows[0].id)
			return result.rows[0]
		}).catch(e=>{
			console.error(e)
		})
	}

	static async getProcedimiento(id) {
		return global.pool.query("\
		SELECT id, nombre, abrev, descripcion\
		FROM procedimiento\
		WHERE id=$1",[id])
		.then(result=>{
			if(result.rows.length<=0) {
				console.log("procedimiento no encontrado")
				return
			}
			const procedimiento = new internal.procedimiento(result.rows[0].nombre,result.rows[0].abrev,result.rows[0].descripcion)
			procedimiento.id = result.rows[0].id
			return procedimiento
		})
	}
	
	static async getProcedimientos(filter) {
		const valid_filters = {id: "numeric", nombre: "regex_string",abrev: "regex_string",descripcion: "regex_string"}
		var filter_string = internal.utils.control_filter(valid_filters,filter)
		if(!filter_string) {
			return Promise.reject(new Error("invalid filter value"))
		}
		//~ console.log("filter_string:" + filter_string)
		return global.pool.query("SELECT *\
		 FROM procedimiento \
		 WHERE 1=1 " + filter_string)
		.then(res=>{
			//~ console.log(res)
			var procedimientos = res.rows.map(row=>{
				const procedimiento = new internal.procedimiento(row.nombre,row.abrev,row.descripcion)
				procedimiento.id = row.id
				return procedimiento
				console.log(procedimiento.toString())
			})
			return procedimientos
		})
		.catch(e=>{
			console.error(e)
			return null
		})
	}
	
	// UNIDADES //
	
	static async upsertUnidades(unidades) {
		return new Promise((resolve,reject)=>{
			if(!unidades.id) {
				resolve(unidades.getId(global.pool))
			} else {
				resolve(unidades)
			}
		})
		.then(()=>{
			return global.pool.query(this.upsertUnidadesQuery(unidades))
		}).then(result=>{
			if(result.rows.length<=0) {
				console.error("Upsert failed")
				return
			}
			console.log("Upserted unidades.id=" + result.rows[0].id)
			return result.rows[0]
		}).catch(e=>{
			console.error(e)
			return
		})
	}

	static upsertUnidadesQuery(unidades) {
		var query = "\
		INSERT INTO unidades (id,nombre, abrev,  \"UnitsID\", \"UnitsType\") \
		VALUES ($1, $2, $3, $4, $5)\
		ON CONFLICT (id) DO UPDATE SET \
			nombre=excluded.nombre,\
			abrev=excluded.abrev,\
			\"UnitsID\"=excluded.\"UnitsID\",\
			\"UnitsType\"=excluded.\"UnitsType\"\
		RETURNING *"
		var params =[unidades.id, unidades.nombre, unidades.abrev, unidades.UnitsID, unidades.UnitsType]
		return internal.utils.pasteIntoSQLQuery(query,params)
	}

	static async upsertUnidadeses(unidadeses) {
		const upserted = []
		for(const unit of unidadeses) {
			const result = await this.upsertUnidades(unit)
			if(result) {
				upserted.push(result)
			}
		}
		return upserted
	}
			
	static async deleteUnidades(id) {
		return global.pool.query("\
			DELETE FROM unidades\
			WHERE id=$1\
			RETURNING *",[id]
		).then(result=>{
			if(result.rows.length<=0) {
				console.log("id not found")
				return
			}
			console.log("Deleted unidades.id=" + result.rows[0].id)
			return result.rows[0]
		}).catch(e=>{
			console.error(e)
		})
	}

	static async getUnidad(id) {
		return global.pool.query("\
		SELECT id, nombre, abrev,  \"UnitsID\", \"UnitsType\"\
		FROM unidades\
		WHERE id=$1",[id])
		.then(result=>{
			if(result.rows.length<=0) {
				console.log("unidades no encontrado")
				return
			}
			const unidades = new internal.unidades(result.rows[0].nombre,result.rows[0].abrev,result.rows[0].UnitsID,result.rows[0].UnitsType)
			unidades.id = result.rows[0].id
			return unidades
		})
	}
	
	static async getUnidades(filter) {
		const valid_filters = {
			id: {
				type: "numeric"
			}, 
			nombre: {
				type: "regex_string"
			},
			abrev: {
				type: "regex_string"
			},
			UnitsID: {
				type: "numeric"
			},
			UnitsType: {
				type:  "string"
			}
		}
		var filter_string = internal.utils.control_filter2(valid_filters,filter, "unidades", true)
		if(!filter_string) {
			return Promise.reject(new Error("invalid filter value"))
		}
		console.log("filter_string:" + filter_string)
		return global.pool.query("SELECT *\
		 FROM unidades \
		 WHERE 1=1 " + filter_string + " ORDER BY id")
		.then(res=>{
			//~ console.log(res)
			var unidades = res.rows.map(row=>{
				const unidad = new internal.unidades(row.nombre,row.abrev,row.UnitsID,row.UnitsType)
				unidad.id = row.id
				return unidad
				console.log(unidad.toString())
			})
			return unidades
		})
		.catch(e=>{
			console.error(e)
			return null
		})
	}
	
	// FUENTE //
			
	static async upsertFuente(fuente) {
		await fuente.getId(global.pool)
		// console.log(fuente)
		var query = this.upsertFuenteQuery(fuente)
		// if(config.verbose) {
		// 	console.log("crud.upsertFuente: " + query)
		// }
		const result = await global.pool.query(query)
		if(result.rows.length<=0) {
			throw("Upsert failed")
		}
		console.log("Upserted fuentes.id=" + result.rows[0].id)
		return new internal.fuente(result.rows[0])
	}

	static upsertFuenteQuery(fuente) {
		var query = "\
			INSERT INTO fuentes (id, nombre, data_table, data_column, tipo, def_proc_id, def_dt, hora_corte, def_unit_id, def_var_id, fd_column, mad_table, scale_factor, data_offset, def_pixel_height, def_pixel_width, def_srid, def_extent, date_column, def_pixeltype, abstract, source) \
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, st_geomfromtext($18), $19, $20, $21, $22)\
			ON CONFLICT (id) DO UPDATE SET \
				id=excluded.id, \
				nombre=excluded.nombre, \
				data_table=excluded.data_table,\
				data_column=excluded.data_column,\
				tipo=excluded.tipo,\
				def_proc_id=excluded.def_proc_id,\
				def_dt=excluded.def_dt,\
				hora_corte=excluded.hora_corte,\
				def_unit_id=excluded.def_unit_id,\
				def_var_id=excluded.def_var_id,\
				fd_column=excluded.fd_column,\
				mad_table=excluded.mad_table,\
				scale_factor=excluded.scale_factor,\
				data_offset=excluded.data_offset,\
				def_pixel_height=excluded.def_pixel_height,\
				def_pixel_width=excluded.def_pixel_width,\
				def_srid=excluded.def_srid,\
				def_extent=excluded.def_extent,\
				date_column=excluded.date_column,\
				def_pixeltype=excluded.def_pixeltype,\
				abstract=excluded.abstract,\
				source=excluded.source\
			RETURNING id,nombre,data_table,data_column,tipo,def_proc_id,def_dt,hora_corte,def_unit_id,def_var_id,fd_column,mad_table,scale_factor,data_offset,def_pixel_height,def_pixel_width,def_srid,ST_AsGeoJson(def_extent)::json AS def_extent,date_column,def_pixeltype,abstract,source"
		var params = [fuente.id, fuente.nombre, fuente.data_table, fuente.data_column, fuente.tipo, fuente.def_proc_id, fuente.def_dt, fuente.hora_corte, fuente.def_unit_id, fuente.def_var_id, fuente.fd_column, fuente.mad_table, fuente.scale_factor, fuente.data_offset, fuente.def_pixel_height, fuente.def_pixel_width, fuente.def_srid, (fuente.def_extent) ? fuente.def_extent.toString() : null, fuente.date_column, fuente.def_pixeltype, fuente.abstract, fuente.source]
		return internal.utils.pasteIntoSQLQuery(query,params)
	}
	
	static async upsertFuentes(fuentes) {
		return Promise.all(fuentes.map(f=>{
			return this.upsertFuente(new internal.fuente(f))
		}))
		.then(result=>{
			if(result) {
				return result.filter(f=>f)
			} else {
				return
			}
		})
	}
			
	static async deleteFuente(id) {
		return global.pool.query("\
			DELETE FROM fuentes\
			WHERE id=$1\
			RETURNING *",[id]
		).then(result=>{
			if(result.rows.length<=0) {
				console.log("id not found")
				return
			}
			console.log("Deleted fuentes.id=" + result.rows[0].id)
		})
	}

	static async getFuente(id,isPublic) {
		const query = "\
		SELECT id, nombre, data_table, data_column, tipo, def_proc_id, def_dt, hora_corte, def_unit_id, def_var_id, fd_column, mad_table, scale_factor, data_offset, def_pixel_height, def_pixel_width, def_srid, st_asgeojson(def_extent)::json def_extent, date_column, def_pixeltype, abstract, source,public\
		FROM fuentes\
		WHERE id=$1"
		// console.log(query)
		var result = await global.pool.query(query,[id])
		if(result.rows.length<=0) {
			console.log("fuentes no encontrado")
			return
		}
		if (isPublic) {
			if (!result.rows[0].public) {
				throw("El usuario no posee autorización para acceder a esta fuente")
			}
		}
		// nombre, data_table, data_column, tipo, def_proc_id, def_dt, hora_corte, def_unit_id, def_var_id, fd_column, mad_table, scale_factor, data_offset, def_pixel_height, def_pixel_width, def_srid, def_extent, date_column, def_pixeltype, abstract, source
		var row = result.rows[0]
		row.constraints = await this.getTableConstraints(row.data_table,undefined)
		const fuente = new internal.fuente(row) //(row.nombre, row.data_table, row.data_column, row.tipo, row.def_proc_id, row.def_dt, row.hora_corte, row.def_unit_id, row.def_var_id, row.fd_column, row.mad_table, row.scale_factor, row.data_offset, row.def_pixel_height, row.def_pixel_width, row.def_srid, new internal.geometry(def_extent.type, def_extent.coordinates), row.date_column, row.def_pixeltype, row.abstract, row.source)
		fuente.id = row.id
		return fuente
	}
	
	static async getFuentes(filter) {
		if(filter && filter.geom) {
			filter.def_extent = filter.geom
		}
		const valid_filters = {id: "numeric", nombre: "regex_string", data_table: "string", data_column: "string", tipo: "string", def_proc_id: "numeric", def_dt: "string", hora_corte: "string", def_unit_id: "numeric", def_var_id: "numeric", fd_column: "string", mad_table: "string", scale_factor: "numeric", data_offset: "numeric", def_pixel_height: "numeric", def_pixel_width: "numeric", def_srid: "numeric", def_extent: "geometry", date_column: "string", def_pixeltype: "string", abstract: "regex_string", source: "regex_string",public:"boolean_only_true"}
		var filter_string = internal.utils.control_filter(valid_filters,filter)
		if(!filter_string) {
			return Promise.reject(new Error("invalid filter value"))
		}
		console.log("filter_string:" + filter_string)
		var query = "SELECT id, nombre, data_table, data_column, tipo, def_proc_id, def_dt, hora_corte, def_unit_id, def_var_id, fd_column, mad_table, scale_factor, data_offset, def_pixel_height, def_pixel_width, def_srid, st_asgeojson(def_extent)::json def_extent, date_column, def_pixeltype, abstract, source, public\
		FROM fuentes \
		WHERE 1=1 " + filter_string + " ORDER BY id"
		if(filter.is_table) {
			query = "SELECT id, nombre, data_table, data_column, tipo, def_proc_id, def_dt, hora_corte, def_unit_id, def_var_id, fd_column, mad_table, scale_factor, data_offset, def_pixel_height, def_pixel_width, def_srid, st_asgeojson(def_extent)::json def_extent, date_column, def_pixeltype, abstract, source, public\
			FROM fuentes, pg_catalog.pg_tables\
			WHERE fuentes.data_table=pg_catalog.pg_tables.tablename " + filter_string + " ORDER BY id"
		}
		return global.pool.query(query)
		.then(res=>{
			//~ console.log(res)
			var fuentes = res.rows.map(row=>{
				const fuente = new internal.fuente(row) //(row.id, row.nombre, row.data_table, row.data_column, row.tipo, row.def_proc_id, row.def_dt, row.hora_corte, row.def_unit_id, row.def_var_id, row.fd_column, row.mad_table, row.scale_factor, row.data_offset, row.def_pixel_height, row.def_pixel_width, row.def_srid, row.def_extent, row.date_column, row.def_pixeltype, row.abstract, row.source)
				fuente.id = row.id
				return fuente
				console.log(fuente.toString())
			})
			return fuentes
		})
		.catch(e=>{
			console.error(e)
			return null
		})
	}
	
	static async getFuentesAll(filter={}) {
		var fuentes_areal
		var fuentes_puntual
		try {
			fuentes_areal = await this.getFuentes(filter)
			fuentes_puntual = await this.getRedes(filter)
		} catch(e) {
			throw(e)
		}
		fuentes_areal = fuentes_areal.map(f=>{
			return {
				"tipo": "raster",
				"id": f.id,
				"nombre": f.nombre
			}
		})
		fuentes_puntual = fuentes_puntual.map(f=>{
			return {
				"tipo": "puntual",
				"id": f.id,
				"nombre": f.nombre
			}
		})
		return [...fuentes_areal,...fuentes_puntual]
	}
			
	// SERIE //
	
	static async upsertSerie(serie,options={}) {
		if(!serie.tipo) {
			serie.tipo = "puntual"
		}
		if(serie.id) { // if id is given, looks for a match
			const serie_match = await internal.serie.read({tipo:serie.tipo,id:serie.id})
			if(serie_match) {  // if exists, updates
				return serie_match.update(serie)
			} else {
				console.log("serie " + serie.tipo + " " + serie.id + " not found. Creating")
			}
		} else {
			await serie.getId()
		}
		var result = await global.pool.query(this.upsertSerieQuery(serie))
		if(result.rows.length == 0) {
			console.log("nothing inserted")
			return null
		}
		console.log("Upserted serie " + serie.tipo + " id: " + result.rows[0].id)
		result.rows[0].tipo = serie.tipo
		result = result.rows[0]
		if(options.series_metadata) {
			console.log("adding series metadata")
			return this.initSerie(result)
		} else {
			return new internal.serie({
				tipo: result.tipo,
				id: result.id, 
				estacion: { id: result.estacion_id },
				"var": { id: result.var_id },
				procedimiento: { id: result.proc_id },
				unidades: { id: result.unit_id },
				fuentes: { id: result.fuentes_id}
			})
		}
	}

	static upsertSerieQuery(serie) {
		var query = ""
		var params = []
		if(serie.tipo == "areal") {
			if(!serie.estacion.id || !serie["var"].id || !serie.procedimiento.id || !serie.unidades.id || !serie.fuente.id) {
				throw(new Error("Unable to insert/update serie: serie.estacion.id or serie.var.id or serie.procedimiento.id or serie.unidades.id or serie.fuente.id"))
			}
			if(serie.id) {
				query = "\
				INSERT INTO series_areal (id,area_id,var_id,proc_id,unit_id,fuentes_id)\
				VALUES ($1,$2,$3,$4,$5,$6)\
				ON CONFLICT (area_id,var_id,proc_id,unit_id,fuentes_id)\
				DO update set id=excluded.id\
				RETURNING id,area_id AS estacion_id,var_id,proc_id,unit_id,fuentes_id"
				params = [serie.id,serie.estacion.id,serie["var"].id,serie.procedimiento.id,serie.unidades.id,serie.fuente.id]
			} else {
				query = "\
				INSERT INTO series_areal (area_id,var_id,proc_id,unit_id,fuentes_id)\
				VALUES ($1,$2,$3,$4,$5)\
				ON CONFLICT (area_id,var_id,proc_id,unit_id,fuentes_id)\
				DO update set area_id=excluded.area_id\
				RETURNING id,area_id AS estacion_id,var_id,proc_id,unit_id,fuentes_id"
				params = [serie.estacion.id,serie["var"].id,serie.procedimiento.id,serie.unidades.id,serie.fuente.id]
			}
		} else if (serie.tipo == "rast" || serie.tipo == "raster") {
			if(!serie.estacion.id || !serie["var"].id || !serie.procedimiento.id || !serie.unidades.id || !serie.fuente.id) {
				throw(new Error("Unable to insert/update serie: serie.estacion.id or serie.var.id or serie.procedimiento.id or serie.unidades.id or serie.fuente.id"))
			}
			if(serie.id) {
				query = "\
				INSERT INTO series_rast (escena_id,var_id,proc_id,unit_id,fuentes_id,id)\
				VALUES ($1,$2,$3,$4,$5,$6)\
				ON CONFLICT (escena_id,var_id,proc_id,unit_id,fuentes_id)\
				DO update set escena_id=excluded.escena_id\
				RETURNING id,escena_id AS estacion_id,var_id,proc_id,unit_id,fuentes_id"
				params = [serie.estacion.id,serie["var"].id,serie.procedimiento.id,serie.unidades.id,serie.fuente.id,serie.id]
			} else {
				query = "\
				INSERT INTO series_rast (escena_id,var_id,proc_id,unit_id,fuentes_id)\
				VALUES ($1,$2,$3,$4,$5)\
				ON CONFLICT (escena_id,var_id,proc_id,unit_id,fuentes_id)\
				DO update set escena_id=excluded.escena_id\
				RETURNING id,escena_id AS estacion_id,var_id,proc_id,unit_id,fuentes_id"
				params = [serie.estacion.id,serie["var"].id,serie.procedimiento.id,serie.unidades.id,serie.fuente.id]
			}
		} else {
			if(!serie.estacion.id || !serie["var"].id || !serie.procedimiento.id || !serie.unidades.id) {
				throw(new Error("Unable to insert/update serie: serie.estacion.id or serie.var.id or serie.procedimiento.id or serie.unidades.id"))
			}

			if(serie.id) { // SI SE PROVEE ID Y YA EXISTE LA TUPLA ESTACION+VAR+PROC, ACTUALIZA ID
				query = "\
				INSERT INTO series (estacion_id,var_id,proc_id,unit_id,id)\
				VALUES ($1,$2,$3,$4,$5)\
				ON CONFLICT (estacion_id,var_id,proc_id)\
				DO UPDATE SET unit_id=excluded.unit_id, id=excluded.id\
				RETURNING *"
				params = [serie.estacion.id,serie["var"].id,serie.procedimiento.id,serie.unidades.id,serie.id]
			} else { // SI NO PROVEE ID GENERA UNO NUEVO
				query = "\
				INSERT INTO series (estacion_id,var_id,proc_id,unit_id)\
				VALUES ($1,$2,$3,$4)\
				ON CONFLICT (estacion_id,var_id,proc_id)\
				DO UPDATE SET unit_id=excluded.unit_id\
				RETURNING *"
				params = [serie.estacion.id,serie["var"].id,serie.procedimiento.id,serie.unidades.id]
			}
		} 
		return internal.utils.pasteIntoSQLQuery(query,params)
	}

	static async checkSerieIdExists(tipo,id,client) {
		var release_client = false
		if(!client) {
			client = await global.pool.connect()
			release_client = true
		}
		try {
			var existing_serie = await this.getSerie(tipo,id,undefined,undefined,undefined,undefined,undefined,client)
		} catch(e) {
			throw(e)
		}
		if(!existing_serie) {
			console.log("id not taken")
			if(release_client) {
				client.release()
			}
			return false
		}
		console.log("id already taken")
		if(release_client) {
			client.release()
		}
		return true
	}
	
	/**
	 * create or update series
	 * @param {internal.serie[]} series - the series instances to create/update
	 * @param {Boolean} [all=false] - option to create/update all dependencies (not just the series table)
	 * @param {Boolean} [upsert_estacion=false] - option to create/update estacion dependency (of table estaciones, areas_pluvio or escenas)
	 * @param {Boolean} [generate_id=false] - option to generate new id when the provided id is already taken
	 * @param {pg.Client=} client - pg client to use within transaction block 
	 * @returns {Promise<internal.serie[]>} The created/updated series
	 */
	static async upsertSeries(series,all=false,upsert_estacion=false,generate_id=false,client, upsert_fuente=true, update_obs) {
		// var promises=[]
		// console.log({all:all})
		var series_result=[]
		var release_client = false
		if(!client) {
			client  = await global.pool.connect() 
			release_client = true
			try {
				await client.query("BEGIN")
			} catch(e) {
				throw(e)
			}
		}
		if(!Array.isArray(series)) {
			series = [series]
		}
		try {
			for(var i=0; i<series.length; i++) {
				// console.debug("new serie: " + JSON.stringify(series[i]))
				const serie = (series[i] instanceof internal.serie) ? series[i] : new internal.serie(series[i])
				// console.debug("new serie (parsed): " + JSON.stringify(serie))
				var serie_props = {}
				if(all) {
					if(serie["var"] instanceof internal["var"]) {
						// promises.push(this.upsertVar(serie["var"]))
						var result = await client.query(this.upsertVarQuery(serie["var"]))
						serie_props["var"] = new internal["var"](result.rows[0])
					}
					if(serie.procedimiento instanceof internal.procedimiento) {
						// promises.push(this.upsertProcedimiento(serie.procedimiento))
						var result = await client.query(this.upsertProcedimientoQuery(serie.procedimiento))
						serie_props.procedimiento = new internal.procedimiento(result.rows[0])
					}
					if(serie.unidades instanceof internal.unidades) {
						// promises.push(this.upsertUnidades(serie.unidades))
						var result = await client.query(this.upsertUnidadesQuery(serie.unidades))
						serie_props.unidades = new internal.unidades(result.rows[0])
					}
					if(serie.fuente instanceof internal.fuente) {
						// promises.push(this.upsertFuente(serie.fuente))
						var result = await client.query(this.upsertFuenteQuery(serie.fuente))
						serie_props.fuente = new internal.fuente(result.rows[0])
					}
				} else {
					if(!serie.var.id) {
						throw(new Error("var.id missing"))
					}
					serie_props["var"] = await internal.var.read({id:serie.var.id})
					if(!serie_props["var"]) {
						throw(new Error("var " + serie.var.id + " not found"))
					}
					if(!serie.procedimiento.id) {
						throw(new Error("procedimiento.id missing"))
					}
					serie_props["procedimiento"] = await internal.procedimiento.read({id:serie.procedimiento.id})
					if(!serie_props["procedimiento"]) {
						throw(new Error("procedimiento " + serie.procedimiento.id + " not found"))
					}
					if(!serie.unidades.id) {
						throw(new Error("unidades.id missing"))
					}
					serie_props["unidades"] = await internal.unidades.read({id:serie.unidades.id})
					if(!serie_props["unidades"]) {
						throw(new Error("unidades " + serie.unidades.id + " not found"))
					}
					if(["areal","rast","raster"].indexOf(serie.tipo) >= 0 && (!serie.fuente || !serie.fuente.id ) ) {
						throw(new Error("fuente.id missing"))
					}
					if(upsert_fuente && serie.fuente instanceof internal.fuente) {
						// promises.push(this.upsertFuente(serie.fuente))
						console.debug("Upsert fuente: " + JSON.stringify(serie.fuente))
						var result = await client.query(this.upsertFuenteQuery(serie.fuente))
						serie_props.fuente = new internal.fuente(result.rows[0])
					} else {				
						serie_props["fuente"] = (serie.fuente.id != null) ? await internal.fuente.read({id:serie.fuente.id}) : {}
					}
				} 
				if (all || upsert_estacion) {
					// console.debug("Upsert estacion of new serie")
					if(serie.estacion instanceof internal.estacion) {
						serie_props.estacion = await this.upsertEstacion(serie.estacion,undefined,client) //await client.query(this.upsertEstacionQuery(serie.estacion,{no_update_id:no_update_estacion_id}))
						// serie_props.estacion = new internal.estacion(result.rows[0])
						// console.log("estacion: " + serie_props.estacion.toString())
					} else if (serie.estacion instanceof internal.area) {
						//~ console.log("estacion is internal.area")
						// promises.push(this.upsertArea(serie.estacion))
						var result = await client.query(this.upsertAreaQuery(serie.estacion))
						serie_props.estacion = new internal.area(result.rows[0])
					} else if (serie.estacion instanceof internal.escena) {
						var result = await this.upsertEscena(serie.estacion,client) // client.query(this.upsertEscenaQuery(serie.estacion))
						serie_props.estacion = new internal.escena(result)
					} else {
						var result = await this.upsertEstacion(new internal.estacion(serie.estacion),client) // client.query(this.upsertEscenaQuery(serie.estacion))
						serie_props.estacion = new internal.estacion(result)
					}
				} else {
					// console.log("Searching for estacion: " + JSON.stringify(serie.estacion))
					// console.log("serie.tipo: " + serie.tipo)
					// console.log(`serie is instance of serie: ${serie instanceof internal.serie}`)
					// console.log(`estacion is instance of estacion: ${serie.estacion instanceof internal.estacion}`)
					if(serie.estacion instanceof internal.estacion) {
						if(serie.estacion.id) {
							serie_props.estacion = await internal.estacion.read({id:serie.estacion.id})
						} else if(serie.estacion.id_externo && serie.estacion.tabla) {
							serie_props.estacion = await internal.estacion.read({id_externo:serie.estacion.id_externo,tabla:serie.estacion.tabla})
							if(!serie_props.estacion.length) {
								console.error("Estacion with id_externo: " + serie.estacion.id_externo + " and tabla: " + serie.estacion.tabla + " not found. Skipping serie upsert")
								continue
							}
							serie_props.estacion = serie_props.estacion[0]
						} else {
							console.error("Missing serie.estacion.id or series.estacion.id_externo + series.estacion.tabla. Skipping serie upsert")
							continue
						}
						if(!serie_props.estacion) {
							console.error("estacion " + serie.estacion.id + " not found. Skipping serie upsert")
							continue
						}
					} else if (serie.estacion instanceof internal.area) {
						serie_props.estacion = await internal.area.read({id:serie.estacion.id})
						if(!serie_props.estacion) {
							console.error("area " + serie.estacion.id + " not found. Skipping serie upsert")
							continue
						}
					} else if (serie.estacion instanceof internal.escena) {
						serie_props.estacion = await internal.escena.read({id:serie.estacion.id})
						if(!serie_props.estacion) {
							console.error("escena " + serie.estacion.id + " not found. Skipping serie upsert")
							continue
						}
					}
				}
				Object.assign(serie,serie_props) 
				// keys(serie_props).forEach(key=>{
				// 	serie[key] = serie_props[key]
				// })
				if(["areal", "rast", "raster"].indexOf(serie.tipo) >= 0 && serie.fuente == undefined) {
					throw("Specified Fuente not found")
				}

				var query_string
				// check if series exists already
				var series_match = await this.getSeries(serie.tipo,{estacion_id:serie.estacion.id,var_id:serie.var.id,proc_id:serie.procedimiento.id,unit_id:serie.unidades.id,fuentes_id:serie.fuente.id},{},client)
				if(series_match.length) {
					// match found
					if(serie.id) {
						if(series_match[0].id == serie.id) {
							// serie already exists and with the same id, do nothing
							// console.log("serie already exists and with the same id, do nothing")
						} else {
							// check if the id is already taken
							console.log("existing id: " + series_match[0].id + ", new id: " + serie.id)
							var id_exists = await this.checkSerieIdExists(serie.tipo,serie.id,client)
							if(id_exists) {
								if(generate_id) {
									// provided id already taken, will ignore it and generate a new one
									serie.id = undefined
									query_string = this.upsertSerieQuery(serie)
								} else {
									// throws error
									throw("series.id already taken. Try with generate_id=true")
								}
							} else {
								// id not taken, will update
								query_string = this.upsertSerieQuery(serie)
							}
						}
					} else {
						serie.id = series_match[0].id
						// console.debug("serie already exists and no new id provided, only update observaciones (if present)")
					}
				} else {
					// this is a new serie
					console.log("no match for " + JSON.stringify({estacion_id:serie.estacion.id,var_id:serie.var.id,proc_id:serie.procedimiento.id,unit_id:serie.unidades.id,fuentes_id:serie.fuente.id}))
					if(serie.id) {
						var id_exists = await this.checkSerieIdExists(serie.tipo,serie.id,client)
						if(id_exists) {
							if(generate_id) {
								// provided id already taken, will ignore it and generate a new one
								serie.id = undefined
								query_string = this.upsertSerieQuery(serie)
							} else {
								// throws error
								throw("series.id " + serie.id + " already taken. Try with generate_id=true")
							}
						} else {
							// id not taken, will update
							query_string = this.upsertSerieQuery(serie)
						}
					} else {
						query_string = this.upsertSerieQuery(serie)
					}
				}
				// var query_string = this.upsertSerieQuery(serie)
				var new_serie
				if(query_string) {
					var result = await client.query(query_string)
					new_serie = result.rows[0]
					new_serie.tipo = serie.tipo
				} else {
					new_serie = serie
				}
				Object.keys(serie_props).forEach(key=>{
					new_serie[key] = serie_props[key]
				})
				new_serie = new internal.serie(new_serie)
				if(serie.observaciones instanceof internal.observaciones) {
					new_serie.setObservaciones(serie.observaciones)
					new_serie.observaciones = new_serie.observaciones.removeDuplicates()
					new_serie.tipo_guess()
					new_serie.idIntoObs()
					var new_observaciones = await this.upsertObservaciones(serie.observaciones,new_serie.tipo,new_serie.id,{no_update: (update_obs) ? false: true}, client) // client.query(this.upsertObservacionesQuery(serie.observaciones,serie.tipo))
					new_serie.setObservaciones(new_observaciones)
				}
				series_result.push(new_serie)
			}
			if(release_client) {
				console.log("COMMIT")
				await client.query("COMMIT")
			}
		}
		catch (e) {
			console.log("ROLLBACK")
			client.query("ROLLBACK")
			if(release_client) {
				await client.release()
			}
			throw(e)
		}
		if(release_client) {
			await client.release()
		}
		console.log("upsertSeries: returning " + series_result.length + " series")
		return series_result
	}

	static async executeQueryArray(queries,internalClass) {
		var results = []
		for(var i in queries) {
			try {
				var result = await global.pool.query(queries[i])
			} catch (e) {
				console.error(e)
				continue
			}
			var results_ = (internalClass) ? result.rows.map(r=>new internalClass(r)) : result.rows
			results.push(...results_)
		}
		return results
	}
	
	static async deleteSerie(tipo,id) {
		if(tipo == "areal") {
			return global.pool.query("\
				DELETE FROM series_areal\
				WHERE id=$1\
				RETURNING 'areal' AS tipo,*",[id]
			).then(result=>{
				if(result.rows.length<=0) {
					console.log("id not found")
					return
				}
				console.log("Deleted series_areal.id=" + result.rows[0].id)
				return new internal.serie(result.rows[0])
			}).catch(e=>{
				throw(e)
			})
		} else if(tipo == "rast" || tipo == "raster") {
			return global.pool.query("\
				DELETE FROM series_rast\
				WHERE id=$1\
				RETURNING 'raster' AS tipo,*",[id]
			).then(result=>{
				if(result.rows.length<=0) {
					console.log("id not found")
					return
				}
				console.log("Deleted series_rast.id=" + result.rows[0].id)
				return new internal.serie(result.rows[0])
			}).catch(e=>{
				throw(e)
			})
		} else {
			return global.pool.query("\
				DELETE FROM series\
				WHERE id=$1\
				RETURNING 'puntual' AS tipo,*",[id]
			).then(result=>{
				if(result.rows.length<=0) {
					console.log("id not found")
					return
				}
				console.log("Deleted series.id=" + result.rows[0].id)
				return new internal.serie(result.rows[0])
			}).catch(e=>{
				throw(e)
				//~ console.error(e)
			})
		}
	}
	
	static async deleteSeries(filter) {
		var series_table = (filter.tipo) ? (filter.tipo == "puntual") ? "series" : (filter.tipo == "areal") ? "series_areal" : (filter.tipo == "rast" || filter.tipo == "raster") ? "series_rast" : "puntual" : "puntual"
		return this.getSeries(filter.tipo,filter)
		.then(series=>{
			if(series.length == 0) {
				return []
			}
			var ids = series.map(s=>s.id)
			return global.pool.query("\
				DELETE FROM " + series_table + "\
				WHERE id IN (" + ids.join(",") + ")\
				RETURNING *")
			.then(result=>{
				return result.rows
			})
		})
	}
	
	static async deleteSeries_Old(filter) {   // ELIMINAR
		var valid_filters
		if(filter.tipo.toLowerCase() == "areal") {
			valid_filters = {id:"integer",area_id:"integer",var_id:"integer",proc_id:"integer",unit_id:"integer",fuentes_id:"integer"}
			var filter_string = internal.utils.control_filter(valid_filters, filter, "series_areal")
			return global.pool.query("\
				DELETE FROM series_areal\
				WHERE series_areal.id=coalesce($1,series_areal.id)\
					  " + filter_string + "\
				RETURNING *",[filter.id]
			).then(result=>{
				if(result.rows.length<=0) {
					console.log("series not found")
					return []
				}
				console.log("Deleted " + result.rows.length + " series_areal")
				return result.rows
			}).catch(e=>{
				console.error(e)
			})
		} else if(filter.tipo.toLowerCase()=="raster"){
			valid_filters = {id:"integer",escena_id:"integer",var_id:"integer",proc_id:"integer",unit_id:"integer",fuentes_id:"integer"}
			var filter_string = internal.utils.control_filter(valid_filters, filter, "series_raster")
			return global.pool.query("\
				DELETE FROM series_raster\
				WHERE series_raster.id=coalesce($1,series_raster.id)\
				" + filter_string + "\
				RETURNING *",[filter.id]
			).then(result=>{
				if(result.rows.length<=0) {
					console.log("series not found")
					return []
				}
				console.log("Deleted " + result.rows.length + " series_raster")
				return result.rows
			}).catch(e=>{
				console.error(e)
			})
		} else {  // puntual
			valid_filters = {var_id:"integer",proc_id:"integer",unit_id:"integer",estacion_id:"integer",tabla:"string",id_externo:"string", id: "integer"}
			var filter_string = internal.utils.control_filter(valid_filters, filter)
			return global.pool.query("\
				DELETE FROM series\
				USING estaciones\
				WHERE series.estacion_id=estaciones.unid\
				AND series.id=coalesce($1,series.id)\
				" + filter_string + "\
				RETURNING *",[filter.id]
			).then(result=>{
				if(result.rows.length<=0) {
					console.log("series not found")
					return []
				}
				console.log("Deleted " + result.rows.length + " series")
				return result.rows
			}).catch(e=>{
				console.error(e)
			})
		}
	}
	
	static async getSerie(tipo,id,timestart,timeend,options={},isPublic,timeupdate,client) {
		var release_client = false
		if(!client) {
			client = await global.pool.connect()
			release_client = true
		}
		if(tipo == "areal") {
			var result = await client.query("\
			SELECT series_areal.id,series_areal.area_id,series_areal.var_id,series_areal.proc_id,series_areal.unit_id,series_areal.fuentes_id,fuentes.public,series_areal_date_range.timestart,series_areal_date_range.timeend,series_areal_date_range.count FROM series_areal join fuentes on (series_areal.fuentes_id=fuentes.id) left join series_areal_date_range on (series_areal.id=series_areal_date_range.series_id)\
			WHERE series_areal.id=$1",[id])
			if(result.rows.length<=0) {
				console.log("crud.getSerie: serie no encontrada")
				if(release_client) {
					await client.release()
				}
				return
			}
			if(isPublic) {
				if(!result.rows[0].public) {
					if(release_client) {
						await client.release()
					}
					console.error("El usuario no posee autorización para acceder a esta serie")
					return
				}
			}
			// console.log("crud.getSerie: serie " + tipo + " " + id + " encontrada")
			var row = result.rows[0]
			row.date_range = {timestart: row.timestart, timeend: row.timeend, count: row.count}
			delete row.timestart
			delete row.timeend
			delete row.count
			var s = []
			if(options.no_metadata) {
				s =[{id:row.area_id},{id:row.var_id},{id:row.proc_id},{id:row.unit_id},{id:row.fuentes_id}]
			} else {
				s = [await this.getArea(row.area_id), await this.getVar(row.var_id), await this.getProcedimiento(row.proc_id), await this.getUnidad(row.unit_id), await this.getFuente(row.fuentes_id)]
			}
			const serie = new internal.serie({estacion:s[0],"var":s[1],procedimiento:s[2],unidades: s[3], tipo:"areal", fuente:s[4]})  // estacion,variable,procedimiento,unidades,tipo,fuente
			serie.date_range = row.date_range
			serie.id=row.id
			var observaciones
			if(timestart && timeend) {
				options.obs_type = serie["var"].type
				// console.log(JSON.stringify(["areal",{series_id:row.id,timestart:timestart,timeend:timeend},options]))
				observaciones =  await this.getObservacionesRTS("areal",{series_id:row.id,timestart:timestart,timeend:timeend},options,serie)
			} else if(timeupdate) {
				options.obs_type = serie["var"].type
				observaciones = await this.getObservacionesRTS("areal",{series_id:row.id,timeupdate:timeupdate},options,serie)
			}
			if(options.asArray) {
				serie.observaciones = observaciones
			} else {
				serie.setObservaciones(observaciones)
			}
			if(options) {
				if(options.getStats) {
					await serie.getStats(client)
					console.log("got stats for series")
				}
				if(options.getMonthlyStats || options.getWeibullPercentiles) {
					serie.getWeibullPercentiles()
				}
			}
			if(release_client) {
				await client.release()
			}
			return serie
		} else if (tipo=="rast" || tipo=="raster") {
			var result = await client.query("\
			SELECT series_rast.id,series_rast.escena_id,series_rast.var_id,series_rast.proc_id,series_rast.unit_id,series_rast.fuentes_id,fuentes.public,series_rast_date_range.timestart,series_rast_date_range.timeend,series_rast_date_range.count FROM series_rast JOIN fuentes ON (series_rast.fuentes_id=fuentes.id) left join series_rast_date_range on (series_rast.id=series_rast_date_range.series_id)\
			WHERE series_rast.id=$1",[id])
			if(result.rows.length<=0) {
				console.log("serie no encontrada")
				if(release_client) {
					await client.release()
				}
				// throw("serie no encontrada")
				return
			}
			if(isPublic) {
				if(!result.rows[0].public) {
					if(release_client) {
						await client.release()
					}
					console.error("El usuario no posee autorización para acceder a esta serie")
					return
				}
			}
			var row = result.rows[0]
			row.date_range = {timestart: row.timestart, timeend: row.timeend, count: row.count}
			delete row.timestart
			delete row.timeend
			delete row.count
			var s = []
			if(options.no_metadata) {
				s =[{id:row.area_id},{id:row.var_id},{id:row.proc_id},{id:row.unit_id},{id:row.fuentes_id}]
			} else { 
				s = [await this.getEscena(row.escena_id),await this.getVar(row.var_id),await this.getProcedimiento(row.proc_id),await this.getUnidad(row.unit_id),await this.getFuente(row.fuentes_id)]
			}
			if(timestart && timeend) {
				if(!options.format) {
					options.format="hex"
				}
				row.observaciones = new internal.observaciones(await this.getObservacionesRTS("rast",{series_id:row.id,timestart:timestart,timeend:timeend},options))
			} else if(timeupdate) {
				if(!options.format) {
					options.format="hex"
				}
				row.observaciones = new internal.observaciones(await this.getObservacionesRTS("rast",{series_id:row.id,timeupdate:timeupdate},options))
			}			
			const serie = new internal.serie({estacion:s[0],"var":s[1],procedimiento:s[2],unidades:s[3], tipo:"rast", fuente:s[4]})  // estacion,variable,procedimiento,unidades,tipo,fuente
			serie.setObservaciones(row.observaciones)
			serie.id = row.id
			serie.date_range = row.date_range
			if(options) {
				if(options.getStats) {
					await serie.getStats(client)
					console.log("got stats for series")
				}
			}
			if(release_client) {
				await client.release()
			}
			return serie
		} else {
			var result = await client.query("\
			SELECT series.id,series.estacion_id,series.var_id,series.proc_id,series.unit_id,redes.public,series_date_range.timestart,series_date_range.timeend,series_date_range.count FROM series \
			JOIN estaciones ON series.estacion_id=estaciones.unid \
			JOIN redes ON estaciones.tabla=redes.tabla_id\
			LEFT JOIN series_date_range on series.id=series_date_range.series_id\
			WHERE series.id=$1",[id])
			if(result.rows.length<=0) {
				console.log("serie no encontrada")
				if(release_client) {
					await client.release()
				}
				// throw("serie no encontrada")
				return
			}
			if(isPublic) {
				if(!result.rows[0].public) {
					if(release_client) {
						await client.release()
					}
					console.error("El usuario no posee autorización para acceder a esta serie")
					return
				}
			}
			var row = result.rows[0]
			row.date_range = {timestart: row.timestart, timeend: row.timeend, count: row.count}
			delete row.timestart
			delete row.timeend
			delete row.count
			var s = [await this.getEstacion(row.estacion_id), await this.getVar(row.var_id), await this.getProcedimiento(row.proc_id), await this.getUnidad(row.unit_id)]
			const serie = new internal.serie({estacion:s[0],"var":s[1],procedimiento:s[2],unidades:s[3], tipo:"puntual"})  // estacion,variable,procedimiento,unidades,tipo,fuente
			serie.id=row.id
			serie.date_range = row.date_range
			if(timestart && timeend) {
				options.obs_type = serie["var"].type
				var observaciones
				if(options.regular) {
					observaciones = await this.getRegularSeries("puntual",row.id,(options.dt) ? options.dt : "1 days", timestart, timeend,options)
				} else {
					observaciones = await this.getObservacionesRTS("puntual",{series_id:row.id,timestart:timestart,timeend:timeend,timeupdate:timeupdate},options,serie)
				}
			} else if(timeupdate) {
				options.obs_type = serie["var"].type
				observaciones = await this.getObservacionesRTS("puntual",{series_id:row.id,timeupdate:timeupdate},options,serie)
			}
			if(options.asArray) {
				serie.observaciones = observaciones
			} else {
				serie.setObservaciones(observaciones)
			}
			if(options && options.getStats) {
				await serie.getStats(client)
				console.log("got stats for series")
			}
			if(options && options.getMonthlyStats) {
				if(serie.var.id == 48) {
					serie.getWeibullPercentiles()
				} else {
					serie.monthlyStats = await this.getMonthlyStats("puntual",serie.id).values
				}
			}
			
			if(release_client) {
				await client.release()
			}
			return serie
		}
	}
	
	static async initSerie(serie) {
		//~ console.log({serie:serie})
		var promises=[]
		if(!serie.estacion) {
			if(serie.tipo == "areal") {
				promises.push(this.getArea(serie.estacion_id,undefined))
			} else if (serie.tipo == "rast") {
				promises.push(this.getEscena(serie.estacion_id,undefined))
			} else {
				promises.push(this.getEstacion(serie.estacion_id,undefined))
			}
		} else if (serie.estacion.id) {
			if(serie.tipo == "areal") {
				if(! serie.estacion instanceof internal.area) {
					promises.push(this.getArea(serie.estacion.id,undefined))
				} else {
					promises.push(serie.estacion)
				}
			} else if (serie.tipo == "rast") {
				if(! serie.estacion instanceof internal.escena) {
					promises.push(this.getEscena(serie.estacion.id,undefined))
				} else {
					promises.push(serie.estacion)
				}
			} else {
				if(! serie.estacion instanceof internal.estacion) {
					console.log("running getEstacion")
					promises.push(this.getEstacion(serie.estacion.id,undefined))
				} else {
					promises.push(serie.estacion)
				}
			}
		} else {
			promises.push(null)
		}
		if(!serie["var"]) {
			promises.push(this.getVar(serie.var_id))
		} else if (serie["var"].id) {
			if(! serie["var"] instanceof internal["var"]) {
				promises.push(this.getVar(serie["var"].id))
			} else {
				promises.push(serie["var"])
			}
		} else {
			promises.push(null)
		}
		if(!serie.procedimiento) {
			promises.push(this.getProcedimiento(serie.proc_id))
		} else if (serie.procedimiento.id) {
			if(! serie.procedimiento instanceof internal.procedimiento) {
				promises.push(this.getProcedimiento(serie.procedimiento.id))
			} else {
				promises.push(serie.procedimiento)
			}
		} else {
			promises.push(null)
		}
		if(!serie.unidades) {
			promises.push(this.getUnidad(serie.unit_id))
		} else if (serie.unidades.id) {
			if(!serie.unidades instanceof internal.unidades) {
				promises.push(this.getUnidad(serie.unidades.id))
			} else {
				promises.push(serie.unidades)
			}
		} else {
			promises.push(null)
		}
		if(serie.tipo == "areal") {
			if(!serie.fuente) {
				promises.push(this.getFuente(serie.fuentes_id,undefined))
			} else if (serie.fuente.id) {
				if(!serie.fuente instanceof internal.fuente) {
					promises.push(this.getFuente(serie.fuente.id,undefined))
				} else {
					promises.push(serie.fuente)
				}
			} else {
				promises.push(null)
			}
		} else {
			promises.push(null)
		}
		return Promise.all(promises)
		.then(res=> {
			serie.estacion = res[0]
			this["var"] = res[1]
			serie.procedimiento = res[2]
			serie.unidades = res[3]
			serie.fuente = res[4]
			//~ console.log({serie:serie})
			return new internal.serie(serie)
		})
		.catch(e=>{
			console.error(e)
		})
	}

	static async getSeriesJson(filter={},options={},client) {
		var series
		if(!filter.tipo || filter.tipo == "puntual") {
			series = await this.getSeriesPuntualesJson(filter,options,client)
		} else if (filter.tipo == "areal") {
			series = await this.getSeriesArealesJson(filter,options,client)
		} else if (filter.tipo == "raster" || filter.tipo == "rast") {
			series = await this.getSeriesRasterJson(filter,options,client)
		}
		return series
	}

	static async getSeriesPuntualesJson(filter={},options={},client) {
		const filter_string = internal.utils.control_filter_json({
				id:{
					path: ["serie","id"],
					type:"arrInteger"
				},
				var_id:{
					path: ["serie","var","id"],
					type:"arrInteger"
				},
				proc_id:{
					path: ["serie","procedimiento","id"],
					type:"arrInteger"
				},
				unit_id:{
					path: ["serie","unidades","id"],
					type:"arrInteger"
				},
				estacion_id:{
					path: ["serie","estacion","id"],
					type:"arrInteger"
				},
				tabla:{
					path: ["serie","estacion","tabla"],
					type:"string"
				},
				tabla_id:{
					path: ["serie","estacion","tabla"],
					type:"string"
				},
				id_externo:{
					path: ["serie","estacion","id_externo"],
					type:"string"
				},
				geom:{
					path: ["serie","estacion","geom"],
					type: "geometry"
				},
				red_id:{
					path: ["serie","estacion","red_id"],
					type:"arrInteger"
				},
				public: {
					path: ["serie","estacion","public"],
					type:"boolean_only_true"
				},
				timestart_min: {
					path: ["serie","date_range","timestart"],
					type: "date_min"
				},
				date_range_before: {
					path: ["serie","date_range","timestart"],
					type: "timeend"
				},
				date_range_after: {
					path: ["serie","date_range","timeend"],
					type: "timestart"
				},
				count: {
					path: ["serie","date_range","count"],
					type: "numeric_min"
				}
			},filter,"series_json")
		console.log(filter_string)
		if(client) {
			var result = await client.query("SELECT serie FROM series_json WHERE 1=1 " + filter_string)			
		} else {
			var result = await global.pool.query("SELECT serie FROM series_json WHERE 1=1 " + filter_string)
		}
		result = result.rows.map(r=>r.serie)
		if(options.no_metadata) {
			result = result.map(s=>{
				return {
					tipo: "puntual",
					id: s.id,
					estacion_id: s.estacion.id,
					estacion_nombre: s.estacion.nombre,
					proc_id: s.procedimiento.id,
					proc_nombre: s.procedimiento.nombre,
					var_id: s.var.id,
					var_nombre: s.var.nombre,
					unit_id: s.unidades.id,
					unit_nombre: s.unidades.nombre,
					id_externo: s.estacion.id_externo,
					tabla: s.estacion.tabla,
					geom: s.estacion.geom,
					red_nombre: s.estacion.red_nombre,
					public: s.estacion.public,
					timestart: s.date_range.timestart,
					timeend: s.date_range.timeend,
					count: s.date_range.count
				}
			})
			return result
		}
		return result.map(s=>new internal.serie(s))		
	}

	static async getSeriesArealesJson(filter={},options={},client) {
		const valid_json_filters = {
			red_id:{
				path: ["serie","estacion","exutorio","red_id"],
				type:"arrInteger"
			},
			public: {
				path: ["serie","fuente","public"],
				type:"boolean_only_true"
			},
			date_range_before: {
				path: ["serie","date_range","timestart"],
				type: "timeend"
			},
			date_range_after: {
				path: ["serie","date_range","timeend"],
				type: "timestart"
			},
			count: {
				path: ["serie","date_range","count"],
				type: "numeric_min"
			}
		}
		const valid_row_filters = {
			id:{
				type:"integer"
			},
			var_id:{
				type:"integer"
			},
			GeneralCategory:{
				type:"string"
			},
			proc_id:{
				type:"integer"
			},
			unit_id:{
				type:"integer"
			},
			area_id:{
				column: "estacion_id",
				type:"integer"
			},
			estacion_id:{
				type:"integer"
			},
			tabla:{
				type:"string"
			},
			tabla_id:{
				column: "tabla",
				type:"string"
			},
			fuentes_id:{
				type: "integer",
			},
			series_id: {type: "integer", table: "series_areal_json", column:"id"},
			area_id: {type: "integer", table: "series_areal_json", column: "estacion_id"},
			red_id: {type: "integer", table: "series_areal_json", column:"red_id"},
			geom: {type: "geometry", table: "series_areal_json", column: "extent"},
			public: {type: "boolean", table: "series_areal_json"},
			cal_id: {type: "integer", table: "series_prono_date_range_last"},
			cal_grupo_id: {type: "integer", table: "series_prono_date_range_last"},
			search: {
				type: "search", 
				table: "series_areal_json", 
				columns: [
					{name: "tabla"},
					{name: "nombre"},
					{name: "estacion_id"},
					{name: "id_externo"},
					{name: "rio"},
					{name: "var_nombre"},
					{name: "fuentes_nombre"}
				],
				case_insensitive: true
			}
		}
		// if(filter.geom) {
		// 	const areas_filter = {
		// 		id: filter.area_id ?? filter.estacion_id,
		// 		geom: filter.geom
		// 	}
		// 	const matching_areas = await internal.area.read(areas_filter,{no_geom:true})
		// 	if(!matching_areas.length) {
		// 		// geom matches no areas, dont query series
		// 		console.log("No areas matched the specified geometry")
		// 		return []
		// 	}
		// 	const matching_areas_id = matching_areas.map(a=>a.id)
		// 	if (filter.estacion_id) {
		// 		if(Array.isArray(filter.estacion_id)) {
		// 			filter.estacion_id.push(...matching_areas_id)
		// 		} else {
		// 			filter.estacion_id = [filter.estacion_id, ...matching_areas_id]
		// 		}
		// 	} else {
		// 		filter.estacion_id = matching_areas_id
		// 	}
		// }
		// console.log(JSON.stringify(filter))
		if(options.no_geom || !options.include_geom) {
			const json_filter_string = internal.utils.control_filter_json(valid_json_filters,filter,"series_areal_json_no_geom")
			const row_filter_string = internal.utils.control_filter2(valid_row_filters,filter,"series_areal_json_no_geom")
			if(!row_filter_string) {
				throw("Invalid filters")
			}
			var stmt = "SELECT serie FROM series_areal_json_no_geom WHERE 1=1 " + json_filter_string + " " + row_filter_string
			// console.log(stmt)
		} else {			
			const json_filter_string = internal.utils.control_filter_json(valid_json_filters,filter,"series_areal_json")
			const row_filter_string = internal.utils.control_filter2(valid_row_filters,filter,"series_areal_json")
			if(!row_filter_string) {
				throw("Invalid filters")
			}
			var stmt = "SELECT serie FROM series_areal_json WHERE 1=1 " + json_filter_string + " " + row_filter_string
			// console.log(stmt)
		}
		if(client) {
			var result = await client.query(stmt)
		} else {
			var result = await global.pool.query(stmt)
		}
		result = result.rows.map(r=>r.serie)
		if(options.no_metadata) {
			result = result.map(s=>{
				return {
					tipo: "areal",
					id: s.id,
					area_id: s.estacion.id,
					area_nombre: s.estacion.nombre,
					proc_id: s.procedimiento.id,
					proc_nombre: s.procedimiento.nombre,
					var_id: s.var.id,
					var_nombre: s.var.nombre,
					unit_id: s.unidades.id,
					unit_nombre: s.unidades.nombre,
					fuentes_id: s.fuente.id,
					fuentes_nombre: s.fuente.nombre,
					tabla: s.estacion.exutorio.tabla,
					timestart: s.date_range.timestart,
					timeend: s.date_range.timeend,
					count: s.date_range.count
				}
			})
			return result
		}
		return result.map(s=>new internal.serie(s))
	}

	static async getSeriesRasterJson(filter={},options={},client) {
		const valid_filters = {
			id:{
				path: ["serie","id"],
				type:"arrInteger"
			},
			var_id:{
				path: ["serie","var","id"],
				type:"arrInteger"
			},
			proc_id:{
				path: ["serie","procedimiento","id"],
				type:"arrInteger"
			},
			unit_id:{
				path: ["serie","unidades","id"],
				type:"arrInteger"
			},
			escena_id:{
				path: ["serie","estacion","id"],
				type:"arrInteger"
			},
			estacion_id:{
				path: ["serie","estacion","id"],
				type:"arrInteger"
			},
			geom:{
				path: ["serie","estacion","geom"],
				type: "geometry"
			},
			fuentes_id:{
				path: ["serie","fuente","id"],
			},
			public: {
				path: ["serie","fuente","public"],
				type:"boolean_only_true"
			},
			date_range_before: {
				path: ["serie","date_range","timestart"],
				type: "timeend"
			},
			date_range_after: {
				path: ["serie","date_range","timeend"],
				type: "timestart"
			},
			count: {
				path: ["serie","date_range","count"],
				type: "numeric_min"
			}
		}
		const filter_string = internal.utils.control_filter_json(valid_filters,filter,"series_rast_json")
		console.log(filter_string)
		if(client) {
			var result = await client.query("SELECT serie FROM series_rast_json WHERE 1=1 " + filter_string)

		} else {
			var result = await global.pool.query("SELECT serie FROM series_rast_json WHERE 1=1 " + filter_string)
		}
		result = result.rows.map(r=>r.serie)
		if(options.no_metadata) {
			result = result.map(s=>{
				return {
					tipo: "raster",
					id: s.id,
					nombre: s.nombre,
					escena_id: s.estacion.id,
					escena_nombre: s.estacion.nombre,
					proc_id: s.procedimiento.id,
					proc_nombre: s.procedimiento.nombre,
					var_id: s.var.id,
					var_nombre: s.var.nombre,
					unit_id: s.unidades.id,
					unit_nombre: s.unidades.nombre,
					fuentes_id: s.fuente.id,
					fuentes_nombre: s.fuente.nombre,
					timestart: s.date_range.timestart,
					timeend: s.date_range.timeend,
					count: s.date_range.count
				}
			})
			return result
		}
		return result.map(s=>new internal.serie(s))
	}
	
	static async getSeries(tipo,filter={},options={},client,req) {
		console.debug({options:options})
		console.debug({filter:filter})
		if(tipo) {
			filter.tipo = tipo
		}
		var release_client = false
		if(!client) {
			client = await global.pool.connect()
			release_client = true
		}
		try {
			var query = internal.serie.build_read_query(filter,options)
		}
		catch(e) {
			if(release_client) {
				client.release()
			}
			throw(e)
		}
		// console.debug(query)
		try {
			var res = await client.query(query)
		} catch(e) {
			if(release_client) {
				client.release()
			}
			throw(e)
		}
		// for(var i in res.rows) {
		// 	res.rows[i].date_range = {timestart: res.rows[i].timestart, timeend: res.rows[i].timeend, count: res.rows[i].count}
		// 	delete res.rows[i].timestart
		// 	delete res.rows[i].timeend
		// 	delete res.rows[i].count
		// }
		//				GET PAGE PROPERTIES
		var [total, is_last_page, next_offset] = internal.utils.getPageProperties(filter.limit,filter.offset,res.rows)
		if(!is_last_page) {
			var next_page_url = internal.utils.makeGetSeriesNextPageUrl(tipo,next_offset,req,filter,options)
		} else {
			var next_page_url = undefined
		}
		if(options.no_metadata) {  // RETURN WITH NO METADATA (SOLO IDS)
			if(release_client) {
				client.release()
			}
			if(options.pagination) {
				return {
					rows: res.rows,
					total: total,
					is_last_page: is_last_page,
					next_offset: next_offset // (is_last_page) ? undefined : offset + filter.limit
				}
			} else {
				return res.rows
			}
		}
		if(options.format && options.format.toLowerCase() == "geojson") {
			if(release_client) {
				client.release()
			}
			return {
				"type": "FeatureCollection",
				"features": res.rows.map(row=> {
					return {
						type: "Feature",
						id: row.id,
						geometry: row.estacion.geom,
						properties: {
							tipo: tipo,
							id: row.id,
							series_id: row.id,
							nombre: row.estacion.nombre,
							estacion_id: row.estacion.id,
							rio: row.estacion.rio,
							var_id: row.var.id,
							proc_id: row.procedimiento.id,
							unit_id: row.unidades.id,
							var_nombre: row.var.nombre,
							GeneralCategory: row.var.GeneralCategory,
							timestart: (row.date_range) ? row.date_range.timestart : null,
							timeend: (row.date_range) ? row.date_range.timeend : null,
							count: (row.date_range) ? row.date_range.count : null,
							forecast_date: row.forecast_date,
							data_availability: row.date_range.data_availability,
							fuente: (row.fuente) ? row.fuente.nombre : (row.estacion.tabla) ? row.estacion.tabla : null,
							id_externo: row.estacion.id_externo,
							public: (row.fuente) ? row.fuente.public : (row.estacion.public !== undefined) ? row.estacion.public : null
						}
					}
				}),
				"limit": filter.limit,
				"offset": filter.offset,
				"is_last_page" : is_last_page,
				"next_page_url": next_page_url,
				"total": total				   
			}
		}
		// 	const result = res.rows.map(r=> {
		// 		//~ delete r.geom
		// 		//~ console.log(r.geom)
		// 		if(r.geom) {
		// 			r.geom = JSON.parse(r.geom)
		// 		} 
		// 		r.tipo = tipo
		// 		return r
		// 	})
		// 	if(release_client) {
		// 		client.release()
		// 	}
		// 	return result
		// }
		var series = []
		for(var row of res.rows) {
			delete row.estacion.longitude
			delete row.estacion.latitude
			delete row.estacion.red_id
			delete row.estacion.red_nombre
			try {
				var serie = new internal.serie(row)
			} catch(e) {
				if(release_client) {
					client.release()
				}
				throw(e)
			}
			if(filter.timestart && filter.timeend && !options.no_data) {
				options.print_observaciones = true
				try {
					if(options.guardadas) {
						if(filter.public) {
							console.log("Unauthenticated user: can't have access to ObservacionesGuardadas")
						} else {
							serie.setObservaciones(await this.getObservacionesGuardadas(tipo,{series_id:row.id,timestart:filter.timestart,timeend:filter.timeend}))
						}
					} else {
						serie.setObservaciones(await this.getObservaciones(tipo,{series_id:row.id,timestart:filter.timestart,timeend:filter.timeend}))
					}
				} catch(e){
					if(release_client) {
						client.release()
					}	
					throw(e)
				}
				if(options.getWeibullPercentiles) {
					serie.getWeibullPercentiles()
				}
			} 
			// if(tipo && tipo.toUpperCase() == "AREAL") {
			// 	tipo = "areal"
			// 	try {
			// 		serie.estacion = await this.getArea(row.area_id,{no_geom:true})
			// 		serie.fuente = await this.getFuente(row.fuentes_id)
			// 		serie.date_range = row.date_range
			// 	} catch(e){
			// 		if(release_client) {
			// 			client.release()
			// 		}	
			// 		throw(e)
			// 	}
				
			// } else if (tipo && (tipo.toUpperCase() == "RAST" || tipo.toUpperCase() == "RASTER")) {
			// 	tipo="raster"
			// 	try {
			// 		serie.estacion = await this.getEscena(row.escena_id)
			// 		serie.fuente = await this.getFuente(row.fuentes_id)
			// 	} catch(e) {
			// 		if(release_client) {
			// 			client.release()
			// 		}	
			// 		throw(e)
			// 	}
			// 	serie.date_range = row.date_range
			// 	//~ const s = new internal.serie(row.area_id, row.var_id, row.proc_id, row.unit_id, "areal", row.fuentes_id)		
			// } else { // puntual
			// 	tipo="puntual"
			// 	try {
			// 		serie.estacion = await this.getEstacion(row.estacion_id)
			// 	} catch(e) {
			// 		if(release_client) {
			// 			client.release()
			// 		}	
			// 		throw(e)
			// 	}
			// 	serie.date_range = row.date_range
			// 	//~ const s = new internal.serie(row.estacion_id, row.var_id, row.proc_id, row.unit_id, "puntual")
			// }
			// serie.tipo = tipo
			if(options.getStats) {
				await serie.getStats(client)	
			}
			series.push(serie)
		}
		if(options.getMonthlyStats && !options.getWeibullPercentiles) {
			var series_id_list = series.map(s=>s.id)
			try {
				var monthly_stats = await this.getMonthlyStatsSeries(tipo,series_id_list)
				var ms = {}
				monthly_stats.forEach(i=>{
					ms[i.series_id] = i.stats
				})
			} catch(e) {
				if(release_client) {
					client.release()
				}
				throw(e)
			}
			for(var i in series) {
				if(ms[series[i].id]) {
					if(!series[i].monthlyStats) {
						// set monthlyStats only if hasn't been already set (by serie.getWeibullPercentiles) 
						series[i].monthlyStats = ms[series[i].id]
					}
				}
			}
		}
		if(filter.percentil || options.getPercentiles) {
			var series_id_list = series.map(s=>s.id)
			try {
				var percentiles = await this.getPercentiles(tipo,series_id_list,filter.percentil)
				var perc = {}
				percentiles.forEach(i=>{
					perc[i.series_id] = i.percentiles
				})
			} catch(e) {
				throw(e)
			}
			for(var i in series) {
				if(perc[series[i].id]) {
					series[i].percentiles = perc[series[i].id]
				}
			}
		}
		if(release_client) {
			await client.release()
		}
		if(options.pagination) {
			return {
				rows: series,
				total: total,
				is_last_page: is_last_page,
				next_offset: next_offset // (is_last_page) ? undefined : offset + filter.limit
			}
		} else {
			return series
		}
	}

	static async getPercentiles(tipo="puntual",series_id,percentiles,isPublic) {
		var filter
		var params
		if(!series_id) {
			filter = "WHERE tipo=$1" 
			params = [tipo]
		} else if(Array.isArray(series_id)) {
			if(series_id.length) {
				var id_list = series_id.map(id=>parseInt(id)).join(",")
				filter = "WHERE tipo=$1 AND series_id IN (" + id_list + ")"
				params = [tipo]
			} else {
				filter = "WHERE tipo=$1" 
				params = [tipo]
			}
		} else {
			filter = "WHERE tipo=$1 AND series_id=$2"
			params = [tipo,series_id]
		}
		if(percentiles) {
			if(Array.isArray(percentiles)) {
				filter = `${filter} AND percentil::text IN (${percentiles.map(p=>`${parseFloat(p).toString()}::text`).join(",")})`
			} else {
				filter = `${filter} AND percentil::text=${parseFloat(percentiles).toString()}::text`
			}
		}
		var stmt = "SELECT tipo,series_id,json_agg(json_build_object('percentile',percentil,'count',count,'timestart',timestart,'timeend',timeend,'valor',valor) ORDER BY percentil) AS percentiles FROM series_percentiles " + filter + " GROUP BY tipo,series_id ORDER BY tipo,series_id"
		console.log(stmt)
		return global.pool.query(stmt,params)
		.then(result=>{
			return result.rows.map(row=>new internal.percentiles(row))
		})
	}
	
	// OBSERVACION //
	
	static removeDuplicatesMultiSeries(observaciones) {
		var series_id = new Set(observaciones.map(o=>o.series_id))
		var result = []
		for(var id of series_id) {
			var subset = observaciones.filter(o=>(o.series_id==id))
			if(subset.length) {
				var filtered = this.removeDuplicates(subset)
				for(var x of filtered) result.push(x)
			}
		}
		return result
	}

	static removeDuplicates(observaciones) {   // elimina observaciones con timestart duplicado
		// var timestarts = []
		// console.log("sorting...")
		observaciones.sort((a,b) => (a.timestart - b.timestart))
		// console.log("done")
		var previous_timestart = new Date("1800-01-01")
		// console.log("filtering...")
		return observaciones.filter(o=>{ 
			if(o.timestart - previous_timestart == 0) {
				console.log("removing duplicate observacion, timestart:"+o.timestart)
				return false
			} else {
				previous_timestart = o.timestart
				return true
			}
		})
	}

	static async upsertObservacion(observacion,no_update) {
		//~ console.log(observacion)
		if (!(observacion instanceof internal.observacion)) {
			//~ console.log("create observacion")
			observacion = new internal.observacion(observacion)
		}
		await observacion.getId()
		observacion.timestart = (typeof observacion.timestart) == 'string' ? new Date(observacion.timestart) : observacion.timestart
		observacion.timeend = (typeof observacion.timeend) == 'string' ? new Date(observacion.timeend) : observacion.timeend
		if(!observacion.timestart || !observacion.timeend || !observacion.valor || !observacion.series_id) {
			throw new Error("Can't create observacion. Required: timestart, timeend, valor, series_id")
		}
		if(config.verbose) {
			console.info("crud.upsertObservacion: " + observacion.toString())
		}
		if(observacion.tipo != "rast" && observacion.tipo != "raster") {
			const val_type = (Array.isArray(observacion.valor)) ? "numarr" : "num"
			const obs_tabla = (observacion.tipo == "areal") ? "observaciones_areal" : "observaciones"
			const val_tabla = (observacion.tipo == "areal") ? (val_type == "numarr") ? "valores_numarr_areal" : "valores_num_areal" : (val_type == "numarr") ? "valores_numarr" : "valores_num"
			var on_conflict_clause_obs = (no_update) ? " NOTHING " : (config.crud.update_observaciones_timeupdate) ? " UPDATE SET nombre=excluded.nombre,\
							descripcion=excluded.descripcion,\
							unit_id=excluded.unit_id,\
							timeupdate=excluded.timeupdate " : " NOTHING "
			var on_conflict_clause_val = (no_update) ? " NOTHING " : " UPDATE SET valor=excluded.valor "
			const client = await global.pool.connect()
			var res = await client.query('BEGIN')
			const queryText = "INSERT INTO " + obs_tabla + " (series_id,timestart,timeend,nombre,descripcion,unit_id,timeupdate)\
				VALUES ($1,$2,$3,$4,$5,$6,coalesce($7,now()))\
				ON CONFLICT (series_id,timestart,timeend) DO " + on_conflict_clause_obs + "\
				RETURNING *"
			var stmt = internal.utils.pasteIntoSQLQuery(queryText,[observacion.series_id,observacion.timestart,observacion.timeend,observacion.nombre,observacion.descripcion,observacion.unit_id,observacion.timeupdate])
			// console.log(stmt)
			res = await client.query(stmt)
			// return client.query(queryText,[observacion.series_id,observacion.timestart,observacion.timeend,observacion.nombre,observacion.descripcion,observacion.unit_id,observacion.timeupdate])
			if(res.rows.length == 0) {
				// NO UPSERTED OBS
				if(!config.crud.update_observaciones_timeupdate) {
					// TRY TO UPDATE VAL
					const obs = new internal.observacion({series_id: observacion.series_id, timestart: observacion.timestart, timeend:observacion.timeend, nombre: observacion.nombre,descripcion: observacion.descripcion, unit_id: observacion.unit_id, timeupdate: observacion.timeupdate})
					const insertValorText = "UPDATE " + val_tabla + " SET valor=$1 FROM " + obs_tabla + " WHERE " + obs_tabla + ".id=" + val_tabla + ".obs_id AND series_id=$2 AND timestart=$3 AND timeend=$4 RETURNING obs_id,valor"
					// console.log(pasteIntoSQLQuery(insertValorText,[observacion.valor, observacion.series_id, observacion.timestart, observacion.timeend]))
					res = await client.query(insertValorText, [observacion.valor, observacion.series_id, observacion.timestart, observacion.timeend])
					if(!res.rows.length) {
						console.log("No se actualizó observación")
						res = await client.query('ROLLBACK')
						if(client.notifications) {
							throw(client.notifications.map(n=>n.message).join(","))
						} else {
							client.release()
							throw("No se insertó observación")
						}
					}
					obs.tipo=observacion.tipo
					obs.id = res.rows[0].obs_id
					obs.valor=res.rows[0].valor
					res = await client.query("COMMIT")
					client.release()
					return new internal.observacion(obs)
				}
				console.log("No se inserto observacion")
				res = await client.query('ROLLBACK')
				if(client.notifications) {
					throw(client.notifications.map(n=>n.message).join(","))
				} else {
					client.release()
					throw("No se insertó observación")
				}
			} else {
				const obs= new internal.observacion(res.rows[0])
				const insertValorText = "INSERT INTO " + val_tabla + " (obs_id,valor)\
					VALUES ($1,$2)\
					ON CONFLICT (obs_id)\
					DO " + on_conflict_clause_val + "\
					RETURNING *"
				// console.log(insertValorText)
				res = await client.query(insertValorText, [obs.id, observacion.valor])
				obs.tipo=observacion.tipo
				obs.valor=res.rows[0].valor
				res = await client.query("COMMIT")
				client.release()
				return new internal.observacion(obs)
			}
		} else {   // RAST //
			var stmt
			var args
			var valor_string
			if(observacion.valor instanceof Buffer) {
				valor_string = "\\x" + observacion.valor.toString('hex')
			} else {
				valor_string = observacion.valor
			}
			// console.log("valor_string:" + valor_string)
			var on_conflict_clause = (no_update) ? " NOTHING " : " UPDATE SET valor=excluded.valor, timeupdate=excluded.timeupdate "
			if(observacion.scale) {
				var scale = parseFloat(observacion.scale)
				var offset = (observacion.offset) ? parseFloat(observacion.offset) : 0
				var expression = "[rast]*" + scale + "+" + offset
				stmt = "INSERT INTO observaciones_rast (series_id, timestart, timeend, valor, timeupdate)\
				VALUES ($1, $2, $3, ST_mapAlgebra(ST_FromGDALRaster($4),'32BF',$5), $6)\
				ON CONFLICT (series_id, timestart, timeend)\
				DO " + on_conflict_clause + "\
				RETURNING id,series_id,timestart,timeend,st_asgdalraster(valor,'GTiff') valor,timeupdate"    // '\\x'||encode(st_asgdalraster(valor,'GTiff')::bytea,'hex')
				args = [observacion.series_id, observacion.timestart, observacion.timeend, valor_string, expression, observacion.timeupdate]
			} else {
				stmt = "INSERT INTO observaciones_rast (series_id, timestart, timeend, valor, timeupdate)\
				VALUES ($1, $2, $3, ST_FromGDALRaster($4), $5)\
				ON CONFLICT (series_id, timestart, timeend)\
				DO " + on_conflict_clause + "\
				RETURNING id,series_id,timestart,timeend,st_asgdalraster(valor, 'GTIff') valor,timeupdate" // '\\x'||encode(st_asgdalraster(valor,'GTiff')::bytea,'hex')
				args = [observacion.series_id, observacion.timestart, observacion.timeend, valor_string, observacion.timeupdate]
			}	
			//~ if(options.hex) {
				//~ args[3] = '\\x' + args[3]
			//~ }
			// console.log(pasteIntoSQLQuery(stmt,args))
			const upserted = await global.pool.query(stmt,args)
			if(upserted.rows.length == 0) {
				console.log("nothing upserted")
				throw("nothing upserted")
			} else {
				//~ console.log("raster upserted")
				upserted.rows[0].tipo="rast"
				return new internal.observacion(upserted.rows[0])
			}
		}
	}
	
	// static upsertObservacionesQuery(observaciones,tipo) {
	// 	var query
	// 	switch(tipo.toLowerCase()) {
	// 		case "areal":
	// 			break;
	// 		case "rast":
	// 		case "raster":
	// 			break;
	// 		default: // "puntual"
	// 	}
		
	// 	var query = "\
	// 		INSERT INTO fuentes (id, nombre, data_table, data_column, tipo, def_proc_id, def_dt, hora_corte, def_unit_id, def_var_id, fd_column, mad_table, scale_factor, data_offset, def_pixel_height, def_pixel_width, def_srid, def_extent, date_column, def_pixeltype, abstract, source) \
	// 		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, st_geomfromtext($18), $19, $20, $21, $22)\
	// 		ON CONFLICT (id) DO UPDATE SET \
	// 			id=excluded.id, \
	// 			nombre=excluded.nombre, \
	// 			data_table=excluded.data_table,\
	// 			data_column=excluded.data_column,\
	// 			tipo=excluded.tipo,\
	// 			def_proc_id=excluded.def_proc_id,\
	// 			def_dt=excluded.def_dt,\
	// 			hora_corte=excluded.hora_corte,\
	// 			def_unit_id=excluded.def_unit_id,\
	// 			def_var_id=excluded.def_var_id,\
	// 			fd_column=excluded.fd_column,\
	// 			mad_table=excluded.mad_table,\
	// 			scale_factor=excluded.scale_factor,\
	// 			data_offset=excluded.data_offset,\
	// 			def_pixel_height=excluded.def_pixel_height,\
	// 			def_pixel_width=excluded.def_pixel_width,\
	// 			def_srid=excluded.def_srid,\
	// 			def_extent=excluded.def_extent,\
	// 			date_column=excluded.date_column,\
	// 			def_pixeltype=excluded.def_pixeltype,\
	// 			abstract=excluded.abstract,\
	// 			source=excluded.source\
	// 		RETURNING *"
	// 	var params = [fuente.id, fuente.nombre, fuente.data_table, fuente.data_column, fuente.tipo, fuente.def_proc_id, fuente.def_dt, fuente.hora_corte, fuente.def_unit_id, fuente.def_var_id, fuente.fd_column, fuente.mad_table, fuente.scale_factor, fuente.data_offset, fuente.def_pixel_height, fuente.def_pixel_width, fuente.def_srid, (fuente.def_extent) ? fuente.def_extent.toString() : null, fuente.date_column, fuente.def_pixeltype, fuente.abstract, fuente.source]
	// 	return internal.utils.pasteIntoSQLQuery(query,params)
	// }

	static async upsertObservaciones(observaciones,tipo,series_id,options={},client) {
		if(!observaciones) {
			return Promise.reject("falta observaciones")
		}
		if(observaciones.length==0) {
			console.log("upsertObservaciones: nothing to upsert (length==0)")
			return Promise.resolve([]) // "upsertObservaciones: nothing to upsert (length==0)")
		}
		var serie
		if(series_id) {
			// console.log("setting series_id in each obs...")
			observaciones = observaciones.map(o=>{
				o.series_id = series_id
				return o
			})
			observaciones = this.removeDuplicates(observaciones)
			// console.log("done")
		} else {
			observaciones = this.removeDuplicatesMultiSeries(observaciones)
		}
		if(!tipo && observaciones[0].tipo) {
			var tipo_guess = observaciones[0].tipo
			var count = 0
			for(var i in observaciones) {
				if (observaciones[i].tipo != tipo_guess) {
					break
				}
				count++
			}
			if(count == observaciones.length) {
				tipo = tipo_guess
			}
		}
		if(series_id && tipo) {
			console.log("try read series_id " + series_id)
			try {
				serie = await this.getSerie(tipo,series_id,undefined,undefined,{no_metadata:true},undefined,undefined,client)
				console.log("try read var_id " + serie.var.id)
				serie.var = await this.getVar(serie.var.id,client) 
			} catch(e) {
				throw(e)
			}
			console.debug("got series, var.timeSupport=" + ((serie.var.timeSupport) ? serie.var.timeSupport.toString() : "null"))
		}
		if(config.verbose) {
			console.debug("crud.upsertObservaciones: tipo: " + tipo)
		}
		if(tipo) {
			if(tipo=="puntual") {
				return this.upsertObservacionesPuntual(observaciones,options.skip_nulls,options.no_update,(serie) ? serie.var.timeSupport : undefined,client)
			} else if(tipo=="areal") {
				return this.upsertObservacionesAreal(observaciones,options.skip_nulls,options.no_update, (serie) ? serie.var.timeSupport : undefined,client)
			}
			observaciones = observaciones.map(o=>{
				o.tipo = tipo
				return o
			})
		} 
		if(config.verbose) {
			console.debug("crud.upsertObservaciones: tipo: " + tipo)
		}
		var upserted = []
		var errors = []
		for(var i=0; i<observaciones.length; i++) {
			const observacion = new internal.observacion(observaciones[i])
			// console.log("pushing obs:"+observacion.toCSVless())
			//~ console.log("valor.length:"+observacion.valor.length)
			if(options.skip_nulls && (observacion.valor === null || observacion.valor === undefined )) {
				console.log("skipping null value, series_id:" + observacion.series_id + " timestart:" + observacion.timestart) 
			} else {
				try {
					var o = await this.upsertObservacion(observacion,options.no_update)
					upserted.push(o)
				} catch (e) {
					errors.push(e)
				}
			}
		}
		if(!upserted.length && errors.length) {
			throw(errors)
		}
		return upserted
		// Promise.allSettled(promises)
		// .then(results=>{
		// 	return results.map(r=>{
		// 		if(r.status == "fulfilled") {
		// 			return r.value //valuetype
		// 		} else {
		// 			console.error("upsert rejected, reason:" + r.reason)
		// 			return
		// 		}
		// 	}).filter(r=>r)
		// })
	}
	
			
	static async upsertObservacionesPuntual(observaciones,skip_nulls,no_update,timeSupport,client) {
		//~ console.log({observaciones:observaciones})	
		var obs_values = []
		observaciones = observaciones.map(observacion=> {
			if(!observacion.series_id) {
				console.error("missing series_id")
				return
			}
			observacion.series_id = parseInt(observacion.series_id)
			if(observacion.series_id.toString()=='NaN')
			{
				console.error("invalid series_id")
				return
			}
			observacion.timestart = !(observacion.timestart instanceof Date) ? new Date(observacion.timestart) : observacion.timestart
			if(observacion.timestart.toString()=='Invalid Date') {
				console.error("invalid timestart")
				return
			}
			observacion.timeend = (timeSupport) ? timeSteps.advanceTimeStep(observacion.timestart,timeSupport) : !(observacion.timeend instanceof Date) ? new Date(observacion.timeend) : observacion.timeend
			if(observacion.timeend.toString()=='Invalid Date')  {
				console.error("invalid timeend")
				return
			}
			observacion.valor = parseFloat(observacion.valor)
			if(observacion.valor.toString()=='NaN')
			{
				console.warn("upsertObservacionesPuntual: NaN value at time " + observacion.timestart.toISOString() + ", skipping")
				return
			}
			if(skip_nulls && observacion.valor === null) {
				console.log("skipping null value, series_id:" + observacion.valor + ", timestart:" + observacion.timestart.toISOString())
				return
			}
			obs_values.push(sprintf("(%d,'%s'::timestamptz,'%s'::timestamptz,'upsertObservacionesPuntual',now(),%f)", observacion.series_id, observacion.timestart.toISOString(), observacion.timeend.toISOString(),observacion.valor))
			return observacion
		}).filter(o=>o)
		//~ console.log({observaciones:observaciones})
		//~ console.log({obs_values:obs_values})
		if(obs_values.length == 0) {
			return Promise.reject(new Error("no valid observations"))
		}
		obs_values = obs_values.join(",")
		var on_conflict_clause_obs = (no_update) ? " NOTHING " : (config.crud.update_observaciones_timeupdate) ? " UPDATE SET nombre=excluded.nombre,\
					  timeupdate=excluded.timeupdate" : " NOTHING "
		var on_conflict_clause_val = (no_update) ? " NOTHING " : " UPDATE SET valor=excluded.valor"
		var disable_trigger = "" // (timeSupport) ? "ALTER TABLE observaciones DISABLE TRIGGER obs_puntual_dt_constraint_trigger;" : ""
		var enable_trigger = "" //(timeSupport) ? "ALTER TABLE observaciones ENABLE TRIGGER obs_puntual_dt_constraint_trigger;" : ""
		var release_client = false
		if(!client) {
			client = await global.pool.connect()
			release_client = true
			await client.query('BEGIN')
		}
		try {
			//~ console.log({obs_values:obs_values})
			await client.query("\
					" + disable_trigger  + "\
					CREATE TEMPORARY TABLE obs (series_id int,timestart timestamp,timeend timestamp,nombre varchar,timeupdate timestamp,valor real) ON COMMIT DROP ;\
					INSERT INTO obs (series_id,timestart,timeend,nombre,timeupdate,valor)\
					VALUES " + obs_values + ";")
			await client.query("\
					INSERT INTO observaciones (series_id,timestart,timeend,nombre,timeupdate)\
					SELECT series_id,timestart,timeend,nombre,timeupdate\
					FROM obs \
					ON CONFLICT (series_id,timestart,timeend)\
					DO " + on_conflict_clause_obs)
			await client.query("\
					INSERT INTO valores_num (obs_id,valor)\
					SELECT observaciones.id,obs.valor\
					FROM observaciones,obs\
					WHERE observaciones.series_id=obs.series_id\
					AND observaciones.timestart=obs.timestart\
					AND observaciones.timeend=obs.timeend \
					ON CONFLICT(obs_id)\
					DO " + on_conflict_clause_val)
			const rows = await executeQueryReturnRows("\
					SELECT observaciones.id,\
							observaciones.series_id,\
							observaciones.timestart,\
							observaciones.timeend,\
							observaciones.nombre,\
							observaciones.timeupdate,\
							valores_num.valor\
					FROM observaciones,valores_num,obs\
					WHERE observaciones.series_id=obs.series_id\
					AND observaciones.timestart=obs.timestart\
					AND observaciones.timeend=obs.timeend\
					AND observaciones.id=valores_num.obs_id\
					ORDER BY observaciones.series_id,observaciones.timestart",undefined,client,false)
			if(!rows) {
				throw("No se insertaron registros, " + client.notifications.map(n=>n.message).join(","))
			}
			if(rows.length==0) {
				if(client.notifications) {
					throw(client.notifications.map(n=>n.message).join(",")) //"No observaciones inserted")
				} else {
					throw("No observaciones inserted")
				}
			}
			await client.query(enable_trigger)
			if(release_client) {
				await client.query("COMMIT;")
				await client.release()
			} else {
				await client.query("DROP table obs")
			}
			// console.log("upserted: " + result.rows.length + " obs_puntuales")
			return rows
		} catch(e) {
			// console.error({message: "upsertObservacionesPuntual error",error:e})
			if(release_client) {
				await client.query("ROLLBACK")
				await client.release()
			}
			throw(e)
		}
	}
	
	static async upsertObservacionesAreal(observaciones,skip_nulls,no_update, timeSupport,client) {
		var obs_values = []
		observaciones = observaciones.map(observacion=> {
			if(!observacion.series_id) {
				console.error("missing series_id")
				return
			}
			observacion.series_id = parseInt(observacion.series_id)
			if(observacion.series_id.toString()=='NaN')
			{
				console.error("invalid series_id")
				return
			}
			observacion.timestart = !(observacion.timestart instanceof Date) ? new Date(observacion.timestart) : observacion.timestart
			if(observacion.timestart.toString()=='Invalid Date') {
				console.error("invalid timestart")
				return
			}
			observacion.timeend = (timeSupport) ? timeSteps.advanceTimeStep(observacion.timestart,timeSupport) : !(observacion.timeend instanceof Date) ? new Date(observacion.timeend) : observacion.timeend
			if(observacion.timeend.toString()=='Invalid Date')  {
				console.error("invalid timeend")
				return
			}
			observacion.valor = parseFloat(observacion.valor)
			if(observacion.valor.toString()=='NaN')
			{
				// console.error("invalid valor")
				return
			}
			if(skip_nulls && observacion.valor === null) {
				console.log("skipping null value, series_id:" + observacion.valor + ", timestart:" + observacion.timestart.toISOString())
				return
			}
			obs_values.push(sprintf("(%d,'%s'::timestamptz,'%s'::timestamptz,'upsertObservacionesAreal',now())", observacion.series_id, observacion.timestart.toISOString(), observacion.timeend.toISOString()))
			return observacion
		}).filter(o=>o)
		if(config.verbose) {
			// console.log("crud.upsertObservacionesAreal: obs_values:" + JSON.stringify(obs_values))
		}
		if(!obs_values.length) {
			console.error("No values to insert")
			return []
		}
		obs_values = obs_values.join(",")
		var on_conflict_clause_obs = (no_update) ? " NOTHING " : (config.crud.update_observaciones_timeupdate) ? " UPDATE SET nombre=excluded.nombre,\
		timeupdate=excluded.timeupdate" : " NOTHING "
		var on_conflict_clause_val = (no_update) ? " NOTHING " : " UPDATE SET valor=excluded.valor "
		var disable_trigger = "" // (timeSupport) ? "ALTER TABLE observaciones_areal DISABLE TRIGGER obs_dt_trig;" : ""
		var enable_trigger = "" // (timeSupport) ? "ALTER TABLE observaciones_areal ENABLE TRIGGER obs_dt_trig;" : ""
		var release_client = false
		if(!client) {
			client = await global.pool.connect()
			release_client = true
			await client.query("BEGIN")
		}
		try {
			await client.query(disable_trigger)
			var rows = await executeQueryReturnRows("\
					INSERT INTO observaciones_areal (series_id,timestart,timeend,nombre,timeupdate)\
					VALUES " + obs_values + "\
					ON CONFLICT (series_id,timestart,timeend)\
					DO " + on_conflict_clause_obs + "\
					RETURNING *",undefined,client,false)
			if(!rows) {
				throw("insert into observaciones_areal query error")
			}
			if(rows.length==0){
				throw("No observaciones_areal inserted")
			}
			//~ console.log(result.rows)
			var val_values = observaciones.map((obs,i)=>{
				if(config.verbose) {
					// console.log("crud.upsertObservacionesAreal: obs:" + JSON.stringify(obs))
				}
				var matches = rows.filter(r=>(obs.series_id == r.series_id && obs.timestart.toISOString() == r.timestart.toISOString() && obs.timeend.toISOString() == r.timeend.toISOString()))
				if(!matches.length) {
					console.log("row not inserted timestart:" + obs.timestart.toISOString())
					return
				}
				const o = matches[0]
				// var o = rows.shift()
				// if(config.verbose) {
				// 	// console.log("crud.upsertObservacionesAreal: o:" + JSON.stringify(o))
				// }
				// if(obs.series_id == o.series_id && obs.timestart.toISOString() == o.timestart.toISOString() && obs.timeend.toISOString() == o.timeend.toISOString()) {
				observaciones[i].id = o.id
					//~ console.log({obs:obs, o:o})
				return sprintf("(%d,%f)", o.id, obs.valor)
			}).filter(o=>o).join(",")
				//~ console.log({val_values:val_values})
			if(!val_values.length) {
				throw("Nothing inserted")
			}
			var values = await executeQueryReturnRows("INSERT INTO valores_num_areal (obs_id,valor)\
					VALUES " + val_values + "\
					ON CONFLICT (obs_id)\
					DO " + on_conflict_clause_val + "\
					RETURNING *",undefined,client,false)
			await client.query(enable_trigger) 
			if(release_client) {
				await client.query("COMMIT")
			}
			return observaciones
		} catch(e) {
			if(release_client) {
				await client.query("ROLLBACK")
			}
			throw(e)
		} finally {
			if(release_client) {
				client.release()
			}
		}
	}
	
	static async upsertObservacionesFromCSV(tipo="puntual",csvFile) {
		if(!csvFile) {
			return Promise.reject("csvFile missing")
		}
		if(!fs.existsSync(csvFile)) {
			return Promise.reject("file " + csvFile + " not found")
		}
					//~ if(tipo.toLowerCase() == "puntual") {
		var on_conflict_clause = (config.crud.update_observaciones_timeupdate) ? " UPDATE SET timeupdate=excluded.timeupdate " : " NOTHING "
		return this.runSqlCommand(this.config.database,"BEGIN;\
			CREATE TEMPORARY TABLE obs_temp (series_id int,timestart timestamptz,timeend timestamptz,valor real) ON COMMIT DROP;\
			COPY obs_temp (series_id,timestart,timeend,valor) FROM STDIN WITH DELIMITER ',';\
			INSERT INTO observaciones (series_id,timestart,timeend)\
				SELECT series_id,timestart::timestamp,timeend::timestamp FROM obs_temp \
				ON CONFLICT(series_id,timestart,timeend) \
				DO " + on_conflict_clause + "\
				RETURNING *;\
				INSERT INTO valores_num (obs_id,valor) select observaciones.id,obs_temp.valor from observaciones,obs_temp where observaciones.series_id=obs_temp.series_id and observaciones.timestart=obs_temp.timestart::timestamp and observaciones.timeend=obs_temp.timeend::timestamp ON CONFLICT(obs_id) DO UPDATE SET valor=excluded.valor;\
			COMMIT;",csvFile)
		//~ return new Promise( (resolve, reject) => {
			//~ (async () => {
				//~ const client = await global.pool.connect()
				//~ var results
				//~ try {
					//~ await client.query('BEGIN;')
					//~ await client.query("CREATE TEMPORARY TABLE obs_temp (series_id int,timestart timestamp,timeend timestamp,valor real) ON COMMIT DROP;")
					//~ await client.query('\copy obs_temp (series_id,timestart,timeend,valor) FROM \''+csvFile+"' WITH DELIMITER ','")
					//~ if(tipo.toLowerCase() == "puntual") {
						//~ results = await client.query("INSERT INTO observaciones (series_id,timestart,timeend,valor)\
							//~ SELECT series_id,timestart,timeend,valor FROM obs_temp \
							//~ ON CONFLICT(series_id,timestart,timeend) \
							//~ DO UPDATE SET valor=excluded.valor, timeupdate=excluded.timeupdate\
							//~ RETURNING *")
					//~ } else if (tipo.toLowerCase() == "areal") {
						//~ results = await client.query("INSERT INTO observaciones_areal (series_id,timestart,timeend,valor)\
							//~ SELECT series_id,timestart,timeend,valor FROM obs_temp \
							//~ ON CONFLICT(series_id,timestart,timeend) \
							//~ DO UPDATE SET valor=excluded.valor, timeupdate=excluded.timeupdate\
							//~ RETURNING *")
					//~ } else if (tipo.toLowerCase() == "rast" || tipo.toLowerCase() == "raster") {
						//~ results = await client.query("INSERT INTO observaciones_rast (series_id,timestart,timeend,valor)\
							//~ SELECT series_id,timestart,timeend,valor FROM obs_temp \
							//~ ON CONFLICT(series_id,timestart,timeend) \
							//~ DO UPDATE SET valor=excluded.valor, timeupdate=excluded.timeupdate\
							//~ RETURNING *")
					//~ } else {
						//~ throw("bad 'tipo'")
					//~ }
					//~ client.query("COMMIT")
				//~ } catch (e) {
					//~ console.error({message:"query error",error:e})
					//~ client.query("ROLLBACK")
					//~ client.end()
					//~ reject(e)
					//~ return
				//~ }
				//~ client.end()
				//~ if(!results) {
					//~ reject("query error")
					//~ return
				//~ }
				//~ if(results.rows.length==0) {
					//~ console.log("0 registros insertados")
					//~ resolve([])
				//~ }
				//~ console.log(results.rows.length + " registros insertados")
				//~ resolve(results.rows.map(r=>{
					//~ return internal.observacion(r)
				//~ }))
			//~ })()
		//~ })	
	}
	
	// removeDuplicates(observaciones) {
	// 	var unique_date_series = []
	// 	return observaciones.filter(o=> {
	// 		if(unique_date_series.indexOf(o.series_id + "_" + o.timestart.toString()) >= 0) {
	// 			return false
	// 		} else {
	// 			unique_date_series.push(o.series_id + "_" + o.timestart.toString())
	// 			return true
	// 		}
	// 	})
	// }
	
	static async runSqlCommand(dbconfig,sql,input_file) {
		return new Promise( (resolve,reject) =>{
			var sys_call = "PGPASSWORD=" + dbconfig.password + " psql " + dbconfig.database + " " + dbconfig.user + " -h " + dbconfig.host + " -p " + dbconfig.port + " -c \"" + sql + "\" -w" 
			if(input_file) {
				sys_call = "less " + input_file + " | " + sys_call
			}
			exec(sys_call, (err, stdout, stderr)=>{
				if(err) {
					reject(err)
				}
				resolve(stdout)
			})
		})
	}
	
	static async updateObservacionById(o) {
		var observacion = new internal.observacion(o)
		//~ console.log({observacion:observacion})
		var validFieldsObs = ["timestart","timeend","timeupdate"]
		if(! observacion.id || ! observacion.tipo) {
			console.log("missing observacion.id y/o observacion.tipo")
			return Promise.reject()
		}
		var promises = []
		var fieldsObs = validFieldsObs.filter(f=>observacion[f])
		var obstable = (observacion.tipo == "areal") ? "observaciones_areal" : "observaciones"
		var valtable = (observacion.tipo == "areal") ? "valores_num_areal" : "valores_num"
		return global.pool.connect()
		.then(client=>{
			if(fieldsObs.length > 0) {
				var setfieldsObs = fieldsObs.map( (f,i) => f + "=$" + (i+1))
				var valuesObs = fieldsObs.map(f=> observacion[f])
				var promises = []
				var stmt = "UPDATE " + obstable + " SET " + setfieldsObs.join(",") + " WHERE id=$" + (fieldsObs.length+1) + " RETURNING *"
				valuesObs.push(observacion.id)
				promises.push(client.query(stmt, valuesObs))
				console.log(internal.utils.pasteIntoSQLQuery(stmt,valuesObs))
			} else {
				promises.push(client.query("SELECT * from " + obstable + " WHERE id=$1",[observacion.id]))
				console.log(internal.utils.pasteIntoSQLQuery("SELECT * from " + obstable + " WHERE id=$1",[observacion.id]))
			}
			if(observacion.valor) {
				var stmt = "UPDATE " + valtable + " SET valor=$1 WHERE obs_id=$2 RETURNING *"
				promises.push(client.query(stmt,[observacion.valor, observacion.id]))
				console.log(internal.utils.pasteIntoSQLQuery(stmt,[observacion.valor, observacion.id]))
			} else {
				promises.push(client.query("SELECT * from " + valtable + " WHERE obs_id=$1",[observacion.id]))
				console.log(internal.utils.pasteIntoSQLQuery("SELECT * from " + valtable + " WHERE obs_id=$1",[observacion.id]))
			}
			return Promise.all(promises)
			.then(result=>{
				if(!result[0].rows) {
					var notification = client.notifications[client.notifications.length-1].message.toString()
					client.release()
					throw notification
					
				}
				if(result[0].rows.length == 0) {
					if(client.notifications && client.notifications.length > 0) {
						var notification = client.notifications[client.notifications.length-1].message.toString()
						client.release()
						throw notification
					} else {
						throw "id de observación no encontrado"
					}
				}
				var obs = result[0].rows[0]
				if(result[1].rows) {
					obs.valor = result[1].rows[0].valor
					obs.tipo = "puntual"
					client.release()
					return new internal.observacion(obs)
				} else {
					var notification = client.notifications[client.notifications.length-1].message.toString()
					client.release
					throw notification
				}
			})
		})
	}
		
	static async deleteObservacion(tipo,id,client) {
		if(parseInt(id).toString() == "NaN") {
			return Promise.reject("id inválido")
		}
		const obs_tabla = (tipo == "areal") ? "observaciones_areal" : (tipo == "rast" || tipo == "raster") ? "observaciones_rast" : "observaciones"
		const val_tabla = (tipo == "areal") ? "valores_num_areal" : (tipo == "rast" || tipo == "raster") ? "" : "valores_num"
		if(!client) {
			var release_client = true
		}
		client = (client) ? client : await global.pool.connect()
		try {
			if(release_client) {
				await client.query('BEGIN')
			}
			var deleted_valores
			var deleteObsText
			if(val_tabla != "") { // PUNTUAL o AREAL
				const deleteValorText = "DELETE FROM " + val_tabla + "\
					WHERE obs_id=$1\
					RETURNING *"
				deleted_valores = await executeQueryReturnRows(deleteValorText,[id],client)
				deleteObsText = "DELETE FROM " + obs_tabla + "\
					WHERE id=$1\
					RETURNING *"
			} else { // RASTER
				deleteObsText = "DELETE FROM " + obs_tabla + "\
					WHERE id=$1\
					RETURNING series_id,id,timestart,timeend,'\\x' || encode(st_asgdalraster(valor, 'GTIff')::bytea,'hex') valor, timeupdate"
			}
			//~ var obs=res.rows[0]
			const deleted_observaciones = await executeQueryReturnRows(deleteObsText, [id],client)
					//~ obs.tipo=tipo
					//~ obs.valor=res.rows[0].valor
			if(release_client) {
				await client.query("COMMIT")
				await client.release()
			}
			if(val_tabla != "") { // PUNTUAL o AREAL
				if(deleted_valores.length>0 && deleted_observaciones.length>0) {
					deleted_observaciones[0].valor = deleted_valores[0].valor
					return new internal.observacion(deleted_observaciones[0]) // {id:deleted_observaciones[0].id,tipo:deleted_observaciones[0].tipo, series_id:deleted_observaciones[0].series_id, timestart:deleted_observaciones[0].timestart, timeend:deleted_observaciones[0].timeend, nombre:deleted_observaciones[0].nombre, descripcion:deleted_observaciones[0].descripcion, unit_id:deleted_observaciones[0].unit_id, timeupdate:deleted_observaciones[0].timeupdate, valor:deleted_valores[0].valor})
				} else {
					console.error("observacion delete attempt resulted in zero rows")
					return
				}
			} else { // RASTER
				if(deleted_observaciones.length>0) {
					deleted_observaciones[0].tipo = "raster"
					return new internal.observacion(deleted_observaciones[0]) // {id:deleted_observaciones[0].id,tipo:"raster", series_id:deleted_observaciones[0].series_id, timestart:deleted_observaciones[0].timestart, timeend:deleted_observaciones[0].timeend, timeupdate:deleted_observaciones[0].timeupdate, valor:deleted_observaciones[0].valor})
				} else {
					console.error("observacion delete attempt resulted in zero rows")
					return
				}
			}
		} catch(e) {
			if(release_client) {
				await client.query('ROLLBACK')
				await client.release()
			}
			throw e
		}
	}
	
	static async deleteObservacionesById(tipo,id,no_send_data=false,client) {
		// var max_ids_per_statements = 500
		if(Array.isArray(id)) { 	
			if(no_send_data) {
				for(var i of id) {
					try {
						await this.deleteObservacion(tipo,i,client)
					} catch (e) {
						console.error(e.toString())
					}
				}
				return
			} else {
				const deleted = []
				for(var i of id) {
					deleted.push(await this.deleteObservacion(tipo,i,client))
				}
				return deleted.filter(d=>d)
			}
		} else {
			const deleted = await this.deleteObservacion(tipo,id,client)
			if(no_send_data) {
				return
			} else {
				return deleted
			}
		}
	}
	
	static async deleteObservaciones2(tipo,filter,options) {
		var stmt = this.build_delete_observaciones_query(tipo,filter,options)
		return global.pool.query(stmt)
		.then(result=>{
			return (options && options.no_send_data) ? result.rows.length : new internal.observaciones(result.rows)
		})	
	}

	static async deleteObservaciones(tipo,filter,options,client) { // con options.no_send_data devuelve count de registros eliminados, si no devuelve array de registros eliminados
		tipo = (tipo.toLowerCase() == "areal") ? "areal" : (tipo.toLowerCase() == "rast" || tipo.toLowerCase() == "raster") ? "raster" : "puntual"
		if(tipo == "raster") {
			var stmt
			var args
			var returning_clause = (options && options.no_send_data) ? " RETURNING 1 AS d" : " RETURNING series_id,id,timestart,timeend,ST_AsGDALRaster(valor, 'GTIff') valor, timeupdate"
			var select_deleted_clause = (options && options.no_send_data) ? " SELECT count(d) FROM deleted" : "SELECT * FROM deleted"
			if(filter.id) {
				stmt = "WITH deleted AS (DELETE FROM observaciones_rast WHERE id=$1 " + returning_clause + ") " +  select_deleted_clause
				args = [parseInt(filter.id)]
			} else {
				var valid_filters = {
					series_id: {type:"integer"},
					timestart:{type:"timestart"},
					timeend:{type:"timeend"},
					timeupdate:{type:"string"}
				}
				var filter_string = internal.utils.control_filter2(valid_filters,filter)
				if(!filter_string) {
					return Promise.reject(new Error("invalid filter value"))
				}
				stmt = "WITH deleted AS (DELETE FROM observaciones_rast WHERE 1=1 " + filter_string + returning_clause + ") " + select_deleted_clause
				args=[]
			}
			// console.log(pasteIntoSQLQuery(stmt,args))
			var result = await executeQueryReturnRows(stmt,args,client)
			if(!result) {
				console.log("Error in transaction: no rows returned")
				return (options && options.no_send_data) ? 0 : []
			}
			if(result.length == 0) {
				console.log("0 rows deleted")
				return (options && options.no_send_data) ? 0 : []
			}
			if (options && options.no_send_data) {
				console.log(result[0].count + " rows deleted")
				return parseInt(result[0].count)
			} else {
				result = result.map(o=>{
					o.tipo = "raster"
					return o
				})
				var observaciones = new internal.observaciones(result) // []
				// for(var i=0; i< result.length;i++) {
				// 	const deleted_observacion = new internal.observacion(tipo, result[i].series_id, result[i].timestart, result[i].timeend, result[i].timeupdate, result[i].valor)
				// 	deleted_observacion.id = result[i].id
				// 	observaciones.push(deleted_observacion)
				// }
				return observaciones
			}
		} else {
			const obs_tabla = (tipo == "areal") ? "observaciones_areal" : "observaciones"
			const val_tabla = (tipo == "areal") ? "valores_num_areal" : "valores_num"
			const series_tabla = (tipo == "areal") ? "series_areal" : "series"
			var deleteValorText
			var deleteObsText
			var returning_clause = (options && options.no_send_data) ? " RETURNING 1 AS d" : " RETURNING *"
			var select_deleted_clause = (options && options.no_send_data) ? " SELECT count(d) FROM deleted" : "SELECT * FROM deleted"
			if(filter.id) {
				if(Array.isArray(filter.id)) {
					if(filter.id.length == 0) {
						console.info("crud/deleteObservaciones: Nothing to delete (passed zero length id array)")
						return Promise.resolve([])
					}
					deleteValorText = "WITH deleted AS (DELETE FROM " + val_tabla + " WHERE obs_id IN (" + filter.id.join(",") + ") " + returning_clause + ") " + select_deleted_clause
					deleteObsText = "WITH deleted AS (DELETE FROM " + obs_tabla + " WHERE id IN (" + filter.id.join(",") + ") " + returning_clause + ") " + select_deleted_clause
				} else {
					deleteValorText = "WITH deleted AS (DELETE FROM " + val_tabla + " WHERE obs_id=" + parseInt(filter.id) + " " + returning_clause + ") " + select_deleted_clause
					deleteObsText = "WITH deleted AS (DELETE FROM " + obs_tabla + " WHERE id=" + parseInt(filter.id)  + returning_clause + ") " + select_deleted_clause
				}
			} else {
				var valid_filters = {
					series_id:{type:"integer"},
					timestart:{type:"timestart"},
					timeend:{type:"timeend"},
					unit_id:{type:"integer"},
					timeupdate:{type:"string"},
					valor:{type:"numeric_interval"},
					var_id:{type:"integer",table:series_tabla},
					proc_id:{type:"integer",table:series_tabla},
					unit_id:{type:"integer",table:series_tabla}
				}
				var join_clause = ""
				var obs_using_clause = ""
				var obs_where_clause = "WHERE 1=1"
				if(filter.var_id != undefined || filter.proc_id != undefined || filter.unit_id != undefined || filter.estacion_id != undefined || filter.area_id != undefined || filter.tabla != undefined || filter.tabla_id != undefined || filter.fuentes_id != undefined || filter.id_externo != undefined || filter.geom != undefined) {
					join_clause += `JOIN ${series_tabla} ON (${series_tabla}.id = ${obs_tabla}.series_id)`
					obs_using_clause = `USING ${series_tabla}`
					obs_where_clause = `WHERE ${series_tabla}.id = ${obs_tabla}.series_id`
					if(tipo == "areal") {
						join_clause += `
								JOIN areas_pluvio ON (areas_pluvio.unid = series_areal.area_id)`
						obs_using_clause += `
								JOIN areas_pluvio ON (areas_pluvio.unid = series_areal.area_id)`
						valid_filters = Object.assign(
							valid_filters, 
							{
								area_id: {
									type: "integer",
									table: "series_areal",
									alias: "estacion_id"
								},
								fuentes_id: {
									type: "integer",
									table: "series_areal"
								},
								geom: {
									type: "geometry",
									table: "areas_pluvio"
								}
							}
						)
					} else {
						join_clause += `
								JOIN estaciones ON (estaciones.unid = series.estacion_id)`
						obs_using_clause += `
								JOIN estaciones ON (estaciones.unid = series.estacion_id)`
						valid_filters = Object.assign(
							valid_filters, 
							{
								estacion_id: {
									type: "integer",
									table: "series"
								},
								tabla: {
									type: "string",
									table: "estaciones",
									alias: "tabla_id"
								},
								id_externo: {
									type: "string",
									table: "estaciones"
								},
								geom: {
									type: "geometry",
									table: "estaciones"
								}
							}
						)
					}
				}
				const invalid_filter_keys = Object.keys(filter).filter(key => (Object.keys(valid_filters).indexOf(key) < 0))
				if(invalid_filter_keys.length) {
					throw(new Error("Invalid filter keys: " + invalid_filter_keys.toString()))
				}
				var filter_string = internal.utils.control_filter2(valid_filters,filter,undefined,true)
				if(!filter_string) {
					return Promise.reject(new Error("invalid filter value"))
				}
				deleteValorText = `
					WITH deleted AS (
						DELETE FROM ${val_tabla}
						USING ${obs_tabla}
						${join_clause}
						WHERE ${obs_tabla}.id=${val_tabla}.obs_id 
						${filter_string}
						${returning_clause}
					)
					${select_deleted_clause}`
				deleteObsText = `
					WITH deleted AS (
						DELETE FROM ${obs_tabla}
						${obs_using_clause}
						${obs_where_clause}
						${filter_string}
						${returning_clause}
					)
					${select_deleted_clause}`
			}
			// console.debug(deleteObsText)
			// console.debug(deleteValorText)
			if(!client) {
				var release_client = true
				client = await global.pool.connect()
			}
			try {
				if(release_client) {
					await client.query('BEGIN')
				}
				var result = await executeQueryReturnRows(deleteValorText,undefined,client)
				if(result.length == 0) {
					console.info("No se eliminó ningun valor")
				}
				var deleted_valores={}
				if(options && options.no_send_data) {
					console.debug(result[0].count + " valores marked for deletion")
				} else {
					result.forEach(row=> {
						deleted_valores[row.obs_id] = row.valor 
					})
				}
				result = await executeQueryReturnRows(deleteObsText,undefined,client)
				if(result.length == 0) {
					console.info("No se eliminó ninguna observacion")
				}
				var deleted_observaciones
				if(options && options.no_send_data) {
					console.debug(result[0].count + " observaciones marked for deletion")
				} else {
					deleted_observaciones = result
				}
				if(release_client) {
					await client.query("COMMIT")
					client.release()
				}
				if(options && options.no_send_data) {
					console.log("Deleted " + result[0].count + " rows from " + obs_tabla)
					return parseInt(result[0].count)
				} else {
					console.log("Deleted " + deleted_observaciones.length + " observaciones from " + obs_tabla)
					var observaciones=[]
					for(var i=0; i< deleted_observaciones.length;i++) {
						deleted_observaciones[i].valor = deleted_valores[deleted_observaciones[i].id]
						deleted_observaciones[i].tipo = tipo
						// const deleted_observacion = new internal.observacion(tipo, deleted_observaciones[i].series_id, deleted_observaciones[i].timestart, deleted_observaciones[i].timeend, deleted_observaciones[i].nombre, deleted_observaciones[i].descripcion, deleted_observaciones[i].unit_id, deleted_observaciones[i].timeupdate, valor)
						// deleted_observacion.id = deleted_observaciones[i].id
						// observaciones.push(deleted_observacion)
					}
					return new internal.observaciones(deleted_observaciones)
				}
			} catch (e) {
				if(release_client) {
					await client.release()
				}
			}
		}
	}

	static async getObservacion(tipo,id,filter) {
		var stmt
		var valid_filters = {series_id:"integer",timestart:"timestart",timeend:"timeend",unit_id:"integer",timeupdate:"string"}
		var filter_string = internal.utils.control_filter(valid_filters,filter)
		if(!filter_string) {
			return Promise.reject(new Error("invalid filter value"))
		}
		if (tipo.toLowerCase() == "areal") {
			stmt = "SELECT observaciones_areal.*, valores_num_areal.valor,fuentes.public FROM observaciones_areal,valores_num_areal,series_areal,fuentes WHERE observaciones_areal.series_id=series_areal.id AND series_areal.fuentes_id=fuentes.id AND observaciones_areal.id=valores_num_areal.obs_id AND observaciones_areal.id=$1 " + filter_string
		} else if (tipo.toLowerCase() == "rast" || tipo.toLowerCase() == "raster") {
			stmt = "SELECT observaciones_rast.id,observaciones_rast.series_id,observaciones_rast.timestart,observaciones_rast.timeend,ST_AsGDALRaster(observaciones_rast.valor, 'GTIff') valor,observaciones_rast.timeupdate,fuentes.public FROM observaciones_rast,series_rast,fuentes WHERE observaciones_rast.series_id=series_rast.id AND series_rast.fuentes_id=fuentes.id AND observaciones_rast.id=$1 " + filter_string
		} else {
			stmt = "SELECT observaciones.*, valores_num.valor,redes.public FROM observaciones,valores_num,series,estaciones,redes WHERE observaciones.series_id=series.id AND series.estacion_id=estaciones.unid AND estaciones.tabla=redes.tabla_id AND observaciones.id=valores_num.obs_id AND observaciones.id=$1 " + filter_string
		}
		return global.pool.query(stmt,[id])
		.then(result=>{
			if(result.rows.length<=0) {
				console.log("observación no encontrada")
				return
			}
			if(filter.public) {
				if(!result.rows[0].public) {
					throw("El usuario no posee autorización para acceder a esta observación")
				}
			}
			var obs = result.rows[0]
			const observacion = new internal.observacion(tipo, obs.series_id, obs.timestart, obs.timeend, obs.nombre, obs.descripcion, obs.unit_id, obs.timeupdate, obs.valor)
			observacion.id = obs.id
			// console.log({observacion:observacion})
			return observacion
		})
	} 

	static getObservacionesGroupByClause(tipo,columns) {
		if(!columns) {
			return
		}
		if(typeof columns == "string") {
			columns = columns.split(",")
		}
		var valid_columns = {
			series_id:{},
			timestart:{},
			timeend:{},
			date:{type:"date",column:"timestart"},
			millennium:{column:"timestart",date_trunc:"millennium"},
			century:{column:"timestart",date_trunc:"century"},
			decade:{column:"timestart",date_trunc:"decade"},
			year:{column:"timestart",date_trunc:"year"},
			quarter:{column:"timestart",date_trunc:"quarter"},
			month:{column:"timestart",date_trunc:"month"},
			week:{column:"timestart",date_trunc:"week"},
			day:{column:"timestart",date_trunc:"day"},
			hour:{column:"timestart",date_trunc:"hour"},
			minute:{column:"timestart",date_trunc:"minute"},
			second:{column:"timestart",date_trunc:"second"},
			millisecond:{column:"timestart",date_trunc:"millisecond"},
			microsecond:{column:"timestart",date_trunc:"microsecond"},
			part_millennium:{column:"timestart",date_part:"millennium"},
			part_century:{column:"timestart",date_part:"century"},
			part_decade:{column:"timestart",date_part:"decade"},
			part_year:{column:"timestart",date_part:"year"},
			part_quarter:{column:"timestart",date_part:"quarter"},
			part_month:{column:"timestart",date_part:"month"},
			part_week:{column:"timestart",date_part:"week"},
			part_day:{column:"timestart",date_part:"day"},
			part_hour:{column:"timestart",date_part:"hour"},
			part_minute:{column:"timestart",date_part:"minute"},
			part_second:{column:"timestart",date_part:"second"},
			part_millisecond:{column:"timestart",date_part:"millisecond"},
			part_microsecond:{column:"timestart",date_part:"microsecond"}
		}
		var default_table
		switch (tipo) {
			case "puntual":
				valid_columns['unit_id'] = {table:"series"}
				valid_columns['var_id'] = {table:"series"}
				valid_columns['proc_id'] = {table:"series"}
				valid_columns['estacion_id'] = {table:"series"}
				valid_columns['red_id'] = {table:"redes",column:"id"}
				valid_columns['tabla_id'] = {table:"estaciones",column:"tabla"}
				valid_columns['timeupdate'] = {}
				default_table = "observaciones"
				break;
			case "areal":
				valid_columns['unit_id'] = {table:"series_areal"}
				valid_columns['var_id'] = {table:"series_areal"}
				valid_columns['proc_id'] = {table:"series_areal"}
				valid_columns['estacion_id'] = {table:"series_areal"}
				valid_columns['fuentes_id'] = {table:"series_areal"}
				default_table = "observaciones_areal"
				break;
			case "raster":
				valid_columns['unit_id'] = {table:"series_rast"}
				valid_columns['var_id'] = {table:"series_rast"}
				valid_columns['proc_id'] = {table:"series_rast"}
				valid_columns['estacion_id'] = {table:"series_rast"}
				valid_columns['fuentes_id'] = {table:"series_rast"}
				default_table = "observaciones_rast"
				break;
			default:
				break;
		} 
		return internal.utils.build_group_by_clause(valid_columns,columns,default_table)
	}
	
	// STUB
	static async getObservacionesCount(tipo,filter,options={}) { 
		var filter_string = (filter) ? this.getObservacionesFilterString(tipo,filter,options) : ""
		// console.log("filter string:" + filter_string)
		var group_by_clause = this.getObservacionesGroupByClause(tipo,options.group_by)
		var group_by_string = (group_by_clause) ? " GROUP BY " + group_by_clause.group_by.join(",") : ""
		// console.log("group by string:" + group_by_string)
		var order_by_string = (group_by_clause) ? " ORDER BY " + group_by_clause.order_by.join(",") : ""
		var stmt
		if (tipo.toLowerCase() == "areal") {
			var select_string = "count(observaciones_areal.timestart) AS count"
			if (group_by_clause) {
				group_by_clause.select.push("count(observaciones_areal.timestart) AS count")
				select_string = group_by_clause.select.join(",")
			}
			stmt =  "SELECT " + select_string + " FROM observaciones_areal,series_areal,fuentes WHERE observaciones_areal.series_id=series_areal.id AND series_areal.fuentes_id=fuentes.id " + filter_string + group_by_string + order_by_string
		} else if (tipo.toLowerCase() == "puntual") {
			var select_string = "count(observaciones.timestart) AS count"
			if(group_by_clause) {
				group_by_clause.select.push("count(observaciones.timestart) AS count")
				select_string = group_by_clause.select.join(",")
			}
			stmt = "SELECT " + select_string  + " FROM observaciones, series,estaciones,redes WHERE observaciones.series_id=series.id AND series.estacion_id=estaciones.unid AND estaciones.tabla=redes.tabla_id " + filter_string + group_by_string + order_by_string
		} else {
			var select_string = "count(observaciones_rast.timestart) AS count"
			if(group_by_clause) {
				group_by_clause.select.push("count(observaciones_rast.timestart) AS count")
				select_string = group_by_clause.select.join(",")
			}
			stmt = "SELECT " + select_string + " FROM observaciones_rast,series_rast,fuentes WHERE observaciones_rast.series_id=series_rast.id AND series_rast.fuentes_id=fuentes.id " + filter_string + filter_string + group_by_string + order_by_string
		}
		console.log(stmt)
		return global.pool.query(stmt)
		.then(result=>{
			return result.rows
		})
	}

	static getObservacionesFilterString(tipo,filter,options) {
		var series_table = (tipo=="puntual") ? "series" : (tipo=="areal") ? "series_areal" : "series_rast"
		var observaciones_table = (tipo=="puntual") ? "observaciones" : (tipo=="areal") ? "observaciones_areal" : "observaciones_rast" 
		var valores_table = (tipo.toLowerCase() == "areal") ? (options && options.obs_type && options.obs_type.toLowerCase() == 'numarr') ? "valores_numarr_areal" : "valores_num_areal" : (tipo.toLowerCase() == "rast" || tipo.toLowerCase() == "raster") ? "observaciones_rast" : (options && options.obs_type && options.obs_type.toLowerCase() == 'numarr') ? "valores_numarr" : "valores_num"
		var fuentes_table = (tipo.toLowerCase() == "areal") ? "fuentes" : (tipo.toLowerCase() == "rast" || tipo.toLowerCase() == "raster") ? "fuentes" : "redes"
		var valid_filters = {
			id:{type:"integer"},
			series_id:{type:"integer"},
			timestart:{type:"timestart"},
			timeend:{type:"timeend"},
			unit_id:{type:"integer"},
			timeupdate:{type:"date"},
			var_id:{type:"integer",table:series_table},
			proc_id:{type:"integer",table:series_table}
		}
		if(tipo=="puntual") {
			valid_filters["red_id"] = {type:"integer",table:"redes",column:"id"}
			valid_filters["tabla_id"] = {type:"integer",table:"redes"}
			valid_filters["tabla"] = {type: "string", table: "estaciones"}
			valid_filters["estacion_id"] = {type:"integer",table: series_table}
		} else if(tipo=="areal") {
			valid_filters["fuentes_id"] = {type:"integer",table:series_table}
			valid_filters["area_id"] = {type:"integer",table: series_table}
		} else { // "raster"
			valid_filters["fuentes_id"] = {type:"integer",table:series_table}
			valid_filters["escena_id"] = {type:"integer",table: series_table}
		}
		var filter_string = internal.utils.control_filter2(valid_filters,filter,observaciones_table,true)
		if(!filter_string) {
			throw(new Error("Invalid filter values"))
		}
		// filter_string += internal.utils.control_filter({id:"integer",series_id:"integer",timestart:"timestart",timeend:"timeend",unit_id:"integer",timeupdate:"string"},filter, (tipo.toLowerCase() == "areal") ? "observaciones_areal" : (tipo.toLowerCase() == "rast" || tipo.toLowerCase() == "raster") ? "observaciones_rast" : "observaciones")
		var filter_string_valor = internal.utils.control_filter({valor:"numeric_interval"},filter, valores_table)
		if(!filter_string_valor) {
			throw(new Error("Invalid filter values: valor"))
		}
		filter_string += filter_string_valor

		var filter_string_public = internal.utils.control_filter({public:"boolean_only_true"},filter, fuentes_table)
		if(!filter_string_public) {
			throw(new Error("Invalid filter values: public"))
		}
		filter_string += filter_string_public
		return filter_string
	}

	static async getObservacionesGuardadas(tipo,filter,options) {
		const filter_string = this.getObservacionesGuardadasFilterString(tipo,filter,options)
		const observaciones_table = (tipo) ? (tipo == "areal") ? "observaciones_areal_guardadas" : (tipo == "raster" || tipo == "rast") ? "observaciones_rast_guardadas" : "observaciones_guardadas" : "observaciones_guardadas"
		const stmt = `SELECT * FROM ${observaciones_table} WHERE 1=1 ${filter_string} ORDER BY series_id,timestart`
		// console.log(stmt)
		const result = await global.pool.query(stmt)
		return new internal.observacionesGuardadas(result.rows)
	}

	static getObservacionesGuardadasFilterString(tipo,filter,options) {
		var series_table = (tipo=="puntual") ? "series" : (tipo=="areal") ? "series_areal" : "series_rast"
		var observaciones_table = (tipo=="puntual") ? "observaciones_guardadas" : (tipo=="areal") ? "observaciones_areal_guardadas" : "observaciones_rast_guardadas" 
		var fuentes_table = (tipo.toLowerCase() == "areal") ? "fuentes" : (tipo.toLowerCase() == "rast" || tipo.toLowerCase() == "raster") ? "fuentes" : "redes"
		var filter_string = "" 
		var valid_filters = {
			id:{type:"integer"},
			series_id:{type:"integer"},
			timestart:{type:"timestart"},
			timeend:{type:"timeend"},
			unit_id:{type:"integer"},
			timeupdate:{type:"timestamp"},
			var_id:{type:"integer",table:series_table},
			proc_id:{type:"integer",table:series_table}
		}
		if(tipo=="puntual") {
			valid_filters["red_id"] = {type:"integer",table:"redes",column:"id"}
			valid_filters["tabla_id"] = {type:"integer",table:"redes"}
			valid_filters["estacion_id"] = {type:"integer",table: series_table}
		} else if(tipo=="areal") {
			valid_filters["fuentes_id"] = {type:"integer",table:series_table}
			valid_filters["area_id"] = {type:"integer",table: series_table}
		} else { // "raster"
			valid_filters["fuentes_id"] = {type:"integer",table:series_table}
			valid_filters["escena_id"] = {type:"integer",table: series_table}
		}
		filter_string += internal.utils.control_filter2(valid_filters,filter,observaciones_table)
		// filter_string += internal.utils.control_filter({id:"integer",series_id:"integer",timestart:"timestart",timeend:"timeend",unit_id:"integer",timeupdate:"string"},filter, (tipo.toLowerCase() == "areal") ? "observaciones_areal" : (tipo.toLowerCase() == "rast" || tipo.toLowerCase() == "raster") ? "observaciones_rast" : "observaciones")
		filter_string += internal.utils.control_filter({valor:"numeric_interval"},filter, observaciones_table)
		return filter_string
	}

	static getTipo(tipo="puntual") {
		if(tipo.toLowerCase() == "areal") {
			return "areal"
		} else if (tipo.toLowerCase() == "rast" || tipo.toLowerCase() == "raster") {
			return "raster"
		} else {
			return "puntual"
		}
	}

	static async backupObservaciones(tipo,filter,options) {
		try {
			var series = await this.getSeries(tipo,filter)
		} catch (e) {
			throw(e)
		}
		if(!series.length) {
			throw("No series found")
		}
		var results = []
		for(var i in series) {
			var series_id = series[i].id
			var tipo = series[i].tipo
			var output = (options.output) ? path.resolve(options.output) : path.resolve(this.config.crud.backup_base_dir,'obs',tipo,series_id,'observaciones_' + new Date().toISOString() + ".json")
			try {
				var result = await this.streamObservaciones(tipo,{series_id:series_id,timestart:filter.timestart,timeend:filter.timeend},options,output)
			} catch(e) {
				console.error(e)
			}
			results.push(result)
		}
		return results
	}

	static async streamObservaciones(tipo,filter,options,output) {
		try {
			var stmt = this.build_observaciones_query(tipo,filter,options)
			await this.streamQuery(stmt,options,output)
		} catch(e) {
			throw(e)
		}
		return
	}

	static async streamQuery(stmt,options,output) {
		return new Promise((resolve,reject)=>{
			var file = fs.createWriteStream(output)
			global.pool.connect((err,client,done)=>{
				if (err) {
					reject(err)
				}
				var query = new QueryStream(stmt)
				var stream = client.query(query)
				stream.on('end',()=>{
					done()
					resolve()
				})
				stream.pipe(JSONStream.stringify()).pipe(file)
			})
		})
	}

	static build_delete_observaciones_query(tipo,filter,options={}) {
		tipo = this.getTipo(tipo)
		var filter_string = this.getObservacionesFilterString(tipo,filter,options) 
		//~ console.log({filter_string:filter_string})
		if(filter_string == "") {
			throw "invalid filter value"
		}
		var stmt
		if (tipo.toLowerCase() == "raster") {
			const returning_clause = (options.no_send_data  && !options.save) ? "" : "RETURNING %s.id,%s.series_id,%s.timestart,%s.timeend,%s.timeupdate,%s.valor".replace(/%s/g,"observaciones_rast")
			var select_deleted_clause = (options.no_send_data && !options.save) ? "SELECT 1" : "SELECT deleted_obs.id,deleted_obs.series_id,deleted_obs.timestart,deleted_obs.timeend,deleted_obs.timeupdate,deleted_obs.valor FROM deleted_obs WHERE deleted_obs.valor IS NOT NULL"
			if(options.save) {
				select_deleted_clause = this.build_save_query(tipo,select_deleted_clause,options)
			}
			stmt = "WITH deleted_obs AS (DELETE FROM observaciones_rast USING series_rast WHERE observaciones_rast.series_id=series_rast.id\
			" + filter_string + " \
			" + returning_clause + ") " + select_deleted_clause
		} else {
			const obs_tabla = (tipo == "areal") ? "observaciones_areal" : "observaciones"
			const val_tabla = (tipo == "areal") ? (options.obs_type && options.obs_type.toLowerCase() == 'numarr') ?"valores_numarr_areal" : "valores_num_areal" : (options.obs_type && options.obs_type.toLowerCase() == 'numarr') ? "valores_numarr" : "valores_num"
			const val_using_clause = (tipo == "areal") ? "USING observaciones_areal JOIN series_areal ON (observaciones_areal.series_id=series_areal.id)" : "USING observaciones JOIN series ON (observaciones.series_id=series.id) JOIN estaciones ON (series.estacion_id=estaciones.unid) JOIN redes ON (estaciones.tabla = redes.tabla_id)"
			const obs_using_clause = (tipo == "areal") ? "USING series_areal WHERE observaciones_areal.series_id=series_areal.id" : "USING series JOIN estaciones ON (series.estacion_id=estaciones.unid) JOIN redes ON (estaciones.tabla = redes.tabla_id) WHERE observaciones.series_id=series.id"
			const returning_clause = (options.no_send_data  && !options.save) ? "RETURNING 1 as d" : "RETURNING %s.id,%s.series_id,%s.timestart,%s.timeend,%s.nombre,%s.descripcion,%s.unit_id,%s.timeupdate".replace(/%s/g,obs_tabla)
			var select_deleted_clause = (options.no_send_data && !options.save) ? "SELECT count(d) from deleted_obs" : "SELECT deleted_obs.id,deleted_obs.series_id,deleted_obs.timestart,deleted_obs.timeend,deleted_obs.nombre,deleted_obs.descripcion,deleted_obs.unit_id,deleted_obs.timeupdate,deleted_val.valor FROM deleted_val JOIN deleted_obs ON (deleted_val.obs_id=deleted_obs.id) WHERE deleted_val.valor IS NOT NULL"
			if(options.save) {
				select_deleted_clause = this.build_save_query(tipo,select_deleted_clause,options)
			}

			stmt = "WITH deleted_val AS (DELETE FROM " + val_tabla + " " + val_using_clause + " \
						WHERE " + obs_tabla + ".id=" + val_tabla + ".obs_id " + filter_string + " \
						RETURNING obs_id,valor),\
						deleted_obs AS (DELETE FROM " + obs_tabla + " " + obs_using_clause + "\
						" + filter_string + " \
						" + returning_clause + ") " + select_deleted_clause
		}
		return stmt
	}

	static build_save_query(tipo,select_clause,options) {
		var insert_stmt
		var returning_clause = (options.no_send_data) ? " RETURNING 1 AS d" : " RETURNING *"
		var on_conflict_clause = (options.no_update) ? " ON CONFLICT (series_id, timestart, timeend) DO NOTHING" : " ON CONFLICT (series_id, timestart, timeend) DO UPDATE SET timeupdate=excluded.timeupdate, valor=excluded.valor"
		switch(tipo) {
			case "puntual":
				insert_stmt = "INSERT INTO observaciones_guardadas (id,series_id,timestart,timeend,nombre,descripcion,unit_id,timeupdate,valor) " + select_clause + on_conflict_clause + returning_clause
				break;
			case "areal":
				insert_stmt = "INSERT INTO observaciones_areal_guardadas (id,series_id,timestart,timeend,nombre,descripcion,unit_id,timeupdate,valor) " + select_clause + on_conflict_clause  + returning_clause
				break;
			case "raster":
				insert_stmt = "INSERT INTO observaciones_rast_guardadas (id,series_id,timestart,timeend,timeupdate,valor) " + select_clause + on_conflict_clause  + returning_clause
		}
		var select_return_clause = (options.no_send_data) ? "SELECT count(d) FROM saved" : "SELECT * from saved"
		var stmt = ", saved AS (" + insert_stmt + ") " + select_return_clause 
		return stmt
	}

	static build_observaciones_query(tipo,filter,options) {
		tipo = this.getTipo(tipo)
		var filter_string = this.getObservacionesFilterString(tipo,filter,options) 
		// console.debug({filter_string:filter_string})
		if(filter_string == "") {
			throw "invalid filter value"
		}
		var stmt
		if (tipo.toLowerCase() == "areal") {
			var valtablename = (options) ? (options.obs_type) ? (options.obs_type.toLowerCase() == 'numarr') ? "valores_numarr_areal" : "valores_num_areal" : "valores_num_areal" : "valores_num_areal"
			stmt =  "SELECT observaciones_areal.id, \
			observaciones_areal.series_id,\
			observaciones_areal.timestart,\
			observaciones_areal.timeend,\
			observaciones_areal.nombre,\
			observaciones_areal.descripcion,\
			observaciones_areal.unit_id,\
			observaciones_areal.timeupdate,\
			" + valtablename + ".valor \
			FROM observaciones_areal, " + valtablename + ",series_areal,fuentes\
			WHERE observaciones_areal.series_id=series_areal.id \
			AND series_areal.fuentes_id=fuentes.id \
			AND observaciones_areal.id=" + valtablename + ".obs_id \
			" + filter_string + "\
			ORDER BY timestart"
		} else if (tipo.toLowerCase() == "rast" || tipo.toLowerCase() == "raster") {
			var format = (options) ? (options.format) ? options.format : "GTiff" : "GTiff"
			switch(format.toLowerCase()) {
				case "postgres":
					stmt =  "SELECT observaciones_rast.id,observaciones_rast.series_id,observaciones_rast.timestart,observaciones_rast.timeend,observaciones_rast.valor,observaciones_rast.timeupdate FROM observaciones_rast,series_rast,fuentes WHERE observaciones_rast.series_id=series_rast.id AND series_rast.fuentes_id=fuentes.id " + filter_string + " ORDER BY timestart"
					break;				case "GTiff":
					stmt =  "SELECT observaciones_rast.id,observaciones_rast.series_id,observaciones_rast.timestart,observaciones_rast.timeend,ST_AsGDALRaster(observaciones_rast.valor,'GTIff') valor,(st_summarystats(observaciones_rast.valor)).*,observaciones_rast.timeupdate FROM observaciones_rast,series_rast,fuentes WHERE observaciones_rast.series_id=series_rast.id AND series_rast.fuentes_id=fuentes.id " + filter_string + " ORDER BY timestart"
					break;
				case "hex":
					stmt =  "SELECT observaciones_rast.id,observaciones_rast.series_id,observaciones_rast.timestart,observaciones_rast.timeend,'\\x' || encode(ST_AsGDALRaster(observaciones_rast.valor,'GTIff')::bytea,'hex') valor,(st_summarystats(observaciones_rast.valor)).*,observaciones_rast.timeupdate FROM observaciones_rast,series_rast,fuentes WHERE observaciones_rast.series_id=series_rast.id AND series_rast.fuentes_id=fuentes.id " + filter_string + " ORDER BY timestart"
					break;
				case "png":
					var width = (options.width) ? parseInt(options.width) : 300
					var height = (options.height) ? parseInt(options.height) : 300
					stmt =  "SELECT id,series_id,timestart,timeend,ST_asGDALRaster(st_colormap(st_resize(st_reclass(valor,'[' || (st_summarystats(valor)).min || '-' || (st_summarystats(valor)).max || ']:1-255, ' || st_bandnodatavalue(valor) || ':0','8BUI')," + width + "," + height + "),1,'grayscale','nearest'),'PNG') valor,(st_summarystats(valor)).*,timeupdate FROM observaciones_rast,series_rast,fuentes WHERE observaciones_rast.series_id=series_rast.id AND series_rast.fuentes_id=fuentes.id " + filter_string + " ORDER BY timestart"
					break;
				case "geojson":
				case "json":
					stmt = "WITH values AS (\
								SELECT observaciones_rast.id,\
					               observaciones_rast.series_id,\
					               observaciones_rast.timestart,\
								   observaciones_rast.timeend,\
								   (ST_PixelAsCentroids(observaciones_rast.valor, 1, true)).*,\
								   (st_summarystats(observaciones_rast.valor)).*,\
								   observaciones_rast.timeupdate \
								FROM observaciones_rast,series_rast,fuentes WHERE observaciones_rast.series_id=series_rast.id AND series_rast.fuentes_id=fuentes.id " + filter_string + " ORDER BY timestart)\
							SELECT id,\
							       series_id,\
							       timestart,\
							       timeend,\
							       json_build_object('type','Feature', 'geometry', json_build_object('type', 'GeometryCollection', 'geometries', json_agg(json_build_object('type', 'Point', 'coordinates', ARRAY[ST_X(geom),ST_Y(geom),values.val]))), 'properties',json_build_object('id',values.id, 'series_id', values.series_id, 'timestart', values.timestart, 'timeend', values.timeend, 'stats', json_build_object('count',values.count, 'sum', values.sum, 'mean', values.mean, 'stddev', values.stddev, 'min', values.min, 'max', values.max))) valor,\
							       values.count,\
							       values.sum,\
							       values.mean,\
							       values.stddev,\
							       values.min,\
							       values.max\
							FROM values\
							GROUP BY id, series_id, timestart, timeend, count, sum, mean, stddev, min, max\
							ORDER BY timestart"
					break
				default:
					stmt =  "SELECT observaciones_rast.id,observaciones_rast.series_id,observaciones_rast.timestart,observaciones_rast.timeend,ST_AsGDALRaster(observaciones_rast.valor,'GTIff') valor,(st_summarystats(observaciones_rast.valor)).*,observaciones_rast.timeupdate FROM observaciones_rast,series_rast,fuentes WHERE observaciones_rast.series_id=series_rast.id AND series_rast.fuentes_id=fuentes.id " + filter_string + " ORDER BY timestart"
					break;
			}
		} else {
			var valtablename = (options && options.obs_type && options.obs_type.toLowerCase() == 'numarr') ? "valores_numarr" : "valores_num"
			stmt =  "SELECT observaciones.id,\
			observaciones.series_id,\
			observaciones.timestart,\
			observaciones.timeend,\
			observaciones.nombre,\
			observaciones.descripcion,\
			observaciones.unit_id,\
			observaciones.timeupdate,\
			"+valtablename+".valor \
			FROM observaciones, "+valtablename+",series,estaciones,redes \
			WHERE observaciones.series_id=series.id \
			AND series.estacion_id=estaciones.unid \
			AND estaciones.tabla=redes.tabla_id \
			AND observaciones.id="+valtablename+".obs_id \
			" + filter_string  + "\
			ORDER BY timestart"
		}
		if(filter.limit) {
			stmt += " LIMIT " + parseInt(filter.limit)
		}
		return stmt
	}

	static async restoreObservaciones(tipo,filter,options={}) {
		var stmt = this.buildRestoreObservacionesQuery(tipo,filter,options)
		return global.pool.query(stmt)
		.then(result=>{
			return (options.no_send_data) ? result.rows.length : new internal.observaciones(result.rows)
		})
	}

	static buildRestoreObservacionesQuery(tipo,filter,options={}) {
		tipo = this.getTipo(tipo)
		var filter_string = this.getObservacionesGuardadasFilterString(tipo,filter,options)
		var stmt
		if(tipo == "raster") {
			var select_return_clause = (options.no_send_data) ? "SELECT count(id) AS count FROM restored_obs" : "SELECT restored_obs.id, restored_obs.series_id, restored_obs.timestart, restored_obs.timeend, restored_obs.timeupdate, restored_obs.valor\
			FROM restored_obs"
			stmt = "WITH deleted_obs AS (\
				DELETE FROM observaciones_rast_guardadas \
				USING series_rast \
				WHERE observaciones_rast_guardadas.series_id=series_rast.id\
				" + filter_string + " \
				RETURNING observaciones_rast_guardadas.id, observaciones_rast_guardadas.series_id, observaciones_rast_guardadas.timestart, observaciones_rast_guardadas.timeend, observaciones_rast_guardadas.timeupdate, observaciones_rast_guardadas.valor\
			), restored_obs AS (\
				INSERT INTO observaciones_rast (series_id,timestart,timeend,timeupdate,valor)\
					SELECT deleted_obs.series_id, deleted_obs.timestart, deleted_obs.timeend, deleted_obs.timeupdate, deleted_obs.valor\
					FROM deleted_obs\
				ON CONFLICT (series_id,timestart,timeend)\
				DO UPDATE SET timeupdate=excluded.timeupdate, valor=excluded.valor\
				RETURNING id,series_id,timestart,timeend,timeupdate, valor\
			) " + select_return_clause
		} else if (tipo == "puntual") {
			var select_return_clause = (options.no_send_data) ? "SELECT count(id) AS count FROM restored_obs" : "SELECT restored_obs.id, restored_obs.series_id, restored_obs.timestart, restored_obs.timeend, restored_obs.nombre, restored_obs.descripcion, restored_obs.unit_id, restored_obs.timeupdate, restored_val.valor\
			FROM restored_obs\
			JOIN restored_val ON (restored_obs.id=restored_val.obs_id)"
			stmt = "WITH deleted_obs AS (\
				DELETE FROM observaciones_guardadas \
				USING series \
				JOIN estaciones ON (series.estacion_id=estaciones.unid) \
				JOIN redes ON (estaciones.tabla=redes.tabla_id) \
				WHERE observaciones_guardadas.series_id=series.id\
				" + filter_string + " \
				RETURNING observaciones_guardadas.id, observaciones_guardadas.series_id, observaciones_guardadas.timestart, observaciones_guardadas.timeend, observaciones_guardadas.nombre, observaciones_guardadas.descripcion, observaciones_guardadas.unit_id, observaciones_guardadas.timeupdate, observaciones_guardadas.valor\
			), restored_obs AS (\
				INSERT INTO observaciones (series_id,timestart,timeend,nombre,descripcion,unit_id,timeupdate)\
					SELECT deleted_obs.series_id, deleted_obs.timestart, deleted_obs.timeend, deleted_obs.nombre, deleted_obs.descripcion, deleted_obs.unit_id, deleted_obs.timeupdate\
					FROM deleted_obs\
				ON CONFLICT (series_id,timestart,timeend)\
				DO UPDATE SET nombre=excluded.nombre, descripcion=excluded.descripcion, unit_id=excluded.unit_id, timeupdate=excluded.timeupdate\
				RETURNING id,series_id,timestart,timeend,nombre,descripcion,unit_id,timeupdate\
			), restored_val AS (\
				INSERT INTO valores_num (obs_id,valor)\
					SELECT restored_obs.id, deleted_obs.valor\
					FROM restored_obs\
					JOIN deleted_obs ON (restored_obs.series_id=deleted_obs.series_id AND restored_obs.timestart=deleted_obs.timestart AND restored_obs.timeend=deleted_obs.timeend)\
					ON CONFLICT (obs_id) \
					DO UPDATE SET valor=excluded.valor  \
					RETURNING obs_id,valor\
			) " + select_return_clause
		} else { // AREAL 
			var select_return_clause = (options.no_send_data) ? "SELECT count(id) AS count FROM restored_obs" : "SELECT restored_obs.id, restored_obs.series_id, restored_obs.timestart, restored_obs.timeend, restored_obs.nombre, restored_obs.descripcion, restored_obs.unit_id, restored_obs.timeupdate, restored_val.valor\
			FROM restored_obs\
			JOIN restored_val ON (restored_obs.id=restored_val.obs_id)"
			stmt = "WITH deleted_obs AS (\
				DELETE FROM observaciones_areal_guardadas \
				USING series_areal \
				WHERE observaciones_areal_guardadas.series_id=series_areal.id\
				" + filter_string + " \
				RETURNING observaciones_areal_guardadas.id, observaciones_areal_guardadas.series_id, observaciones_areal_guardadas.timestart, observaciones_areal_guardadas.timeend, observaciones_areal_guardadas.nombre, observaciones_areal_guardadas.descripcion, observaciones_areal_guardadas.unit_id, observaciones_areal_guardadas.timeupdate, observaciones_areal_guardadas.valor\
			), restored_obs AS (\
				INSERT INTO observaciones_areal (series_id,timestart,timeend,nombre,descripcion,unit_id,timeupdate)\
					SELECT deleted_obs.series_id, deleted_obs.timestart, deleted_obs.timeend, deleted_obs.nombre, deleted_obs.descripcion, deleted_obs.unit_id, deleted_obs.timeupdate\
					FROM deleted_obs\
				ON CONFLICT (series_id,timestart,timeend)\
				DO UPDATE SET nombre=excluded.nombre, descripcion=excluded.descripcion, unit_id=excluded.unit_id, timeupdate=excluded.timeupdate\
				RETURNING id,series_id,timestart,timeend,nombre,descripcion,unit_id,timeupdate\
			), restored_val AS (\
				INSERT INTO valores_num_areal (obs_id,valor)\
					SELECT restored_obs.id, deleted_obs.valor\
					FROM restored_obs\
					JOIN deleted_obs ON (restored_obs.series_id=deleted_obs.series_id AND restored_obs.timestart=deleted_obs.timestart AND restored_obs.timeend=deleted_obs.timeend)\
					ON CONFLICT (obs_id) \
					DO UPDATE SET valor=excluded.valor  \
					RETURNING obs_id,valor\
			) " + select_return_clause
		}
		return stmt
	}

	static async guardarObservaciones(tipo,filter,options={}) {
		if(!filter || !filter.series_id) {
			console.log("no series_id")
			return this.guardarObservacionesPartedBySerie(tipo,filter,options)
		}
		tipo = this.getTipo(tipo)
		console.log("guardar observaciones series_id:" + filter.series_id)
		try {
			var select_stmt = this.build_observaciones_query(tipo,filter,options)
			var insert_stmt
			var returning_clause = (options.no_send_data) ? " RETURNING 1 AS d" : " RETURNING *"
			var on_conflict_clause = (options.no_update) ? " ON CONFLICT (series_id, timestart, timeend) DO NOTHING" : " ON CONFLICT (series_id, timestart, timeend) DO UPDATE SET timeupdate=excluded.timeupdate, valor=excluded.valor"
			switch(tipo) {
				case "puntual":
					insert_stmt = "INSERT INTO observaciones_guardadas (id,series_id,timestart,timeend,nombre,descripcion,unit_id,timeupdate,valor) " + select_stmt + on_conflict_clause + returning_clause
					break;
				case "areal":
					insert_stmt = "INSERT INTO observaciones_areal_guardadas (id,series_id,timestart,timeend,nombre,descripcion,unit_id,timeupdate,valor) " + select_stmt + on_conflict_clause  + returning_clause
					break;
				case "raster":
					options.format = "postgres"
					insert_stmt = "INSERT INTO observaciones_rast_guardadas (id,series_id,timestart,timeend,valor,timeupdate) " + select_stmt + on_conflict_clause  + returning_clause
			}
			//~ console.log({stmt:stmt})
			var res = await global.pool.query(insert_stmt)
			console.log("Saved " + res.rows.length + " observaciones")
			return (options.no_send_data) ? res.rows.length : new internal.observaciones(res.rows)
		} catch(e) {
			throw(e)
		}
	}

	static async guardarObservacionesPartedBySerie(tipo="puntual",filter={},options) {
		var series_filter = {...filter}
		delete series_filter.timestart
		delete series_filter.timeend
		return this.getSeries(tipo,series_filter)
		.then(async series=>{
			console.log("Found " + series.length + " series to Save")
			var guardadas = []
			for (var serie of series) {
				var this_serie_filter = {...filter}
				this_serie_filter.series_id = serie.id
				var result = await this.guardarObservaciones(tipo,this_serie_filter,options)
				guardadas.push(...result)
			}
			return guardadas
		})
	}

	static async getObservaciones(tipo="puntual",filter,options,client) {
		try {
			var stmt = this.build_observaciones_query(tipo,filter,options)
			// console.debug(stmt)
			if(!client) {
				var res = await global.pool.query(stmt)
			} else {
				var res = await client.query(stmt)
			}
		} catch(e) {
			throw(e)
		}	//~ console.log({stmt:stmt})
		var observaciones = [] // new internal.observaciones
		for(var i = 0; i < res.rows.length; i++) {
			var obs=res.rows[i]
			if(tipo.toLowerCase()=="rast") {
				const observacion = new internal.observacion({tipo:tipo, series_id:obs.series_id, timestart:obs.timestart, timeend:obs.timeend, nombre:obs.nombre, descripcion:obs.descripcion, unit_id: obs.unit_id, timeupdate: obs.timeupdate, valor:obs.valor, stats: {count: obs.count, mean: obs.mean, stddev: obs.stddev, min: obs.min, max: obs.max}})
				observacion.id = obs.id
				observaciones.push(observacion)
			} else if (options && options.asArray) {
				observaciones.push([obs.timestart, obs.timeend, obs.valor, obs.id])
			} else {
				const observacion = new internal.observacion(tipo, obs.series_id, obs.timestart, obs.timeend, obs.nombre, obs.descripcion, obs.unit_id, obs.timeupdate, obs.valor)
				observacion.id = obs.id
				observaciones.push(observacion)
			}
		}
		if(options && options.pivot) {
			if(options.dt && options.t_offset) {
				// ---
			} else if(filter.var_id) {
				const var_id_filter = (Array.isArray(filter.var_id)) ? filter.var_id[0] : filter.var_id
				try {
					var variable = await this.getVar(var_id_filter)
					options.dt = variable.dt
					options.t_offset = variable.t_offset
				} catch(e) {
					throw(e)
				}
			} else if(filter.series_id) { // toma dt y t_offset de 
				const series_id_filter = (Array.isArray(filter.series_id)) ? filter.series_id[0] : filter.series_id
				try {
					var serie = await this.getSerie(filter.tipo,series_id_filter)
					options.dt = serie.var.dt
					options.t_offset = serie.var.t_offset
				} catch(e) {
					throw(e)
				}
			} 
			else {
				// throw("Pivot error: missing dt, var_id or series_id")
				options.dt = {days:1}
				options.t_offset = {days:1}
			}
			options.dt = timeSteps.createInterval(options.dt)
			options.t_offset = timeSteps.createInterval(options.t_offset)
			options.timestart = filter.timestart
			options.timeend = filter.timeend
			options.pivot = true
			observaciones = new internal.observaciones(observaciones,options)
		} else if (!options || !options.asArray) {
			//~ console.log({observaciones:observaciones})
			observaciones = new internal.observaciones(observaciones,options)
		}
		return observaciones
	}

	static getObsTable(tipo="puntual") {
		var t  = tipo.toLowerCase()
		return (t == "areal") ? "observaciones_areal" : (t == "rast" || t == "raster") ? "observaciones_rast" : "observaciones"
	}

	static getValTable(tipo="puntual") {
		var t  = tipo.toLowerCase()
		return (t == "areal") ? "valores_num_areal" : (t == "rast" || t == "raster") ? "observaciones_rast" : "valores_num"
	}

	static async getObservacionesDateRange(tipo,filter,options) {
		var tipo = this.getTipo(tipo)
		var obs_table = this.getObsTable(tipo)
		var filter_string = this.getObservacionesFilterString(tipo,filter,options) 
		//~ console.log({filter_string:filter_string})
		if(filter_string == "") {
			throw "invalid filter value"
		}
		try {
			var result = await global.pool.query("SELECT min(timestart),max(timestart) FROM " + obs_table + " WHERE 1=1 " + filter_string)
			return result.rows[0]
		} catch(e) {
			throw(e)
		}
	}
	
	static async getObservacionesRTS(tipo,filter={},options={},serie) {
		//~ console.log({options:options})
		if(options.skip_nulls || !filter.series_id || Array.isArray(filter.series_id)) {
			return this.getObservaciones(tipo,filter,options)
		}
		if(!filter.timestart || !filter.timeend) {
			if(!filter.timeupdate) {
				// return Promise.reject("Faltan parametros: series_id, timestart, timeend, timeupdate")
				throw("Faltan parametros: series_id, timestart, timeend, timeupdate")
			}
		}
		var serie_result
		if(serie) {
			if(filter.public && !serie.estacion.public) {
				console.log("usuario no autorizado para acceder a la serie seleccionada")
				// return Promise.reject("usuario no autorizado para acceder a la serie seleccionada")
				throw("usuario no autorizado para acceder a la serie seleccionada")
			}
			serie_result = serie
		} else {
			try {
				serie_result = await this.getSerie(tipo,filter.series_id,undefined,undefined,undefined,filter.public) // tipo,id,timestart,timeend,options={},isPublic
			} catch(e) {
				throw(e)
			}
		}
		serie = serie_result
		if(!serie) {
			throw("Serie no encontrada")
		}
		if(!serie.var.timeSupport) {
			// console.log("no timeSupport")
			var getObsFilter = {...filter}
			getObsFilter.series_id = serie.id
			return this.getObservaciones(tipo,getObsFilter)
		} 
		try {
			var dateRange = await this.getObservacionesDateRange(tipo,{series_id:serie.id,timestart:filter.timestart,timeend:filter.timeend})
		} catch(e) {
			throw(e)
		}
		if(!dateRange.min) {
			console.error("No observaciones found in date range")
			return []
		}
		var timestart = dateRange.min // new Date(filter.timestart)
		var timeend =  dateRange.max // new Date(filter.timeend)
		var interval = timeSteps.interval2epochSync(serie.var.timeSupport)
		// console.log("interval:" + interval)
		if(!interval) {
			// console.log("timeSupport inválido o nulo")
			if(!options.obs_type && serie.var.type) {
				options.obs_type = serie.var.type
			}
			return  this.getObservaciones(tipo,{series_id:serie.id,timestart:timestart,timeend:timeend,timeupdate:filter.timeupdate},options)
		}
		var interval_string = timeSteps.interval2string(serie.var.timeSupport)
		const obs_tabla = this.getObsTable(tipo)
		const val_tabla = this.getValTable(tipo)
		//~ console.log({keys:Object.keys(serie.var.def_hora_corte).join(","),tostr:timeSteps.interval2string(serie.var.def_hora_corte)})
		var t_offset = (serie.fuente && serie.fuente.hora_corte) ? timeSteps.interval2string(serie.fuente.hora_corte) : (serie.var.def_hora_corte)  ? timeSteps.interval2string(serie.var.def_hora_corte) : "00:00:00"
		// console.log("t_offset:" + t_offset)
		var valuequerystring = (tipo == "rast" || tipo == "raster") ? this.rasterValueQueryString(options.format,options) : val_tabla + ".valor"
		// console.log(valuequerystring)
		var query = "WITH seq AS (\
			SELECT generate_series('" + timestart.toISOString() + "'::date + $3::interval,'" + timeend.toISOString() + "'::timestamp,$2::interval) date)\
			SELECT " + obs_tabla + ".id, \
					$1 AS series_id, \
					seq.date timestart, \
					seq.date + $2::interval AS timeend, \
					" + obs_tabla + ".timeupdate,\
					" + valuequerystring + " valor\
			FROM seq\
			LEFT JOIN " + obs_tabla + " ON (\
				" + obs_tabla + ".series_id=$1\
				AND seq.date=" + obs_tabla + ".timestart\
				AND seq.date>=$4\
				AND seq.date<=$5\
			) "
		if(tipo == "puntual" || tipo == "areal") {
			query += "\
			LEFT JOIN " + val_tabla + " ON (\
				" + obs_tabla + ".id=" + val_tabla + ".obs_id\
			) "
		}
		var query_params = [serie.id, interval_string, t_offset, timestart, timeend]
		if(filter.timeupdate) {
			query += " WHERE " + obs_tabla + ".timeupdate=$6"
			query_params.push(filter.timeupdate)
		}
		query += "ORDER BY seq.date"
		// console.log(internal.utils.pasteIntoSQLQuery(query,[serie.id,interval_string,t_offset, timestart, timeend]))
		try {
			var result = await global.pool.query(query, query_params)
		} catch(e){
			throw(e)
		}
		var count = result.rows.map(r=>{
			return (r.valor !== null) ? 1 : 0
		}).reduce((a,b)=>a+b)
		if(count == 0) {
			console.log("No se encontraron registros")
			return []
		}
		//~ console.log({options:options})
		var observaciones = []
		for(var i = 0; i < result.rows.length; i++) {
			var obs=result.rows[i]
			// console.log(JSON.stringify(obs))
			if(tipo.toLowerCase()=="rast") {
				const observacion = new internal.observacion({tipo:tipo, series_id:obs.series_id, timestart:obs.timestart, timeend:obs.timeend, nombre:obs.nombre, descripcion:obs.descripcion, unit_id: obs.unit_id, timeupdate: obs.timeupdate, valor:obs.valor, stats: {count: obs.count, mean: obs.mean, stddev: obs.stddev, min: obs.min, max: obs.max}})
				observacion.id = obs.id
				observaciones.push(observacion)
			} else if (options && options.asArray) {
				observaciones.push([obs.timestart, obs.timeend, obs.valor, obs.id])
			} else {
				const observacion = new internal.observacion({tipo:tipo, series_id:obs.series_id, timestart:obs.timestart, timeend:obs.timeend, nombre:obs.nombre, descripcion:obs.descripcion, unit_id:obs.unit_id, timeupdate:obs.timeupdate, valor:obs.valor})
				observacion.id = obs.id
				observaciones.push(observacion)
			}
		}
		//~ console.log(observaciones)
		return observaciones
	}
	
	static rasterValueQueryString(format,options) {
		switch(format) {
			case "GTiff":
				return "ST_AsGDALRaster(observaciones_rast.valor,'GTIff')"
				break;
			case "hex":
				return "'\\x' || encode(ST_AsGDALRaster(observaciones_rast.valor,'GTIff')::bytea,'hex')"
				break;
			case "png":
				var width = (options && options.width) ? options.width : 300
				var height = (options && options.height) ? options.height : 300
				return "ST_asGDALRaster(st_colormap(st_resize(st_reclass(valor,'[' || (st_summarystats(valor)).min || '-' || (st_summarystats(valor)).max || ']:1-255, ' || st_bandnodatavalue(valor) || ':0','8BUI')," + width + "," + height + "),1,'grayscale','nearest'),'PNG')"
				break;
			default:
				return "ST_AsGDALRaster(observaciones_rast.valor,'GTIff')"
		}
	}

	
	static async getObservacionesTimestart(tipo, filter, options) {		// DEVUELVE ARRAY DE OBSERVACIONES CON EL TIMESTART EXACTO INDICADO Y OTROS FILTROS 
		if(!filter) {
			return Promise.reject("filter missing")
		}
		if(!filter.timestart && !filter.date) {
			return Promise.reject("timestart missing")
		}
		var date = (filter.timestart) ? new Date(filter.timestart) : (filter.date) ? new Date(filter.date) : new Date()
		filter.timestart = date
		if(date=='Invalid Date') {
			return Promise.reject("Bad date")
		}
		var series_id = filter.series_id
		var estacion_id = filter.estacion_id
		var area_id = filter.area_id
		var var_id = filter.var_id
		var proc_id = (filter.proc_id) ? filter.proc_id : [1,2] 
		var precision = (filter.precision) ? filter.precision : 3
		var fuentes_id = filter.fuentes_id
		if(!/^\d+$/.test(precision)) {
			return Promise.reject("Bad precision")
		}
		if(!date instanceof Date) {
			return Promise.reject("Bad date")
		}
		var series_table = (tipo.toLowerCase()=='areal') ? "series_areal" : "series"
		var obs_table = (tipo.toLowerCase()=='areal') ? "observaciones_areal" : "observaciones"
		var val_table = (tipo.toLowerCase()=='areal') ? "valores_num_areal" : "valores_num"
		var est_column = (tipo.toLowerCase()=='areal') ? "area_id" : "estacion_id"
		var public_table = (tipo.toLowerCase()=='areal') ? "fuentes" : "redes"
		var public_filter = ""
		if(filter.public) {
			public_filter = " AND " + public_table + ".public=true "
		}
		var query
		if(series_id) {   																			// CON SERIES_ID
			if(Array.isArray(series_id)) {
				series_id= series_id.join(",")
			} 
			if(!/^\d+(,\d+)*$/.test(series_id)) {
				console.error("bad series_id")
				return Promise.reject("bad series_id")
			}
			var stmt
			if(tipo.toLowerCase()=='areal') {
				stmt = "SELECT observaciones_areal.timestart,\
								observaciones.timeend,\
								observaciones_areal.series_id,\
								series_areal.var_id,\
								series_areal.proc_id,\
								series_areal.unit_id,\
								series_areal.fuentes_id,\
								series_areal.area_id,\
							   round(valores_num_areal.valor::numeric,$2::int) valor,\
							   fuentes.public\
						FROM observaciones_areal,valores_num_areal,series_areal,fuentes\
						WHERE observaciones_areal.id=valores_num_areal.obs_id\
						AND series_areal.id=observaciones_areal.series_id\
						AND observaciones_areal.series_id IN ("+series_id+")\
						AND observaciones_areal.timestart=$1\
						AND series_areal.fuentes_id=fuentes.id " + public_filter + "\
						ORDER BY observaciones_areal.series_id;"
			} else if(tipo.toLowerCase()=="puntual") {
				stmt = "SELECT observaciones.timestart,\
								observaciones.timeend,\
								observaciones.series_id,\
								series.var_id,\
								series.proc_id,\
								series.unit_id,\
								series.estacion_id,\
							   round(valores_num.valor::numeric,$2::int) valor,\
							   redes.public\
						FROM observaciones,valores_num,series,estaciones,redes\
						WHERE observaciones.id=valores_num.obs_id\
						AND series.id=observaciones.series_id\
						AND observaciones.series_id IN ("+series_id+")\
						AND observaciones.timestart=$1\
						AND series.estacion_id=estaciones.unid\
						AND estaciones.tabla=redes.tabla_id " + public_filter + "\
						ORDER BY observaciones.series_id;"
			} else {
				return Promise.reject("Bad tipo")
			}
			query = global.pool.query(stmt,[date,precision])
		} else {													// SIN SERIES_ID
			var stmt
			if (tipo.toLowerCase()=='areal') {						// AREAL
				var area_id_filter = ""
				if(area_id) {
					if(Array.isArray(area_id)) {
						area_id = area_id.join(",")
					}
					if(!/^\d+(,\d+)*$/.test(area_id)) {
						console.error("bad area_id")
						return Promise.reject("bad area_id")
					}
					area_id_filter = " AND series_areal.area_id IN ("+area_id+")"
				}
				if(!fuentes_id) {
					return Promise.reject("fuentes_id or series_id missing")
				}
				if(Array.isArray(fuentes_id)) {
					fuentes_id = fuentes_id.join(",")
				}
				if(!/^\d+(,\d+)*$/.test(fuentes_id)) {
					return Promise.reject("bad fuentes_id")
				}
				stmt = "SELECT observaciones_areal.timestart,\
							observaciones.timeend,\
							observaciones_areal.series_id,\
							series_areal.var_id,\
							series_areal.proc_id,\
							series_areal.unit_id,\
							series_areal.fuentes_id,\
							series_areal.area_id,\
							round(valores_num_areal.valor::numeric,$2::int) valor,\
							fuentes.public\
						FROM observaciones_areal,valores_num_areal,series_areal,fuentes\
						WHERE observaciones_areal.id=valores_num_areal.obs_id\
						AND series_areal.id=observaciones_areal.series_id\
						AND observaciones_areal.timestart=$1\
						"+area_id_filter+"\
						AND series_areal.fuentes_id IN ("+fuentes_id+")\
						AND series_areal.fuentes_id=fuentes.id " + public_filter + "\
						ORDER BY observaciones_areal.series_id;"
			} else {												// PUNTUAL, sin SERIES_ID
				var estacion_id_filter = ""
				if(estacion_id) {
					if(Array.isArray(estacion_id)) {
						estacion_id = estacion_id.join(",")
						console.log("estacion_id is array")
					}
					if(!/^\d+(,\d+)*$/.test(estacion_id)) {
						console.error("bad estacion_id")
						return Promise.reject("bad estacion_id")
					}
				estacion_id_filter = " AND series.estacion_id IN ("+estacion_id+")"
				}
				if(!var_id) {
					return Promise.reject("var_id or series_id missing")
				}
				if(Array.isArray(var_id)) {
					var_id = var_id.join(",")
				}
				if(!/^\d+(,\d+)*$/.test(var_id)) {
					//~ console.error("bad var_id")
					return Promise.reject("bad var_id")
				}
				if(Array.isArray(proc_id)) {
					proc_id=proc_id.join(",")
				}
				if(!/^\d+(,\d+)*$/.test(proc_id)) {
					//~ console.error("bad proc_id")
					return Promise.reject("bad proc_id")
				}
				stmt = "SELECT observaciones.timestart,\
							observaciones.timeend,\
							observaciones.series_id,\
							series.var_id,\
							series.proc_id,\
							series.unit_id,\
							series.estacion_id,\
						   round(valores_num.valor::numeric,$2::int) valor,\
						   redes.public\
					FROM observaciones,valores_num,series,estaciones,redes\
					WHERE observaciones.id=valores_num.obs_id\
					AND series.id=observaciones.series_id\
					AND series.var_id IN ("+var_id+")\
					AND series.proc_id IN ("+proc_id+")\
					AND observaciones.timestart=$1\
					"+estacion_id_filter+"\
					AND series.estacion_id=estaciones.unid\
					AND estaciones.tabla=redes.tabla_id " + public_filter + "\
					ORDER BY observaciones.series_id;"
			}
			//~ console.log(stmt)
			query = global.pool.query(stmt,[date,precision])
		}
		return query.then(result=>{
			if(!result.rows) {
				return []
			}
			if(result.rows.length == 0) {
				console.log("no se encontraron observaciones")
				return []
			}
			var observaciones = result.rows
			console.log("got "+observaciones.length+" obs for timestart:" + filter.timestart.toISOString())
			if(options.format) {
				if(options.format.toLowerCase() == "geojson") {
					if(tipo.toLowerCase() == "areal") {
						return this.getAreas({series_id:observaciones.map(obs=>obs.area_id)})
						.then(areas=>{
							//~ var features = []
							var o = []
							areas.forEach(area=>{
								observaciones.forEach(obs=>{
									if(area.id == obs.area_id) {
										obs.geom = area.geom
										obs.area_nombre = area.nombre
										obs.valor = parseFloat(obs.valor)
										o.push(obs)
									}
								})
							})
							return o
						})
					} else { // puntual
						return this.getEstaciones({series_id:observaciones.map(obs=>obs.area_id)})
						.then(estaciones=>{
							//~ var features = []
							var o = []
							estaciones.forEach(estacion=>{
								observaciones.forEach(obs=>{
									if(estacion.id == obs.estacion_id) {
										obs.geom = estacion.geom
										obs.estacion_nombre = estacion.nombre
										obs.fuentes_id = estacion.tabla
										obs.id_externo = estacion.id_externo
										obs.valor = parseFloat(obs.valor)
										o.push(obs)
									}
								})
							})
							return o
						})
					}
				} else {
					return observaciones
				}
			} else {
				return observaciones
			}
		})
	}

	static async getObservacionesDia(tipo, filter, options) {
//		date=new Date(),tipo="puntual",series_id,estacion_id,var_id,proc_id=[1,2],agg_func="avg")
		return new Promise( (resolve,reject) => {
			if(!filter) {
				reject("filter missing")
				return
			}
			var date = (filter.date) ? new Date(filter.date) : new Date()
			if(date=='Invalid Date') {
				reject("Bad date")
				return
			}
			date = new Date(date.getTime() + date.getTimezoneOffset()*60*1000).toISOString().substring(0,10)
			var series_id = filter.series_id
			var estacion_id = filter.estacion_id
			var area_id = filter.area_id
			var var_id = filter.var_id
			var proc_id = (filter.proc_id) ? filter.proc_id : [1,2] 
			var agg_func = (filter.agg_func) ? filter.agg_func : "avg"
			var precision = (filter.precision) ? filter.precision : 3
			var fuentes_id = filter.fuentes_id
			if(!/^\d+$/.test(precision)) {
				reject("Bad precision")
				return
			}
			if(!date instanceof Date) {
				reject("Bad date")
				return
			}
			if(! {avg:1,sum:1,min:1,max:1,count:1}[agg_func.toLowerCase()]) {
				reject("Bad agg_func")
				return
			}
			var series_table = (tipo.toLowerCase()=='areal') ? "series_areal" : "series"
			var obs_table = (tipo.toLowerCase()=='areal') ? "observaciones_areal" : "observaciones"
			var val_table = (tipo.toLowerCase()=='areal') ? "valores_num_areal" : "valores_num"
			var est_column = (tipo.toLowerCase()=='areal') ? "area_id" : "estacion_id"
			var public_table = (tipo.toLowerCase()=='areal') ? "fuentes" : "redes"
			var public_filter = ""
			if(filter.public) {
				public_filter = " AND " + public_table + ".public=true "
			}
		
			if(series_id) {
				if(Array.isArray(series_id)) {
					series_id= series_id.join(",")
				} 
				if(!/^\d+(,\d+)*$/.test(series_id)) {
					console.error("bad series_id")
					reject("bad series_id")
					return
				}
				var stmt
				if(tipo.toLowerCase()=='areal') {
					stmt = "SELECT observaciones_areal.timestart::date::text date,\
									observaciones_areal.series_id,\
									series_areal.var_id,\
									series_areal.proc_id,\
									series_areal.unit_id,\
									series_areal.fuentes_id,\
									series_areal.area_id,\
								   round("+agg_func+"(valores_num_areal.valor)::numeric,$2::int) valor,\
								   fuentes.public\
							FROM observaciones_areal,valores_num_areal,series_areal,fuentes\
							WHERE observaciones_areal.id=valores_num_areal.obs_id\
							AND series_areal.id=observaciones_areal.series_id\
							AND observaciones_areal.series_id IN ("+series_id+")\
							AND observaciones_areal.timestart::date=$1::date\
							AND series_areal.fuentes_id=fuentes.id " + public_filter + "\
							GROUP BY observaciones_areal.timestart::date::text,\
									 observaciones_areal.series_id,\
									series_areal.var_id,\
									series_areal.proc_id,\
									series_areal.unit_id,\
									series_areal.fuentes_id,\
									series_areal.area_id,\
									fuentes.public\
							ORDER BY observaciones_areal.series_id;"
				} else if(tipo.toLowerCase()=="puntual") {
					stmt = "SELECT observaciones.timestart::date::text date,\
									observaciones.series_id,\
									series.var_id,\
									series.proc_id,\
									series.unit_id,\
									series.estacion_id,\
								   round("+agg_func+"(valores_num.valor)::numeric,$2::int) valor,\
								   redes.public\
							FROM observaciones,valores_num,series,estaciones,redes\
							WHERE observaciones.id=valores_num.obs_id\
							AND series.id=observaciones.series_id\
							AND observaciones.series_id IN ("+series_id+")\
							AND observaciones.timestart::date=$1::date\
							AND series.estacion_id=estaciones.unid\
							AND estaciones.tabla=redes.tabla_id " + public_filter + "\
							GROUP BY observaciones.timestart::date::text,\
									 observaciones.series_id,\
									series.var_id,\
									series.proc_id,\
									series.unit_id,\
									series.estacion_id,\
									redes.public\
							ORDER BY observaciones.series_id;"
				} else {
					reject("Bad tipo")
					return
				}
				resolve(global.pool.query(stmt,[date,precision]))
				return
			} else {
				var stmt
				if (tipo.toLowerCase()=='areal') {
					var area_id_filter = ""
					if(area_id) {
						if(Array.isArray(area_id)) {
							area_id = area_id.join(",")
						}
						if(!/^\d+(,\d+)*$/.test(area_id)) {
							console.error("bad area_id")
							reject("bad area_id")
							return
						}
						area_id_filter = " AND series_areal.area_id IN ("+area_id+")"
					}
					if(!fuentes_id) {
						reject("fuentes_id or series_id missing")
						return
					}
					if(Array.isArray(fuentes_id)) {
						fuentes_id = fuentes_id.join(",")
					}
					if(!/^\d+(,\d+)*$/.test(fuentes_id)) {
						reject("bad fuentes_id")
						return
					}
					stmt = "SELECT observaciones_areal.timestart::date::text date,\
									observaciones_areal.series_id,\
									series_areal.var_id,\
									series_areal.proc_id,\
									series_areal.unit_id,\
									series_areal.fuentes_id,\
									series_areal.area_id,\
								   round("+agg_func+"(valores_num_areal.valor)::numeric,$2::int) valor,\
								   fuentes.public\
							FROM observaciones_areal,valores_num_areal,series_areal,fuentes\
							WHERE observaciones_areal.id=valores_num_areal.obs_id\
							AND series_areal.id=observaciones_areal.series_id\
							AND observaciones_areal.timestart::date=$1::date\
							AND series_areal.fuentes_id=fuentes.id " + public_filter + "\
							"+area_id_filter+"\
							AND series_areal.fuentes_id IN ("+fuentes_id+")\
							GROUP BY observaciones_areal.timestart::date::text,\
									 observaciones_areal.series_id,\
									series_areal.var_id,\
									series_areal.proc_id,\
									series_areal.unit_id,\
									series_areal.area_id,\
									series_areal.fuentes_id,\
									fuentes.public\
							ORDER BY observaciones_areal.series_id;"
				} else {
					var estacion_id_filter = ""
					if(estacion_id) {
						if(Array.isArray(estacion_id)) {
							estacion_id = estacion_id.join(",")
							console.log("estacion_id is array")
						}
						if(!/^\d+(,\d+)*$/.test(estacion_id)) {
							console.error("bad estacion_id")
							reject("bad estacion_id")
							return
						}
						estacion_id_filter = " AND series.estacion_id IN ("+estacion_id+")"
					}
					if(!var_id) {
						reject("var_id or series_id missing")
						return
					}
					if(Array.isArray(var_id)) {
						var_id = var_id.join(",")
					}
					if(!/^\d+(,\d+)*$/.test(var_id)) {
						//~ console.error("bad var_id")
						reject("bad var_id")
						return
					}
					if(Array.isArray(proc_id)) {
						proc_id=proc_id.join(",")
					}
					if(!/^\d+(,\d+)*$/.test(proc_id)) {
						//~ console.error("bad proc_id")
						reject("bad proc_id")
						return
					}
					stmt = "SELECT observaciones.timestart::date::text date,\
								observaciones.series_id,\
								series.var_id,\
								series.proc_id,\
								series.unit_id,\
								series.estacion_id,\
							   round("+agg_func+"(valores_num.valor)::numeric,$2::int) valor,\
							   redes.public\
						FROM observaciones,valores_num,series,estaciones,redes\
						WHERE observaciones.id=valores_num.obs_id\
						AND series.id=observaciones.series_id\
						AND series.var_id IN ("+var_id+")\
						AND series.proc_id IN ("+proc_id+")\
						AND observaciones.timestart::date=$1::date\
						AND series.estacion_id=estaciones.unid\
						AND estaciones.tabla=redes.tabla_id " + public_filter + "\
						"+estacion_id_filter+"\
						GROUP BY observaciones.timestart::date::text,\
								 observaciones.series_id,\
								series.var_id,\
								series.proc_id,\
								series.unit_id,\
								series.estacion_id,\
								redes.public\
						ORDER BY observaciones.series_id;"
				}
				//~ console.log(stmt)
				resolve(global.pool.query(stmt,[date,precision]))
				return
			}
		})
		.then(result=>{
			if(!result.rows) {
				return []
			}
			console.log("got "+result.rows.length+" daily obs")
			return result.rows
		})
	}

	static async getCubeSerie(fuentes_id,isPublic) {
		return this.getFuente(fuentes_id,isPublic)
		.then(fuente=>{
			if(!fuente.data_table) {
				throw("No cube series found")
			}
			return new internal.serie({
				tipo: "raster",
				fuente: fuente,
				var_id: fuente.def_var_id,
				proc_id: fuente.def_proc_id,
				unit_id: fuente.def_unit_id,
				escena: new internal.escena({
					geom: fuente.def_extent
				}) 
			})
		})
	}

	static async getCubeSeries(fuentes_id,tipo,def_proc_id,def_unit_id,def_var_id,data_table,isPublic,isTable) {
		return this.getFuentes({
			id: fuentes_id,
			tipo: tipo,
			def_proc_id: def_proc_id,
			def_unit_id: def_unit_id,
			def_var_id: def_var_id,
			data_table: data_table,
			public: isPublic,
			is_table: isTable  
		})
		.then(fuentes=>{
			fuentes = fuentes.filter(f=>f.data_table)
			const series = fuentes.map( fuente=>{
				return new internal.serie({
					tipo: "raster",
					fuente: fuente,
					var_id: fuente.def_var_id,
					proc_id: fuente.def_proc_id,
					unit_id: fuente.def_unit_id,
					escena: new internal.escena({
						geom: fuente.def_extent
					}) 
				})
			})
			return series
		})
	}

	static async getRastFromCube(fuentes_id,timestart,timeend,forecast_date,isPublic) {
		if(!fuentes_id) {
			return Promise.reject("Missing fuentes_id")
		}
		var fuente
		return this.getFuente(fuentes_id,isPublic)
		.then(f=>{
			if(!f) {
				return Promise.reject("Data cube not found")
			}
			fuente = f
			var data_table = fuente.data_table
			var data_column = (fuente.data_column) ? fuente.data_column : "rast"
			var date_column = (fuente.date_column) ? fuente.date_column : "date"
			if(!fuente.fd_column) { // 	no forecast date
				if(!timestart || !timeend) {
					return Promise.reject("missing timestart and/or timeend")
				}
				timestart = new Date(timestart)
				if(timestart.toString() == "Invalid Date") {
					return Promise.reject("timestart is invalid")
				}
				timeend = new Date(timeend)
				if(timeend.toString() == "Invalid Date") {
					return Promise.reject("timeend is invalid")
				}
				var query = "SELECT " + date_column + "::timestamp AS timestart,ST_AsGDALRaster(" + data_column + ",'GTIff') AS valor FROM " + data_table + " WHERE " + date_column + ">=$1::timestamptz::timestamp AND " + date_column + "<$2::timestamptz::timestamp ORDER BY " + date_column
				// console.log(query)
				return global.pool.query(query,[timestart,timeend])
			} else {
				// 	forecast date
				var fd_column = fuente.fd_column
				var date_filter = ""
				if(forecast_date) {
					forecast_date = new Date(forecast_date)
					if(forecast_date.toString() == "Invalid Date") {
						return Promise.reject("Invalid forecast date")
					}
					date_filter += " AND " + fd_column + "='" + forecast_date.toISOString() + "'::timestamptz::timestamp"
				}
				if(timestart) {
					timestart = new Date(timestart)
					if(timestart.toString() == "Invalid Date") {
						return Promise.reject("timestart is invalid")
					}
					date_filter += " AND " + date_column + ">='" + timestart.toISOString() + "'::timestamptz::timestamp"
				}
				if(timeend) {
					timeend = new Date(timeend)
					if(timeend.toString() == "Invalid Date") {
						return Promise.reject("timeend is invalid")
					}
					date_filter += " AND " + date_column + "<'" + timeend.toISOString() + "'::timestamptz::timestamp"
				}
				if(date_filter == "") {
					return Promise.reject("missing forecast date or timestart or timeend")
				}
				var query = "SELECT " + fd_column + "::timestamp AS forecast_date," + date_column + "::timestamp AS timestart,ST_AsGDALRaster(" + data_column + ",'GTIff') AS valor FROM " + data_table + " WHERE 1=1 " + date_filter + " ORDER BY " + fd_column + "," + date_column
				// console.log(query)
				return global.pool.query(query)
			}
		})
		.then(result=>{
			if(!result.rows) {
				return []
			}
			return result.rows.map(r=>{
				r.timeend = (fuente.def_dt) ? timeSteps.advanceInterval(r.timestart,fuente.def_dt) : r.timestart 
				r.tipo = "raster"
				return r
			})
		})
	}

	static async upsertRastFromCube(fuentes_id,timestart,timeend,forecast_date,isPublic,series_id,t_offset,hour) {
		if(!series_id) {
			return Promise.reject("Missing destination series_id (from series_rast)")
		}
		return this.getRastFromCube(fuentes_id,timestart,timeend,forecast_date,isPublic)
		.then(results=>{
			if(!results.length) {
				return Promise.reject("Nothing found")
			}
			results = results.map(r=>{
				r.series_id = series_id
				if(t_offset) {
					r.timestart = timeSteps.advanceInterval(r.timestart,t_offset)
					r.timeend = timeSteps.advanceInterval(r.timeend,t_offset)
				} 
				if(hour) {
					r.timestart.setHours(hour)
					r.timeend.setHours(hour)
				}
				return r
			})
			return this.upsertObservaciones(results)
		})
	}

	/**
	 * Query table constraints
	 * @param {string} table_name
	 * @returns {Promise<Array<internal.tableConstraint>>} 
	 */
	static async getTableConstraints(table_name,namespace_name="public",client) {
		const query = pasteIntoSQLQuery("SELECT \
		rel.relname AS table_name,\
		con.conname AS constraint_name,\
		con.contype AS constraint_type, \
		array_agg(kcu.COLUMN_NAME::text) AS column_names\
	FROM pg_catalog.pg_constraint con\
		INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid\
		INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace\
		INNER JOIN information_schema.key_column_usage kcu ON kcu.constraint_name = con.conname \
		WHERE nsp.nspname = $1 and rel.relname = $2 group by rel.relname, con.conname, con.contype;",[namespace_name,table_name])
		// console.log(query)
		if(client) {
			var result = await client.query(query)
		} else {
			var result = await global.pool.query(query)
		}
		return result.rows.map(row=>{
			return new internal.tableConstraint(row)
		}) 
	}

	static async upsertObservacionesCubo(id,observaciones) {
		const client = await global.pool.connect()
		const fuente = await this.getFuente(id,undefined)
		fuente.date_column = (fuente.date_column) ? fuente.date_column : "date"
		var query = `INSERT INTO ${fuente.data_table} (${fuente.date_column},${fuente.data_column})\
		VALUES ($1,ST_FromGDALRaster($2))\
		ON CONFLICT (${fuente.date_column}) DO UPDATE SET ${fuente.data_column}=excluded.${fuente.data_column}\
		RETURNING ${fuente.date_column}`
		if(fuente.fd_column) {
			var on_conflict_clause = (fuente.hasDateFdConstraint()) ? `ON CONFLICT (${fuente.date_column},${fuente.fd_column}) DO UPDATE SET ${fuente.data_column}=excluded.${fuente.data_column}` : (fuente.hasDateConstraint()) ? `ON CONFLICT (${fuente.date_column}) DO UPDATE SET ${fuente.fd_column}=excluded.${fuente.fd_column}, ${fuente.data_column}=excluded.${fuente.data_column}` : `` 
			query = `INSERT INTO ${fuente.data_table} (${fuente.date_column},${fuente.data_column},${fuente.fd_column})\
			VALUES ($1,ST_FromGDALRaster($2),$3)\
			${on_conflict_clause}\
			RETURNING ${fuente.date_column}`
		}
		console.log("fuente id:" + fuente.id + ", obs length:" + observaciones.length)
		console.log("connected")
		await client.query("BEGIN")
		const upserted = []
		for(var i in observaciones) {
			const gdal_buffer = new Buffer.from(observaciones[i].valor)
			// console.log(gdal_buffer)
			const args = (fuente.fd_column) ? [observaciones[i].timestart,gdal_buffer,observaciones[i].forecast_date] : [observaciones[i].timestart,gdal_buffer]
			try {
				const ups_row = await client.query(query,args)
				upserted.push(ups_row)
			} catch(e) {
				await client.query("ROLLBACK")
				await client.release()
				throw(e)
			}
		}
		await client.query("COMMIT")
		client.release()
		return upserted
	}

	static async updateCubeFromSeries(series_id,timestart,timeend,forecast_date,isPublic,fuentes_id) {
		const client = await global.pool.connect()
		if(!series_id) {
			throw "Missing source series_id (from series_rast)"
		}
		if(!timestart)  {
			throw "Missing timestart"
		}
		if(!timeend)  {
			throw "Missing timeend"
		}
		try{
			var serie = await this.getSerie("rast",series_id,undefined,undefined,undefined,isPublic,undefined,client)
		} catch(e) {
			client.release()
			throw(e)
		}
		if(!serie) {
			throw ("Serie not found")
		}
		if(!fuentes_id) {
			fuentes_id = serie.fuente.id
		}
		if(!fuentes_id) {
			throw("Missing fuentes_id")
		}
		try {
			var fuente = await this.getFuente(fuentes_id,isPublic,client)
		} catch(e) {
			client.release()
			throw(e)
		}
		if(!fuente) {
			throw("Fuente not found")
		}
		if(!fuente.data_table) {
			client.release()
			throw("Missing data_table from fuente")
		}
		// var client  = await global.pool.connect()
		const data_table = client.escapeIdentifier(fuente.data_table)
		const data_column = (fuente.data_column) ? client.escapeIdentifier(fuente.data_column) : "rast"
		const fd_column = (fuente.fd_column) ? client.escapeIdentifier(fuente.fd_column) : undefined
		const date_column = (fuente.date_column) ? client.escapeIdentifier(fuente.date_column) : "date"
		series_id = parseInt(series_id)
		timestart = timeSteps.DateFromDateOrInterval(timestart)
		if(timestart.toString() == "Invalid Date") {
			client.release()
			throw("Invalid timestart")
		}
		timestart = client.escapeLiteral(timestart.toISOString())
		timeend = timeSteps.DateFromDateOrInterval(timeend)
		if(timeend.toString() == "Invalid Date") {
			client.release()
			throw("Invalid timeend")
		}
		timeend = client.escapeLiteral(timeend.toISOString())
		var query = `INSERT INTO ${data_table} (${date_column},${data_column})\
			SELECT timestart,valor\
			FROM observaciones_rast\
			WHERE series_id=${series_id}\
			AND timestart>=${timestart}::timestamptz\
			AND timestart<${timeend}::timestamptz\
			ON CONFLICT (${date_column}) DO UPDATE SET ${data_column}=excluded.${data_column}\
			RETURNING ${date_column} AS timestart, ST_AsGdalRaster(${data_column},'GTiff') AS valor`
		if(fd_column) {
			var forecast_date_filter = ""
			if(forecast_date) {
				forecast_date = new Date(forecast_date)
				if(forecast_date.toString() == "Invalid Date") {
					client.release()
					throw("Invalid forecast_date")
				}
				forecast_date = client.escapeLiteral(forecast_date.toISOString())
				forecast_date_filter = `AND ${fd_column}=${forecast_date}::timestamptz`
			}
			query = `INSERT INTO ${data_table} (${date_column},${data_column},${fd_column})\
			SELECT timestart,valor,timeupdate\
			FROM observaciones_rast\
			WHERE series_id=${series_id}\
			AND timestart>=${timestart}::timestamptz\
			AND timestart<${timeend}::timestamptz\
			${forecast_date_filter}\
			ON CONFLICT (${date_column},${fd_column}) DO UPDATE SET ${data_column}=excluded.${data_column}\
			RETURNING ${date_column} AS timestart, ${fd_column} AS forecast_date, ST_AsGdalRaster(${data_column},'GTiff') AS valor`
		}
		try {
			var result = await client.query(query)
		} catch(e) {
			client.release()
			throw(e)
		}
		console.log("Saved " + result.rows.length + " rows into " + data_table)
		client.release()
		return result.rows.map(r=>{
			r.tipo = "raster"
			return new internal.observacion(r)
		}) // client.end()
	}

	static async deleteObservacionesCubo(fuentes_id,filter,options,client) {
		if(!fuentes_id) {
			throw("Missing id")
		}
		return this.getFuente(fuentes_id,undefined,client)
		.then(fuente=>{
			if(!fuente) {
				throw("Fuente not found")
			}
			var stmt
			var args = []
			var date_column = (fuente.date_column) ? fuente.date_column : "date"
			var returning_clause = (options && options.no_send_data) ? " RETURNING 1 AS d" : (fuente.fd_column) ? ` RETURNING "${date_column}" AS timestart,'\\x' || encode(st_asgdalraster("${fuente.data_column}", 'GTIff')::bytea,'hex') AS valor, "${fuente.fd_column}" AS forecast_date` : ` RETURNING "${date_column}" AS timestart,'\\x' || encode(st_asgdalraster("${fuente.data_column}", 'GTIff')::bytea,'hex') AS valor`
			var select_deleted_clause = (options && options.no_send_data) ? " SELECT count(d) FROM deleted" : "SELECT * FROM deleted"
			var filter_string = ""
			var filter_index = 0
			if(filter.timestart) {
				filter.timestart = new DateFromDateOrInterval(filter.timestart)
				if(filter.timestart.toString() == 'Invalid Date') {
					throw("Invalid timestart")
				}
				args.push(filter.timestart)
				filter_index = filter_index + 1
				filter_string = filter_string + ` AND "${date_column}">=$${filter_index}`
			}
			if(filter.timeend) {
				filter.timeend = new DateFromDateOrInterval(filter.timeend)
				if(filter.timeend.toString() == 'Invalid Date') {
					throw("Invalid timeend")
				}
				args.push(filter.timeend)
				filter_index = filter_index + 1
				filter_string = filter_string + ` AND "${date_column}"<$${filter_index}`
			}
			if(filter.forecast_date) {
				if(!fuente.fd_column) {
					"Invalid filter: forecast_date. Selected cube doesn't have forecast date"
				}
				filter.forecast_date = new DateFromDateOrInterval(filter.forecast_date)
				if(filter.forecast_date.toString() == 'Invalid Date') {
					throw("Invalid forecast_date")
				}
				args.push(filter.forecast_date)
				filter_index = filter_index + 1
				filter_string = filter_string + ` AND "${fuente.fd_column}"<$${filter_index}`
			}
			if(filter_index == 0) {
				throw("At least one filter must be set")
			}
			stmt = `WITH deleted AS (DELETE FROM "${fuente.data_table}" WHERE 1=1 ` + filter_string + returning_clause + ") " + select_deleted_clause
			// console.log(stmt)
			// console.log(args)
			if(client) {
				return client.query(stmt,args)
			} else {
				return global.pool.query(stmt,args)
			}
		})
		.then(result=>{
			if(options.no_send_data) {
				return result.rows[0].count
			} else {
				return result.rows
			}
		})
	}

	static async rastExtract(series_id,timestart,timeend,options,isPublic,client,cal_id,cor_id,forecast_date,qualifier) {
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		const serie = await this.getSerie("raster",series_id,undefined,undefined,undefined,isPublic,undefined,client)
		if(!serie) {
			console.error("serie no encontrada")
			if(release_client) {
				client.release()
			}
			return
		}
		if(!serie.id) {
			console.log("serie no encontrada")
			if(release_client) {
				client.release()
			}
			return
		}
		options.bbox = (!options.bbox) ? serie.fuente.def_extent : options.bbox
		options.pixel_height = (!options.pixel_height) ? serie.fuente.def_pixel_height : options.pixel_height
		options.pixel_width = (!options.pixel_width) ? serie.fuente.def_pixel_width : options.pixel_width
		options.srid = (!options.srid) ? serie.fuente.def_srid : options.srid
		var valid_func = [ 'LAST', 'FIRST', 'MIN', 'MAX', 'COUNT', 'SUM', 'MEAN', 'RANGE']
		options.funcion = (!options.funcion) ? "SUM" : options.funcion.toUpperCase()
		if(valid_func.indexOf(options.funcion) < 0) {
			if(release_client) {
				client.release()
			}
			return Promise.reject("'funcion' inválida. Opciones: 'LAST', 'FIRST', 'MIN', 'MAX', 'COUNT', 'SUM', 'MEAN', 'RANGE'")
		}
		options.format = (!options.format) ? "GTiff" : options.format
		const valid_formats = ["gtiff", "png"]
		if(valid_formats.map(f=> (f == options.format.toLowerCase()) ? 1 : 0).reduce( (a,b)=>a+b) == 0) {
			console.error("Invalid format:" + options.format)
			if(release_client) {
				client.release()
			}
			return
		}
		options.height = (!options.height) ? 300 : options.height
		options.width = (!options.width) ? 300 : options.width
		var rescale_band = (serie.fuente.scale_factor && serie.fuente.data_offset) ? "ST_mapAlgebra(rast,1,'32BF','[rast]*" + series.fuente.scale_factor + "+" + serie.fuente.data_offset + "')" : (serie.fuente.data_offset) ? "ST_MapAlgebra(rast,1,'32BF','[rast]+"  + serie.fuente.data_offset + "')" : "rast";
		if(cor_id || cal_id && forecast_date) {
			var data_table = "pronosticos_rast JOIN corridas ON pronosticos_rast.cor_id=corridas.id"
			var timeupdate_column = "corridas.date"
			if(cor_id) {
				var prono_filter = control_filter2(
					{
						"cor_id": {
							type: "integer",
							table: "pronosticos_rast"
						},
						"qualifier": {
							type: "string",
							table: "pronosticos_rast"
						}
					},
					{
						cor_id: cor_id,
						qualifier: qualifier
					},
					"pronosticos_rast"
				)
			} else {
				var prono_filter = control_filter2(
					{
						"cal_id": {
							type: "integer",
							table: "corridas"
						},
						"forecast_date": {
							type: "date",
							table: "corridas",
							column: "date"
						},
						"qualifier": {
							type: "string",
							table: "pronosticos_rast"
						}
					},
					{
						cal_id: cal_id,
						forecast_date: forecast_date,
						qualifier: qualifier
					},
					"pronosticos_rast"
				)
			}
		} else {
			var data_table = "observaciones_rast"
			var prono_filter = ""
			var timeupdate_column = "observaciones_rast.timeupdate"
		}
		if(options.source_time_support) {
			var timeend_column = pasteIntoSQLQuery(`${data_table}.timestart + $1::interval`, [options.source_time_support])
		} else {
			var timeend_column = `${data_table}.timeend`
		}
		var stmt
		var args
		if(options.format.toLowerCase() == "png") {
			stmt = `WITH rasts AS (
				SELECT series_id,
						count(timestart) as c,
						min(timestart) timestart,
						max(${timeend_column}) timeend, 
						max(${timeupdate_column}) timeupdate, 
						st_union(ST_Clip(st_rescale(valor,$1),'{1}',st_geomfromtext($2,$3)),$4) as rast
				FROM ${data_table}
				WHERE series_id=$5 AND timestart>=$6 AND timeend<=$7
				${prono_filter}
				GROUP by series_id),
				raststats AS (
				SELECT st_summarystats(rast) stats
				FROM rasts)
				SELECT series_id, 
						timestart, 
						timeend, 
						timeupdate, 
						c AS obs_count, 
						(stats).*,
						ST_asGDALRaster(st_colormap(st_resize(st_reclass(rast,'[' || (st_summarystats(rast)).min || '-' || (st_summarystats(rast)).max || ']:1-255, ' || st_bandnodatavalue(rast) || ':0','8BUI'),$8,$9),1,'grayscale','nearest'),$10) valor
				FROM rasts, raststats`
			if(options.min_count) {
				stmt += sprintf(" WHERE rasts.c>=%d", options.min_count)
			}
			args = [options.pixel_height, options.bbox.toString(), options.srid, options.funcion, serie.id, timestart, timeend, options.height, options.width, options.format]
			
		} else {
			const min_count_filter = (options.min_count) ? sprintf(" WHERE agg.count>=%d", options.min_count) : ""
			stmt = `WITH dest_interval as (
				SELECT
					$4::timestamptz::timestamp AS timestart,
					$5::timestamptz::timestamp AS timeend
			),
			  rasts as (
				SELECT 
						${data_table}.series_id, 
						${data_table}.timestart as source_timestart, 
						${timeend_column} as source_timeend, 
						dest_interval.timestart, 
						dest_interval.timeend, 
						greatest(extract( epoch from ${data_table}.timestart ), extract( epoch from dest_interval.timestart)) AS inter_timestart,
						least(extract(epoch from ${timeend_column}), extract( epoch from dest_interval.timeend)) AS inter_timeend,
						${timeupdate_column} AS timeupdate, 
						ST_Clip(
								${data_table}.valor,
								'{1}',
								ST_GeomFromText($1, $2)
						) AS rast
				FROM 
					${data_table},
					dest_interval
				WHERE
					${data_table}.series_id = $3 
					AND ${timeend_column} >= dest_interval.timestart
					AND ${data_table}.timestart < dest_interval.timeend
				    ${prono_filter}
				),
				agg AS (
						SELECT 
								series_id, 
								timestart,
								timeend, 
								max(timeupdate) timeupdate, 
								count(timestart) count, 
								sum(
									to_timestamp(inter_timeend)::timestamp - to_timestamp(inter_timestart)::timestamp
								) AS time_sum, 
								ST_AddBand(
										ST_Union(
												ST_MapAlgebra(
													${rescale_band}, 
													NULL, 
													'[rast] * ' || least(
														( inter_timeend - inter_timestart) / extract( epoch from (source_timeend - source_timestart)),
														1
													)::text),
												$6
										),
										ST_Union(
												rast,
												'COUNT'
										)
								) AS rast
						FROM rasts 
						GROUP BY series_id, timestart, timeend
				),
				raststats AS (
						SELECT 
								st_summarystats(rast) stats
						FROM agg
				)
				SELECT series_id,
								timestart,
								timeend,
								timeupdate,
								count AS obs_count,
								time_sum,
										(stats).*,
							    ST_AsGDALRaster(
							    rast,
							    $7
							   ) AS valor
				FROM agg, raststats
				${min_count_filter}`
			
			args = [options.bbox.toString(),options.srid,series_id,timestart,timeend,options.funcion,options.format]
		}
		// console.debug(pasteIntoSQLQuery(stmt, args))
		try {
			var result = await client.query(stmt,args)
		} catch(e) {
			if(release_client) {
				client.release()
			}
			throw new Error(e)
			// console.error(new Error(e))
			// return serie
		}
		if(!result.rows) {
			console.log("No raster values found")
			if(release_client) {
				client.release()
			}
			return serie
		}
		if(result.rows.length == 0) {
			console.log("No raster values found")
			if(release_client) {
				client.release()
			}
			return serie
		}
		console.log("Unioned " + result.rows[0].obs_count + " values")
		if(!result.rows[0].valor) {
			console.log("No raster values unioned")
			if(release_client) {
				client.release()
			}
			return serie
		}
		const obs = new internal.observacion(
			{
				tipo:"rast",
				series_id:result.rows[0].series_id,
				timestart:result.rows[0].timestart,
				timeend:result.rows[0].timeend,
				valor:result.rows[0].valor,
				nombre:options.funcion,
				descripcion: "agregación temporal", 
				unit_id: serie.unidades.id, 
				timeupdate: result.rows[0].timeupdate, 
				count: result.rows[0].obs_count, 
				options: options, 
				stats: {
					count: result.rows[0].count, 
					mean: result.rows[0].mean, 
					stddev: result.rows[0].stddev, 
					min: result.rows[0].min, 
					max: result.rows[0].max
				}
			}
		)
		obs.time_sum = result.rows[0].time_sum
		serie.observaciones = [obs]
		if(release_client) {
			client.release()
		}
		return serie
	}

	static async rastExtractByArea(series_id,timestart,timeend,area,options={},client,cor_id, cal_id, forecast_date) {
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		if(!timestart || !timeend) {
			if(release_client) {
				client.release()
			}
			return Promise.reject("falta timestart y/o timeend")
		}
		if(typeof(area) == 'object' || typeof(area) == 'string' && /\,/.test(area)) {
			area = new internal.area({geom:area})
		} else {
			try {
				area = await this.getArea(parseInt(area),undefined,client)
			}catch(e) {
				if(release_client) {
					client.release()
				}
				throw(e)	
			}
		} 
		if(!area) {
			if(release_client) {
				client.release()
			}
			throw new Error("Area not found")
		}
		try {
			var serie = await this.getSerie("rast",series_id,undefined,undefined,undefined,undefined,undefined,
			client)
		} catch(e) {
			if(release_client) {
				client.release()
			}
			throw(e)
		}
		if(!serie) {
			console.error("serie no encontrada")
			if(release_client) {
				client.release()
			}
			return
		}
		if(!serie.id) {
			console.log("serie no encontrada")
			if(release_client) {
				client.release()
			}
			return
		}
		options.funcion = (!options.funcion) ? "mean" : options.funcion
		const valid_funciones = ['mean','sum','count','min','max','stddev']
		if(valid_funciones.map(f=> (f == options.funcion.toLowerCase()) ? 1 : 0).reduce( (a,b)=>a+b) == 0) {
			console.error("Invalid funcion:" + options.funcion)
			if(release_client) {
				client.release()
			}
			return
		}
		serie.estacion = area
		serie.tipo = "areal"
		serie.id= undefined
		// console.log({geom:area.geom.toString(),srid:serie.fuente.def_srid})
		if(cor_id) {
			var stmt = "WITH s as (\
					SELECT timestart timestart,\
						timeend timeend,\
						cor_id cor_id,\
						qualifier qualifier,\
						(st_summarystats(st_clip(st_resample(st_clip(valor,1,st_buffer(st_envelope(st_geomfromtext($1,st_srid(valor))),0.5),-9999,true),0.05,0.05),1,st_geomfromtext($1,st_srid(valor)),-9999,true)))." + options.funcion.toLowerCase() + " valor\
					FROM pronosticos_rast \
					WHERE series_id=$2\
					AND timestart::timestamptz>=$3::timestamptz\
					AND timeend::timestamptz<=$4::timestamptz\
					AND cor_id=$5\
				)\
				SELECT timestart, timeend, qualifier, cor_id, to_char(valor,'S99990.99')::numeric valor\
					FROM s\
					WHERE valor IS NOT NULL\
				ORDER BY timestart;"
			var args = [area.geom.toString(),series_id,timestart,timeend,cor_id]
		} else if(cal_id && forecast_date) {
			var stmt = "WITH s as (\
					SELECT pronosticos_rast.timestart timestart,\
						pronosticos_rast.timeend timeend,\
						pronosticos_rast.cor_id cor_id,\
						qualifier qualifier,\
						(st_summarystats(st_clip(st_resample(st_clip(pronosticos_rast.valor,1,st_buffer(st_envelope(st_geomfromtext($1,st_srid(valor))),0.5),-9999,true),0.05,0.05),1,st_geomfromtext($1,st_srid(valor)),-9999,true)))." + options.funcion.toLowerCase() + " valor\
					FROM pronosticos_rast \
					JOIN corridas ON corridas.id=pronosticos_rast.cor_id \
					WHERE pronosticos_rast.series_id=$2\
					AND pronosticos_rast.timestart::timestamptz>=$3::timestamptz\
					AND pronosticos_rast.timeend::timestamptz<=$4::timestamptz\
					AND corridas.cal_id=$5\
					AND corridas.date::timestamptz=$6::timestamptz\
				)\
				SELECT timestart, timeend, qualifier, cor_id, to_char(valor,'S99990.99')::numeric valor\
					FROM s\
					WHERE valor IS NOT NULL\
				ORDER BY timestart;"
			var args = [area.geom.toString(),series_id,timestart,timeend, cal_id, forecast_date]
		} else {
			var stmt = "WITH s as (\
				SELECT timestart timestart,\
							timeend timeend,\
							(st_summarystats(st_clip(st_resample(st_clip(valor,1,st_buffer(st_envelope(st_geomfromtext($1,st_srid(valor))),0.5),-9999,true),0.05,0.05),1,st_geomfromtext($1,st_srid(valor)),-9999,true)))." + options.funcion.toLowerCase() + " valor\
						FROM observaciones_rast \
						WHERE series_id=$2\
						AND timestart::timestamptz>=$3::timestamptz\
						AND timeend::timestamptz<=$4::timestamptz)\
					SELECT timestart, timeend, to_char(valor,'S99990.99')::numeric valor\
						FROM s\
						WHERE valor IS NOT NULL\
					ORDER BY timestart;"
			var args = [area.geom.toString(),series_id,timestart,timeend] // [serie.fuente.hora_corte,serie.fuente.def_dt, area.geom.toString(),serie.fuente.def_srid,timestart,timeend]
				// console.log(internal.utils.pasteIntoSQLQuery(stmt,args))
		}
		// console.debug(pasteIntoSQLQuery(stmt, args))
		try { 
			var result = await client.query(stmt,args)
		} catch(e) {
			if(release_client) {
				client.release()
			}
			throw(e)
		}
		if(!result.rows) {
			console.log("No raster values found")
			if(options.only_obs) {
				if(release_client) {
					client.release()
				}
				return []
			} else {
				if(release_client) {
					client.release()
				}
				return serie
			}
		}
		if(result.rows.length == 0) {
			console.log("No raster values found")
			if(options.only_obs) {
				if(release_client) {
					client.release()
				}
				return []
			} else {
				if(release_client) {
					client.release()
				}
				return serie
			}
		}
		console.log("Found " + result.rows.length + " values")
		const observaciones = result.rows.map(obs=> {
			//~ console.log(obs)
			if(obs.cor_id) {
				return new internal.Pronostico({
					series_table:"series_areal",
					timestart:obs.timestart,
					timeend:obs.timeend,
					qualifier:obs.qualifier,
					valor:obs.valor, 
					cor_id:obs.cor_id
				})				
			}
			return new internal.observacion({tipo:"areal",timestart:obs.timestart,timeend:obs.timeend,valor:obs.valor, nombre:options.funcion, descripcion: "agregación espacial", unit_id: serie.unidades.id})
		})
		if(options.only_obs) {
			if(release_client) {
				client.release()
			}
			return observaciones
		} else {
			if(cor_id || cal_id && forecast_date) {
				var series_areal_id = undefined
				if(area.id) {
					var series_areales = await internal.serie.read({
						tipo: "areal",
						var_id: serie.var.id,
						proc_id: serie.procedimiento.id,
						unit_id: serie.unidades.id,
						fuentes_id: serie.fuente.id,
						estacion_id: area.id
					})
					console.debug("Found " + series_areales.length + " series areales matching area_id: " + area.id)
					series_areal_id = (series_areales.length) ? series_areales[0].id : undefined
					observaciones.forEach(o=>{
						o.series_id = series_areal_id
					})
				}
				serie = new internal.SerieTemporalSim({
					series_table: "areal",
					cor_id: observaciones[0].cor_id,
					qualifier: observaciones[0].qualifier,
					pronosticos: observaciones,
					series_id: series_areal_id
				})
			} else {
				serie.observaciones = observaciones
			}
			if(release_client) {
				client.release()
			}
			return serie
		}
	}
	
	static async rast2areal(
		series_id,
		timestart,
		timeend,
		area,
		options={},
		client,
		cor_id, 
		cal_id, 
		forecast_date
		) {
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		if(area == "all") {
			var query = "SELECT series_areal.id,series_areal.area_id FROM series_areal,series_rast,areas_pluvio WHERE series_rast.id=$1 AND series_rast.fuentes_id=series_areal.fuentes_id AND areas_pluvio.unid=series_areal.area_id AND areas_pluvio.activar=TRUE ORDER BY series_areal.id"
			// console.log(internal.utils.pasteIntoSQLQuery(query,[series_id]))
			try {
				var result = await client.query(query,[series_id])
			} catch(e) {
				if(release_client) {
					client.release()
				}
				throw(e)	
			}
			if(result.rows.length == 0) {
				console.log("No se encontraron series areal")
				if(release_client) {
					client.release()
				}
				return 
			}
			const results = []
			for(var i=0;i<result.rows.length;i++) {
				const serie_areal = result.rows[i]
				//~ console.log([series_id,timestart,timeend,serie_areal.area_id])
				try {
					var serie = await this.rastExtractByArea(series_id,timestart,timeend,serie_areal.area_id,options,client,cor_id,cal_id,forecast_date)
				} catch(e) {
					console.error(e)
					continue
				}
				if(!serie) {
					console.log("serie rast no encontrada")
					continue
				}
				if(cor_id || cal_id && forecast_date) {
					// pronosticos areales
					if(!serie.pronosticos) {
						console.log("pronosticos no encontrados")
						continue
					}
					if(serie.pronosticos.length == 0) {
						console.log("pronosticos no encontrados: length=0")
						continue
					}
					// serie.series_id = serie_areal.id
					// serie.pronosticos = serie.pronosticos.map(obs=> {
					// 	obs.series_id = serie_areal.id
					// 	obs.tipo = "areal" // just to ensure
					// 	return obs
					// })
				} else {
					if(!serie.observaciones) {
						console.log("observaciones no encontradas")
						continue
					}
					if(serie.observaciones.length == 0) {
						console.log("observaciones no encontradas")
						continue
					}
					console.log("Found serie_areal.id:" + serie_areal.id)
					serie.observaciones = serie.observaciones.map(obs=> {
						obs.series_id = serie_areal.id
						obs.tipo = "areal" // just to ensure
						return obs
					})
				}
				if(options.no_insert) {
					if(release_client) {
						client.release()
					}
					results.push(serie.observaciones)
					continue
				}
				if(cor_id || cal_id && forecast_date) {
					// create pronosticos areales
					var upserted = await internal.CRUD.upsertPronosticos(client,serie.pronosticos)
					if(options.upsert_as_observaciones) {
						try {
							const obs = serie.pronosticos.map(o=>{
								return {
									tipo: "areal",
									series_id: o.series_id,
									timestart: o.timestart,
									timeend: o.timeend,
									valor: o.valor
								}
							})
							// console.debug("To Upserte as obs: " + obs.length)
							const ups_obs = await internal.observaciones.create(obs) //,'areal',serie_areal.id,undefined) // removed client, non-transactional
							console.debug("Prono upserted as obs: " + ups_obs.length)
						} catch(e) {
							console.error(e)
						}	
					}
				} else {
					try {
						var upserted = await internal.observaciones.create(serie.observaciones) //,'areal',serie_areal.id,undefined) // removed client, non-transactional
					} catch(e) {
						console.error(e)
						continue
					}
				}
				console.log("Upserted " + upserted.length + " observaciones")
				results.push(upserted)
			}
			if(results.length == 0) {
				console.log("no observaciones or pronosticos areales created")
				if(release_client) {
					client.release()
				}
				return []
			}
			if(cor_id || cal_id && forecast_date) {
				await this.updateSeriesPronoDateRange({
					cor_id: cor_id,
					cal_id: cal_id,
					forecast_date: forecast_date,
					tipo: "areal"
				})
			}
			const arr = []
			results.map(s=> {
				if(s) {
					s.map(o=>{
						arr.push(o)
					})
				} else {
					return []
				}
			})
			return arr
		} else {
			const serie = await this.rastExtractByArea(series_id,timestart,timeend,area,options,client,cor_id,cal_id,forecast_date)
			if(!serie) {
				console.log("serie rast no encontrada")
				if(release_client) {
					client.release()
				}
				return
			}
			if(cor_id || cal_id && forecast_date) {
				if(!serie.pronosticos) {
					console.log("pronosticos no encontrados")
					return
				}
				if(serie.pronosticos.length == 0) {
					console.log("pronosticos no encontrados: length=0")
					return
				}
				if(options.no_insert) {
					if(release_client) {
						client.release()
					}
					return serie.pronosticos
				}
				var upserted = await this.upsertPronosticos(client, serie.pronosticos)
				await this.updateSeriesPronoDateRange({
					cor_id: cor_id,
					cal_id: cal_id,
					forecast_date: forecast_date,
					tipo: "areal",
					series_id: serie.id
				})
				if(options.upsert_as_observaciones) {
					try {
						const obs = serie.pronosticos.map(o=>{
							// console.debug("dt: " + (o.timeend.getTime() - o.timestart.getTime()))
							return {
								tipo: "areal",
								series_id: o.series_id,
								timestart: o.timestart,
								timeend: o.timeend,
								valor: o.valor
							}
						})
						console.debug("To Upserte as obs: " + obs.length)
						const ups_obs = await internal.observaciones.create(obs) //,'areal',serie_areal.id,undefined) // removed client, non-transactional
						console.debug("Upserted as obs: " + ups_obs.length)
					} catch(e) {
						console.error(e)
					}	
				}
			} else {
				if(!serie.observaciones) {
					console.log("observaciones no encontradas")
					if(release_client) {
						client.release()
					}
					return
				}
				if(serie.observaciones.length == 0) {
					console.log("observaciones no encontradas")
					if(release_client) {
						client.release()
					}
					return
				}
				const serie_areal = new internal.serie({tipo:"areal", "var":serie["var"], procedimiento:serie.procedimiento, unidades:serie.unidades, estacion:serie.estacion, fuente:serie.fuente})
				await serie_areal.getId(undefined,client)
				console.log("Found serie_areal.id:" + serie_areal.id)
				serie.observaciones = serie.observaciones.map(obs=> {
					obs.series_id = serie_areal.id
					return obs
				})
				if(options.no_insert) {
					if(release_client) {
						client.release()
					}
					return serie.observaciones
				}
				try {
					var upserted = await internal.observaciones.create(serie.observaciones) // this.upsertObservaciones(serie.observaciones,undefined,undefined,undefined) // removed client, non-transactional
				} catch(e) {
					if(release_client) {
						client.release()
					}
					throw(e)
				}
			}
			console.log("Upserted " + upserted.length + " observaciones/pronosticos")
			if(release_client) {
				client.release()
			}
			return upserted
		}
	}
	
	static async rastExtractByPoint(series_id,timestart,timeend,point,options={}) {
		if(typeof(point) == 'object' || typeof(point) == 'string' && /\,/.test(point)) {
			var point = new internal.estacion({nombre:"Punto arbitrario", geom: point})
		} else {
			var point = await this.getEstacion(parseInt(point))
		}
		if(!point) {
			throw("Missing point or station not found")
		}
		var serie = await this.getSerie("rast",series_id)
		if(!serie) {
			console.error("serie no encontrada")
			return
		}
		if(!serie.id) {
			console.log("serie no encontrada")
			return
		}
		options.funcion = (!options.funcion) ? "nearest" : options.funcion.toLowerCase()
		const valid_funciones = ['mean','sum','count','min','max','stddev','nearest']
		if(valid_funciones.map(f=> (f == options.funcion.toLowerCase()) ? 1 : 0).reduce( (a,b)=>a+b) == 0) {
			console.error("Invalid funcion:" + options.funcion)
			return
		}
		serie.estacion = point
		serie.tipo = "puntual"
		serie.id= options.output_series_id ?? undefined
		var max_distance = (options.max_distance) ? parseFloat(options.max_distance) : serie.fuente.def_pixel_width
		var buffer = (options.buffer) ? parseFloat(options.buffer) : serie.fuente.def_pixel_width
		var transform = (serie.fuente.scale_factor) ? " * " + serie.fuente.scale_factor : ""
		transform += (serie.fuente.data_offset) ? " + " + serie.fuente.data_offset : ""
		serie.extractionParameters = {funcion:options.funcion, max_distance: max_distance, buffer: buffer}
		var stmt
		var args
		if(options.funcion.toLowerCase() == "nearest") {
			//~ console.log([series_id,timestart,timeend,serie.estacion.geom.toString(),serie.fuente.def_srid,serie.fuente.hora_corte,serie.fuente.def_dt,max_distance])
			stmt= "WITH centroids AS (\
				SELECT timestart,val,x,y,geom,timeupdate\
				FROM (\
				SELECT timestart,timeupdate,dp.*\
				FROM observaciones_rast, lateral st_pixelascentroids(observaciones_rast.valor) AS dp\
				WHERE observaciones_rast.series_id=$1\
				AND observaciones_rast.timestart >= $2\
				AND observaciones_rast.timeend<= $3) foo),\
			distancias as (\
				SELECT timestart,\
						timeupdate,\
						val,\
						x,\
						y,\
						centroids.geom,\
						st_distance(ST_GeomFromText($4,$5),centroids.geom) distance,\
						row_number() over (partition by timestart order by st_distance(st_GeomFromText($4,$5),centroids.geom)) as rk\
				FROM centroids ORDER BY centroids.timestart,centroids.x,centroids.y\
				)\
			SELECT timestart + $6::interval timestart, \
					timestart + $6::interval + $7::interval timeend,\
					round(val::numeric,2) valor,\
					timeupdate\
			FROM distancias\
			WHERE rk=1\
			AND distance<=$8\
			ORDER BY timestart"
			args = [series_id,timestart,timeend,serie.estacion.geom.toString(),serie.fuente.def_srid,serie.fuente.hora_corte,serie.fuente.def_dt,max_distance]
		} else {
				stmt = "SELECT observaciones_rast.timestart + $1::interval timestart, \
							observaciones_rast.timestart + $1::interval + $2::interval timeend,\
							round(((st_summarystats(st_clip(observaciones_rast.valor,st_buffer(st_envelope(st_geomfromtext($3,$4)),$5))))." + options.funcion + " " + transform + ")::numeric,2) valor,\
							timeupdate\
							FROM observaciones_rast\
							WHERE series_id=$6\
							AND timestart>=$7\
							AND timeend<=$8\
							ORDER BY observaciones_rast.timestart"
				args = [serie.fuente.hora_corte, serie.fuente.def_dt, serie.estacion.geom.toString(), serie.fuente.def_srid, buffer, series_id, timestart, timeend]
		}
		var result = await global.pool.query(stmt,args)
		if(!result.rows) {
			console.log("No raster values found")
			return serie
		}
		if(result.rows.length == 0) {
			console.log("No raster values found")
			return serie
		}
		console.log("Found " + result.rows.length + " values")
		const observaciones = result.rows.map(obs=> {
			//~ console.log(obs)
			return new internal.observacion({tipo:"puntual",timestart:obs.timestart,timeend:obs.timeend,valor:obs.valor, nombre:options.funcion, descripcion: "extracción puntual", unit_id: serie.unidades.id})
		})
		serie.observaciones = new internal.observaciones(observaciones)
		if(options.output_series_id) {
			await serie.createObservaciones()
		}
		return serie
	}
	
	static async getRegularSeries(tipo="puntual",series_id,dt="1 days",timestart,timeend,options,client, cal_id, cor_id, forecast_date, qualifier) {  // options: t_offset,aggFunction,inst,timeSupport,precision,min_time_fraction,insertSeriesId,timeupdate,no_insert_as_obs,source_time_support
		// console.debug({tipo:tipo,series_id:series_id,dt:dt,timestart:timestart,timeend:timeend,options:options})
		if(!series_id || !timestart || !timeend) {
			return Promise.reject("series_id, timestart and/or timeend missing")
		}
		timestart = new Date(timestart)
		timeend = new Date(timeend)
		if(timestart.toString() == 'Invalid Date') {
			return Promise.reject("timestart: invalid date")
		}
		if(timeend.toString() == 'Invalid Date') {
			return Promise.reject("timeend: invalid date")
		}
		if(!cor_id && cal_id) {
			if(!forecast_date) {
				return Promise.reject("If cal_id is set, forecast_date must be defined")
			}
		}
		var serie = await this.getSerie(tipo,series_id)
		if(!serie) {
			console.error("serie not found")
			return
		}
		var def_t_offset = (serie.fuente) ? (serie.fuente.hora_corte) ? serie.fuente.hora_corte.toPostgres() : '0 hours' : '0 hours'
		var t_offset = (options.t_offset) ? options.t_offset : def_t_offset
		if(/[';]/.test(t_offset)) {
			console.error("Invalid t_offset")
			return
		}
		var def_inst
		if(serie["var"].datatype.toLowerCase() == "continuous" || serie["var"].datatype.toLowerCase() == "sporadic") {
			def_inst = true
		} else {
			def_inst = false
		}
		var inst = (options.inst) ? new Boolean(options.inst) : def_inst
		var min_time_fraction = (options.min_time_fraction) ? parseFloat(options.min_time_fraction) : 1
		var dt_epoch
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		// RAST //
		if(tipo.toLowerCase() == 'rast' || tipo.toLowerCase() == 'raster')  {
			//~ if (!inst) {
			// SERIE NO INSTANTANEA //
			var timeSupport
			if (!options.timeSupport) {
				timeSupport = serie["var"].timeSupport.toPostgres()
			} else {
				if(/[';]/.test(options.timeSupport)) {
					console.error("Invalid timeSupport")
					return
				} else {
					timeSupport = options.timeSupport.toPostgres()
				}
			}
			// const results = await Promise.all([this.date2obj(timestart),this.date2obj(timeend),this.interval2epoch(dt), this.interval2epoch(t_offset)])
			// console.log({dates:results})
			var timestart = await this.date2obj(timestart)
			var timeend = await this.date2obj(timeend) 
			console.debug({timestart: timestart.toISOString(), timeend: timeend.toISOString()})
			var dt = await this.interval2epoch(dt) * 1000
			dt_epoch = (inst) ? 0 : dt
			var t_offset = await this.interval2epoch(t_offset) * 1000
			var timestart_time = (timestart.getHours()*3600 + timestart.getMinutes()*60 + timestart.getSeconds()) * 1000 + timestart.getMilliseconds() // + timestart.getTimezoneOffset()*60*1000
			if(timestart_time < t_offset) {
				console.log("timestart < t_offset;timestart:" + timestart + ", " + timestart_time + " < " + t_offset)
				timestart.setTime(timestart.getTime() - timestart_time + t_offset)
			} else if (timestart_time > t_offset) {
				console.log("timestart > t_offset;" + timestart + ", " + timestart_time + " > " + t_offset)
				timestart.setTime(timestart.getTime() - timestart_time + t_offset + dt)
			}
			var timeend_time = (timeend.getHours()*3600 + timeend.getMinutes()*60 + timeend.getSeconds())*1000 + timeend.getMilliseconds() + timeend.getTimezoneOffset()*60*1000
			if(timeend_time > t_offset) {
				timeend.setTime(timeend.getTime() - timeend_time + t_offset)
			} else if (timeend_time < t_offset) {
				timeend.setTime(timeend.getTime() - timeend_time + t_offset + dt)
			}
			// console.debug({timestart:timestart,timeend:timeend,dt:dt})
			var obs = []
			for(var i=timestart.getTime();i<timeend.getTime();i=i+dt) {
				var stepstart = new Date(i)
				var stepend = new Date(i+dt)
				if(config.verbose) {
					console.debug({
						series_id: series_id,
						stepstart: stepstart.toISOString(),
						stepend: stepend.toISOString(),
						options: options,
						cal_id: cal_id, 
						cor_id: cor_id, 
						forecast_date: forecast_date, 
						qualifier: qualifier
					})
				}
				obs.push(await this.rastExtract(series_id,stepstart,stepend,options,undefined,client, cal_id, cor_id, forecast_date, qualifier))
			}
			if(obs) {
				var observaciones = obs.map((o,i)=> {
					if(o.observaciones) {
						if(!o.observaciones.length) {
							console.warn("crud.getRegularSeries: obs[" + i + "].observaciones.length = 0")
							return null
						}
						// console.log("crud.getRegularSeries: obs[" + i + "].observaciones[0] = " + o.observaciones[0].toString())
						return o.observaciones[0]
					} else {
						console.warn("crud.getRegularSeries: obs[" + i + "].observaciones undefined")
						return null
					}
				}).filter(o=> o !== null)
				if(dt_epoch) {
					observaciones = observaciones.filter(o=>{
						var time_sum_epoch = timeSteps.interval2epochSync(o.time_sum) * 1000
						// console.debug("crud.getRegularSeries: dt:" + dt_epoch, "o.time_sum:"  + JSON.stringify(o.time_sum) + ", time_sum_epoch: " + time_sum_epoch + ", min_time_fraction: " + min_time_fraction)
						if(time_sum_epoch / dt_epoch < min_time_fraction) {
							console.error("crud.getRegularSeries: la observación no alcanza la mínima fracción de tiempo")
							return false
						} else {
							return true
						}
					})
				}
				if(options.insertSeriesId) {
					observaciones = observaciones.map(o=> {
						o.series_id = options.insertSeriesId
						if (options.timeupdate) {
							o.timeupdate = options.timeupdate
						}
						if(dt / o.time_sum * 1000 < min_time_fraction) {
							console.error("la observación no alcanza la mínima fracción de tiempo")
							return null
						} else {
							return o
						}
					})
					if(cal_id && forecast_date || cor_id) {
						if(!options.no_insert_as_obs) {
							// first, upsert forecast as observations
							await this.upsertObservaciones(observaciones,undefined,undefined,options) 
						}
						// then, upsert forecast as forecast
						return this.upsertSerieSim(
							observaciones,
							{
								cor_id: cor_id,
								cal_id: cal_id,
								forecast_date: forecast_date
							},
							options.insertSeriesId,
							tipo
						)
					} else {
						return this.upsertObservaciones(observaciones,undefined,undefined,options) 
					}
						// removed client, non-transactional
							//~ .then(results=>{
								//~ return results.map(o=>{
									//~ if(o instanceof Buffer) {
										//~ o.valor = o.toString('hex')
									//~ }
								//~ })
							//~ })
				} else if (options.asArray) {
					observaciones = observaciones.map(o=>{
						return [o.timestart, o.timeend, o.valor]
					})
					if(release_client) {
						client.release()
					}
					return observaciones
				} else {
					if(release_client) {
						client.release()
					}
					return observaciones
				}
			} else {
				if(release_client) {
					client.release()
				}
				return []
			}
		// PUNTUAL, AREAL //
		} else {
			if(cor_id || cal_id) {
				var obs_t = ( tipo.toLowerCase() == "areal" ) ? "pronosticos_areal" : "pronosticos"
				var val_t = ( tipo.toLowerCase() == "areal" ) ? "pronosticos_areal" : "valores_prono_num"	
				var join_clause = `JOIN ${obs_t} ON 1=1`
				join_clause += (obs_t != val_t) ? ` JOIN ${val_t} ON ${obs_t}.id=${val_t}.prono_id` : ""
				join_clause += ` JOIN corridas ON ${obs_t}.cor_id=corridas.id`
				if(cor_id) {
					var prono_filter = control_filter2(
						{
							cor_id: {type:"integer", table: obs_t},
							qualifier: {type: "string", table: obs_t}
						},
						{
							cor_id: cor_id,
							qualifier: qualifier
						}
					)
				} else {
					if(!forecast_date) {
						throw(new Error("forecast_date is required if cal_id is set"))
					}					
					var prono_filter = control_filter2(
						{
							cal_id: {type:"integer", table: "corridas"},
							forecast_date: {type: "date", table: "corridas", column: "date"},
							qualifier: {type: "string", table: obs_t}
						},
						{
							cal_id: cal_id,
							forecast_date: forecast_date,
							qualifier: qualifier
						}
					)
				}
			} else {
				var obs_t = ( tipo.toLowerCase() == "areal" ) ? "observaciones_areal" : "observaciones"
				var val_t = ( tipo.toLowerCase() == "areal" ) ? "valores_num_areal" : "valores_num"
				var join_clause = `JOIN ${obs_t} ON 1=1 JOIN ${val_t} ON ${obs_t}.id = ${val_t}.obs_id`
				var prono_filter = ""
			}
			if(options.source_time_support) {
				var timeend_expr = `${obs_t}.timestart + '${(typeof options.source_time_support == "string") ? options.source_time_support : options.source_time_support.toPostgres()}'::interval`
			} else {
				var timeend_expr = `${obs_t}.timeend`
			}
			var stmt
			var args
			var aggFunction
			if (!inst) {
			// SERIE NO INSTANTANEA //
				var timeSupport
				if (!options.timeSupport) {
					timeSupport = serie["var"].timeSupport.toPostgres()
				} else {
					if(/[';]/.test(options.timeSupport)) {
						console.error("Invalid timeSupport")
						if(release_client) {
							client.release()
						}
						throw(new Error("Invalid timeSupport"))
					} else {
						timeSupport = options.timeSupport.toPostgres()
					}
				}
				aggFunction = (options.aggFunction) ? options.aggFunction : "acum"
				var precision = (options.precision) ? parseInt(options.precision) : 2
				var aggStmt
				switch (aggFunction.toLowerCase()) {
					case "acum":
						aggStmt = "round(sum(extract(epoch from tt)/extract(epoch from '" + timeSupport.toPostgres() + "'::interval)*valor)::numeric," + precision + ")"
						break;
					case "mean":
						aggStmt = "round((sum(extract(epoch from tt)*valor)/sum(extract(epoch from tt)))::numeric," + precision + ")"
						break;
					case "sum":
						aggStmt = "round(sum(valor)::numeric," + precision + ")"
						break;
					case "min":
						aggStmt = "round(least(valor)::numeric," + precision + ")"
						break;
					case "max":
						aggStmt = "round(greatest(valor)::numeric," + precision + ")"
						break
					case "count":
						aggStmt = "count(valor)"
						break;
					case "diff":
						aggStmt = "round((max(valor)-min(valor))::numeric," + precision + ")"
						break;
					case "increment":
						aggStmt = "round((max(valor)-first(valor))::numeric," + precision + ")"
						break;
					case "math": 
						aggStmt = options.expression
						break;
					case "first":
						aggStmt = "round(first(valor)::numeric," + precision + ")"
						break;
					case "nearest":
						aggStmt = "round(first(valor)::numeric," + precision + ")"
						break;
					case "last":
						aggStmt = "round(last(valor)::numeric," + precision + ")"
						break;
					default:
						console.error("aggFunction incorrecta")
						if(release_client) {
							client.release()
						}
						throw(new Error("aggFunction incorrecta"))
				}
				args = [timestart,t_offset,timeend,dt,series_id]
				//~ console.log({dt_to_string:timeSteps.interval2string(dt)})
				var timeseries_stmt = (timeSteps.interval2string(dt).toLowerCase()=="1 day" || timeSteps.interval2string(dt).toLowerCase()=="1 days" ) ? "SELECT generate_series($1::date + $2::interval, $3::date + $2::interval - $4::interval, $4::interval) AS dd" : "SELECT generate_series($1::timestamptz + $2::interval, $3::timestamptz + $2::interval - $4::interval, $4::interval) AS dd"
				//~ console.log(timeseries_stmt)
				stmt = `WITH d AS (
					${timeseries_stmt}
				),
				t AS (
					SELECT d.dd as fecha,
					case when ${obs_t}.timestart>=d.dd+$4::interval
					then '0'::interval
					when ${obs_t}.timestart>=d.dd
					then case when ${timeend_expr}>=d.dd+$4::interval
								then d.dd + $4::interval - ${obs_t}.timestart
								else  ${timeend_expr} - ${obs_t}.timestart
								end
					else case when ${timeend_expr} <= d.dd
								then '0'::interval
								when ${timeend_expr}<=d.dd + $4::interval
								then ${timeend_expr} - d.dd
								else $4::interval
								end
					end tt,
					${obs_t}.timestart,
					${timeend_expr} AS timeend,
					${val_t}.valor
					FROM d
					${join_clause}
					WHERE ${obs_t}.series_id = $5
					AND ${timeend_expr} >= $1 
					AND ${obs_t}.timestart <= $3::timestamp + $2::interval + $4::interval
					${prono_filter}
					ORDER BY ${obs_t}.timestart
				),
				v as (
					SELECT fecha,
						${aggStmt} AS valor, 
						count(tt) AS count
					FROM t 
					WHERE extract(epoch from tt) > 0 
					GROUP BY fecha
					ORDER BY fecha
					)
				SELECT d.dd timestart,
						d.dd + $4::interval timeend,
						v.valor,
						v.count
				FROM d
				LEFT JOIN v on (v.fecha=d.dd)
				ORDER BY d.dd`
			} else {
				// SERIE INSTANTANEA //
				aggFunction = (options.aggFunction) ? options.aggFunction : "nearest"
				var precision = (options.precision) ? parseInt(options.precision) : 2
				if(aggFunction.toLowerCase() == "nearest") {
					args = [timestart, t_offset, timeend, dt, series_id, precision]
					if(options && options.dest_time_support) {
						args.push(options.dest_time_support)
					}
					stmt=`WITH d AS (
							SELECT generate_series(
								$1::timestamp + $2::interval, 
								$3::timestamp + $2::interval - $4::interval, 
								$4::interval
							) AS dd
						),
						t as (
							SELECT d.dd as fecha,
								${obs_t}.timestart - d.dd tt,
								${obs_t}.timestart,
								${timeend_expr} AS timeend,
								${val_t}.valor,
								ROW_NUMBER() over(
									partition by d.dd 
									order by abs(extract(epoch from (${obs_t}.timestart - d.dd))::numeric)
								) AS rk
							from d
							${join_clause}
							where ${obs_t}.series_id = $5
							and ${obs_t}.timestart >= $1
							and ${timeend_expr} <= $3::timestamp + $2::interval + $4::interval
							and abs(extract(epoch from (${obs_t}.timestart - dd))::numeric) < extract(epoch from $4::interval)::numeric / 2
							${prono_filter}
						),
						v as (select fecha,
								timestart,
								tt,
								round(valor::numeric,$6) valor
						from t where rk=1
						order by fecha
						)
						SELECT d.dd AS timestart,
								d.dd ${(options && options.dest_time_support) ? `+ $7::interval` : ""} AS timeend,
								v.valor
						FROM d
						LEFT JOIN v on (v.fecha=d.dd)
						ORDER BY d.dd`
				} else {
					var aggFunc
					switch (aggFunction.toLowerCase()) {
						case "mean":
							aggFunc="round(avg(valor)::numeric," + precision +")"
							break
						case "avg":
							aggFunc="round(avg(valor)::numeric," + precision +")"
							break
						case "average":
							aggFunc="round(avg(valor)::numeric," + precision +")"
							break
						case "min":
							aggFunc="round(min(valor)::numeric," + precision +")"
							break
						case "max":
							aggFunc="round(max(valor)::numeric," + precision +")"
							break
						case "count":
							aggFunc="count(valor)"
							break
						case "diff":
							aggFunc="round((max(valor)-min(valor))::numeric," + precision +")"
							break
						case "increment":
							aggFunc = "round((max(valor)-first(valor))::numeric," + precision + ")"
							break
						case "sum":
							aggFunc="round(sum(valor)::numeric," + precision +")"
							break
						default:
							console.error("aggFunction incorrecta")
							if(release_client) {
								client.release()
							}
							throw("Bad aggregate function")
					}
					if (dt.toLowerCase()=="1 days" || dt.toLowerCase()=="1 day" ) {
						console.log("inst, dt 1 days")
						args = [timestart,t_offset, timeend, dt, series_id]
						stmt = `WITH s AS (
								SELECT generate_series(
									$1::date,
									$3::date,
									'1 days'::interval
								) d
							), obs AS (
							SELECT ${obs_t}.timestart,
								${timeend_expr} AS timeend,
								${val_t}.valor
							FROM (SELECT 1) AS foo
							${join_clause}
							WHERE ${obs_t}.series_id = $5
							AND ${obs_t}.timestart >= $1
							AND ${obs_t}.timestart <= $3::timestamp + $2::interval + $4::interval
							${prono_filter}
							ORDER BY ${obs_t}.timestart
							)
							SELECT s.d + $2::interval timestart,
									s.d + $4::interval + $2::interval timeend,
									${aggFunc} AS valor
							FROM s
							LEFT JOIN obs ON (s.d::date=(obs.timestart - $2::interval)::date)
							GROUP BY s.d + $2::interval, s.d + $4::interval + $2::interval
							ORDER BY s.d + $2::interval`
					}
						else if (dt.toLowerCase()=="1 months" || dt.toLowerCase()=="1 month"  || dt.toLowerCase()=="1 mon" ) {
						console.log("inst, dt 1 month")
						args = [timestart,t_offset, timeend, dt, series_id]
						stmt = `WITH s AS (
							SELECT generate_series(
								'${timestart.toISOString()}'::timestamptz,
								'${timeend.toISOString()}'::timestamptz - '1 months'::interval,
								'1 months'::interval) d
							), obs AS (
							SELECT ${obs_t}.timestart,
								${obs_t}timeend,
								${val_t}.valor
							FROM (SELECT 1) AS foo
							${join_clause}
							WHERE ${obs_t}.series_id = $5
							AND ${obs_t}.timestart >= $1
							AND ${obs_t}.timestart <= $3::timestamp + $2::interval + $4::interval
							${prono_filter}
							ORDER BY ${obs_t}.timestart
							)
							SELECT s.d timestart,
									s.d + $4::interval timeend,
									${aggFunc} AS valor
							FROM s
							LEFT JOIN obs ON (
								extract(month from s.d) = extract(month from obs.timestart) 
								AND extract(year from s.d)=extract(year from obs.timestart)
							)
							GROUP BY s.d, s.d + $4::interval
							ORDER BY s.d`
					} 
					else {
						args = [timestart,t_offset, timeend, dt, series_id]
						//~ console.log("SELECT generate_series('" + timestart.toISOString() + "'::timestamptz + '" + t_offset + "'::interval, '" + timeend.toISOString() + "'::timestamptz + '" + t_offset + "'::interval - '" + dt + "'::interval, '" + dt + "'::interval)")
						stmt = `WITH d AS (
								SELECT generate_series(
									$1::timestamp + $2::interval, 
									$3::timestamp + $2::interval - $4::interval, 
									$4::interval
								) AS dd
							),
							data as (
								SELECT ${obs_t}.timestart,
									${val_t}.valor
								FROM (SELECT 1) AS foo
								${join_clause}
								WHERE ${obs_t}.series_id = $5
								AND ${obs_t}.timestart >= $1
								AND ${obs_t}.timestart <= $3::timestamp + $2::interval + $4::interval
								${prono_filter}
								ORDER BY ${obs_t}.timestart
							)
							SELECT d.dd timestart,
									d.dd + $4::interval timeend,
									${aggFunc} AS valor,
									count(valor) count
							FROM d
							LEFT JOIN data ON (
								data.timestart >= d.dd 
								and data.timestart ${(aggFunction.toLowerCase()=="increment") ? "<=" : "<"} d.dd + $4::interval)
							GROUP BY d.dd
							ORDER BY d.dd`
					}
				}
			}
			// console.debug(pasteIntoSQLQuery(stmt,args))
			try {
				var result = await client.query(stmt,args)
			} catch(e) {
				throw(new Error(e))
			}
			if(release_client) {
				client.release()
			}
			if(!result.rows) {
				console.error("Nothing found")
				return []
			}
			if(result.rows.length == 0) {
				console.error("No observaciones found")
				return []
			}
			//~ console.log(result.rows)
			var observaciones = result.rows.map(obs=> new internal.observacion({timestart:obs.timestart, timeend:obs.timeend, valor:obs.valor, tipo:tipo, nombre:aggFunction, descripcion:"serie regular",series_id:series_id}))
			if(options.insertSeriesId) {
				observaciones = observaciones.map(o=> {
					o.series_id = options.insertSeriesId
					if (options.timeupdate) {
						o.timeupdate = options.timeupdate
					}
					return o
				})
				if(options.no_insert) {
					return observaciones
				}
				if(cor_id || cal_id) {
					if(!options.no_insert_as_obs) {
						// first, upsert forecast as observations
						await this.upsertObservaciones(observaciones.filter(o=>o.valor),tipo,options.insertSeriesId,options) 
					} else {
						console.debug("Skipped insert as obs")
					}
					console.debug("Upsert forecast as forecast length: " + observaciones.length)
					// then, upsert forecast as forecast
					return this.upsertSerieSim(
						observaciones.filter(o=>o.valor),
						{
							cor_id: cor_id,
							cal_id: cal_id,
							forecast_date: forecast_date
						},
						options.insertSeriesId,
						tipo
					)
				} else {
					try {
						var obs = await this.upsertObservaciones(observaciones.filter(o=> o.valor),tipo.toLowerCase(),undefined,options)	// filter out null values and return
						console.debug("Inserted " + obs.length + " observaciones")
						if(options.no_send_data) {
							return obs.length
						}
						return obs
					} catch(e) {
						console.error(e)
						return
					}
				}
			} else if (options.asArray) {
				observaciones = observaciones.map(o=>{
					return [o.timestart, o.timeend, o.valor]
				})
				return observaciones
			} else {
				return observaciones
			}
		}
	}	

	static async upsertSerieSim(
		pronosticos,
		corrida_filter,
		series_id,
		tipo = "puntual"
	) {
		const corridas = await internal.corrida.read({
			id: corrida_filter.cor_id,
			cal_id: corrida_filter.cal_id,
			forecast_date: corrida_filter.forecast_date
		})
		if(!corridas.length) {
			throw(new Error("Corrida not found"))
		}
		await corridas[0].setSeries([
			new internal.SerieTemporalSim({
				series_table: internal.serie.getSeriesTable(tipo),
				series_id: series_id,
				pronosticos: pronosticos
			})
		])
		await corridas[0].create()
		if(!corridas[0].series.length) {
			console.warn("No forecast series upserted")
			return []
		}
		return corridas[0].series[0].pronosticos
	}
	
	static async getMultipleRegularSeries(series,dt="1 days",timestart,timeend,options) {
		// series: [{tipo:...,id:...},{..},...]
		// returns 2d array with dates in rows and series in columns
		return Promise.all(series.map(s=>{
			return this.getSerie((s.tipo) ? s.tipo : "puntual",{id:s.id})
		}))
		.then(seriesData=>{
			seriesData = seriesData.map(s=>s[0])
			var header0 = ["series_id"]
			var header1 = ["estacion"]
			var header2 = ["variable"]
			return Promise.all(seriesData.map(s=>{
				header0.push(s.id)
				header1.push(s.estacion.nombre)
				header2.push(s.var.nombre)
				return this.getRegularSeries( (s.tipo) ? s.tipo : "puntual",s.id,dt,timestart,timeend,options)
			}))
			.then(regularSeries=>{
				var multipleRegularSeries = [header0,header1,header2]
				if(regularSeries.length>0) {
					regularSeries[0].forEach((r,i)=>{
						var row = [r.timestart.toISOString()]
						regularSeries.forEach(s=>{
							row.push(s[i].valor)
						})
						multipleRegularSeries.push(row)
					})
				}
				return multipleRegularSeries
			})
		})
	}
	
	// getCampo2: obtiene set de series regulares de una variable puntual para un periodo y paso temporal dados, opcionalmente filtrado por recorte espacial (geom), procedimiento, array de ids de estación, id o array de id de red 
	static async getCampo2(var_id,timestart,timeend,filter={},options={}) {// filter: proc_id,proc_id=1,unit_id,geom,estacion_id,red_id   options: dt="1 days", t_offset,aggFunction,inst,timeSupport,precision,min_time_fraction,timeupdate}) 
		var proc_id = (filter.proc_id) ? filter.proc_id : (var_id==4) ? 2 : 1
		if(!var_id || ! proc_id || !timestart || !timeend) {
			return Promise.reject("Missing parameters. required: var_id proc_id unit_id timestart timeend")
		}
		if(options.agg_func) {
			if(["mean","avg","average","min","max","count","diff","increment","sum","nearest"].indexOf(options.agg_func.toLowerCase()) < 0) {
				return Promise.reject("Bad agg_func. valid values: mean,avg,average,min,max,count,diff,increment,sum")
			}
		}
		var campo = {var_id:var_id, proc_id: proc_id, timestart: timestart, timeend: timeend, filter: {geom: filter.geom,estacion_id: filter.estacion_id,red_id: filter.red_id},options:{aggFunction:options.agg_func,dt:options.dt,t_offset:options.t_offset,inst:options.inst,precision:options.precision}}
		return Promise.all([this.getVar(campo.var_id),this.getProcedimiento(campo.proc_id)])
		.then(results=>{
			if(!results[0]) {
				throw("var_id:"+campo.var_id+" not found")
			}
			if(!results[1]) {
				throw("proc_id:"+campo.proc_id+"not found")
			}
			campo.variable = results[0]
			campo.procedimiento = results[1]
			campo.unit_id = (filter.unit_id) ? filter.unit_id : campo.variable.def_unit_id
			return this.getUnidad(campo.unit_id)
		})
		.then(results=>{
			if(!results) {
				throw("unit_id:" + campo.unit_id + " not found")
			}
			campo.unidades = results
			campo.dt = (options.dt) ? options.dt : (campo.variable.timeSupport) ? campo.variable.timeSupport : "1 days"
			//~ campo.options.t_offset = (options.t_offset) ? options.t_offset : 0
			//~ campo.options.aggFunction = (options.agg_func) ? options.agg_func : "mean" 
			return this.getSeries("puntual",{var_id:campo.var_id,proc_id:campo.proc_id,unit_id:campo.unit_id,red_id:filter.red_id,geom:filter.geom,estacion_id:filter.estacion_id})
		})
		.then(series=>{
			if(!series) {
				throw("series not found")
			}
			return Promise.all(series.map(serie=>{
				return this.getRegularSeries("puntual",serie.id,campo.dt,campo.timestart,campo.timeend,campo.options)  // options: t_offset,aggFunction,inst,timeSupport,precision,min_time_fraction,insertSeriesId,timeupdate
				.then(observaciones=>{
					return {series_id: serie.id, estacion: serie.estacion, observaciones: observaciones}
					//~ return {fromSeries:serie,timestart:campo.timestart,timeend:campo.timeend,data:observaciones,dt:campo.dt,t_offset:campo.t_offset, funcion: campo.aggFunction})

					//~ return new internal.serieRegular({fromSeries:series.id,timestart:timestart,timeend:timeend,data:observaciones,dt:dt,t_offset:t_offset, funcion: aggFunction})
				})
			}))
		})
		.then(seriesRegulares=>{
			campo.data = seriesRegulares
			return campo
				//~ return new internal.campo({var_id:var_id,proc_id:proc_id,unit_id:unit_id,timestart:timestart,timeend:timeend,geom:filter.geom,dt:dt,estacion_id:filter.estacion_id,red_id:filter.red_id,options:options,seriesRegulares:seriesRegulares})
			//~ })
		})
	}
	
	//getCampo: obtiene campo de una variable para un intervalo dado, opcionalmente filtrado por red, estacion, geometría (envolvente). Agregación temporal según parámetro agg_func (default: acum)	
	static async getCampo(var_id,timestart,timeend,filter={},options={}) {  // options: t_offset,aggFunction,inst,timeSupport,precision,min_time_fraction,insertSeriesId,timeupdate,min_count
		return this.initCampo(var_id,timestart,timeend,filter,options)
		.then(campo=>{
			return this.getSingleCampo(campo)
		})
	}
	// getCampoSerie  GENERA SERIE TEMPORAL CON INTERVALO options.dt (DEFAULT 1 days)  ITERA SOBRE GETSINGLECAMPO, DEVUELVE ARREGLO 
	static async getCampoSerie(var_id,timestart,timeend,filter={},options={}) {
		try { 
		 var campo = await this.initCampo(var_id,timestart,timeend,filter,options)
		} catch(e) {
			return Promise.reject(e)
		}
		//~ console.log({campo:campo})
		var dt = (campo.options.dt) ? campo.options.dt : (campo.variable.timeSupport) ? campo.variable.timeSupport : {days: 1} 
		var dates = []
		var timestart = new Date(campo.timestart)
		var timeend = new Date(campo.timeend)
		var campos = []
		var dtepoch
		try {
			var seconds = await this.interval2epoch(dt)
			//~ console.log({seconds:seconds})
			dtepoch = seconds*1000
		} catch(e) {
			return Promise.reject({message:"Bad dt",error:e})
		}
		//~ console.log({dtepoch:dtepoch})
		if(dtepoch.toString() == "NaN") {
			return Promise.reject({message:"Bad dt"})
		}
		for(var i=timestart;i<timeend;i=new Date(i.getTime()+dtepoch)) {
			//~ console.log({i:i})
			var thiscampo = { ...campo }
			dates.push(i)
			thiscampo.timestart = i
			thiscampo.timeend = new Date(i.getTime() +  dtepoch)
			try {
				var result = await this.getSingleCampo(thiscampo)
				//~ console.log({result:result})
				campos.push(result)
			} catch(e) {
				console.error(e)
			}	
		}
		return campos
	}
	
	static async initCampo(var_id,timestart,timeend,filter={},options={}) {
		var proc_id = (filter.proc_id) ? filter.proc_id : (var_id==4) ? 2 : 1
		if(!var_id || ! proc_id || !timestart || !timeend) {
			return Promise.reject("Missing parameters. required: var_id proc_id unit_id timestart timeend")
		}
		if(options.agg_func) {
			if(["mean","avg","average","min","max","count","diff","increment","sum","nearest","array"].indexOf(options.agg_func.toLowerCase()) < 0) {
				return Promise.reject("Bad agg_func. valid values: mean,avg,average,min,max,count,diff,increment,sum")
			}
		}
		var campo = {var_id:var_id, proc_id: proc_id, timestart: timestart, timeend: timeend, filter: {geom: filter.geom,estacion_id: filter.estacion_id, red_id: filter.red_id}, options: {aggFunction: options.agg_func,dt:options.dt,t_offset:options.t_offset,inst:options.inst,precision:options.precision,timeSupport:options.timeSupport,min_count:options.min_count}}
		return Promise.all([this.getVar(campo.var_id),this.getProcedimiento(campo.proc_id)])
		.then(results=>{
			if(!results[0]) {
				throw("var_id:"+campo.var_id+" not found")
			}
			if(!results[1]) {
				throw("proc_id:"+campo.proc_id+"not found")
			}
			campo.variable = results[0]
			campo.procedimiento = results[1]
			campo.unit_id = (filter.unit_id) ? filter.unit_id : campo.variable.def_unit_id
			if(!campo.options.timeSupport) {
				campo.options.timeSupport = campo.variable.timeSupport
			}
			return this.getUnidad(campo.unit_id)
		})
		.then(results=>{
			if(!results) {
				throw("unit_id:" + campo.unit_id + " not found")
			}
			campo.unidades = results
			if(!campo.options.inst) {
				if(!campo.options.timeSupport) {
					campo.options.inst = true
				} else if (timeSteps.interval2epochSync(campo.options.timeSupport) == 0) {
					campo.options.inst = true
				} else {
					campo.options.inst = false
				}
			}
//			if(serie["var"].datatype.toLowerCase() == "continuous" || serie["var"].datatype.toLowerCase() == "sporadic") {
//				def_inst = true
//			} else {
//				def_inst = false
//			}
			var valid_filters = {estacion_id: "numeric", red_id: "numeric", geom: "geometry", series_id: "numeric", public: "boolean_only_true"}
			return global.pool.query("SELECT series.series_id series_id, series.proc_id, series.var_id, series.unit_id, series.estacion_id, estaciones.tabla, st_x(estaciones.geom) geom_x, st_y(estaciones.geom) geom_y, redes.red_id red_id, estaciones.nombre, estaciones.id_externo, estaciones.public \
			from (select series.id series_id, series.estacion_id, series.proc_id, series.var_id, series.unit_id from series) series,(select unid, tabla,geom,estaciones.nombre,id_externo,public from estaciones,redes where estaciones.tabla=redes.tabla_id AND habilitar=true) estaciones,(select tabla_id,id red_id from redes) redes \
			where var_id=$1 AND proc_id=$2 AND unit_id=$3 AND estaciones.unid=series.estacion_id and redes.tabla_id=estaciones.tabla " + internal.utils.control_filter(valid_filters, {estacion_id: filter.estacion_id, red_id: filter.red_id, geom: filter.geom, series_id: filter.series_id, public:filter.public}) + " ORDER BY series_id",[campo.variable.id,campo.procedimiento.id,campo.unidades.id])
			//~ return this.getSeries("puntual",{var_id:campo.var_id,proc_id:campo.proc_id,unit_id:campo.unit_id,red_id:filter.red_id,geom:filter.geom,estacion_id:filter.estacion_id})
		})
		.then(result=>{
			if(!result) {
				throw("series not found")
			}
			if(result.rows.length==0) {
				throw("no series match")
			}
			console.log("got " + result.rows.length + " series")
			campo.series = result.rows.map(s=>{
				return {id: s.series_id, estacion: {id: s.estacion_id, geom: new internal.geometry({type: "Point", coordinates: [s.geom_x, s.geom_y]}), tabla: s.tabla, red_id: s.red_id, nombre: s.nombre, id_externo: s.id_externo, public: s.public}}
			}) 
			campo.options.min_time_fraction = (options.min_time_fraction) ? parseFloat(options.min_time_fraction) : 1
			return campo
		})
	}

	
	static async getSingleCampo(campo) {
		var promise
		var stmt
		var args
		var min_count
		if(campo.options.min_count) {
			min_count = parseInt(campo.options.min_count)
		}
		if (!campo.options.inst) {
			// SERIE NO INSTANTANEA //
			if(!campo.options.timeSupport) {
				return Promise.reject("timeSupport missing or null")
			}
			if (!campo.options.aggFunction) {
				campo.options.aggFunction = "acum"
			}
			if (!campo.options.precision) {
				campo.options.precision = 2
			} else {
				campo.options.precision = parseInt(campo.options.precision)
				if(campo.options.precision.toString == "NaN") {
					return Promise.reject("Bad precision. must be integer")
				}
			}
			var aggStmt
			switch (campo.options.aggFunction.toLowerCase()) {
				case "acum":
					aggStmt = "round(sum(extract(epoch from overlap)/extract(epoch from '" + campo.options.timeSupport.toPostgres() + "'::interval)*valor)::numeric," + campo.options.precision + ")"
					break;
				case "mean":
					aggStmt = "round((sum(extract(epoch from overlap)*valor)/sum(extract(epoch from overlap)))::numeric," + campo.options.precision + ")"
					break;
				case "sum":
					aggStmt = "round(sum(valor)::numeric," + campo.options.precision + ")"
					break;
				case "min":
					aggStmt = "round(least(valor)::numeric," + campo.options.precision + ")"
					break;
				case "max":
					aggStmt = "round(greatest(valor)::numeric," + campo.options.precision + ")"
					break
				case "count":
					aggStmt = "count(valor)"
					break;
				case "diff":
					aggStmt = "round((max(valor)-min(valor))::numeric," + campo.options.precision + ")"
					break;
				case "increment":
					aggStmt = "round((max(valor)-first(valor))::numeric," + campo.options.precision + ")"
					break
				case "array":
					aggStmt = "json_agg(json_build_object('timestart',timestart,'valor',valor))"
					break
				default:
					return Promise.reject(new Error("aggFunction incorrecta"))
					break
			}
				  // tsrange(timestart,timeend,'[]') * tsrange($1,$2,'[]') overlap
				  
			promise = global.pool.query("with o as (\
				select series_id,\
			   observaciones.timestart,\
			   valores_num.valor,\
			   case when timestart < $1\
			   then case when timeend < $1 then '00:00:00'::interval\
					else case when timeend < $2 then timeend - $1\
						 else $2::timestamp - $1::timestamp end\
					end\
				else case when timeend < $2 then timeend - timestart\
					 else case when timestart < $2 then $2::timestamp - timestart\
						  else '00:00:00'::interval end\
					 end\
				end AS overlap\
				from observaciones,valores_num \
				where series_id in (" + campo.series.map(s=>s.id).join(",") + ")\
				and observaciones.timeend>$1 \
				and timestart<$2 \
				and observaciones.id=valores_num.obs_id)\
		select o.series_id,\
		   " + aggStmt + " valor,\
		   count(o.valor)\
		from o \
		group by o.series_id \
		order by o.series_id;",[campo.timestart,campo.timeend])
		} else {
				// SERIE INSTANTANEA //
			if(!campo.options.aggFunction) {
				campo.options.aggFunction =  "nearest"
			}
			if (!campo.options.precision) {
				campo.options.precision = 2
			} else {
				campo.options.precision = parseInt(campo.options.precision)
				if(campo.options.precision.toString == "NaN") {
					return Promise.reject("Bad precision. must be integer")
				}
			}
			if(campo.options.aggFunction.toLowerCase() == "nearest") {     // NEAREST  -> toma timestamp más próximo al centro del intervalo timestart-timeend
				promise = global.pool.query("with o as (\
				select series_id,\
			   observaciones.timestart,\
			   valores_num.valor,\
			   rank() over (partition by series_id order by abs(extract(epoch from observaciones.timestart - ($2 - ($2 - $1)/2))),timestart) rank\
				from observaciones,valores_num \
				where series_id in (" + campo.series.map(s=>s.id).join(",") + ")\
				and observaciones.timestart>=$1 \
				and timestart<$2 \
				and observaciones.id=valores_num.obs_id)\
				select o.series_id,\
				   o.timestart,\
				   o.valor,\
				   1 AS count\
				from o \
				where rank=1",[campo.timestart,campo.timeend])
			} else if(campo.options.aggFunction.toLowerCase() == "array") {     // ARRAY  -> toma todas las tuplas contenidas en el intervalo timestart-timeend
				promise = global.pool.query("with o as (\
				select series_id,\
			   observaciones.timestart,\
			   valores_num.valor\
				from observaciones,valores_num \
				where series_id in (" + campo.series.map(s=>s.id).join(",") + ")\
				and observaciones.timestart>=$1 \
				and timestart<$2 \
				and observaciones.id=valores_num.obs_id)\
				select o.series_id,\
				   json_agg(json_build_object('timestart',o.timestart,'valor',o.valor)) as valor,\
				   count(o.valor)\
				from o \
				group by series_id\
				order by series_id",[campo.timestart,campo.timeend])
			} else {
				var aggFunc
				switch (campo.options.aggFunction.toLowerCase()) {
					case "mean":
						aggFunc="round(avg(valor)::numeric," + campo.options.precision +")"
						break
					case "avg":
						aggFunc="round(avg(valor)::numeric," + campo.options.precision +")"
						break
					case "average":
						aggFunc="round(avg(valor)::numeric," + campo.options.precision +")"
						break
					case "min":
						aggFunc="round(min(valor)::numeric," + campo.options.precision +")"
						break
					case "max":
						aggFunc="round(max(valor)::numeric," + campo.options.precision +")"
						break
					case "count":
						aggFunc="count(valor)"
						break
					case "diff":
						aggFunc="round((max(valor)-min(valor))::numeric," + campo.options.precision +")"
						break
					case "increment":
						aggFunc = "round((max(valor)-first(valor))::numeric," + campo.options.precision + ")"
						break
					case "sum":
						aggFunc="round(sum(valor)::numeric," + campo.options.precision +")"
						break
					default:
						return Promise.reject("Bad aggregate function")
						break
				}
				promise = global.pool.query("SELECT series_id,\
			   " + aggFunc + " valor,\
			    count(observaciones.timestart)\
				from observaciones,valores_num \
				where series_id in (" + campo.series.map(s=>s.id).join(",") + ")\
				and observaciones.timestart>=$1 \
				and timestart<$2 \
				and observaciones.id=valores_num.obs_id\
				group by series_id\
				order by series_id",[campo.timestart,campo.timeend])
			}
		}
		return promise.then(data=>{
			if(!data) {
				throw("no data found")
			}
			if(data.rows.length==0) {
				throw("query returned empty, no data found")
			}
			var datadir = {}
			var countdir = {}
			data.rows.forEach(r=>{
				countdir[r.series_id] = parseInt(r.count)
				if(typeof r.valor == "object") {
					datadir[r.series_id] = r.valor
				} else {
					var number = parseFloat(r.valor)
					if(number.toString() ==  "NaN") {
						datadir[r.series_id] = null
					} else {
						datadir[r.series_id] = number
					}
				}
			})
			campo.series = campo.series.map(s=>{        // AGREGA PROPIEDAD VALOR Y COUNT A CAMPO.SERIES[]
				if(s.id in datadir) {
					s.valor = datadir[s.id]
					s.count = countdir[s.id]
					return s
				} else {
					return
				}
			}).filter(s=>{								//  FILTRA NULOS Y puntos con count < min_count (si campo.options.min_count)
				if(s) {
					if(min_count) {
						if(s.count < min_count) {
							return false
						} else {
							return true
						}
					} else {
						return true
					}
				} else {
					return false
				}
			})
			//~ campo.data = data.rows
			if(campo.options.insertSeriesId) {
				return new internal.campo(campo).toGrid({series_id:campo.options.insertSeriesId})
				.then(obs=>{
					return this.upsertObservacion(obs)
				})
			}
			//~ console.log({campo:campo})
			return new internal.campo(campo)
			//~ return campo
		})		
	}	
	
	// asociaciones
	
	static async getAsociaciones(filter={source_tipo:"puntual",dest_tipo:"puntual"},options={}, client) {
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		// console.log({filter:filter})
		//~ var tabla_sitios = (filter.source_tipo=="areal") ? "areas_pluvio" : (filter.source_tipo=='raster') ? "escenas" : "estaciones"
		//~ var tabla_series = (filter.source_tipo=="areal") ? "series_areal" : (filter.source_tipo=='raster') ? "series_rast" : "series"
		//~ var tabla_dest_series = (filter.dest_tipo=="areal") ? "series_areal" : (filter.dest_tipo=='raster') ? "series_rast" : "series"
		//~ var series_site_id_col = (filter.source_tipo=="areal") ? "area_id" : (filter.source_tipo=='raster') ? "escena_id" : "estacion_id"
		//~ var site_id_col = (filter.source_tipo=="areal") ? "unid" : (filter.source_tipo=='raster') ? "id" : "unid"
		//~ var provider_col = (filter.source_tipo=="areal") ? "s.fuentes_id" : (filter.source_tipo=='raster') ? "s.fuentes_id" : "e.tabla"
		//~ var params = [filter.source_tipo,filter.source_series_id,filter.estacion_id,filter.provider_id,filter.source_var_id,filter.source_proc_id,filter.dest_tipo,filter.dest_series_id,filter.dest_var_id,filter.dest_proc_id,options.agg_func,options.dt,options.t_offset,filter.habilitar]
	
		var filter_string = internal.utils.control_filter2(
			{
				id: {type:"integer"},
				source_tipo: {type: "string"}, 
				source_series_id: {type: "number"}, 
				source_estacion_id: {type: "number"}, 
				source_fuentes_id: {type: "string"}, 
				source_var_id: {type: "number"},  
				source_proc_id: {type: "number"}, 
				dest_tipo: {type: "string"}, 
				dest_series_id: {type: "number"}, 
				dest_var_id: {type: "number"}, 
				dest_proc_id: {type: "number"}, 
				agg_func: {type: "string"}, 
				dt: {type: "interval"}, 
				t_offset: {type: "interval"},
				habilitar: {type: "boolean"}, 
				cal_id:{type: "integer"}}, 
			{
				id: filter.id,
				source_tipo: filter.source_tipo, 
				source_series_id: filter.source_series_id, 
				source_estacion_id: filter.estacion_id, 
				source_fuentes_id: filter.provider_id ?? filter.tabla_id ?? filter.tabla, 
				source_var_id: filter.source_var_id, 
				source_proc_id: filter.source_proc_id, 
				dest_tipo: filter.dest_tipo, 
				dest_series_id: filter.dest_series_id, 
				dest_var_id: filter.dest_var_id, 
				dest_proc_id: filter.dest_proc_id, 
				agg_func: options.agg_func ?? filter.agg_func, 
				dt: options.dt ?? filter.dt, 
				t_offset: options.t_offset ?? filter.t_offset, 
				cal_id: filter.cal_id
			},
			"asociaciones_view")
		var query = "SELECT * \
		    FROM asociaciones_view\
		    WHERE 1=1 " + filter_string + " ORDER BY id"
		    //~ source_tipo=coalesce($1,source_tipo)\
		    //~ AND source_series_id=coalesce($2,source_series_id)\
		    //~ AND source_estacion_id=coalesce($3,source_estacion_id)\
		    //~ AND source_fuentes_id=coalesce($4::text,source_fuentes_id::text)\
		    //~ AND source_var_id=coalesce($5,source_var_id)\
		    //~ AND source_proc_id=coalesce($6,source_proc_id)\
		    //~ AND dest_tipo=coalesce($7,dest_tipo)\
		    //~ AND dest_series_id=coalesce($8,dest_series_id)\
		    //~ AND dest_var_id=coalesce($9,dest_var_id)\
		    //~ AND dest_proc_id=coalesce($10,dest_proc_id)\
		    //~ AND agg_func=coalesce($11,agg_func)\
		    //~ AND dt=coalesce($12,dt)\
		    //~ AND t_offset=coalesce($13,t_offset)\
		    //~ AND habilitar=coalesce($14,habilitar)"
		//~ console.log(internal.utils.pasteIntoSQLQuery(query,params))
		//~ return global.pool.query(query,params)
		//~ "SELECT a.id,\
									   //~ a.source_tipo, \
									   //~ a.source_series_id, \
									   //~ a.dest_tipo, \
									   //~ a.dest_series_id, \
									   //~ a.agg_func, \
									   //~ a.dt::text, \
									   //~ a.t_offset::text, \
									   //~ a.precision, \
									   //~ a.source_time_support::text, \
									   //~ a.source_is_inst, \
									   //~ row_to_json(s) source_series, \
									   //~ row_to_json(d) dest_series, \
									   //~ row_to_json(e) site\
		//~ FROM asociaciones a," + tabla_series + " s, "+ tabla_dest_series + " d," + tabla_sitios + " e\
		//~ WHERE a.source_series_id=s.id\
		//~ AND a.dest_series_id=d.id\
		//~ AND s."+series_site_id_col + "=e."+site_id_col+" \
		//~ AND a.source_tipo=coalesce($1,a.source_tipo) \
		//~ and a.source_series_id=coalesce($2,a.source_series_id)\
		//~ AND a.dest_tipo=coalesce($3,a.dest_tipo) \
		//~ and a.dest_series_id=coalesce($4,a.dest_series_id)\
		//~ AND a.agg_func=coalesce($5,a.agg_func)\
		//~ AND a.dt=coalesce($6,a.dt)\
		//~ AND a.t_offset=coalesce($7,a.t_offset)\
		//~ AND s.var_id=coalesce($8,s.var_id)\
		//~ AND d.var_id=coalesce($9,d.var_id)\
		//~ AND s.proc_id=coalesce($10,s.proc_id)\
		//~ AND d.proc_id=coalesce($11,d.proc_id)\
		//~ AND e."+site_id_col+"=coalesce($12,e."+site_id_col+")\
		//~ AND "+provider_col+"=coalesce($13,"+provider_col+")\
		//~ ORDER BY a.id",[filter.source_tipo,filter.source_series_id,filter.dest_tipo,filter.dest_series_id,options.agg_func,options.dt,options.t_offset,filter.source_var_id,filter.dest_var_id,filter.source_proc_id,filter.dest_proc_id,filter.estacion_id,filter.provider_id])
		// console.log(query)
		return client.query(query)
		.then(result=>{
			if(release_client) {
				client.release()
			}
			return result.rows
		})
		.catch(e=>{
			if(release_client) {
				client.release()
			}
			console.error(e)
			return
		})
	}
	
	static async getAsociacion(id) {
		return global.pool.query("SELECT * from asociaciones WHERE id=$1",[id])
		.then(result=>{
			if(!result) {
				throw("query error")
			}
			if(result.rows.length==0) {
				throw("No asociacion found with id " + id)
			}
			var asociacion = result.rows[0]
			var tabla_sitios = (asociacion.source_tipo=="areal") ? "areas_pluvio" : (asociacion.source_tipo=='raster') ? "escenas" : "estaciones"
			var tabla_series = (asociacion.source_tipo=="areal") ? "series_areal" : (asociacion.source_tipo=='raster') ? "series_rast" : "series"
			var tabla_dest_series = (asociacion.dest_tipo=="areal") ? "series_areal" : (asociacion.dest_tipo=='raster') ? "series_rast" : "series"
			var series_site_id_col = (asociacion.source_tipo=="areal") ? "area_id" : (asociacion.source_tipo=='raster') ? "escena_id" : "estacion_id"
			var site_id_col = (asociacion.source_tipo=="areal") ? "unid" : (asociacion.source_tipo=='raster') ? "id" : "unid"
			var provider_col = (asociacion.source_tipo=="areal") ? "s.fuentes_id" : (asociacion.source_tipo=='raster') ? "s.fuentes_id" : "e.tabla"
			return global.pool.query(`
				SELECT a.id,
				    a.source_tipo, 
					a.source_series_id, 
					a.dest_tipo, 
					a.dest_series_id, 
					a.agg_func, 
					a.dt::text,
					a.t_offset::text, 
					a.precision, 
					a.source_time_support::text, 
					a.source_is_inst, 
					a.expresion,
					a.cal_id,
					row_to_json(s) source_series, 
					row_to_json(d) dest_series, 
					row_to_json(e) site
				FROM asociaciones a
				LEFT JOIN ${tabla_series} s ON (s.id=a.source_series_id)
				LEFT JOIN ${tabla_dest_series} d ON (d.id=a.dest_series_id)
				LEFT JOIN ${tabla_sitios} e ON (e.${site_id_col}=s.${series_site_id_col})
				WHERE a.id=$1 
				`,[asociacion.id])
			.then(result=>{
				if(!result) {
					throw("query error")
				}
				if(result.rows.length==0) {
					throw("Asociacion not found")
				}
				//~ console.log({result:result.rows})
				return result.rows[0]
			})
		})
	}
	
	static async upsertAsociacion(asociacion) {
		if(!asociacion.source_series_id) {
			return Promise.reject("missing source_series_id")
		}
		if(!asociacion.dest_series_id) {
			return Promise.reject("missing dest_series_id")
		}
		asociacion = new internal.asociacion(asociacion)
		const client = await global.pool.connect()
		const result = await client.query("INSERT INTO asociaciones (source_tipo, source_series_id, dest_tipo, dest_series_id, agg_func, dt, t_offset, precision, source_time_support, source_is_inst, habilitar, expresion, cal_id) \
VALUES (coalesce($1,'puntual'),$2,coalesce($3,'puntual'),$4,$5,$6,$7,$8,$9,$10,coalesce($11,true),$12,$13)\
ON CONFLICT (dest_tipo, dest_series_id) DO UPDATE SET\
	source_tipo=excluded.source_tipo,\
	source_series_id=excluded.source_series_id,\
	agg_func=excluded.agg_func,\
	dt=excluded.dt,\
	t_offset=excluded.t_offset,\
	precision=excluded.precision,\
	source_time_support=excluded.source_time_support,\
	source_is_inst=excluded.source_is_inst,\
	habilitar=excluded.habilitar,\
	expresion=excluded.expresion,\
	cal_id=excluded.cal_id\
	RETURNING *",[asociacion.source_tipo, asociacion.source_series_id, asociacion.dest_tipo, asociacion.dest_series_id, asociacion.agg_func, asociacion.dt, asociacion.t_offset, asociacion.precision, asociacion.source_time_support, asociacion.source_is_inst, asociacion.habilitar, asociacion.expresion,asociacion.cal_id])
		//~ console.log({result:result})
		if(!result) {
			console.error("query error")
			throw("query error")
		}
		if(result.rows.length==0){
			throw("Nothing upserted")
		}
		if(asociacion.id) {
			try {
				const updated_result = await client.query(`
					UPDATE asociaciones
					SET id=$1
					WHERE id=$2
					RETURNING id
				`, [asociacion.id, result.rows[0].id])
				result.rows[0].id = updated_result.rows[0].id
			} catch(e) {
				await client.query("ROLLBACK")
				await client.release()
				throw(e)
			}
		}
		await client.query("commit")
		await client.release()
		return new internal.asociacion(result.rows[0])
	}
	
	static async upsertAsociaciones(asociaciones) {
		if(!asociaciones || asociaciones.length == 0) {
			return Promise.reject("Faltan asociaciones")
		}
		var upserted = []
		for(var asociacion of asociaciones) {
			upserted.push(await this.upsertAsociacion(asociacion))
		}
		if(upserted.length == 0) {
			throw("Nada fue acualizado/creado")
		}			
		return upserted	
	}
	
	static async runAsociaciones(filter,options={},client) {
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		return this.getAsociaciones(filter,options,client)
		.then(async asociaciones=>{
			if(asociaciones.length==0) {
				console.log("No se encontraron asociaciones")
				return []
			}
			// filter out no habilitadas
			asociaciones = asociaciones.filter(a=>a.habilitar)
			var results = []
			for(var a of asociaciones) {
				var opt = {aggFunction: a.agg_func, t_offset: a.t_offset, insertSeriesId: a.dest_series_id, insertSeriesTipo: a.dest_tipo}
				if(a.source_time_support) {
					opt.source_time_support = a.source_time_support
				}
				if(a.precision) {
					opt.precision = a.precision
				}
				if(options.inst) {
					opt.inst = options.inst
				} else if (a.source_is_inst) {
					opt.inst = a.source_is_inst
				}
				if(options.no_insert) {
					opt.no_insert = true
				}
				if(options.no_send_data) {
					opt.no_send_data = options.no_send_data
				}
				if(options.no_update) {
					opt.no_update = true
				}
				if(options.no_insert_as_obs) {
					opt.no_insert_as_obs = true
				}
				opt.dest_time_support = a.dest_time_support
				//~ promises.push(a)
				console.log("asociacion " + a.id)
				var result
				try {
					if(a.agg_func) {
						if(a.agg_func == "math") {
							if(a.source_tipo != "puntual") {
								throw("Tipo inválido para convertir por expresión (math)")
							}
							result = await this.getSerieAndConvert(
								a.source_series_id,
								filter.timestart,
								filter.timeend,
								a.expresion,
								a.dest_series_id,
								a.cal_id,
								filter.cor_id,
								filter.forecast_date,
								a.source_tipo)
						} else if (a.agg_func == "pulse") {
							if(a.source_tipo != "puntual" && a.source_tipo != "areal") {
								throw("Tipo inválido para convertir a pulsos")
							}
							result = await this.getSerieAndExtractPulses(a.source_tipo,a.source_series_id,filter.timestart,filter.timeend,a.dest_series_id)
						} else if ( (a.dt.toPostgres() == "1 month" || a.dt.toPostgres() == "1 mon" || a.dt.toPostgres() == "1 months") && a.source_tipo !="raster" && a.source_tipo != "rast") {
							console.log("running aggregateMonthly")
							const serie = await internal.serie.read({tipo:a.source_tipo,id:a.source_series_id,timestart:filter.timestart,timeend:filter.timeend})
							const observaciones = serie.aggregateMonthly(filter.timestart,filter.timeend,a.agg_func,a.precision,opt.source_time_support,a.expression,opt.inst)
							result = await this.upsertObservaciones(observaciones,a.dest_tipo,a.dest_series_id,undefined) // remove client, non-transactional
						} else {
							if(a.source_tipo != a.dest_tipo) {
								throw("Invalid asociacion: source_tipo and dest_tipo must coincide if agg_func is set")
							}
							result =  await this.getRegularSeries(
								a.source_tipo,
								a.source_series_id,
								a.dt.toPostgres(),
								filter.timestart,
								filter.timeend,
								opt,
								client,
								a.cal_id, 
								filter.cor_id, 
								filter.forecast_date, 
								filter.qualifier
							)
						}
					} else if(a.source_tipo=="raster" && a.dest_tipo=="areal") {
						console.log("Running asociacion raster to areal")
						result = await this.getSerie('areal',a.dest_series_id,undefined,undefined,{no_metadata:true},undefined,undefined,client)
						.then(series=>{
							return this.rast2areal(a.source_series_id,filter.timestart,filter.timeend,series.estacion.id,options,client)
						})
					} else {
						console.error("asociacion " + a.id + "not run")
					}
					results.push(result)
				} catch (e) {
					console.error(e)
				} 
			}
			return results
		})
		.then(inserts=>{
			if(release_client) {
				client.release()
			}
			if(!inserts) {
				return []
			}
			if(inserts.length==0) {
				return []
			}
			if(options.no_send_data) {
				return inserts.reduce((a,b)=>a+b)
			}
			return flatten(inserts)
			//~ var allinserts = []
			//~ inserts.forEach(i=>{
				//~ allinserts.push(...i)
			//~ })
			//~ return allinserts // .flat()
		})
		//~ .catch(e=>{
			//~ console.error(e)
			//~ return
		//~ })
	}
	
	static async runAsociacion(id,filter={},options={}) {
		const a = await this.getAsociacion(id)
		console.debug("Got asociacion " + a.id)
		var opt = {aggFunction: a.agg_func, t_offset: a.t_offset, insertSeriesId: a.dest_series_id}
		if(a.source_time_support) {
			opt.source_time_support = a.source_time_support
		}
		if(a.precision) {
			opt.precision = a.precision
		}
		if(options.inst) {
			opt.inst = options.inst
		} else if (a.source_is_inst) {
			opt.inst = a.source_is_inst
		}
		if(options.no_insert) {
			opt.no_insert = true
		}
		if(options.no_send_data) {
			opt.no_send_data = options.no_send_data
		}
		if(options.no_insert_as_obs) {
			opt.no_insert_as_obs = true
		}
		if(!filter.timestart || !filter.timeend) {
			throw("missing timestart and/or timeend")
		}
		var timestart = new Date(filter.timestart)
		var timeend = new Date(filter.timeend)
		if(timestart.toString() == "Invalid Date") {
			throw("invalid timestart")
		}
		if(timeend.toString() == "Invalid Date") {
			throw("invalid timeend")
		}
		if(a.agg_func && a.agg_func == "math") {
			if(a.source_tipo != "puntual") {
				throw("Tipo inválido para convertir por expresión (math)")
			}
			return this.getSerieAndConvert(
				a.source_series_id,
				timestart,
				timeend,
				a.expresion,
				a.dest_series_id, 
				a.cal_id, 
				filter.cor_id, 
				filter.forecast_date, 
				a.source_tipo)
		} else if (a.agg_func && a.agg_func == "pulse"){
			if(a.source_tipo != "puntual" && a.source_tipo != "areal") {
				throw("Tipo inválido para convertir por expresión (pulse)")
			}
			return this.getSerieAndExtractPulses(a.source_tipo,a.source_series_id,timestart,timeend,a.dest_series_id)
		} else if ( (a.dt == "1 month" || a.dt == "1 mon" || a.dt == "1 months") && a.source_tipo !="raster" && a.source_tipo != "rast") {
			console.debug("running aggregateMonthly")
			const serie = await internal.serie.read({tipo:a.source_tipo,id:a.source_series_id,timestart:timestart,timeend:timeend})
			const observaciones = serie.aggregateMonthly(timestart,timeend,a.agg_func,a.precision,a.timeSupport,a.expression)
			return this.upsertObservaciones(observaciones,a.dest_tipo,a.dest_series_id)
		} else {
			return this.getRegularSeries(
				a.source_tipo,
				a.source_series_id,
				a.dt,
				timestart,
				timeend,
				opt,
				undefined,
				a.cal_id,
				filter.cor_id,
				filter.forecast_date,
				filter.qualifier)
		}
	}
	
	static async getSerieAndExtractPulses(series_tipo,series_id,timestart,timeend,dest_series_id) {
		return this.getSerie(series_tipo,series_id,timestart,timeend)
		.then(serie=>{
			if(!serie.observaciones) {
				throw("No se encontraron observaciones")
			}
			if(serie.observaciones.length == 0) {
				throw("No se encontraron observaciones")
			}
			var observaciones = new internal.observaciones(internal.utils.acum2pulses(serie.observaciones).map(o=>{
				o.series_id = (dest_series_id) ? dest_series_id : null
				return o
			}))
			if(dest_series_id) {
				return this.upsertObservaciones(observaciones,series_tipo,dest_series_id)
			} else {
				return observaciones
			}
		})
	}

	/**
	 * Read serie and convert values using expression
	 *
	**/  
	static async getSerieAndConvert(
		series_id,
		timestart,
		timeend,
		expresion,
		dest_series_id,
		cal_id,
		cor_id,
		forecast_date,
		tipo="puntual"
	) {
		if(!cor_id && cal_id) {
			if(!forecast_date) {
				return Promise.reject("If cal_id is set, forecast_date must be defined")
			}
		}
		if(cor_id || cal_id) {
			var corridas = await internal.pronostico.read(
				{
					series_id: series_id,
					cal_id: cal_id,
					cor_id: cor_id,
					forecast_date: forecast_date,
					tipo: tipo
				},{
					includeProno: true
				})
			if(!corridas.length) {
				throw(new Error("Forecast not found"))
			}
			if(!corridas[0].series.length) {
				throw(new Error("Forecast series not found"))
			}
			if(!corridas[0].series[0].pronosticos.length) {
				throw(new Error("Forecast series time-value tuples not found"))
			}
			var observaciones = corridas[0].series[0].pronosticos
			
		} else {
			var serie = await this.getSerie('puntual',series_id,timestart,timeend)
			if(!serie.observaciones) {
				throw("No se encontraron observaciones")
			}
			if(serie.observaciones.length == 0) {
				throw("No se encontraron observaciones")
			}
			var observaciones = serie.observaciones
		}
		const observaciones_converted = observaciones.map(o=>{
			o.series_id = (dest_series_id) ? dest_series_id : null
			var valor = parseFloat(o.valor)
			if(valor === undefined || valor == null || valor.toString() == 'NaN') {
				return
			}
			o.valor = eval(expresion)
			// console.log(`convert: ${valor} -> ${o.valor}`)
			return o
		})
		.filter(o=>o)
		if(dest_series_id) {
			if(cal_id || cor_id) {
				corridas[0].series[0].series_id = dest_series_id
				corridas[0].series[0].pronosticos = observaciones_converted
				await corridas[0].create() // series[0].create()
				return corridas[0].series[0].pronosticos
			}
			return this.upsertObservacionesPuntual(observaciones_converted)
		} else {
			return observaciones_converted
		}
	}
	
	static async deleteAsociacion(id) {
		return global.pool.query("DELETE FROM asociaciones WHERE id=$1 RETURNING *",[id])
		.then(result=>{
			if(!result) {
				throw("query error")
			}
			if(result.rows.length==0) {
				throw("nothing deleted")
			}
			return result.rows[0]
		})
	}
	
	// ACCESSORS //
	
	static async getRedesAccessors(filter={}) {
		var filter_string = internal.utils.control_filter2({"tipo":{type: "string"},"tabla_id":{type:"string"},"var_id":{type:"integer"},"accessor":{type:"string"},"asociacion":{type:"boolean"}},filter,"redes_accessors")
		// console.log(filter_string)
		return global.pool.query("SELECT * from redes_accessors WHERE 1=1 " + filter_string)
		.then(result=>{
			return result.rows
		})
	}

	static async updateAccessorToken(accessor_id,token,token_expiry_date) {
		return global.pool.query("UPDATE accessors SET token=$1, token_expiry_date=$2 WHERE class=$3 RETURNING class AS accessor_id,token,token_expiry_date",[token,token_expiry_date,accessor_id])
		.then(result=>{
			if(!result.rows.length) {
				throw("updateToken failed, no rows affected")
			}
			return result.rows[0]
		})
	}

	static async getAccessorToken(accessor_id) {
		return global.pool.query("SELECT class AS accessor_id,token,token_expiry_date FROM accessors WHERE class=$1",[accessor_id])
		.then(result=>{
			if(!result.rows.length) {
				throw("getToken failed, no rows matched")
			}
			return result.rows[0]
		})
	}
	
	
	static async interval2epoch(interval) {
		if(interval instanceof Object) {
			interval = timeSteps.interval2string(interval)
			//~ var seconds = 0
			//~ Object.keys(interval).map(k=>{
				//~ switch(k) {
					//~ case "seconds":
					//~ case "second":
						//~ seconds = seconds + interval[k]
						//~ break
					//~ case "minutes":
					//~ case "minute":
						//~ seconds = seconds + interval[k] * 60
						//~ break
					//~ case "hours":
					//~ case "hour":
						//~ seconds = seconds + interval[k] * 3600
						//~ break
					//~ case "days":
					//~ case "day":
						//~ seconds = seconds + interval[k] * 86400
						//~ break
					//~ case "weeks":
					//~ case "week":
						//~ seconds = seconds + interval[k] * 86400 * 7
						//~ break
					//~ case "months":
					//~ case "month":
					//~ case "mon":
						//~ seconds = seconds + interval[k] * 86400 * 31
						//~ break
					//~ case "years":
					//~ case "year":
						//~ seconds = seconds + interval[k] * 86400 * 365
						//~ break
					//~ default:
						//~ break
				//~ }
			//~ })
			//~ return Promise.resolve(seconds)
		}
		if(!interval) {
			return 0
		} 
		//~ else {
			// console.log({interval:interval})
			//~ var client2 = new Client2()
			//~ client2.connectSync(this.dbConnectionString)
			//~ var result = client2.querySync("SELECT extract(epoch from $1::interval) AS epoch",[interval.toString()])
			return global.pool.query("SELECT extract(epoch from $1::interval) AS epoch",[interval.toString()])
			.then(result=>{
				return result.rows[0].epoch
			})
			//~ client2.end()
			//~ console.log({result:result})
			//~ return result[0].epoch
		//~ }
	}
	
	static async date2epoch(date) {
		return global.pool.query("SELECT extract(epoch from $1::timestamp) AS epoch",[date])
		.then((result)=>{
			return result.rows[0].epoch
		})
		.catch(e=>{
			console.error(e)
		})
	}
	
	static async date2obj(date) {
		if(date instanceof Date) {
			return date
		}
		return global.pool.query("SELECT $1::timestamp AS date",[date])
		.then((result)=>{
			return result.rows[0].date
		})
		.catch(e=>{
			console.error(e)
		})
	}
	
	static async getAlturas2Mnemos(estacion_id,startdate,enddate) {
		return new Promise( (resolve, reject) => {
			if(!estacion_id) {
				reject("falta estacion_id")
			}
			if(! parseInt(estacion_id)) {
				reject("estacion_id incorrecto")
			}
			if(! startdate) {
				reject("falta startdate")
			}
			var sd = new Date(startdate) 
			if(isNaN(sd)) {
				reject("startdate incorrecto")
			}
			if(! enddate) {
				reject("falta enddate")
			}
			var ed = new Date(enddate)
			if(isNaN(ed)) {
				reject("enddate incorrecto")
			}
			resolve(global.pool.query("SELECT series.estacion_id codigo_de_estacion,1 codigo_de_variable,to_char(observaciones.timestart,'DD') dia, to_char(observaciones.timestart,'MM') mes, to_char(observaciones.timestart,'YYYY') anio,extract(hour from timestart) hora,extract(minute from timestart) minuto,valor FROM series,observaciones,valores_num where series.estacion_id=$1 AND series.var_id=2 and series.proc_id=1 AND series.id=observaciones.series_id and observaciones.id=valores_num.obs_id AND timestart>=$2 and timestart<=$3 order by timestart", [estacion_id,sd,ed]))
		})
	}
	
	// ESTADISTICAS
	
	static async getCuantilesDiariosSuavizados(tipo="puntual",series_id,timestart='1974-01-01',timeend='2020-01-01',range=15,t_offset='0 hours', precision=3,isPublic) {
		if(!series_id) {
			return Promise.reject("missing series_id")
		}
		var obs_t = ( tipo.toLowerCase() == "areal" ) ? "observaciones_areal" : "observaciones"
		var val_t = ( tipo.toLowerCase() == "areal" ) ? "valores_num_areal" : "valores_num"
		return this.getSerie(tipo,series_id,undefined,undefined,undefined,isPublic)
		.then(serie=>{
			return global.pool.query("WITH s AS (\
				SELECT generate_series($1::date,$3::date,'1 days'::interval) d\
				), obs AS (\
				SELECT timestart,timeend,valor\
				FROM " + obs_t + "," + val_t + "\
				WHERE series_id=$5\
					AND " + obs_t + ".id=obs_id \
					AND timestart>=$1\
					AND timestart<=$3::timestamp+$2::interval+$4::interval\
				), obs_diaria as (\
				SELECT s.d+$2::interval timestart,\
					   s.d+$4::interval+$2::interval timeend,\
					   avg(obs.valor) valor\
					   FROM s\
					   LEFT JOIN obs ON (s.d::date=(obs.timestart-$2::interval)::date)\
					   GROUP BY s.d+$2::interval, s.d+$4::interval+$2::interval\
				), doys as ( select generate_series(1,366,1) doy\
				), wfunc as (\
	   select doys.doy,\
			  extract(doy from obs_diaria.timestart) obs_doy,\
			  obs_diaria.timestart,\
			  obs_diaria.valor\
	   from obs_diaria, doys\
	   where is_within_doy_range(doys.doy,obs_diaria.timestart::date,$6::int) = true\
	   and obs_diaria.valor is not null\
	   )\
		select $8::text tipo,\
				  $5 series_id,\
			   doy,\
			   round(min(valor)::numeric,$7::integer) as min,\
			   round(avg(valor)::numeric,$7::integer) as mean,\
			   round(max(valor)::numeric,$7::integer) as max,\
			   count(valor) as count,\
			   round(percentile_cont(0.01) within group (order by valor)::numeric,$7::integer) as p01 ,\
			   round(percentile_cont(0.1) within group (order by valor)::numeric,$7::integer) as p10 ,\
			   round(percentile_cont(0.5) within group (order by valor)::numeric,$7::integer) as p50 ,\
			   round(percentile_cont(0.9) within group (order by valor)::numeric,$7::integer) as p90 ,\
			   round(percentile_cont(0.99) within group (order by valor)::numeric,$7::integer) as p99, \
			   $6::integer as window_size,\
			   min(timestart)::date timestart,\
			   max(timestart)::date timeend\
		from wfunc\
		group by doy\
		order by doy",[timestart,t_offset, timeend, '1 days', series_id, range, precision, tipo])
	    }).then(result=>{
		   if(!result.rows) {
			   throw("Nothing found")
			   return
		   }
		   return new internal.dailyStatsList(result.rows)
	   })
	}
	
	static async getCuantilDiarioSuavizado(tipo="puntual",series_id,cuantil,timestart='1974-01-01',timeend='2020-01-01',range=15,t_offset='0 hours', precision=3,isPublic) {
		if(!series_id) {
			return Promise.reject("missing series_id")
		}
		if(!cuantil) {
			return Promise.reject("missing cuantil (0-1)")
		}
		return this.getSerie(tipo,series_id,undefined,undefined,undefined,isPublic)
		.then(series=>{
			//~ console.log("got series_id:"+series_id+", tipo:"+tipo)
			var obs_t = ( tipo.toLowerCase() == "areal" ) ? "observaciones_areal" : "observaciones"
			var val_t = ( tipo.toLowerCase() == "areal" ) ? "valores_num_areal" : "valores_num"
			return global.pool.query("WITH s AS (\
				SELECT generate_series($1::date,$3::date,'1 days'::interval) d\
				), obs AS (\
				SELECT timestart,timeend,valor\
				FROM " + obs_t + "," + val_t + "\
				WHERE series_id=$5\
					AND " + obs_t + ".id=obs_id \
					AND timestart>=$1\
					AND timestart<=$3::timestamp+$2::interval+$4::interval\
				), obs_diaria as (\
				SELECT s.d+$2::interval timestart,\
					   s.d+$4::interval+$2::interval timeend,\
					   avg(obs.valor) valor\
					   FROM s\
					   LEFT JOIN obs ON (s.d::date=(obs.timestart-$2::interval)::date)\
					   GROUP BY s.d+$2::interval, s.d+$4::interval+$2::interval\
				), doys as ( select generate_series(1,366,1) doy\
				), wfunc as (\
	   select doys.doy,\
			  extract(doy from obs_diaria.timestart) obs_doy,\
			  obs_diaria.timestart,\
			  obs_diaria.valor\
	   from obs_diaria, doys\
	   where is_within_doy_range(doys.doy,obs_diaria.timestart::date,$6::int) = true\
	   and obs_diaria.valor is not null\
	   )\
		select $8::text tipo,\
			   $5::integer series_id,\
			   $9::numeric as cuantil,\
			   $6::integer as window_size,\
			   doy,\
			   min(timestart)::date timestart,\
			   max(timestart)::date timeend,\
			   count(valor) as count,\
			   round(percentile_cont($9) within group (order by valor)::numeric,$7::integer) as valor\
		from wfunc\
		group by doy\
		order by doy",[timestart,t_offset, timeend, '1 days', series_id, range, precision, tipo, cuantil])
	   })
	   .then(result=>{
		   if(!result.rows) {
			   throw("Nothing found")
			   return
		   }
		   return result.rows
	   })
	}
	
	static async upsertDailyDoyStats2(tipo,series_id,timestart,timeend,range,t_offset,precision,is_public) {
		return this.getCuantilesDiariosSuavizados(tipo,series_id,timestart,timeend,range,t_offset,precision,is_public)
		.then(dailyStats=>{
			//~ console.log(dailyStats)
			return this.upsertDailyDoyStats(dailyStats)
		})
	}

	static async upsertDailyDoyStats(dailyStatsList) {
		var promises = dailyStatsList.values.map(i=>{
			return global.pool.query("INSERT INTO series_doy_stats (tipo, series_id,doy, count, min, max, mean, p01, p10, p50, p90, p99, window_size, timestart, timeend) VALUES\
		 ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)\
		 ON CONFLICT (tipo,series_id,doy)\
		 DO UPDATE SET count=excluded.count,\
					   min=excluded.min,\
					   max=excluded.max,\
					   mean=excluded.mean,\
					   p01=excluded.p01,\
					   p10=excluded.p10,\
					   p50=excluded.p50,\
					   p90=excluded.p90,\
					   p99=excluded.p99,\
					   window_size=excluded.window_size,\
					   timestart=excluded.timestart,\
					   timeend=excluded.timeend\
		 RETURNING *",[i.tipo, i.series_id,i.doy, i.count, i.min, i.max, i.mean, i.p01, i.p10, i.p50, i.p90, i.p99, i.window_size, i.timestart, i.timeend])
			.then(result=>{
				return result.rows[0]
			})
		})
	 return Promise.all(promises)
	 .then(results=>{
		 return new internal.dailyStatsList(results)
	 })
    }
    
    static async getDailyDoyStats(tipo="puntual",series_id,isPublic) {
		return this.getSerie(tipo,series_id,undefined,undefined,undefined,isPublic)
		.then(serie=>{
			return global.pool.query("SELECT * FROM series_doy_stats WHERE tipo=$1 AND series_id=$2 ORDER BY doy",[tipo,series_id])
		})
		.then(result=>{
			if(!result.rows) {
				throw("Nothing found")
				return
			}
			return new internal.dailyStatsList(result.rows)
		})
	}
    
	static async upsertMonthlyStats(tipo,series_id) {
		return this.getDailyDoyStats(tipo,series_id)
		.then(dailyStatsList=>{
			var cuantiles_mensuales = dailyStatsList.getMonthlyValues2()
			var cuantiles_mensuales_array = Object.keys(cuantiles_mensuales).map(key=>{
				var m = cuantiles_mensuales[key]
				m.mon = parseInt(key)
				m.series_id = (series_id) ? series_id : m.series_id
				m.tipo = (m.tipo) ? m.tipo : "puntual"
				return m
			})
			return this.insertMonthlyStats(cuantiles_mensuales_array)
		})
	}

	static async insertMonthlyStats(cuantiles_mensuales) {
		var values = cuantiles_mensuales.map(m=>{
			// console.log("monthyStats:" + (m instanceof internal.monthlyStats))
			if(!m.series_id) {
				throw("Missing series_id")
			}
			return internal.utils.pasteIntoSQLQuery("($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",[m.tipo,m.series_id,m.mon,m.count,m.min,m.max,m.mean,m.p01,m.p10,m.p50,m.p90,m.p99,m.timestart.toISOString(),m.timeend.toISOString()])
		}).join(",")
		if(!values.length) {
			console.log("No stats found")
			return []
		}
		var stmt = `INSERT INTO series_mon_stats (tipo, series_id, mon,count ,min ,max ,mean ,p01 ,p10 ,p50 ,p90 ,p99 ,timestart ,timeend) \
		VALUES ${values} \
		ON CONFLICT (tipo,series_id,mon) DO UPDATE SET count=excluded.count ,min=excluded.min ,max=excluded.max ,mean=excluded.mean ,p01=excluded.p01 ,p10=excluded.p10 ,p50=excluded.p50 ,p90=excluded.p90 ,p99=excluded.p99 ,timestart=excluded.timestart ,timeend=excluded.timeend RETURNING *`
		// console.log(stmt)
		return global.pool.query(stmt)
		.then(result=>{
			return new internal.monthlyStatsList(result.rows)
		})
	}

	static async getMonthlyStats(tipo="puntual",series_id,isPublic,as_array) {
		return this.getSerie(tipo,series_id,undefined,undefined,undefined,isPublic)
		.then(serie=>{
			return global.pool.query("SELECT * FROM series_mon_stats WHERE tipo=$1 AND series_id=$2 ORDER BY mon",[tipo,series_id])
		})
		.then(result=>{
			if(!result.rows) {
				throw("Nothing found")
				return
			}
			if(as_array) {
				return result.rows.map(r=>new internal.monthlyStats(r))
			}
			return new internal.monthlyStatsList(result.rows)
		})
	}

	static async getMonthlyStatsSeries(tipo="puntual",series_id,isPublic) {
		var filter
		var params
		if(!series_id) {
			filter = "WHERE tipo=$1" 
			params = [tipo]
		} else if(Array.isArray(series_id)) {
			if(series_id.length) {
				var id_list = series_id.map(id=>parseInt(id)).join(",")
				filter = "WHERE tipo=$1 AND series_id IN (" + id_list + ")"
				params = [tipo]
			} else {
				filter = "WHERE tipo=$1" 
				params = [tipo]
			}
		} else {
			filter = "WHERE tipo=$1 AND series_id=$2"
			params = [tipo,series_id]
		}
		var stmt = "SELECT tipo,series_id,json_agg(json_build_object('mon',mon,'count',count,'min',min,'max',max,'mean',mean,'p01',p01,'p10',p10,'p50',p50,'p90',p90,'p99',p99,'timestart',timestart,'timeend',timeend) ORDER BY mon) AS stats FROM series_mon_stats " + filter + " GROUP BY tipo,series_id ORDER BY tipo,series_id"
		console.log(stmt)
		return global.pool.query(stmt,params)
		.then(result=>{
			return result.rows
		})
	}

	static extract_doy_from_date(date) {
		date = new Date(date)
		var first = new Date(date.getUTCFullYear(),0,1)
		return Math.round((date - first)/24/3600/1000 + 1,0)
	}
	
	static is_within_doy_range(date,doy,range) {
		if(Math.abs(doy - this.extract_doy_from_date(date)) < range) {
			return true
		} else if (Math.abs(doy - (this.extract_doy_from_date(date)-365)) < range) {
			return true
		} else if (Math.abs((doy-365) - this.extract_doy_from_date(date)) < range) {
			return true
		} else {
			return false
		}          
	}

	static roundTo(value,precision) {
		var regexp = new RegExp("^(\\d+\\." + "\\d".repeat(precision) + ")\\d+$")
		return parseFloat((parseFloat(value)+.5/10**precision).toString().replace(regexp,"$1"))
	}

	//~ getCuantilesDiariosSuavizadosTodos
	static async calcPercentilesDiarios(tipo="puntual",series_id,timestart='1974-01-01',timeend='2020-01-01',range=15,t_offset='0 hours', precision=3,isPublic,update=false) {
		if(!series_id) {
			return Promise.reject("missing series_id")
		}
		return this.getSerie(tipo,series_id,undefined,undefined,undefined,isPublic)
		.then(serie=>{
			var obs_t = ( tipo.toLowerCase() == "areal" ) ? "observaciones_areal" : "observaciones"
			var val_t = ( tipo.toLowerCase() == "areal" ) ? "valores_num_areal" : "valores_num"
			// queries for daily means
			return global.pool.query("WITH s AS (\
				SELECT generate_series($1::date,$3::date,'1 days'::interval) d\
				), obs AS (\
				SELECT timestart,timeend,valor\
				FROM " + obs_t + "," + val_t + "\
				WHERE series_id=$4\
					AND " + obs_t + ".id=obs_id \
					AND timestart>=$1\
					AND timestart<=$3::timestamp+$2::interval+'1 days'::interval\
					AND valor is not null\
				) \
				SELECT s.d+$2::interval timestart,\
					   s.d+'1 days'::interval+$2::interval timeend,\
					   avg(obs.valor) valor\
					   FROM s\
					   JOIN obs ON (s.d::date=(obs.timestart-$2::interval)::date)\
					   GROUP BY s.d+$2::interval, s.d+'1 days'::interval+$2::interval",[timestart,t_offset,timeend,series_id])
		})
		.then(result=>{
			if(!result.rows) {
				throw("No observations found")
				return
			}
			if(result.rows.length==0){
				throw("No observations found")
				return
			}
			var obs_diarias=result.rows
			//~ var doys = []
			var percentiles = []
			for(var doy=1; doy<=366;doy++) {
				var obs = obs_diarias.filter(d=> this.is_within_doy_range(d.timestart,doy,range))
				var valores = obs.map(o=>parseFloat(o.valor)).sort((a,b)=>a-b)
				//~ console.log(valores.join(","))
				var dates = obs.map(o=>o.timestart).sort()
				var obslength=obs.length
				var timestart = dates[0]
				var timeend=dates[obslength-1]
				for(var percentil=1;percentil<=99;percentil++) {
					var index = obslength*percentil/100
					var value
					if(Math.round(index,0) == index) {
						value = (valores[index-1] + valores[index])/2
					} else {
						value = valores[Math.round(index)-1]
					}
					//~ value = (precision != 0) ? 1/(10 ** precision) * Math.round(value * 10 ** precision) : value
					var v = this.roundTo(value,precision) // value.toString().replace(/^(\d+\.\d\d\d)\d+$/,"$1") // .replace(/\.\d+$/,x=> x.substring(0,precision+1))
					//~ console.log({percentil:percentil,value:v})
					//~ var v = value
					//~ console.log(typeof v)
					percentiles.push(new internal.doy_percentil({doy:doy,percentil:percentil/100,valor:v,window_size:range,timestart:timestart,timeend:timeend,count:obslength}))
				}
				//~ doys.push({doy:doy,count:obslength,percentiles:percentiles})
			}
			return percentiles // doys
		})
		.then(percentiles=>{
			if(update) {
				return this.upsertPercentilesDiarios(tipo,series_id,percentiles)
			} else {
				return percentiles
			}
		})
	}
	
	static async upsertPercentilesDiarios(tipo="puntual",series_id,percentiles) {
		if(!series_id) {
			return Promise.reject("Missing series_id")
		}
		if(percentiles.length==0) {
			return Promise.reject("missing percentiles, length 0")
		}
		var rows = percentiles.map(d=> {
			var d_clean = {}
			for (let [key, value] of Object.entries(d)) {
				d_clean[key] = (typeof value=='string') ? value.replace(/'/g,"") : (value instanceof Date) ? value.toISOString().substring(0,10) : value
			}
			return d_clean
		}).map(d=> {
			if(d.valor.toString() == "NaN") {
				return
			}
			return `('${tipo}',${series_id}, ${d.doy}, ${d.percentil}, ${d.valor}, ${d.window_size}, '${d.timestart}'::date, '${d.timeend}'::date, ${d.count})`
		}).filter(r=>r)
		return global.pool.query("INSERT INTO series_doy_percentiles (tipo,series_id, doy, percentil, valor, window_size, timestart, timeend, count)\
		    VALUES " + rows.join(",") + " \
		    ON CONFLICT (tipo,series_id,doy,percentil) \
		    DO UPDATE SET valor=excluded.valor, window_size=excluded.window_size, timestart=excluded.timestart, timeend=excluded.timeend, count=excluded.count \
		    RETURNING *")
		.then(result=>{
			if(!result.rows) {
				throw("Nothing upserted")
				return
			}
			console.log("Upserted " + result.rows.length + " percentiles")
			return result.rows.map(r=>new internal.doy_percentil(r))
		})
	}
	
	static async getPercentilesDiarios(tipo="puntual",series_id,percentil,doy,isPublic) {
		if(!series_id) {
			return Promise.reject("Missing series_id")
		}
		return this.getSerie(tipo,series_id,undefined,undefined,undefined,isPublic)
		.then(serie=>{
			var doy_filter = ""
			if(doy) {
				if(Array.isArray(doy)) {
					doy_filter = " AND series_doy_percentiles.doy IN (" + doy.map(d=>parseInt(d)).join(",") + ")"
				} else {
					doy_filter = " AND series_doy_percentiles.doy = " + parseInt(doy)
				}
			}
			var promise
			if(percentil) {
				if(Array.isArray(percentil)) {
					if(Array.isArray(series_id)) {
						promise = global.pool.query("SELECT * from series_doy_percentiles WHERE tipo=$1 AND series_id IN (" + series_id.map(s=>parseInt(s)).join(",") + ") AND percentil IN (" + percentil.map(p=>parseFloat(p)).join(",") + ") " + doy_filter + " ORDER BY percentil,doy",[tipo])
					} else {					
						promise = global.pool.query("SELECT * from series_doy_percentiles WHERE tipo=$1 AND series_id=$2 AND percentil IN (" + percentil.map(p=>parseFloat(p)).join(",") + ") " + doy_filter + " ORDER BY percentil,doy",[tipo,series_id])
					}
				} else {
					if(Array.isArray(series_id)) {
						promise = global.pool.query("SELECT * from series_doy_percentiles WHERE tipo=$1 AND series_id IN (" + series_id.map(s=>parseInt(s)).join(",") + ") AND percentil=$2  " + doy_filter + " ORDER BY percentil,doy",[tipo,percentil])
					} else {
						promise = global.pool.query("SELECT * from series_doy_percentiles WHERE tipo=$1 AND series_id=$2 AND percentil=$3  " + doy_filter + " ORDER BY percentil,doy",[tipo,series_id,percentil])
					}
				}
			} else {
				promise = global.pool.query("SELECT * from series_doy_percentiles WHERE tipo=$1 AND series_id=$2  " + doy_filter + " ORDER BY percentil,doy",[tipo,series_id])
			}
			return promise
		})
		.then(result=>{
			if(!result.rows) {
				throw("Nothing found")
				return
			}
			return result.rows.map(r=>new internal.doy_percentil(r))
		})
	}
	
	static async getPercentilesDiariosBetweenDates(tipo="puntual",series_id,percentil,timestart,timeend,isPublic) {
		if(!series_id || !timestart || !timeend) {
			return Promise.reject("Missing series_id, timestart or timeend")
		}
		return this.getSerie(tipo,series_id,undefined,undefined,undefined,isPublic)
		.then(serie=>{
			var percentil_filter = ""
			if(percentil) {
				if(Array.isArray(percentil)) {
					percentil_filter = " AND percentil IN (" + percentil.map(p=>parseFloat(p)).join(",") + ")"
					//~ promise = global.pool.query("SELECT * from series_doy_percentiles WHERE tipo=$1 AND series_id=$2  ORDER BY percentil,doy",[tipo,series_id])
				} else {
					percentil_filter = "AND percentil::numeric=" + parseFloat(percentil) + "::numeric"
					//~ promise = global.pool.query("SELECT * from series_doy_percentiles WHERE tipo=$1 AND series_id=$2 AND percentil=$3  ORDER BY percentil,doy",[tipo,series_id,percentil])
				}
			} 
			return global.pool.query("WITH dates as (\
				SELECT generate_series($1::date,$2::date,'1 days'::interval) date\
				), data as (\
				SELECT series_doy_percentiles.percentil AS percentil,\
						 json_build_object('date',dates.date,'doy',series_doy_percentiles.doy,'tipo',series_doy_percentiles.tipo,'series_id',series_doy_percentiles.series_id,'valor',series_doy_percentiles.valor) AS data\
				FROM dates, series_doy_percentiles\
				WHERE extract(doy from dates.date)=series_doy_percentiles.doy\
				AND series_doy_percentiles.tipo=$3\
				AND series_doy_percentiles.series_id=$4\
				"+ percentil_filter+"\
				ORDER BY percentil,dates.date\
				) SELECT data.percentil,array_agg(data) AS data FROM DATA\
				GROUP BY percentil\
				ORDER BY percentil",[timestart,timeend,tipo,series_id])
		})
		.then(result=>{
			if(!result.rows) {
				throw("Nothing found")
				return
			}
			console.log("found " + result.rows.length + " percentile arrays")
			return result.rows // .map(r=>new internal.doy_percentil(r))
		})
	}
	
	static async matchPercentil(obs) {
		var date = new Date(obs.date)
		var doy = this.extract_doy_from_date(date)
		//~ console.log({date:date,doy:doy})
		obs.doy = doy
		return this.getPercentilesDiarios(obs.tipo,obs.series_id,undefined,doy)
		.then(percentiles=>{
			if(percentiles.length>0) {
				for(var i=0;i<percentiles.length;i++) {
					if(obs.valor <= percentiles[i].valor) {
						obs.cume_dist = percentiles[i].percentil
						break
					}
				}
				if(!obs.cume_dist) {
					obs.cume_dist = 1
				}
			} else {
				obs.cume_dist = null
			}
			return obs
		})
	}
	
	//~ getCuantilesDiarios(series_id) {
		//~ return global.pool.query("\
			//~ SELECT * \
			//~ FROM obs_diaria_cuantiles\
			//~ WHERE series_id=$1\
			//~ ORDER BY doy",[series_id])
		//~ .then(stats=>{
			//~ if(stats.rows) {
				//~ return new internal.dailyStatsList(stats.rows)
			//~ } else {
				//~ return {}
			//~ }
		//~ })
		//~ .catch(e=>{
			//~ console.error(e)
		//~ })
	//~ }
	
	// MODELOS
	
	static async deleteModelos(model_id,tipo) {
		var modelos
		try {
			modelos = await this.getModelos(model_id,tipo)
		} catch(e) {
			throw(e)
		}
		var deleted = []
		for(var i=0;i<modelos.length;i++) {
			try {
				var query = internal.utils.pasteIntoSQLQuery("DELETE FROM modelos WHERE id=$1 RETURNING *",[modelos[i].id])
				// console.log(query)
				var result = await global.pool.query(query)
				var m = result.rows[0]
				deleted.push(m)
			} catch (e) {
				throw(e)
			} 
		}
		return deleted
	}

	static async getModelos(model_id,tipo,name_contains) {
		return global.pool.query("WITH mod as (\
SELECT modelos.id, \
       modelos.nombre, \
       modelos.tipo, \
       modelos.def_var_id, \
       modelos.def_unit_id\
		FROM modelos\
		WHERE modelos.id = coalesce($1, modelos.id)\
		AND modelos.tipo = coalesce($2::text, modelos.tipo)\
		AND modelos.nombre ~ coalesce($3::text,'')\
		ORDER BY modelos.id),\
par as (SELECT mod.id model_id,\
               json_agg(parametros.*) parametros \
        FROM parametros,mod \
        WHERE parametros.model_id=mod.id \
        GROUP BY mod.id), \
est as (SELECT mod.id model_id,\
               json_agg(estados.*) estados \
        FROM estados,mod \
        WHERE estados.model_id=mod.id \
        GROUP BY mod.id), \
forz as (SELECT mod.id model_id,\
                json_agg(modelos_forzantes.*) forzantes \
         FROM modelos_forzantes,mod \
         WHERE modelos_forzantes.model_id=mod.id \
         GROUP BY mod.id), \
out as (SELECT mod.id model_id,\
               json_agg(modelos_out.*) outputs \
        FROM modelos_out,mod \
        WHERE modelos_out.model_id=mod.id \
        GROUP BY mod.id) \
SELECT mod.id, \
       mod.nombre, \
       par.parametros, \
       est.estados, \
       forz.forzantes, \
       out.outputs, \
       mod.tipo, \
       mod.def_var_id, \
       mod.def_unit_id\
		FROM mod\
		LEFT JOIN par ON par.model_id = mod.id\
		LEFT JOIN est ON est.model_id = mod.id\
		LEFT JOIN forz ON forz.model_id = mod.id\
		LEFT JOIN out ON out.model_id = mod.id\
		ORDER BY mod.id;",[model_id, tipo, name_contains])
		.then(result=>{
			if(result.rows) {
				return result.rows
			} else {
				return []
			}
		})
	}

	static async upsertModelos(modelos) {
		if(!modelos) {
			return Promise.reject("crud.upsertModelos: arguments missing")
		}
		if(!Array.isArray(modelos)) {
			modelos = [modelos]
		}
		if(modelos.length<=0) {
			return Promise.reject("crud.upsertModelos: empty array")
		}
		var rows = modelos.map(m=>{
			const required_fields = ["nombre", "tipo", "def_var_id", "def_unit_id"]
			required_fields.forEach(key=>{
				if(typeof m[key] === undefined) {
					throw("Invalid modelo. Missing " + key)
				}
				if(m[key] == null) {
					throw("Invalid modelo. " + key + " is null")
				}
			})
			var modelo = new internal.modelo(m)
			if(m.id) {
				return sprintf("(%d,'%s','%s',%d,%d)",modelo.id, modelo.nombre, modelo.tipo, modelo.def_var_id, modelo.def_unit_id)
			} else {
				return sprintf("(nextval('modelos_id_seq'::regclass),%d,'%s','%s',%d,%d)",modelo.id, modelo.nombre, modelo.tipo, modelo.def_var_id, modelo.def_unit_id)
			}
		})
		const client = await global.pool.connect()
		try {
			await client.query("BEGIN")
			var result = await client.query("INSERT INTO modelos (id,nombre,tipo,def_var_id,def_unit_id) VALUES " + rows.join(",") + " ON CONFLICT (nombre) DO UPDATE SET tipo=excluded.tipo, def_var_id=excluded.def_var_id, def_unit_id=excluded.def_unit_id RETURNING *")
			for(var i = 0;i<result.rows.length;i++) {
				modelos[i].id = result.rows[i].id
				if(modelos[i].parametros) {
					for(var j=0;j<modelos[i].parametros.length;j++) {
						// console.log(modelos[i].parametros[j])
						modelos[i].parametros[j].model_id = result.rows[i].id 
						const parametro = await this.upsertParametroDeModelo(client,modelos[i].parametros[j])
						modelos[i].parametros[j].id = parametro.id
					}
				}
				if(modelos[i].estados) {
					for(var j=0;j<modelos[i].estados.length;j++) {
						// console.log(modelos[i].estados[j])
						modelos[i].estados[j].model_id = result.rows[i].id 
						const estado = await this.upsertEstadoDeModelo(client,modelos[i].estados[j])
						modelos[i].estados[j].id = estado.id
					}
				}
				if(modelos[i].forzantes) {
					for(var j=0;j<modelos[i].forzantes.length;j++) {
						modelos[i].forzantes[j].model_id = result.rows[i].id 
						const forzante = await this.upsertForzanteDeModelo(client,modelos[i].forzantes[j])
						modelos[i].forzantes[j].id = forzante.id
					}
				}
				if(modelos[i].outputs) {
					for(var j=0;j<modelos[i].outputs.length;j++) {
						modelos[i].outputs[j].model_id = result.rows[i].id 
						const output = await this.upsertOutputDeModelo(client,modelos[i].outputs[j])
						modelos[i].outputs[j].id = output.id
					}
				}
			}
			await client.query("COMMIT")
		} catch(e) {
			console.log("ROLLBACK")
			client.query("ROLLBACK")
			throw(e)
		} finally {
			client.release()
		}
		return modelos
	}
	
	static async getCalibrados_(
		estacion_id,
		var_id,
		includeCorr=false,
		timestart,
		timeend,
		cal_id,
		model_id,
		qualifier,
		isPublic,
		grupo_id,
		no_metadata,
		group_by_cal,
		forecast_date,
		includeInactive,
		series_id) {
		console.debug({includeCorr:includeCorr, isPublic: isPublic})
		var public_filter = (isPublic) ? "AND calibrados.public=true" : ""
		var activar_filter = (includeInactive) ? "" : "AND calibrados.activar = TRUE"
		var grupo_filter = (grupo_id) ? "AND series_prono_last.cal_grupo_id=" + parseInt(grupo_id) : ""
		var cal_join = (estacion_id || var_id || includeCorr || timestart || timeend || grupo_id || series_id) ? "JOIN" : "LEFT OUTER JOIN"
		var base_query
		const series_filter = internal.utils.control_filter2(
			{
				"estacion_id": {
					type: "integer",
					table: "series"
				},
				"var_id": {
					type: "integer",
					table: "series",
				},
				"cal_id": {
					type: "integer",
					table: "series_prono_last"
				},
				"model_id": {
					type: "integer",
					table: "series_prono_last"
				},
				"series_id": {
					type: "integer",
					table: "series_prono_last"
				}
			},{
				estacion_id: estacion_id,
				var_id: var_id,
				cal_id: cal_id,
				model_id: model_id,
				series_id: series_id
			}
		)
		const calibrados_filter = internal.utils.control_filter2(
			{
				id: {
					type: "integer",
					table: "calibrados"
				},
				model_id: {
					type: "integer",
					table: "calibrados"
				}
			},{
				id: cal_id,
				model_id: model_id
			}
		)
		if(group_by_cal) {
			base_query = "WITH pronos as (\
				select series_prono_last.cal_id,\
				series_prono_last.cor_id,\
				series_prono_last.fecha_emision,\
				json_agg(json_build_object('estacion_id',series.estacion_id,'var_id',series.var_id,'proc_id',series.proc_id,'unit_id',series.unit_id)) series,\
				json_agg(series.estacion_id) out_id\
				from series_prono_last,series\
				WHERE series_prono_last.series_id=series.id\
				" + series_filter + "\
				" + grupo_filter + "\
				GROUP BY series_prono_last.cal_id,series_prono_last.cor_id,series_prono_last.fecha_emision\
			  ),\
			  cal as (\
		        SELECT calibrados.id cal_id, \
				 pronos.series, \
				 pronos.out_id, \
				 calibrados.area_id, \
				 calibrados.in_id, \
				 calibrados.nombre, \
				 calibrados.model_id, \
				 calibrados.modelo, \
				 calibrados.activar, \
				 calibrados.selected, \
				 calibrados.dt, \
				 calibrados.t_offset \
		        FROM calibrados \
				" + cal_join + " pronos\
				ON (calibrados.id=pronos.cal_id) \
				WHERE 1=1 \
				" + calibrados_filter + "\
		        " + activar_filter + "\
				" + public_filter + "\
		    )"
		} else {
			base_query = "WITH pronos as (\
				select series_prono_last.cal_id,\
				series_prono_last.cor_id,\
				series_prono_last.fecha_emision,\
				series.estacion_id,\
				series.var_id,\
				series.proc_id,\
				series.unit_id\
				from series_prono_last,series\
				WHERE series_prono_last.series_id=series.id\
				" + series_filter + "\
				" + grupo_filter + "\
			  ),\
			  cal as (\
				SELECT calibrados.id cal_id, \
				pronos.estacion_id out_id, \
				pronos.var_id, \
				pronos.unit_id, \
				calibrados.area_id, \
				calibrados.in_id, \
				calibrados.nombre, \
				calibrados.modelo, \
				calibrados.model_id, \
				calibrados.activar, \
				calibrados.selected, \
				calibrados.dt, \
				calibrados.t_offset \
				FROM calibrados \
				" + cal_join + " pronos\
				ON (calibrados.id=pronos.cal_id) \
				WHERE 1=1 \
				" + calibrados_filter + "\
		        " + activar_filter + "\
				" + public_filter + "\
			)"
		}
		var query
		if(no_metadata) {
			query = `${base_query} SELECT cal.cal_id id, \
			cal.out_id, \
			cal.area_id, \
			cal.in_id, \
			cal.nombre, \
			cal.modelo, \
			cal.model_id, \
			cal.activar, \
			cal.selected, \
			cal.dt, \
			cal.t_offset \
	 FROM cal ORDER BY cal.cal_id`
		} else {
			query = `${base_query},  pars as ( \
	select cal_pars.cal_id, \
		   json_agg(cal_pars) arr \
	from cal_pars, cal \
	where cal_pars.cal_id=cal.cal_id \
	group by cal_pars.cal_id\
),\
states as ( \
	select cal_estados.cal_id,\
		   json_agg(cal_estados) arr \
	from cal_estados, cal \
	where cal_estados.cal_id=cal.cal_id \
	group by cal_estados.cal_id \
),\
forcings as (\
	select forzantes.cal_id, \
		   json_agg(forzantes) arr \
	from forzantes, cal \
	where forzantes.cal_id=cal.cal_id \
	group by forzantes.cal_id \
)\
SELECT cal.cal_id id, \
       cal.out_id, \
       cal.area_id, \
       cal.in_id, \
       cal.nombre, \
       cal.modelo, \
	   cal.model_id, \
       cal.activar, \
       cal.selected, \
       cal.dt, \
       cal.t_offset, \
       pars.arr parametros, \
       states.arr estados_iniciales, \
       forcings.arr forzantes, \
	   row_to_json(extra_pars.*) as extra_pars \
FROM cal \
LEFT OUTER JOIN extra_pars ON (extra_pars.cal_id=cal.cal_id) \
LEFT OUTER JOIN pars  ON (cal.cal_id=pars.cal_id ) \
LEFT OUTER JOIN states ON (states.cal_id=cal.cal_id) \
LEFT OUTER JOIN forcings ON (forcings.cal_id=cal.cal_id) \
ORDER BY cal.cal_id`
		}
		// console.debug(internal.utils.pasteIntoSQLQuery(query,[estacion_id,var_id,cal_id,model_id]))
		const result = await global.pool.query(query) //,[estacion_id,var_id,cal_id,model_id])
		if(!result.rows) {
			return Promise.reject()
		}
		var calibrados = result.rows
		// console.log({calibrados:calibrados})
		if(cal_id) {
			console.log("Querying for cal_out")
			const cal_filter = internal.utils.control_filter2(
				{
					cal_id: {
						type: "integer",
						table: "cal_out"
					}
				},{
					cal_id: cal_id
				}
			)
			const cal_out = await global.pool.query("SELECT  * from cal_out WHERE 1=1 " + cal_filter + " order by orden")
			if(cal_out.rows) {
				console.log("Got " + cal_out.rows.length + " records from cal_out")
				calibrados.forEach((c,i)=>{
					//~ console.log("cal i:"+i)
					c.outputs = cal_out.rows
				})
			}
		}
		if(includeCorr) {
				//~ var promises = []
			console.log("Querying for corridas")
			for(var c of calibrados) {
				if(forecast_date) {
					const corridas = await this.getPronosticos(
						undefined,
						c.id,
						undefined,
						undefined,
						forecast_date,
						timestart,
						timeend,
						qualifier,
						c.out_id,
						var_id,
						true,
						isPublic,
						undefined,
						false,
						undefined,
						true
					)
					if(corridas.length > 0) {
						calibrados[i].corrida = corridas[0]
					} else {
						calibrados[i].corrida = null
					}
				} else {
					const estacion_id = (Array.isArray(c.out_id)) ? c.out_id.map(s=>(typeof s == "number") ? s : s.estacion_id) : c.out_id
					// console.log("estacion_id: " + estacion_id)
					const corrida = await this.getLastCorrida(estacion_id,var_id,c.id,timestart,timeend,qualifier,isPublic)
					calibrados[i].corrida = corrida
				}
			}
		}
		if(!no_metadata) {
			for(var cal of calibrados) {
				if(!cal.forzantes || !cal.forzantes.length) {
					continue
				}
				// console.log("getting forzantes")
				for(var i in cal.forzantes) {
					const f = new internal.forzante(cal.forzantes[i])
					await f.getSerie()
					cal.forzantes[i] = f
				}
			}
		}
		return calibrados
	}
	
	static async getCalibradosGrupos(cal_grupo_id) {
		var promise
		if(cal_grupo_id) {
			if(parseInt(cal_grupo_id).toString() == "NaN") {
				return Promise.reject("Bad parameter: cal_grupo_id must be an integer")
			}
			promise = global.pool.query("SELECT * FROM calibrados_grupos WHERE id=$1",[cal_grupo_id]) 
		} else {
			promise = global.pool.query("SELECT * FROM calibrados_grupos ORDER BY id")
		}
		return promise.then(results=>{
			return results.rows
		})
	}

	static async getSeriesArealPronoLast(series_id,forecast_date) {
		var filter_string = internal.utils.control_filter2(
			{
				series_id: {type: "integer"},
				date: {type: "date", table: "corridas"}
			},
			{
				series_id: series_id,
				date: forecast_date
			},
			"series_areal_prono_date_range"
		)
		var stmt = `WITH last_cor AS (
			SELECT 
				corridas.cal_id,
				series_areal_prono_date_range.series_id,
				max(corridas.date) AS date
			FROM series_areal_prono_date_range
			JOIN corridas ON corridas.id=series_areal_prono_date_range.cor_id
			WHERE 1=1 ${filter_string}
			GROUP BY 
				corridas.cal_id, 
				series_areal_prono_date_range.series_id
			)
			SELECT 
				last_cor.cal_id,
				last_cor.series_id,
				last_cor.date AS fecha_emision,
				corridas.id AS cor_id,
				calibrados.nombre,
				calibrados.modelo,
				calibrados.model_id,
				calibrados.public,
				calibrados.grupo_id AS cal_grupo_id,
				series_areal_prono_date_range.begin_date AS timestart,
				series_areal_prono_date_range.end_date AS timeend,
				series_areal_prono_date_range.count,
				areas_pluvio.nombre AS estacion_nombre,
				areas_pluvio.unid AS estacion_id,
				var.nombre AS var_nombre,
				var.id AS var_id
			FROM last_cor
			JOIN corridas ON last_cor.date = corridas.date 
			JOIN calibrados ON last_cor.cal_id=calibrados.id
			JOIN series_areal_prono_date_range ON (
				corridas.id = series_areal_prono_date_range.cor_id 
				AND series_areal_prono_date_range.series_id = last_cor.series_id
			)
			JOIN series_areal ON series_areal.id = last_cor.series_id
			JOIN areas_pluvio ON areas_pluvio.unid = series_areal.area_id
			JOIN var ON var.id = series_areal.var_id
			ORDER BY last_cor.series_id, last_cor.date`
		// params = [series_id]
		const result = await global.pool.query(stmt) // ,params)
		return result.rows
	}

	static async getSeriesPronoLast(filter={}) {
		// console.log({filter:filter})
		var filter_string = internal.utils.control_filter2(
			{
				estacion_id: {type: "integer", table: "series"},
				var_id: {type: "integer", table: "series"},
				cal_id: {type: "integer", table: "series_prono_last"},
				model_id: {type: "integer", table: "series_prono_last"},
				series_id: {type: "integer", column: "id", table: "series"},
				grupo_id: {type: "integer", column: "cal_grupo_id", table: "series_prono_last"}
			},
			{
				estacion_id: filter.estacion_id,
				var_id: filter.var_id,
				cal_id: filter.cal_id,
				model_id: filter.model_id,
				series_id: filter.series_id,
				grupo_id: filter.grupo_id
			},
			"series"
		)
		var query = "\
		SELECT series_prono_last.cal_id,\
			series_prono_last.cor_id,\
			series_prono_last.fecha_emision,\
			json_agg(json_build_object('id',series.id,'estacion_id',series.estacion_id,'var_id',series.var_id,'proc_id',series.proc_id,'unit_id',series.unit_id)) series\
			from series_prono_last,series\
		WHERE series_prono_last.series_id=series.id\
		" + filter_string + "\
		GROUP BY series_prono_last.cal_id,series_prono_last.cor_id,series_prono_last.fecha_emision"
		return global.pool.query(query) // ,[filter.estacion_id,filter.var_id,filter.cal_id,filter.model_id,filter.series_id])
		.then(result=>{
			return result.rows
		})
	}

	static async getCalibrado(id) {
		if(!id) {
			throw("missing id")
		}
		const engine = new internal.engine(global.pool)
		var filter = {id:id}
		try {
			var calibrados = await engine.read("Calibrado",filter)
		} catch(e) {
			throw(e)
		}
		if(!calibrados.length) {
			throw("Calibrado not found")
		}
		return calibrados[0]
	}

	static async getCalibrados(
		estacion_id,
		var_id,
		includeCorr=false,
		timestart,
		timeend,
		cal_id,
		model_id,
		qualifier,
		isPublic,
		grupo_id,
		no_metadata,
		group_by_cal=true,
		forecast_date,
		includeInactive,
		series_id,
		nombre,
		tipo="puntual"
	) {
		const engine = new internal.engine(global.pool)
		var filter = {id:cal_id,model_id:model_id,grupo_id:grupo_id,nombre:nombre}
		if(!includeInactive) {
			filter.activar = true
		}
		var calibrados
		var series_prono_last
		if(series_id && tipo == "areal") {
			series_prono_last = await this.getSeriesArealPronoLast(series_id,forecast_date)
			console.debug({series_prono_last:series_prono_last})
			const cal_ids = new Set(series_prono_last.map(result=>result.cal_id))
			filter.id = Array.from(cal_ids)
			if(!filter.id.length) {
				console.error("No series_prono found")
				return []
			}
			calibrados = await this.getCalibrados_(filter.estacion_id,filter.var_id,false,filter.timestart,filter.timeend,filter.id,filter.model_id,filter.qualifier,filter.public,filter.grupo_id,no_metadata,group_by_cal,filter.forecast_date,includeInactive) // await engine.read("Calibrado",filter)
		} else if(estacion_id || var_id || includeCorr || qualifier || forecast_date || series_id) {
			series_prono_last = await this.getSeriesPronoLast({cal_id:cal_id,model_id:model_id,grupo_id:filter.grupo_id,estacion_id:estacion_id,var_id:var_id,forecast_date:forecast_date,series_id:series_id})
			// console.log(JSON.stringify({series_prono_last:series_prono_last},null,2))
			const cal_ids = new Set(series_prono_last.map(result=>result.cal_id))
			filter.id = Array.from(cal_ids)
			if(!filter.id.length) {
				console.error("No series_prono found")
				return []
			}
			// console.log({cal_id:filter.id})
			calibrados = await this.getCalibrados_(estacion_id,var_id,false,filter.timestart,filter.timeend,filter.id,filter.model_id,filter.qualifier,filter.public,filter.grupo_id,no_metadata,group_by_cal,filter.forecast_date,includeInactive,series_id) // engine.read("Calibrado",filter)
		} else {
			calibrados = await this.getCalibrados_(estacion_id,var_id,false,filter.timestart,filter.timeend,filter.id,filter.model_id,filter.qualifier,filter.public,filter.grupo_id,no_metadata,group_by_cal,filter.forecast_date,includeInactive) // engine.read("Calibrado",filter)
		}
		if(includeCorr) {
			var series_id
			if(series_prono_last && series_prono_last.length) {
				const series_ids = Array.from(new Set(flatten(series_prono_last.map(p=> {
					// console.log(p)
					if(p.series) {
						return p.series.map(s=>s.id)
					} else {
						console.log("series missing from series_prono_last, returning series_id")
						return p.series_id
					}
				}))))
				if(series_ids.length) {
					series_id = series_ids
				}
			}
			if(forecast_date) {
				for(var i in calibrados) {
					const corridas = await this.getPronosticos(
						undefined,
						calibrados[i].id,
						undefined,
						undefined,
						forecast_date,
						timestart,
						timeend,
						qualifier,
						calibrados[i].out_id,
						var_id,
						true,
						isPublic,
						series_id,
						false,
						undefined,
						true, 
						undefined, 
						tipo
					)
					if(corridas.length > 0) {
						calibrados[i].corrida = corridas[0]
					} else {
						calibrados[i].corrida = null
					}
				}
			} else {
				for(var i in calibrados) {
					// console.log(`this.getLastCorrida(${calibrados[i].out_id},${var_id},${calibrados[i].id},${timestart},${timeend},${qualifier},true,${isPublic},${series_id},undefined,true,undefined,undefined)`)
					const estacion_id_filter = (Array.isArray(calibrados[i].out_id)) ? calibrados[i].out_id.map(s=>(typeof s == "number") ? s : s.estacion_id) : calibrados[i].out_id
					// console.log("estacion_id_filter: " + estacion_id_filter)
					const corrida = await this.getLastCorrida(estacion_id_filter,var_id,calibrados[i].id,timestart,timeend,qualifier,true,isPublic,series_id,undefined,true,tipo,undefined)
					calibrados[i].corrida = corrida
					// console.log(JSON.stringify(calibrados[i].corrida,null,2))
				}
			}
		}
		return calibrados
	}

	static async getLastCorrida(estacion_id,var_id,cal_id,timestart,timeend,qualifier,includeProno=false,isPublic,series_id,series_metadata,group_by_qualifier,tipo,tabla) {
		var corrida = {}
		var public_filter = (isPublic) ? "AND calibrados.public=true" : ""
		//~ console.log([estacion_id,var_id,cal_id,timestart,timeend])
		var query = "with last as (\
			select max(date) \
			from corridas,calibrados \
			where cal_id=$1 \
			AND corridas.cal_id=calibrados.id \
			" + public_filter + ") \
		select corridas.* \
		from corridas,last \
		where cal_id=$1 \
		and date=last.max;"
		// console.log(internal.utils.pasteIntoSQLQuery(query,[cal_id]))
		const result = await global.pool.query(query,[cal_id])
		if (!result.rows.length) {
			console.log("No rows found for cal_id:"+cal_id)
			return []
		} 
		// console.log({cal_id:cal_id,rows:result.rows})
		corrida = {cor_id:result.rows[0].id,cal_id:result.rows[0].cal_id,forecast_date:result.rows[0].date}
		const corridas = await internal.CRUD.getPronosticos(corrida.cor_id,corrida.cal_id,undefined,undefined,corrida.forecast_date,timestart,timeend,qualifier,estacion_id,var_id,includeProno,isPublic,series_id,series_metadata,undefined,group_by_qualifier,undefined,tipo,tabla)
		return corridas[0]
		//~ console.log(corrida)
		// const corridas = await this.getPronosticos()

		// if(!includeProno) {
		// 	return corrida
		// }
		// var filter_string = internal.utils.control_filter2({timestart:{type:"timestart","table":"pronosticos"},timeend:{type:"timeend","table":"pronosticos"},estacion_id:{type:"integer","table":"series"},var_id:{type:"integer","table":"series"},qualifier:{type:"string","table":"pronosticos"},series_id:{type:"integer","table":"pronosticos"}},{timestart:timestart,timeend:timeend,estacion_id:estacion_id,var_id:var_id,qualifier:qualifier,series_id:series_id})
		// // var date_filter = ""
		// // date_filter += (timestart) ? " AND pronosticos.timestart>='" + new Date(timestart).toISOString() + "' " : "" 
		// // date_filter += (timeend) ?  " AND pronosticos.timeend<='" +  new Date(timeend).toISOString() + "' " : ""
		// var query = internal.utils.pasteIntoSQLQuery("select pronosticos.series_id,\
		// pronosticos.qualifier,\
		// json_agg(ARRAY [to_char(pronosticos.timestart::timestamptz at time zone 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), to_char(pronosticos.timeend::timestamptz at time zone 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'),valores_prono_num.valor::text,pronosticos.qualifier::text]) pronosticos\
		// from series,pronosticos,valores_prono_num  \
		// WHERE valores_prono_num.prono_id=pronosticos.id \
		// and series.id=pronosticos.series_id\
		// AND pronosticos.cor_id=$1\
		// " + filter_string + "\
		// group by pronosticos.series_id,pronosticos.qualifier;",[corrida.cor_id])
		// // console.log(query)
		// return global.pool.query(query) 										
		// 						// "\
		// 						// where cor_id=$1 and valores_prono_num.prono_id=pronosticos.id and series.id=pronosticos.series_id\
		// 						// AND series.estacion_id=coalesce($2,series.estacion_id)\
		// 						// AND series.var_id=coalesce($3,series.var_id)\
		// 						// AND pronosticos.qualifier=coalesce($4,pronosticos.qualifier)\
		// 						// AND series.id=coalesce($5,series.id)\
		// 						// " + date_filter + "\
		// 						// "group by pronosticos.series_id,pronosticos.qualifier;") ,[corrida.cor_id,estacion_id,var_id,qualifier,series_id])
		// .then(result=>{
		// 	if(!result.rows) {
		// 		console.log("No rows found for cor_id:"+corrida.cor_id)
		// 		corrida.series = []
		// 		return corrida 
		// 	} else if (result.rows.length == 0) {
		// 		console.log("No rows found for cor_id:"+corrida.cor_id)
		// 		corrida.series = []
		// 		return corrida
		// 	}
		// 	corrida.series = result.rows
		// 	if(series_metadata) {
		// 		var promises = []
		// 		corrida.series.forEach(serie=>{
		// 			promises.push(this.getSerie("puntual",serie.series_id)
		// 			.then(result=>{
		// 				serie.metadata = result
		// 				return
		// 			}))
		// 		})
		// 		return Promise.all(promises)
		// 		.then(()=>{
		// 			return corrida
		// 		})
		// 	} else {
		// 	//~ .map(s=>{
		// 		//~ return {series_id: s.series_id,
		// 				//~ qualifier: s.qualifier,
		// 				//~ pronosticos = s.json_agg //.map(r=>[r[0]
		// 		//~ }
		// 	//~ })
		// 		return corrida
		// 	}
		// })
		
	}
	
	static async deleteCalibrado(cal_id) {
		return global.pool.query("DELETE FROM calibrados WHERE id=$1 RETURNING *",[cal_id])
		.then(result=>{
			if(result.rows) {
				return result.rows[0]
			} else {
				return
			}
		})
	}

	static async deleteCalibrados(filter) {
		if(filter.cal_id) {
			filter.id = filter.cal_id
		}
		var valid_filters = {"id":{table:"calibrados",type:"integer"},"model_id":{table:"calibrados",type:"integer"}}
		var filter_string = internal.utils.control_filter2(valid_filters,filter,"calibrados") 
		if(filter_string == "") {
			throw("deleteCalibrados error: at least one filter is required")
		}
		var stmt = `DELETE FROM calibrados WHERE id=id ${filter_string} RETURNING *`
		// console.log(stmt)
		return global.pool.query(stmt)
	}
	
	static async upsertCalibrados(calibrados) {
		var upserted = []
		for(var i in calibrados) {
			calibrado = await this.upsertCalibrado(calibrados[i])
			upserted.push(calibrado) 
		}
		return upserted
	}

	static async upsertCalibrado(input_cal) {
		// console.log("is_calibrado:" + input_cal instanceof internal.calibrado)
		// if(! input_cal instanceof internal.calibrado) {
		// 	input_cal = new internal.calibrado(input_cal)
		// }
		// console.log("is_calibrado:" + input_cal instanceof internal.calibrado)

		input_cal.setArrayProperties()
		if(!input_cal.model_id) {
			if(!input_cal.modelo) {
				return Promise.reject("Missing model_id")
			} else {
				const modelo = await internal.modelo.read({name:input_cal.modelo})
				if(!modelo.length) {
					return Promise.reject("Modelo not found with name: " + input_cal.modelo)
				} else {
					input_cal.model_id = modelo[0].id
				}
			}
		}
		var calibrado
		return global.pool.connect()
		.then(async client=>{
			try {
				await client.query("BEGIN")
				var upserted
				if(input_cal.id) {
					var stmt = internal.utils.pasteIntoSQLQuery("INSERT INTO calibrados (id,nombre, modelo, parametros, estados_iniciales, activar, selected, out_id, area_id, in_id, model_id, tramo_id, dt, t_offset) VALUES \
					($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,coalesce($13,'1 days'::interval),coalesce($14,'9 hours'::interval))\
					ON CONFLICT (id)\
					DO UPDATE SET nombre=coalesce(excluded.nombre,calibrados.nombre), modelo=coalesce(excluded.modelo,calibrados.modelo), parametros=coalesce(excluded.parametros,calibrados.parametros), estados_iniciales=coalesce(excluded.estados_iniciales,calibrados.estados_iniciales), activar=coalesce(excluded.activar,calibrados.activar), selected=coalesce(excluded.selected,calibrados.selected), out_id=coalesce(excluded.out_id,calibrados.out_id), area_id=coalesce(excluded.area_id,calibrados.area_id), in_id=coalesce(excluded.in_id,calibrados.in_id), model_id=coalesce(excluded.model_id,calibrados.model_id), tramo_id=coalesce(excluded.tramo_id,calibrados.tramo_id), dt=coalesce(excluded.dt,calibrados.dt), t_offset=coalesce(excluded.t_offset,calibrados.t_offset)\
					RETURNING *",[input_cal.id, input_cal.nombre, input_cal.modelo, input_cal.parametros, input_cal.estados, input_cal.activar, input_cal.selected, (typeof input_cal.out_id == "integer") ? input_cal.out_id : null, input_cal.area_id, input_cal.in_id, input_cal.model_id, input_cal.tramo_id, timeSteps.interval2string(input_cal.dt), timeSteps.interval2string(input_cal.t_offset)])
					// console.log(stmt)
					upserted = await client.query(stmt)
				} else {
					var stmt = internal.utils.pasteIntoSQLQuery("INSERT INTO calibrados (id,nombre, modelo, parametros, estados_iniciales, activar, selected, out_id, area_id, in_id, model_id, tramo_id, dt, t_offset) VALUES \
					(nextval('calibrados_id_seq'),$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,coalesce($12,'1 days'::interval),coalesce($13,'9 hours'::interval))\
					RETURNING *",[input_cal.nombre, input_cal.modelo, input_cal.parametros, input_cal.estados, input_cal.activar, input_cal.selected, (typeof input_cal.out_id == "integer") ? input_cal.out_id : null, input_cal.area_id, input_cal.in_id, input_cal.model_id, input_cal.tramo_id, timeSteps.interval2string(input_cal.dt), timeSteps.interval2string(input_cal.t_offset)])
					// console.log(stmt)
					upserted = await client.query(stmt)
				}
			} catch(e) {
				await client.query("ROLLBACK")
				client.release()
				throw(e)
			}
			calibrado = upserted.rows[0]
			if(!calibrado) {
				await client.query("ROLLBACK")
				client.release()
				throw("No se insertó calibrado")
			}
			if(input_cal.parametros) {
				console.log({parametros:input_cal.parametros})
				try {
					var parametros = await this.upsertParametros(client,calibrado.id,input_cal.parametros)
				} catch(e) {
					await client.query("ROLLBACK")
					client.release()
					throw(e)
				}
				// var parametros = []
				// for(var i=0;i<input_cal.parametros.length;i++) {
				// 	try {
				// 		var parametro = await this.upsertParametro(client,{cal_id:calibrado.id, orden:i+1, valor:input_cal.parametros[i]})
				// 		if(!parametro) {
				// 			throw("no se insertó parámetro")
				// 		}
				// 		parametros.push(parametro)
				// 	} catch(e) {
				// 		await client.query("ROLLBACK")
				// 		client.end()
				// 		throw(e)
				// 	}
				// }
				calibrado.parametros = parametros
			} 
			if(input_cal.estados) {
				try {
					var estados = await this.upsertEstadosIniciales(client,calibrado.id,input_cal.estados)
				} catch(e) {
					await client.query("ROLLBACK")
					client.release()
					throw(e)
				}
				// var estados_iniciales = []
				// for(var i=0;i>input_cal.estados_iniciales.length;i++) {
				// 	try {
				// 		var estado_inicial =  await this.upsertEstadoInicial(client,{cal_id:calibrado.id, orden:i+1, valor:input_cal.estados_iniciales[i]})
				// 		if(!estado_inicial) {
				// 			throw("no se insertó estado_inicial")
				// 		}
				// 		estados_iniciales.push(estado_inicial)
				// 	} catch(e) {
				// 		await client.query("ROLLBACK")
				// 		client.end()
				// 		throw(e)
				// 	}
				// }
				calibrado.estados_iniciales = estados
			} 
			if(input_cal.forzantes) {
				console.log(input_cal.forzantes)
				try {
					var forzantes = await this.upsertForzantes2(client,calibrado.id,input_cal.forzantes)
				} catch(e) {
					await client.query("ROLLBACK")
					client.release()
					throw(e)
				}
				// var forzantes = []
				// for(var i=0;i>input_cal.forzantes.length;i++) {
				// 	try {
				// 		var forzante =  await this.upsertForzante(client,{cal_id:calibrado.id, orden:(input_cal.forzantes[i].orden)?input_cal.forzantes[i].orden:i+1, series_table:input_cal.forzantes[i].series_table, series_id:input_cal.forzantes[i].series_id})
				// 		if(!forzantes) {
				// 			throw("no se insertó forzante")
				// 		}
				// 		forzantes.push(forzante)
				// 	} catch(e) {
				// 		await client.query("ROLLBACK")
				// 		client.end()
				// 		throw(e)
				// 	}
				// }
				calibrado.forzantes = forzantes
			}
			if(input_cal.outputs) {
				var outputs = []
				for(var i =0;i<input_cal.outputs.length;i++) {
					try {
						var output = await this.upsertOutput(client,{cal_id:calibrado.id, orden:(input_cal.outputs[i].orden)?input_cal.outputs[i].orden:i+1, series_table:input_cal.outputs[i].series_table, series_id:input_cal.outputs[i].series_id})
						if(!output) {
							throw("no se insertó output")
						}
						outputs.push(output)
					} catch(e) {
						endAndThrow(e)
					}
				}
				calibrado.outputs = outputs
			}
			if(input_cal.stats) {
				try {
					var stats = await this.upsertCalStats(client,calibrado.id,input_cal.stats)
				} catch(e) {
					await client.query("ROLLBACK")
					client.release()
					throw(e)
				}
				calibrado.stats = stats
			} 
			try {
				await client.query("COMMIT")
			} catch(e) {
				await client.query("ROLLBACK")
				client.release()
				throw(e)
			}
			client.release()
			return calibrado
		})
	}

	static async upsertCalStats(client,cal_id,stats) {
		var stmt = internal.utils.pasteIntoSQLQuery("INSERT INTO cal_stats (cal_id,timestart,timeend,n_cal,rnash_cal,rnash_val,beta,omega,repetir,iter,rmse,stats_json,pvalues,calib_period) VALUES \
		($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)\
		ON CONFLICT (cal_id) DO UPDATE SET timestart=excluded.timestart,timeend=excluded.timeend, n_cal=excluded.n_cal, rnash_cal=excluded.rnash_cal,rnash_val=excluded.rnash_val, beta=excluded.beta, omega=excluded.omega, repetir=excluded.repetir, iter=excluded.iter, rmse=excluded.rmse, stats_json=excluded.stats_json, pvalues=excluded.pvalues, calib_period=excluded.calib_period RETURNING *",[cal_id,stats.timestart,stats.timeend,stats.n_cal,stats.rnash_cal,stats.rnash_val,stats.beta,stats.omega,stats.repetir,stats.iter,stats.rmse,JSON.stringify(stats),stats.pvalues,stats.calib_period])
		console.log(stmt)
		return Promise.resolve()
		// return client.query(stmt)
		// .then(result=>{
		// 	console.log(result)
		// 	return result.rows[0]
		// })
	}
	
	static async upsertParametro(client,parametro) {
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		var stmt = internal.utils.pasteIntoSQLQuery("INSERT INTO cal_pars (cal_id,orden,valor) VALUES\
		($1,$2,$3)\
		ON CONFLICT (cal_id,orden)\
		DO UPDATE SET valor=excluded.valor\
		RETURNING *",[parametro.cal_id,parametro.orden,parametro.valor])
		// console.log(stmt)
		return client.query(stmt)
		.then(result=>{
			// console.log(result)
			if(release_client) {
				client.release()
			}
			return result.rows[0]
		})
	}

	static async upsertParametros(client,cal_id,parametros) { // parametros::int[]
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		var tuples = parametros.map((p,i)=>{
			return `(${[cal_id, i + 1, p].join(",")})`
		})
		var stmt = `INSERT INTO cal_pars (cal_id,orden,valor) VALUES\
		${tuples.join(",")}\
		ON CONFLICT (cal_id,orden)\
		DO UPDATE SET valor=excluded.valor\
		RETURNING *`
		// console.log(stmt)
		return client.query(stmt)
		.then(result=>{
			// console.log(result)
			if(release_client) {
				client.release()
			}
			return result.rows[0]
		})
	}


	static async upsertParametroDeModelo(client,parametro) {
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		return client.query("INSERT INTO parametros (model_id , nombre , lim_inf , range_min , range_max , lim_sup , orden ) VALUES\
		  ($1,$2,$3,$4,$5,$6,$7)\
		  ON CONFLICT (model_id,orden)\
		  DO UPDATE SET nombre=excluded.nombre,\
		  				lim_inf=excluded.lim_inf,\
						range_min=excluded.range_min,\
						range_max=excluded.range_max,\
							lim_sup=excluded.lim_sup\
		  RETURNING *",[parametro.model_id,parametro.nombre,parametro.lim_inf,parametro.range_min,parametro.range_max,parametro.lim_sup,parametro.orden])
		.then(result=>{
			if(release_client) {
				client.release()
			}
			return new internal.parametroDeModelo(result.rows[0])
		})
	}
	
	static async upsertEstadoInicial(client,estado_inicial) {
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		return client.query("INSERT INTO cal_estados (cal_id,orden,valor) VALUES\
		  ($1,$2,$3)\
		  ON CONFLICT (cal_id,orden)\
		  DO UPDATE SET valor=excluded.valor\
		  RETURNING *",[estado_inicial.cal_id,estado_inicial.orden,estado_inicial.valor])
		.then(result=>{
			if(release_client) {
				client.release()
			}
			return result.rows[0]
		})
	}

	static async upsertEstadosIniciales(client,cal_id,estados_iniciales) { //  estados_iniciales::int[]
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		var tuples = estados_iniciales.map((p,i)=>{
			return `(${[cal_id, i + 1, p].join(",")})`
		})
		var stmt = `INSERT INTO cal_estados (cal_id,orden,valor) VALUES\
		${tuples.join(",")}\
		ON CONFLICT (cal_id,orden)\
		DO UPDATE SET valor=excluded.valor\
		RETURNING *`
		return client.query(stmt)
		.then(result=>{
			if(release_client) {
				client.release()
			}
			return result.rows
		})
	}

	static async upsertEstadoDeModelo(client,estado) {
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		return client.query("INSERT INTO estados (model_id, nombre, range_min, range_max, def_val, orden) VALUES\
		  ($1,$2,$3,$4,$5,$6)\
		  ON CONFLICT (model_id,orden)\
		  DO UPDATE SET nombre=excluded.nombre,\
		                range_min=excluded.range_min,\
						range_max=excluded.range_max,\
						def_val=excluded.def_val\
		  RETURNING *",[estado.model_id,estado.nombre,estado.range_min,estado.range_max,estado.def_val,estado.orden])
		.then(result=>{
			if(release_client) {
				client.release()
			}
			return new internal.estadoDeModelo(result.rows[0])
		})
	}

	static async upsertOutputDeModelo(client,output) {
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		return client.query("INSERT INTO modelos_out (model_id, orden, var_id, unit_id , nombre, inst, series_table) VALUES\
		  ($1,$2,$3,$4,$5,$6,$7)\
		  ON CONFLICT (model_id,orden)\
		  DO UPDATE SET var_id=excluded.var_id,\
		                unit_id=excluded.unit_id,\
						nombre=excluded.nombre,\
						inst=excluded.inst,\
						series_table=excluded.series_table\
		  RETURNING *",[output.model_id,output.orden,output.var_id,output.unit_id,output.nombre,output.inst,output.series_table])
		.then(result=>{
			if(release_client) {
				client.release()
			}
			return new internal.modelo_output(result.rows[0])
		})
	}

	static async getForzante(cal_id,orden) {
		return global.pool.query("SELECT id, cal_id, series_table, series_id, cal, orden, model_id FROM forzantes WHERE cal_id=$1 AND orden=$2",[cal_id,orden])
		.then(result=>{
			if(!result) {
				return Promise.reject("getForzante: Nothing found")
			}
			return new internal.forzante(result.rows[0])
		})
	}
	
	static async getForzantes(cal_id,filter={}) {
		return global.pool.query("SELECT id, cal_id, series_table, series_id, cal, orden, model_id FROM forzantes WHERE cal_id=$1 AND orden=coalesce($2,orden) AND series_table=coalesce($3,series_table) and series_id=coalesce($4,series_id) AND cal=coalesce($5,cal) ORDER BY orden",[cal_id,filter.orden,filter.series_table,filter.series_id,filter.cal])
		.then(result=>{
			return result.rows.map(f=> new internal.forzante(f))
		})
	} 
		
	static async upsertForzante(client,forzante) {
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		return client.query("INSERT INTO forzantes (cal_id,orden,series_table,series_id) VALUES\
		  ($1,$2,$3,$4)\
		  ON CONFLICT (cal_id,orden)\
		  DO UPDATE SET series_table=excluded.series_table, series_id=excluded.series_id\
		  RETURNING *",[forzante.cal_id,forzante.orden,forzante.series_table,forzante.series_id])
		.then(result=>{
			if(release_client) {
				client.release()
			}
			return new internal.forzante(result.rows[0])
		})
	}

	static async upsertForzantes2(client,cal_id,forzantes) { // forzantes = [{series_table:"",series_id:0},...] o [0,1,2] (en el segundo caso asume series_table:"series")
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		var tuples = forzantes.map((p,i)=>{
			var series_table = (p.series_table) ? (p.series_table.toLowerCase() == "series_rast") ? "series_rast" : (p.series_table.toLowerCase() == "series_areal") ? "series_areal" : "series" : "series"
			var series_id = (p.series_id) ? p.series_id : parseInt(p)
			var cal = (p.cal === true)
			return sprintf ("(%d,%d,'%s',%d,%s)", cal_id,i+1,series_table,series_id,cal.toString())
		})
		var stmt = `INSERT INTO forzantes (cal_id,orden,series_table,series_id,cal) VALUES\
		${tuples.join(",")}\
		ON CONFLICT (cal_id,orden)\
		DO UPDATE SET series_table=excluded.series_table, series_id=excluded.series_id\
		RETURNING *`
		return client.query(stmt)
		.then(result=>{
			if(release_client) {
				client.release()
			}
			return result.rows.map(f=>new internal.forzante(f))
		})
	}

	static async upsertForzanteDeModelo(client,forzante) {
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		return client.query("INSERT INTO modelos_forzantes (model_id , orden , var_id , unit_id , nombre , inst , tipo , required) VALUES\
		  ($1,$2,$3,$4,$5,$6,$7,$8)\
		  ON CONFLICT (model_id,nombre)\
		  DO UPDATE SET var_id=excluded.var_id,\
		                unit_id=excluded.unit_id,\
						orden=excluded.orden,\
						inst=excluded.inst,\
						tipo=excluded.tipo,\
						required=excluded.required\
		  RETURNING *",[forzante.model_id,forzante.orden,forzante.var_id,forzante.unit_id,forzante.nombre,forzante.inst,forzante.tipo,forzante.required])
		.then(result=>{
			if(release_client) {
				client.release()
			}
			// if(!result || !result.rows || result.rows.length==0) {
			// 	throw("error trying to create forzanteDeModelo")
			// }
			// console.log(result.rows[0])
			return new internal.forzanteDeModelo(result.rows[0])
		})
	}

	static async upsertForzantes(cal_id,forzantes) {
		if(forzantes.length==0) {
			return Promise.reject("upsertForzantes: missing forzantes") 
		}
		var values = forzantes.map(f=>{
			var series_table = (f.series_table) ? (f.series_tabla == "series_areal") ? "series_areal" : "series" : "series"
			return sprintf("(%d,%d,'%s',%d)", cal_id, f.orden, series_table, f.series_id)
		}).join(",")
		return global.pool.query(`INSERT INTO forzantes (cal_id, orden, series_table, series_id) VALUES\
		  ${values}\
		  ON CONFLICT (cal_id, orden)\
		  DO UPDATE SET series_table=excluded.series_table, series_id=excluded.series_id\
		  RETURNING *`)
		.then(result=>{
			return new internal.forzante(result.rows[0])
		})
	}
	
	static async deleteForzantes(cal_id,filter) {
		return global.pool.query("DELETE \
		FROM forzantes \
		WHERE cal_id=$1 \
		AND orden=coalesce($2,orden) \
		AND series_table=coalesce($3,series_table) \
		AND series_id=coalesce($4,series_id) \
		AND cal=coalesce($5,cal) \
		RETURNING *",[cal_id,filter.orden,filter.series_table,filter.series_id,filter.cal])
		.then(result=>{
			return result.rows.map(f=>new internal.forzante(f))
		})
	} 

	static async deleteForzante(cal_id,orden) {
		return global.pool.query("DELETE \
		FROM forzantes \
		WHERE cal_id=$1 \
		AND orden=$2 \
		RETURNING *",[cal_id,orden])
		.then(result=>{
			return new internal.forzante(result.rows[0])
		})
	} 

	static async upsertOutput(client,output) {
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		//~ console.log({output:output})
		return client.query("INSERT INTO cal_out (cal_id,orden,series_table,series_id) VALUES\
		  ($1,$2,$3,$4)\
		  ON CONFLICT (cal_id,orden)\
		  DO UPDATE SET series_table=excluded.series_table, series_id = excluded.series_id\
		  RETURNING *",[output.cal_id,output.orden,output.series_table,output.series_id])
		.then(result=>{
			var series_table = (output.series_table) ? (output.series_table == "series_areal") ? "series_areal" : "series" : "series" 
			if(!result.rows[0]) {
				if(release_client) {
					client.release()
				}
				throw new Error("error on output upsert")
			}
			return client.query("INSERT INTO calibrados_out (cal_id,out_id)\
				SELECT $1,estacion_id\
				FROM " + series_table + "\
				WHERE id=$2\
				ON CONFLICT (cal_id,out_id) DO NOTHING",[output.cal_id,output.series_id])
			.then(result2=>{
				if(release_client) {
					client.release()
				}
				return new internal.output(result.rows[0])
			})
		})
	}

	static async upsertOutputs(client,cal_id,outputs) { // outputs = = [{series_table:"",series_id:0},...] o [0,1,2] (en el segundo caso asume series_table:"series")
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		var tuples = outputs.map((p,i)=>{
			var series_table = (p.series_table) ? (p.series_table.toLowerCase() == "series_rast") ? "series_rast" : (p.series_table.toLowerCase() == "series_areal") ? "series_areal" : "series" : "series"
			var series_id = (p.series_id) ? p.series_id : parseInt(p)
			return sprintf ("(%d,%d,'%s',%d)", cal_id,i+1,series_table,series_id)
		})
		//~ console.log({output:output})
		var stmt = `INSERT INTO cal_out (cal_id,orden,series_table,series_id) VALUES\
		${tuples.join(",")}\
		ON CONFLICT (cal_id,orden)\
		DO UPDATE SET series_table=excluded.series_table, series_id = excluded.series_id\
		RETURNING *`
		return client.query(stmt)
		.then(result=>{
			if(!result.rows.length) {
				if(release_client) {
					client.release()
				}
				throw new Error("error on output upsert: nothing upserted")
			}
			var calibrados_out = result.rows.map(output=>{
				var series_table = (output.series_table) ? (output.series_table == "series_rast") ? "series_rast" :(output.series_table == "series_areal") ? "series_areal" : "series" : "series" 
				return { cal_id:cal_id, series_table:series_table, series_id: output.series_id}
			})
			return this.upsertCalibradosOut(client,cal_id,calibrados_out)
			.then(result2=>{
				if(release_client) {
					client.release()
				}
				return result.rows.map(r=>new internal.output(r))
			})
	})
	}

	static async upsertCalibradosOut(client,cal_id,calibrados_out) {
		if(!calibrados_out.length) {
			throw("upsertCalibradosOut error: calibrados_out is missing or of length 0")
		}
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		var tuples = []
		for(var o of calibrados.out){
			var tipo = (o.series_table) ? (o.series_table == "series_rast") ? "raster" :(output.series_table == "series_areal") ? "areal" : "puntual" : "puntual"
			try {
				var serie = await this.getSerie(tipo,o.series_id)
			} catch(e) {
				throw(e)
			}
			tuples.push(`(${cal_id},${serie.estacion.id})`)
		}
		var stmt = `INSERT INTO calibrados_out (cal_id,out_id)\
		${tuples.join(",")}\
		ON CONFLICT (cal_id,out_id) DO NOTHING`
		return client.query(stmt)
		.then(result=>{
			if(release_client) {
				client.release()
			}
			return result
		})
	}

	static async getPronosticos(cor_id,cal_id,forecast_timestart,forecast_timeend,forecast_date,timestart,timeend,qualifier,estacion_id,var_id,includeProno=false,isPublic,series_id,series_metadata,cal_grupo_id,group_by_qualifier,model_id,tipo,tabla) {
		const filter_string = control_filter2(
			{
				"cal_id": {"type": "integer", "table": "corridas"},
				"cor_id": { type: "integer", table: "corridas", column: "id"},
				"model_id": { type: "integer", table: "calibrados"},
				"isPublic": { type: "boolean", table: "calibrados", column: "public"},
				"cal_grupo_id": { type: "integer", table: "calibrados", column: "grupo_id"},
				"forecast_timestart": { type: "timestart", table: "corridas", column: "date"},
				"forecast_timeend": { type: "timeend", table: "corridas", column: "date"},
				"forecast_date": { type: "date", table: "corridas", column: "date", trunc: "milliseconds"}
			},
			{
				cal_id: cal_id,
				cor_id: cor_id,
				model_id: model_id,
				isPublic: isPublic,
				cal_grupo_id: cal_grupo_id,
				forecast_timestart: forecast_timestart,
				forecast_timeend: forecast_timeend,
				forecast_date: forecast_date
			},
			"corridas",
			undefined,
			true
		)

		var query = "SELECT \
			corridas.id, \
			corridas.date AS forecast_date, \
			corridas.cal_id \
		FROM corridas \
		JOIN calibrados \
			ON corridas.cal_id=calibrados.id \
		WHERE 1=1 \
		" + filter_string + "\
		ORDER BY corridas.cal_id, corridas.date"
		// console.debug(query)
		const result = await global.pool.query(query)
		if(!result.rows) {
			return
		}
		const corridas = result.rows.map(r=>{
			r.series = []
			return new internal.corrida(r) // {cor_id:r.id,cal_id:r.cal_id,forecast_date:r.forecast_date}
		})
		if(!includeProno) {
			const promises = []
			const filter = {
				qualifier: qualifier,
				estacion_id: estacion_id,
				var_id: var_id,
				series_id: series_id,
				tipo: tipo,
				tabla: tabla
			} 
			for(var corrida of corridas) {
				promises.push(internal.CRUD.getCorridaSeriesDateRange(corrida,filter,{group_by_qualifier:group_by_qualifier}))
			}
			await Promise.all(promises)
			return corridas
		}
		const promise_array = []
		for(var corrida of corridas) {
			const filter = {
				cor_id: cor_id,
				cal_id: cal_id,
				forecast_timestart: forecast_timestart,
				forecast_timeend: forecast_timeend,
				forecast_date: forecast_date,
				timestart: timestart,
				timeend: timeend,
				qualifier: qualifier,
				estacion_id: estacion_id,
				var_id: var_id,
				public: isPublic,
				series_id: series_id,
				cal_grupo_id: cal_grupo_id,
				model_id: model_id,
				tipo: tipo,
				tabla: tabla
			}
			promise_array.push(corrida.setSeries(undefined,filter,{series_metadata:series_metadata,group_by_qualifier:group_by_qualifier}))
		}
		await Promise.all(promise_array)
		return corridas
	}

	static async getCorridaSeriesDateRange(corrida,filter={},options={}) {
		// const series = []
		// if((!filter.series_id && !filter.tipo) || (filter.tipo=="puntual") || (filter.series_id && !filter.tipo)) {
			// series puntuales
			// var filter_string = internal.utils.control_filter2({
			// 	estacion_id:{type:"integer","table":"series"}, 
			// 	var_id:{type:"integer","table":"series"},
			// 	qualifier:{type:"string","table":"pronosticos"},
			// 	series_id:{type:"integer","table":"pronosticos"}},filter)
		filter.series_table = filter.series_table ?? ((filter.tipo) ? (filter.tipo == "puntual") ? "series" : (filter.tipo == "areal") ? "series_areal" : (filter.tipo == "raster" || filteri.tipo == "rast") ? "series_rast" : undefined : undefined)
		if(options.group_by_qualifier) {
			var filter_string = internal.utils.control_filter2(
				{
					estacion_id:{type:"integer"}, 
					var_id:{type:"integer"},
					qualifier:{type:"string"},
					series_id:{type:"integer"},
					series_table:{type:"string"},
					tabla:{type:"string"}
				},
				filter,
				"series_prono_date_range_by_qualifier"
			)
			var stmt = "SELECT * FROM series_prono_date_range_by_qualifier WHERE cor_id=$1 " + filter_string + " ORDER BY series_id, qualifier"
				// var stmt = "SELECT \
				// pronosticos.series_id, \
				// 'series' AS series_table, \
				// series.estacion_id, \
				// series.var_id, \
				// pronosticos.qualifier, \
				// min(pronosticos.timestart) as begin_date,\
				// max(pronosticos.timestart) as end_date,\
				// count(pronosticos.timestart) \
				// FROM pronosticos \
				// JOIN series \
				// ON (series.id = pronosticos.series_id)\
				// WHERE pronosticos.cor_id=$1\
				// " + filter_string + "\
				// GROUP BY pronosticos.series_id, series_table, series.estacion_id,series.var_id, pronosticos.qualifier \
				// ORDER BY pronosticos.series_id,pronosticos.qualifier"
		} else {
			var filter_string = internal.utils.control_filter2(
				{
					estacion_id:{type:"integer"}, 
					var_id:{type:"integer"},
					qualifier:{type:"json_array",column:"qualifiers"},
					series_id:{type:"integer"},
					series_table:{type:"string"},
					tabla:{type:"string"}
				},
				filter,
				"series_prono_date_range"
			)
			var stmt = "SELECT * from series_prono_date_range WHERE cor_id=$1 " + filter_string + " ORDER BY series_id"
				// var stmt  = "SELECT \
				// pronosticos.series_id, \
				// 'series' AS series_table, \
				// series.estacion_id, \
				// series.var_id, \
				// json_agg(DISTINCT qualifier) AS qualifiers,\
				// min(pronosticos.timestart) as begin_date,\
				// max(pronosticos.timestart) as end_date,\
				// count(pronosticos.timestart) \
				// FROM pronosticos \
				// JOIN series \
				// ON (series.id = pronosticos.series_id)\
				// WHERE pronosticos.cor_id=$1\
				// " + filter_string + "\
				// GROUP BY pronosticos.series_id, series_table, series.estacion_id,series.var_id \
				// ORDER BY pronosticos.series_id"
		}
		var params = [corrida.id]
		const results = await global.pool.query(stmt,params)
		const series = results.rows.map(s=>new internal.SerieTemporalSim(s))
		// }
		// if((!filter.tipo && !filter.series_id) || filter.tipo == "areal") {
		// 	// series areales
		// 	filter_string = internal.utils.control_filter2({
		// 		estacion_id:{type:"integer","table":"series_areal", column: "area_id"}, 
		// 		var_id:{type:"integer","table":"series_areal"},
		// 		qualifier:{type:"string","table":"pronosticos_areal"},
		// 		series_id:{type:"integer","table":"pronosticos_areal"}},filter)
		// 	if(options.group_by_qualifier) {
		// 		stmt  = "SELECT \
		// 		pronosticos_areal.series_id, \
		// 		'series_areal' AS series_table, \
		// 		series_areal.area_id AS estacion_id, \
		// 		series_areal.var_id, \
		// 		pronosticos_areal.qualifier, \
		// 		min(pronosticos_areal.timestart) as begin_date,\
		// 		max(pronosticos_areal.timestart) as end_date,\
		// 		count(pronosticos_areal.timestart) \
		// 		FROM pronosticos_areal \
		// 		JOIN series_areal \
		// 		ON (series_areal.id = pronosticos_areal.series_id)\
		// 		WHERE pronosticos_areal.cor_id=$1\
		// 		" + filter_string + "\
		// 		GROUP BY pronosticos_areal.series_id, series_table, series_areal.area_id, series_areal.var_id, pronosticos_areal.qualifier \
		// 		ORDER BY pronosticos_areal.series_id, pronosticos_areal.qualifier"
		// 	} else {
		// 		stmt  = "SELECT \
		// 		pronosticos_areal.series_id, \
		// 		'series_areal' AS series_table, \
		// 		series_areal.area_id AS estacion_id, \
		// 		series_areal.var_id, \
		// 		json_agg(DISTINCT qualifier) AS qualifiers,\
		// 		min(pronosticos_areal.timestart) as begin_date,\
		// 		max(pronosticos_areal.timestart) as end_date,\
		// 		count(pronosticos_areal.timestart) \
		// 		FROM pronosticos_areal \
		// 		JOIN series_areal \
		// 		ON (series_areal.id = pronosticos_areal.series_id)\
		// 		WHERE pronosticos_areal.cor_id=$1\
		// 		" + filter_string + "\
		// 		GROUP BY pronosticos_areal.series_id, series_table, series_areal.area_id, series_areal.var_id \
		// 		ORDER BY pronosticos_areal.series_id"
		// 	}
		// 	params = [corrida.id]
		// 	const results_areales = await global.pool.query(stmt,params)
		// 	series.push(...results_areales.rows)
		// }
		corrida.series = series
		return
	}

	static async updateSeriesPronoDateRangeByCorId(cor_id,client) {
		if(!cor_id) {
			throw(new Error("Missing cor_id"))
		}
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		const puntual_date_range = await client.query(`SELECT update_series_puntual_prono_date_range(${cor_id})`)
		const areal_date_range = await client.query(`SELECT update_series_areal_prono_date_range(${cor_id})`)
		// series_rast_prono_date_range es una vista
		if(release_client) {
			client.release()
		}
		return [
			...puntual_date_range.rows,
			...areal_date_range.rows
		]		
	}


	static async updateSeriesPronoDateRange(filter={},options={},client) {
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		if(!filter.tipo || filter.tipo == "puntual") {
			console.log("Update series_puntual_prono_date_range")
			const filter_string = internal.utils.control_filter2({
				cor_id: {type:"integer"},
				series_id: {type: "integer"},
				estacion_id: {type: "integer", table: "series"},
				tabla: {type: "string", table: "estaciones"},
				var_id: {type: "integer", table: "series"},
				cal_id: {type: "integer", table: "corridas"},
				forecast_date: {type: "date", table: "corridas", column: "date"},  //,
				// qualifier: {type: "string"}
				cal_grupo_id: {type: "integer", table: "calibrados", column: "grupo_id"}
			},
			filter,
			"pronosticos")
			const stmt = `INSERT INTO series_puntual_prono_date_range (series_id,cor_id,begin_date,end_date,count,qualifiers)
				SELECT series.id AS series_id,
					pronosticos.cor_id,
					min(pronosticos.timestart) AS begin_date,
					max(pronosticos.timestart) AS end_date,
					count(pronosticos.timestart) AS count,
					json_agg(DISTINCT qualifier) AS qualifiers
				FROM estaciones
				JOIN series ON estaciones.unid = series.estacion_id
				JOIN pronosticos ON series.id = pronosticos.series_id
				JOIN corridas ON corridas.id = pronosticos.cor_id
				JOIN calibrados ON calibrados.id = corridas.cal_id
				WHERE 1=1 ${filter_string}
				GROUP BY series.id,pronosticos.cor_id
			ON CONFLICT (series_id,cor_id) DO UPDATE SET
			begin_date=EXCLUDED.begin_date,
			end_date=EXCLUDED.end_date,
			count=EXCLUDED.count,
			qualifiers=EXCLUDED.qualifiers`
			await client.query(stmt)
		}
		if(!filter.tipo || filter.tipo == "areal") {
			console.log("Update series_areal_prono_date_range")
			const filter_string = internal.utils.control_filter2({
				cor_id: {type:"integer"},
				series_id: {type: "integer"},
				estacion_id: {type: "integer", table: "series_areal", column:"area_id"},
				tabla: {type: "string", table: "estaciones"},
				var_id: {type: "integer", table: "series_areal"},
				cal_id: {type: "integer", table: "corridas"},
				forecast_date: {type: "date", table: "corridas", column: "date"},
				cal_grupo_id: {type: "integer", table: "calibrados", column: "grupo_id"}
				//,
				// qualifier: {type: "string"}
			},
			filter,
			"pronosticos_areal")
			const stmt = `INSERT INTO series_areal_prono_date_range (series_id,cor_id,begin_date,end_date,count,qualifiers)
				SELECT series_areal.id AS series_id,
					pronosticos_areal.cor_id,
					min(pronosticos_areal.timestart) AS begin_date,
					max(pronosticos_areal.timestart) AS end_date,
					count(pronosticos_areal.timestart) AS count,
					json_agg(DISTINCT qualifier) AS qualifiers
				FROM series_areal
				JOIN pronosticos_areal ON series_areal.id = pronosticos_areal.series_id
				JOIN corridas ON corridas.id = pronosticos_areal.cor_id
				JOIN calibrados ON calibrados.id = corridas.cal_id
				JOIN areas_pluvio ON series_areal.area_id = areas_pluvio.unid
				LEFT JOIN estaciones ON areas_pluvio.exutorio_id = estaciones.unid
				WHERE 1=1 ${filter_string}
				GROUP BY series_areal.id,pronosticos_areal.cor_id
			ON CONFLICT (series_id,cor_id) DO UPDATE SET
			begin_date=EXCLUDED.begin_date,
			end_date=EXCLUDED.end_date,
			count=EXCLUDED.count,
			qualifiers=EXCLUDED.qualifiers`
			await client.query(stmt)
		}	
		if(release_client) {
			client.release()
		}	
		return 
	}

	static async updateSeriesPronoDateRangeByQualifier(filter={},options={},client) {
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		if(!filter.tipo || filter.tipo == "puntual") {
			console.log("Update series_puntual_prono_date_range_by_qualifier")
			const filter_string = internal.utils.control_filter2({
				cor_id: {type:"integer"},
				series_id: {type: "integer"},
				estacion_id: {type: "integer", table: "series"},
				tabla: {type: "string", table: "estaciones"},
				var_id: {type: "integer", table: "series"},
				qualifier: {type: "string"}
			},
			filter,
			"pronosticos")
			const stmt = `INSERT INTO series_puntual_prono_date_range_by_qualifier (series_id,cor_id,qualifier,begin_date,end_date,count)
			SELECT series.id AS series_id,
				pronosticos.cor_id,
				pronosticos.qualifier,
				min(pronosticos.timestart) AS begin_date,
				max(pronosticos.timestart) AS end_date,
				count(pronosticos.timestart) AS count
			   FROM estaciones,
				series,
				pronosticos
			  WHERE estaciones.unid = series.estacion_id
			  AND series.id = pronosticos.series_id
			  ${filter_string}
			  GROUP BY series.id, pronosticos.cor_id, pronosticos.qualifier
			ON CONFLICT (series_id,cor_id,qualifier) DO UPDATE SET
			begin_date=EXCLUDED.begin_date,
			end_date=EXCLUDED.end_date,
			count=EXCLUDED.count`
			await client.query(stmt)
		}
		if(!filter.tipo || filter.tipo == "areal") {
			console.log("Update series_areal_prono_date_range_by_qualifier")
			const filter_string = internal.utils.control_filter2({
				cor_id: {type:"integer"},
				series_id: {type: "integer"},
				estacion_id: {type: "integer", table: "series_areal", column:"area_id"},
				tabla: {type: "string", table: "estaciones"},
				var_id: {type: "integer", table: "series_areal"},
				qualifier: {type: "string"}
			},
			filter,
			"pronosticos_areal")
			const stmt = `INSERT INTO series_areal_prono_date_range_by_qualifier (series_id,cor_id,qualifier,begin_date,end_date,count)
			SELECT series_areal.id AS series_id,
				pronosticos_areal.cor_id,
				pronosticos_areal.qualifier,
				min(pronosticos_areal.timestart) AS begin_date,
				max(pronosticos_areal.timestart) AS end_date,
				count(pronosticos_areal.timestart) AS count
			   FROM series_areal
			   JOIN pronosticos_areal ON series_areal.id = pronosticos_areal.series_id
			   JOIN areas_pluvio ON series_areal.area_id = areas_pluvio.unid
			   LEFT JOIN estaciones ON areas_pluvio.exutorio_id = estaciones.unid
			   WHERE 1=1 ${filter_string}
			  GROUP BY series_areal.id, pronosticos_areal.cor_id, pronosticos_areal.qualifier
			ON CONFLICT (series_id,cor_id,qualifier) DO UPDATE SET
			begin_date=EXCLUDED.begin_date,
			end_date=EXCLUDED.end_date,
			count=EXCLUDED.count`
			await client.query(stmt)
		}
		if(!filter.tipo || filter.tipo == "raster" || filter.tipo == "rast") {
			const filter_string = internal.utils.control_filter2({
				cor_id: {type:"integer"},
				series_id: {type: "integer"},
				estacion_id: {type: "integer", table: "series_rast"},
				var_id: {type: "integer", table: "series_rast"},
				qualifier: {type: "string"}
			},
			filter,
			"pronosticos_rast")
			const stmt = `INSERT INTO series_rast_prono_date_range_by_qualifier (series_id,cor_id,qualifier,begin_date,end_date,count)
				SELECT series_rast.id AS series_id,
					pronosticos_rast.cor_id,
					pronosticos_rast.qualifier,
					min(pronosticos_rast.timestart) AS begin_date,
					max(pronosticos_rast.timestart) AS end_date,
					count(pronosticos_rast.timestart) AS count
				FROM series_rast
				JOIN pronosticos_rast ON series_rast.id = pronosticos_rast.series_id
				WHERE 1=1 
				${filter_string}
				GROUP BY series_rast.id, pronosticos_rast.cor_id, pronosticos_rast.qualifier
				ON CONFLICT (series_id,cor_id,qualifier) 
				DO UPDATE SET
					begin_date = EXCLUDED.begin_date,
					end_date = EXCLUDED.end_date,
					count = EXCLUDED.count`
			await client.query(stmt)
		}
		if(release_client) {
			client.release()
		}		
		return 
	}

	static async refreshSeriesJson() {
		await global.pool.query("REFRESH MATERIALIZED VIEW series_json")
		return 
	}

	static async refreshSeriesArealJson() {
		await global.pool.query("REFRESH MATERIALIZED VIEW series_areal_json")
		return 
	}

	static async refreshSeriesArealJsonNoGeom() {
		await global.pool.query("REFRESH MATERIALIZED VIEW series_areal_json_no_geom")
		return 
	}

	static async getPronosticosArray(filter={},options={},client) {
		var result = []
		if((!filter.series_id && !filter.tipo) || (filter.tipo=="puntual") || (filter.series_id && !filter.tipo)) {
			var pronos_puntual = await this.getPronosticosPuntual(filter,client)
			result.push(...pronos_puntual)
		}
		if((!filter.tipo && !filter.series_id) || filter.tipo == "areal") {
			var pronos_areal = await this.getPronosticosAreal(filter,client)
			result.push(...pronos_areal)
		}
		if((!filter.tipo && !filter.series_id) || filter.tipo == "rast" || filter.tipo == "raster") {
			var pronos_rast = await this.getPronosticosRast(filter,client)
			result.push(...pronos_rast)
		}
		return result
	}

	static async getPronosticosPuntual(filter={},client) {
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		const filter_values = {
			timestart:filter.timestart,
			timeend:filter.timeend,
			cal_id: filter.cal_id,
			cor_id:filter.cor_id,
			forecast_date: filter.forecast_date,
			estacion_id:filter.estacion_id,
			var_id:filter.var_id,
			qualifier:filter.qualifier, 
			series_id:filter.series_id,
			tabla:filter.tabla
		}
		try {
			var filter_string = internal.utils.control_filter2({
				timestart:{type:"timestart","table":"pronosticos"}, 
				timeend:{type:"timeend","table":"pronosticos","column":"timestart"}, 
				cal_id:{type:"integer", table:"corridas"}, 
				cor_id:{type:"integer","table":"pronosticos"}, 
				forecast_date:{type:"date", table: "corridas", column: "date", trunc: "milliseconds"},
				estacion_id:{type: "integer", "table": "series"}, 
				var_id:{type:"integer","table":"series"}, 
				qualifier:{type:"string","table":"pronosticos"}, 
				series_id:{type:"integer","table":"pronosticos"},
				tabla:{type:"string","table":"estaciones"}
			},filter_values,undefined, true)
		} catch (e) {
			throw(new Error("getPronosticosPuntual: Invalid filter values: " + JSON.stringify(filter_values) + ". " + e.toString()))
		}
		const stmt = "SELECT \
		    corridas.id AS cor_id,\
			series.id series_id,\
			'series' AS series_table,\
			series.estacion_id,\
			estaciones.tabla,\
			series.var_id,\
			pronosticos.timestart::timestamptz  timestart, \
			pronosticos.timeend::timestamptz timeend, \
			valores_prono_num.valor,\
			pronosticos.qualifier\
		FROM pronosticos\
		JOIN valores_prono_num ON pronosticos.id = valores_prono_num.prono_id\
		JOIN series ON pronosticos.series_id = series.id\
		JOIN corridas ON pronosticos.cor_id = corridas.id\
		JOIN estaciones ON series.estacion_id = estaciones.unid  \
		WHERE 1=1 " + filter_string + "\
		ORDER BY pronosticos.series_id,pronosticos.timestart"
		// to_char(pronosticos.timestart::timestamptz at time zone 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') timestart,\
		//	to_char(pronosticos.timeend::timestamptz at time zone 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') timeend,\
		console.debug(stmt)
		const result = await client.query(stmt)
		if(release_client) {
			client.release()
		}
		return result.rows.map(row=>{
			return new internal.pronostico(row)
		})
	}

	static async getCorridaSeries(filter={},series_metadata=false,group_by_qualifier=false) {
		const series = {}
		const pronos = await this.getPronosticosArray(filter)
		if(pronos) {
			if(group_by_qualifier) {   // one series element for each series_id+qualifier combination
				internal.CRUD.groupByQualifier(series,pronos)
			} else {    // one series element for each series_id, regardless of qualifier (results in mixed qualifier series)
				internal.CRUD.groupBySeries(series,pronos)
			}
		}
		var series_data = Object.keys(series).sort().map(k=>series[k])
		// corrida.series = series_data 
		if(series_metadata) {
			var promises = []
			for(var serie of series_data) {
				const tipo = (serie.series_table == "series_areal") ? "areal" : "puntual"
				promises.push(this.getSerie(tipo,serie.series_id)
				.then(result=>{
					serie.metadata = result
					return
				}))
			}
			await Promise.all(promises)
		}
		return series_data // corrida
	}

	static async getPronosticosAreal(filter={},client) {
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		var filter_string = internal.utils.control_filter2({
			timestart:{type:"timestart","table":"pronosticos_areal"},
			timeend:{type:"timeend","table":"pronosticos_areal","column":"timestart"}, 
			cal_id:{type:"integer", table:"corridas"}, 
			cor_id:{type:"integer","table":"pronosticos_areal"}, 
			forecast_date:{type:"date", table: "corridas", column: "date", trunc: "milliseconds"},
			estacion_id:{type:"integer","table":"series_areal","column":"area_id"}, 
			var_id:{type:"integer","table":"series_areal"}, 
			qualifier:{type:"string","table":"pronosticos_areal"},
			series_id:{type:"integer","table":"pronosticos_areal"},
			tabla:{type:"string","table":"estaciones"}
		},{
			timestart:filter.timestart,
			timeend:filter.timeend,
			cal_id: filter.cal_id,
			cor_id: filter.cor_id,
			forecast_date: filter.forecast_date,
			estacion_id: filter.estacion_id,
			var_id: filter.var_id,
			qualifier: filter.qualifier, 
			series_id: filter.series_id,
			tabla: filter.tabla
		})
		const result = await client.query("SELECT \
			corridas.id AS cor_id,\
			series_areal.id series_id,\
			'series_areal' AS series_table,\
			series_areal.area_id AS estacion_id,\
			estaciones.tabla,\
			series_areal.var_id,\
			pronosticos_areal.timestart::timestamptz  timestart, \
			pronosticos_areal.timeend::timestamptz timeend, \
			pronosticos_areal.valor,\
			pronosticos_areal.qualifier\
		FROM pronosticos_areal\
		JOIN series_areal ON series_areal.id=pronosticos_areal.series_id\
		JOIN corridas ON pronosticos_areal.cor_id=corridas.id\
		JOIN areas_pluvio ON series_areal.area_id = areas_pluvio.unid\
		LEFT JOIN estaciones ON areas_pluvio.exutorio_id = estaciones.unid\
		WHERE 1=1 " + filter_string + "\
		ORDER BY pronosticos_areal.series_id,pronosticos_areal.timestart")
		// 			to_char(pronosticos_areal.timestart::timestamptz at time zone 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') timestart,\
		//	to_char(pronosticos_areal.timeend::timestamptz at time zone 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') timeend,\
		if(release_client) {
			client.release()
		}
		return result.rows
	}

	static async getPronosticosRast(filter={},client) {
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		var filter_string = internal.utils.control_filter2({
			timestart:{type:"timestart","table":"pronosticos_rast"},
			timeend:{type:"timeend","table":"pronosticos_rast","column":"timestart"}, 
			cal_id:{type:"integer", table:"corridas"}, 
			cor_id:{type:"integer","table":"pronosticos_rast"},
			forecast_date:{type:"date", table: "corridas", column: "date", trunc: "milliseconds"},
			estacion_id:{type:"integer","table":"series_rast","column":"escena_id"}, 
			var_id:{type:"integer","table":"series_rast"}, 
			qualifier:{type:"string","table":"pronosticos_rast"},
			series_id:{type:"integer","table":"pronosticos_rast"}
		},{
			timestart:filter.timestart,
			timeend:filter.timeend,
			cal_id: filter.cal_id,
			cor_id: filter.cor_id,
			forecast_date: filter.forecast_date,
			estacion_id: filter.estacion_id,
			var_id: filter.var_id,
			qualifier: filter.qualifier, 
			series_id: filter.series_id
		})
		const result = await client.query("SELECT \
		    pronosticos_rast.id AS id,\
			corridas.id AS cor_id,\
			series_rast.id series_id,\
			'series_rast' AS series_table,\
			series_rast.escena_id AS estacion_id,\
			null as tabla,\
			series_rast.var_id,\
			pronosticos_rast.timestart::timestamptz  timestart, \
			pronosticos_rast.timeend::timestamptz timeend, \
			ST_AsGDALRaster(pronosticos_rast.valor,'GTiff') as valor,\
			pronosticos_rast.qualifier\
		FROM pronosticos_rast\
		JOIN series_rast ON series_rast.id=pronosticos_rast.series_id\
		JOIN corridas ON pronosticos_rast.cor_id=corridas.id\
		WHERE 1=1 " + filter_string + "\
		ORDER BY pronosticos_rast.series_id,pronosticos_rast.timestart")
		if(release_client) {
			client.release()
		}
		return result.rows
	}


	static groupByQualifier(series={},pronosticos=[]) {
		for(var p of pronosticos) {
			var key = p.cor_id + "_" + p.series_id + "_" + p.series_table + "_" + p.qualifier
			if(!series[key]) {
				series[key] = new internal.SerieTemporalSim({
					cor_id: p.cor_id,
					series_id: p.series_id,
					series_table: p.series_table,
					estacion_id: p.estacion_id,
					var_id: p.var_id,
					qualifier: p.qualifier,
					pronosticos: []
				})
			}
			series[key].pronosticos.push({
				timestart: p.timestart,
				timeend: p.timeend,
				valor: p.valor
			}) // cor_id:r.cor_id,series_id:p.series_id,
		}
		return
	}

	static groupBySeries(series={},pronosticos=[]) {
		for(var p of pronosticos) {
			var key = p.cor_id + "_" + p.series_id + "_" + p.series_table
			if(!series[key]) {
				series[key] = new internal.SerieTemporalSim({
					cor_id: p.cor_id,
					series_id: p.series_id,
					series_table: p.series_table,
					estacion_id: p.estacion_id,
					var_id: p.var_id,
					pronosticos: []
				})
			}
			series[key].pronosticos.push(
				new internal.pronostico({
					timestart: p.timestart,
					timeend: p.timeend,
					valor: p.valor,
					qualifier: p.qualifier
				})
			) // cor_id:r.cor_id,series_id:p.series_id,
		}
	}
	
	static async deletePronosticos(cor_id,cal_id,forecast_date,timestart,timeend,only_sim=false,series_id,estacion_id,var_id,tipo,fuentes_id,tabla,qualifier) {
		var deleted_pronosticos = []
		var client = await global.pool.connect()
		try {
			if(!tipo || tipo == "puntual") { 
				const filter_string = internal.utils.control_filter2(
					{
						timestart: {type: "timestart"},
						timeend: {type: "timeend", column: "timestart"},
						series_id: {type: "integer"},
						cor_id: {type: "integer"},
						cal_id: {type: "integer", table: "corridas"},
						forecast_date: {type: "timestamp", table: "corridas", column: "date"},
						estacion_id: {type: "integer", table: "series"},
						var_id: {type: "integer", table: "series"},
						tabla: {type: "string", table: "estaciones"},
						qualifier: {type: "string"}
					},{
						cor_id: cor_id,
						cal_id: cal_id,
						forecast_date: forecast_date,
						timestart: timestart,
						timeend: timeend,
						series_id: series_id,
						estacion_id: estacion_id,
						var_id: var_id,
						tabla: tabla,
						qualifier: qualifier
					},
					"pronosticos"
				)
				if(only_sim) {
					filter_string += " AND pronosticos.timestart<corridas.date"
				}
				await client.query("BEGIN")
				const stmt = "DELETE FROM valores_prono_num USING pronosticos,corridas,series WHERE pronosticos.cor_id=corridas.id AND pronosticos.id=valores_prono_num.prono_id AND series.id=pronosticos.series_id" + filter_string
				// console.log(stmt)
				await client.query(stmt)
				const stmt_p = "DELETE FROM pronosticos USING corridas,series WHERE pronosticos.cor_id=corridas.id AND series.id=pronosticos.series_id" + filter_string  + " RETURNING *"
				// console.log(stmt_p)
				var result = await client.query(stmt_p)
				deleted_pronosticos.push(result.rows.map(r=>{
					r.series_table = 'series'
					return new internal.pronostico(r)
				}))
				await client.query("COMMIT")
			}
			if(!tipo || tipo == "areal") { 
				const filter_string = internal.utils.control_filter2(
					{
						timestart: {type: "timestart"},
						timeend: {type: "timeend", column: "timestart"},
						series_id: {type: "integer"},
						cor_id: {type: "integer"},
						cal_id: {type: "integer", table: "corridas"},
						forecast_date: {type: "timestamp", table: "corridas", column: "date"},
						estacion_id: {type: "integer", table: "series_areal", column: "area_id"},
						var_id: {type: "integer", table: "series_areal"},
						fuentes_id: {type: "integer", table: "series_areal"},
						qualifier: {type: "string"}
					},{
						cor_id: cor_id,
						cal_id: cal_id,
						forecast_date: forecast_date,
						timestart: timestart,
						timeend: timeend,
						series_id: series_id,
						estacion_id: estacion_id,
						var_id: var_id,
						fuentes_id: fuentes_id,
						qualifier: qualifier
					},
					"pronosticos_areal"
				)
				if(only_sim) {
					filter_string += " AND pronosticos_areal.timestart<corridas.date"
				}
				const stmt = "DELETE FROM pronosticos_areal USING corridas,series_areal WHERE pronosticos_areal.cor_id=corridas.id AND series_areal.id=pronosticos_areal.series_id" + filter_string + " returning *"
				// console.log(stmt)
				var result = await client.query(stmt)
				deleted_pronosticos.push(result.rows.map(r=>{
					r.series_table = 'series_areal'
					return new internal.pronostico(r)
				}))
			}
			if(!tipo || tipo == "rast" || tipo == "raster") { 
				const filter_string = internal.utils.control_filter2(
					{
						timestart: {type: "timestart"},
						timeend: {type: "timeend", column: "timestart"},
						series_id: {type: "integer"},
						cor_id: {type: "integer"},
						cal_id: {type: "integer", table: "corridas"},
						forecast_date: {type: "timestamp", table: "corridas", column: "date"},
						estacion_id: {type: "integer", table: "series_rast", column: "escena_id"},
						var_id: {type: "integer", table: "series_rast"},
						fuentes_id: {type: "integer", table: "series_rast"},
						qualifier: {type: "string"}
					},{
						cor_id: cor_id,
						cal_id: cal_id,
						forecast_date: forecast_date,
						timestart: timestart,
						timeend: timeend,
						series_id: series_id,
						estacion_id: estacion_id,
						var_id: var_id,
						fuentes_id: fuentes_id,
						qualifier: qualifier
					},
					"pronosticos_rast"
				)
				if(only_sim) {
					filter_string += " AND pronosticos_rast.timestart<corridas.date"
				}
				const stmt = "DELETE FROM pronosticos_rast USING corridas,series_rast WHERE pronosticos_rast.cor_id=corridas.id AND series_rast.id=pronosticos_rast.series_id" + filter_string + " returning *"
				// console.log(stmt)
				var result = await client.query(stmt)
				deleted_pronosticos.push(result.rows.map(r=>{
					r.series_table = 'series_rast'
					return new internal.pronostico(r)
				}))
			}
		} catch(e) {
			client.release()
			return Promise.reject("crud.deletePronosticos: e.toString()")
		}
		client.release()
		return Promise.resolve(deleted_pronosticos)
	}

	static async deleteCorrida(cor_id,cal_id,forecast_date) {
		// console.log({cor_id:cor_id})
		var corrida={}
		if(!cor_id && !(cal_id && forecast_date)) {
			return Promise.reject("cor_id or cal_id+forecast_date missing")
		}
		var client = await global.pool.connect()
		await client.query("BEGIN")
		try {
			if(cor_id) {
				if(Array.isArray(cor_id) && !cor_id.length)	{
					client.release()
					throw("crud.deleteCorrida: cor_id is empty array")
				}
				// delete from series_puntual_prono_date_range
				var cor_filter = internal.utils.control_filter2(
					{
						cor_id: {
							type: "integer"
						}
					},
					{
						cor_id: cor_id
					}
				)
				if(!cor_filter) {
					throw("Invalid cor_id filter")
				}
				var query = "DELETE FROM series_puntual_prono_date_range WHERE 1=1 " + cor_filter
				console.log(query)
				var result = await client.query(query)
				query = "DELETE FROM series_areal_prono_date_range WHERE 1=1 " + cor_filter
				console.log(query)
				result = await client.query(query)
				var query = "DELETE FROM series_puntual_prono_date_range_by_qualifier WHERE 1=1 " + cor_filter
				console.log(query)
				var result = await client.query(query)
				query = "DELETE FROM series_areal_prono_date_range_by_qualifier WHERE 1=1 " + cor_filter
				console.log(query)
				result = await client.query(query)
				// delete from valores_prono_num
				cor_filter = internal.utils.control_filter2(
					{
						cor_id: {
							type: "integer",
							table: "pronosticos"
						}
					},
					{
						cor_id: cor_id
					}
				)
				if(!cor_filter) {
					throw("Invalid cor_id filter")
				}
				query = "DELETE FROM valores_prono_num USING pronosticos WHERE  pronosticos.id=valores_prono_num.prono_id " + cor_filter
				console.log(query)
				var result = await client.query(query)
			} else {
				var cor_filter = internal.utils.control_filter2(
					{
						cal_id: {
							type: "integer",
							table: "corridas"
						},
						date: {
							type: "timestamp",
							table: "corridas"
						}
					},{
						cal_id: cal_id,
						date: forecast_date
					}
				)
				if(!cor_filter) {
					throw("Invalid cal_id+forecast_date filter")
				}
				// delete from series_puntual_prono_date_range
				var result = await client.query("DELETE FROM series_puntual_prono_date_range USING corridas WHERE corridas.id=series_prono_date_range.cor_id" + cor_filter)				
				// delete from series_areal_prono_date_range
				result = await client.query("DELETE FROM series_areal_prono_date_range USING corridas WHERE corridas.id=series_prono_date_range.cor_id" + cor_filter)				
				// delete from series_puntual_prono_date_range_by_qualifier
				var result = await client.query("DELETE FROM series_puntual_prono_date_range_by_qualifier USING corridas WHERE corridas.id=series_prono_date_range.cor_id" + cor_filter)				
				// delete from series_areal_prono_date_range
				result = await client.query("DELETE FROM series_areal_prono_date_range_by_qualifier USING corridas WHERE corridas.id=series_prono_date_range.cor_id" + cor_filter)				
				// delete from valores_prono_num
				result = await client.query("DELETE FROM valores_prono_num USING pronosticos,corridas WHERE corridas.id=pronosticos.cor_id AND pronosticos.id=valores_prono_num.prono_id" + cor_filter)
			}
			// console.log({deleted_valores: result.rows})
			corrida.valores = (result.rows) ? result.rows : undefined
			if(cor_id) {
				if(Array.isArray(cor_id)) {
					result = await client.query("DELETE FROM pronosticos WHERE pronosticos.cor_id IN (" + cor_id.join(",") + ") RETURNING *")	
				} else {
					result = await client.query("DELETE FROM pronosticos WHERE pronosticos.cor_id=$1 RETURNING *",[cor_id])
				}
			} else {
				result = await client.query("DELETE FROM pronosticos USING corridas WHERE corridas.cal_id=$1 AND corridas.date::date=$2::date AND pronosticos.cor_id=corridas.id RETURNING *",[cal_id,forecast_date])
			}
			corrida.pronosticos = (result.rows) ? result.rows : undefined
			// console.log({deleted_pronos: corrida.pronosticos})
			if(cor_id) {
				if(Array.isArray(cor_id)) {
					result = await client.query("DELETE FROM corridas WHERE id IN (" + cor_id.join(",") + ")  RETURNING *")
				} else {
					result = await client.query("DELETE FROM corridas WHERE id=$1 RETURNING *",[cor_id])
				}
			} else {
				result = await client.query("DELETE FROM corridas WHERE cal_id=$1 and date::date=$2::date RETURNING *",[cal_id,forecast_date])
			}
			if(!result.rows) {
				client.release()
				throw("prono not found")
			}
			if(result.rows.length == 0) {
				client.release()
				throw("prono not found")
			}
			corrida.cor_id = result.rows[0].cor_id
			corrida.cal_id = result.rows[0].cal_id
			corrida.forecast_date = result.rows[0].forecast_date
			await client.query("COMMIT")
		} catch(e) {
			throw(e)
		} finally {
			client.release()
		}
		return corrida
	}

	static async deleteCorridas(filter={},options={}) {
		if(filter.skip_cal_id && !Array.isArray(filter.skip_cal_id)) {
			filter.skip_cal_id = [filter.skip_cal_id]
		}
		var getPronoPromise
		if(filter.id) {
			console.log("filter.id")
			getPronoPromise = this.getPronosticos(filter.id)
		} else if(filter.cor_id) {
			console.log("filter.cor_id")
			getPronoPromise = this.getPronosticos(filter.cor_id)
		} else if (filter.cal_id) {
			console.log("filter.cal_id")	
			if(filter.forecast_date || filter.date || (filter.forecast_timestart && filter.forecast_timeend)) {
				if(filter.forecast_date) { // exact timestamp
					console.log(" + filter.forecast_date")
					getPronoPromise = this.getPronosticos(undefined,filter.cal_id,undefined,undefined,filter.forecast_date)
				}
				else if(filter.date) { // forecast date whole day (no time)  
					console.log("+ filter.date")
					var [ts, te] = timeSteps.date2tste(new Date(filter.date))
					getPronoPromise = this.getPronosticos(undefined,filter.cal_id,ts,te,undefined)
				} else { 
					console.log("filter.forecast_timestart & forecast.timeend")
					getPronoPromise = this.getPronosticos(undefined,filter.cal_id,filter.forecast_timestart,filter.forecast_timeend)
				}
			} else if(filter.forecast_timeend) {
				console.log("   + filter.forecast_timeend")
				getPronoPromise = this.getPronosticos(undefined,filter.cal_id,undefined,filter.forecast_timeend)
			} else {
				return Promise.reject("Invalid options, more filters are required")
			}
		} else if (filter.estacion_id) {
			console.log("filter.estacion_id")
			if(!filter.date) {
				if(!filter.forecast_date) {
					console.error("Falta parametro date o forecast_date")
					return Promise.reject("Falta parametro date o forecast_date")
				} 
				console.log("	+ filter.forecast_date")
				getPronoPromise = this.getPronosticos(undefined,undefined,undefined,undefined,filter.forecast_date,undefined,undefined,undefined,filter.estacion_id)		
			} else {
				console.log("	+ filter.date")
				var [ts, te] = timeSteps.date2tste(new Date(filter.date))
				getPronoPromise = this.getPronosticos(undefined,undefined,ts,te,undefined,undefined,undefined,undefined,filter.estacion_id)
			}
		} else if (filter.model_id) {
			console.log("filter.model_id")
			if(!filter.date) {
				return Promise.reject("crud.deleteCorridas: Falta parametro date")
			}
			var [ts, te] = timeSteps.date2tste(new Date(filter.date))
			return this.getPronosticos(undefined,undefined,ts,te,undefined,undefined,undefined,undefined,undefined,undefined,false,undefined,undefined,undefined,undefined,undefined,filter.model_id)
		} else if (filter.date) {
			if(!filter.skip_cal_id) { 
				return Promise.reject("crud.deleteCorridas: missing skip_cal_id")
			} 
			console.log("filter.date + filter.skip_cal_id")
			var [ts, te] = timeSteps.date2tste(new Date(filter.date))
			getPronoPromise = this.getPronosticos(undefined,undefined,ts,te)
		} else if (filter.forecast_date) {
			if(!filter.skip_cal_id) { 
				return Promise.reject("crud.deleteCorridas: missing skip_cal_id")
			} 
			console.log("filter.forecast_date + filter.skip_cal_id")
			getPronoPromise = this.getPronosticos(undefined,undefined,undefined,undefined,filter.forecast_date)
		} else if (filter.forecast_timeend) {
			if(!filter.skip_cal_id) { 
				return Promise.reject("crud.deleteCorridas: missing skip_cal_id")
			} 
			console.log("filter.forecast_timeend + filter.skip_cal_id")
			getPronoPromise = this.getPronosticos(undefined,undefined,undefined,filter.forecast_timeend)
		} else {
			throw("Missing filter")
		}
		return getPronoPromise
		.then(corridas=>{
			if(corridas.length == 0) {
				console.warn("crud.deleteCorridas: pronosticos not found")
				return
			}
			if(filter.skip_cal_id) {
				corridas = corridas.filter(c=> filter.skip_cal_id.indexOf(c.cal_id) < 0)
			}
			var cor_id = corridas.map(c=>c.id)
			console.log({corridas:corridas,cor_id:cor_id})
			var savePromise
			if(options.save) {
				console.log("options.save, guardando en corridas_guardadas")
				savePromise = this.guardarCorridas(cor_id)
			} else if(options.save_prono) {
				console.log("options.save_prono, guardando prono en corridas_guardadas")
				savePromise = this.guardarCorridas(cor_id,undefined,{only_prono:true})
			} else {
				savePromise = Promise.resolve(corridas)
			}
			return savePromise
			.then(corridas=>{
				if(options.skip_delete) {
					console.log("options.skip_delete")
					return corridas
				}
				if(options.only_sim) {
					console.log("   + options.only_sim")
					return this.deletePronosticos(cor_id,undefined,undefined,undefined,undefined,true)
				}
				return this.deleteCorrida(cor_id)
			})
		})
	}

	static async batchDeleteCorridas(options={n:10,skip_cal_id:[288,308,391,400,439,440,441,442,432,433,439,440,441,442,444,445,446,454,457,455,456,458,459,460,461]}) {
		var deletedCorridas = []
		// ELIMINA CORRIDAS DE n + 10 a n DÍAS ATRÁS (cal_id NOT IN skip_cal_id)
		for(var i=0;i<=9;i++) {
			var cal_id_list = options.skip_cal_id
			var date = new Date()
			date.setTime(date.getTime() + (-1*options.n - 10 + i)*24*3600*1000)
			try {
				var deleted_corrida = await this.deleteCorridas({date:date,skip_cal_id:cal_id_list},{})  // ({cor_id:[22,23]})
				deletedCorridas.push(deleted_corrida)
			} catch (e) {
				console.error(e.toString())
			}
		}
		var forecast_timeend = new Date()
		forecast_timeend.setTime(forecast_timeend.getTime() - options.n*24*3600*1000)
		// ELIMINA PARTE SIMULADA DE CORRIDAS DE MODELOS SELECCIONADOS ANTERIORES A n DÍAS Y ALMACENA LA PARTE PRONOSTICADA
		for(var cal_id of cal_id_list) {
			try {
				var deleted_corrida = await this.deleteCorridas({forecast_timeend:forecast_timeend,cal_id:cal_id},{only_sim:false,save_prono:true})  // ({cor_id:[22,23]})
				deletedCorridas.push(deleted_corrida)
			} catch (e) {
				console.error(e.toString())
			}
		}
		return Promise.resolve(deletedCorridas)
	}
	
	/**
	 * 
	 * @param {internal.corrida} corrida 
	 * @param {boolean} replace_last 
	 * @returns {Promise<internal.corrida>}
	 */
	static async upsertCorrida(corrida,replace_last=false) {
		if(!corrida) {
			return Promise.reject("Faltan parámetros: corrida")
		}
		if(!corrida.cal_id) {
			return Promise.reject("Faltan parámetros: cal_id")
		}
		if(!corrida.forecast_date) {
			return Promise.reject("Faltan parámetros: forecast_date")
		}
		try {
			var client = await global.pool.connect()
			await client.query("BEGIN")
		} catch(e) {
			await client.query("ROLLBACK")
			client.release()
			return Promise.reject(e)
		}
		if(replace_last) {
			try {
				// var last_prono = await this.getLastCorrida(undefined,undefined,corrida.cal_id)
				var last_prono = await client.query("WITH last_prono AS (\
					SELECT max(date) date\
					FROM corridas\
					WHERE cal_id=$1)\
					SELECT *\
					FROM corridas, last_prono\
					WHERE corridas.date=last_prono.date\
					AND corridas.cal_id=$1",[corrida.cal_id])
				if(last_prono.rows.length) {
					await client.query("DELETE FROM valores_prono_num\
						USING pronosticos \
						WHERE pronosticos.cor_id=$1\
						AND pronosticos.id=valores_prono_num.prono_id",[last_prono.rows[0].id])
					await client.query("DELETE FROM pronosticos\
						WHERE pronosticos.cor_id=$1",[last_prono.rows[0].id])
					await client.query("DELETE FROM series_puntual_prono_date_range WHERE cor_id=$1",[last_prono.rows[0].id])
					await client.query("DELETE FROM series_areal_prono_date_range WHERE cor_id=$1",[last_prono.rows[0].id])
					await client.query("DELETE FROM series_puntual_prono_date_range_by_qualifier WHERE cor_id=$1",[last_prono.rows[0].id])
					await client.query("DELETE FROM series_areal_prono_date_range_by_qualifier WHERE cor_id=$1",[last_prono.rows[0].id])
					await client.query("DELETE FROM corridas WHERE id=$1",[last_prono.rows[0].id])
				}
			} catch(e) {
				await client.query("ROLLBACK")
				client.release()
				return Promise.reject(e)	
			}
		}
		var ups_corrida= new internal.corrida({})
		try {
			var stmt = internal.utils.pasteIntoSQLQuery("INSERT INTO corridas (cal_id,date) VALUES ($1,$2::timestamptz)\
			ON CONFLICT (cal_id,date) DO UPDATE set date=excluded.date\
			RETURNING *",[corrida.cal_id,corrida.forecast_date])
			// console.debug(stmt)
			var corridas = await client.query(stmt)
			// var corridas = await client.query("INSERT INTO corridas (cal_id,date) VALUES ($1,$2)\
				//   ON CONFLICT (cal_id,date) DO UPDATE set date=excluded.date\
				//   RETURNING *",[corrida.cal_id,corrida.forecast_date])
			ups_corrida.id  = corridas.rows[0].id
			ups_corrida.forecast_date = corridas.rows[0].date
			ups_corrida.cal_id = corridas.rows[0].cal_id
			ups_corrida.series = []
			// return Promise.all(corrida.series.map(s=>{
				// 	return Promise.all(s.pronosticos.map(p=>{
				// 		var pronostico = {
				// 			cor_id: prono.cor_id,
				// 			cal_id: prono.cal_id,
				// 			series_id: s.series_id,
				// 			timestart: p.timestart,
				// 			timeend: p.timeend,
				// 			valor: p.valor,
				// 			qualifier: (p.qualifier) ? p.qualifier : 'main' 
				// 		} 
				// 		return this.upsertPronostico(client,pronostico)
				// 	}))
				// }))
			for(var i=0;i<corrida.series.length;i++) {
				if(!corrida.series[i].pronosticos || corrida.series[i].pronosticos.length == 0) {
                                        console.log("series " + i + " of corrida is empty (missing pronosticos)")
					continue
				}
				var pronosticos = corrida.series[i].pronosticos.map(p=>{
					if(Array.isArray(p)) {
						return {
							cor_id: ups_corrida.id,
							cal_id: ups_corrida.cal_id,
							series_id: corrida.series[i].series_id,
							series_table: corrida.series[i].series_table,
							timestart: new Date(p[0]),
							timeend: new Date(p[1]),
							valor: p[2],
							qualifier: (p[3]) ? p[3] : (corrida.series[i].qualifier) ? corrida.series[i].qualifier : 'main' 
						} 
					}
					return {
						cor_id: ups_corrida.id,
						cal_id: ups_corrida.cal_id,
						series_id: corrida.series[i].series_id,
						series_table: corrida.series[i].series_table,
						timestart: new Date(p.timestart),
						timeend: new Date(p.timeend),
						valor: p.valor,
						qualifier: (p.qualifier) ? p.qualifier : (corrida.series[i].qualifier) ? corrida.series[i].qualifier : 'main' 
					} 
				})
				var ups_pronos = await this.upsertPronosticos(client,pronosticos)
				ups_corrida.series[i] = {
					"series_id": corrida.series[i].series_id,
					"series_table": corrida.series[i].series_table,
					"pronosticos": ups_pronos
				}
			}
			await client.query("COMMIT")
		}
		catch (e) {
			await client.query("ROLLBACK")
			return Promise.reject(e)
		}
		finally {
			client.release()
		}
		return Promise.resolve(ups_corrida)
	}	

	static async guardarCorridas(cor_id,filter={},options={}) {
		if(!cor_id ) {
			return Promise.reject("crud.guardarCorridas: Missing parameter cor_id")
		}
		else if(Array.isArray(cor_id)) {
			for(var i in cor_id) {
				if(parseInt(cor_id[i]).toString() == "NaN") {
					return Promise.reject("crud.guardarCorridas: Bad parameter cor_id")
				}
			}
			var date_filter = ""
			date_filter += (filter.timestart) ? " AND pronosticos.timestart>='" + new Date(timestart).toISOString() + "' " : "" 
			date_filter += (filter.timeend) ?  " AND pronosticos.timeend<='" +  new Date(timeend).toISOString() + "' " : ""
			date_filter += (options.only_prono) ? " AND pronosticos.timestart>=corridas.date" : ""
			try {
				var client = await global.pool.connect()
				await client.query("BEGIN")
				var query = "WITH to_save AS (select * from corridas where id IN (" + cor_id.join(",") + ")) \
				DELETE FROM corridas_guardadas USING to_save WHERE corridas_guardadas.date=to_save.date AND corridas_guardadas.cal_id=to_save.cal_id"
				// console.log(query)
				await client.query(query)
				query = "WITH to_save AS (select * from corridas where id IN (" + cor_id.join(",") + ")) \
				INSERT INTO corridas_guardadas \
				SELECT * FROM to_save \
				ON CONFLICT (cal_id,date) DO NOTHING"
				// console.log(query)
				await client.query(query)
				query = "INSERT INTO pronosticos_guardados \
				SELECT pronosticos.* from pronosticos,corridas \
				WHERE pronosticos.cor_id=corridas.id \
				AND cor_id IN (" + cor_id.join(",") + ") \
				" + date_filter + " \
				ON CONFLICT (cor_id,series_id,timestart,timeend,qualifier) DO NOTHING"
				// console.log(query)
				await client.query(query)
				query = "insert into valores_prono_num_guardados select valores_prono_num.* from valores_prono_num,pronosticos,corridas where pronosticos.cor_id=corridas.id AND  valores_prono_num.prono_id=pronosticos.id and pronosticos.cor_id IN (" + cor_id.join(",") + ") " + date_filter + " ON CONFLICT (prono_id) DO NOTHING"
				// console.log(query)
				await client.query(query)
				await client.query("COMMIT")
			} catch (e) {
				await client.query("ROLLBACK")
				return Promise.reject("crud.guardarCorridas:" +e.toString())
			}
			finally {
				client.release()
			}
			return this.getCorridasGuardadas(cor_id)
		} else if(parseInt(cor_id).toString() == "NaN") {
			return Promise.reject("crud.guardarCorridas: Bad parameter cor_id")
		}
		try {
			var client = await global.pool.connect()
			await client.query("BEGIN")
			var query = "WITH to_save AS (select * from corridas where id=$1) \
			DELETE FROM corridas_guardadas USING to_save WHERE corridas_guardadas.date=to_save.date AND corridas_guardadas.cal_id=to_save.cal_id"
			await client.query(query,[cor_id])
			query ="WITH to_save AS (select * from corridas where id=$1) \
			INSERT INTO corridas_guardadas \
			SELECT * FROM to_save ON CONFLICT (cal_id,date) DO NOTHING"
			await client.query(query,[cor_id])
			var query = "insert into pronosticos_guardados select * from pronosticos,corridas where pronosticos.cor_id=corridas.id AND  cor_id=$1 " + date_filter + " ON CONFLICT DO NOTHING"
			// console.log(this.internal.utils.pasteIntoSQLQuery(query,[cor_id]))
			await client.query(query,[cor_id])
			await client.query("insert into valores_prono_num_guardados select valores_prono_num.* from valores_prono_num,pronosticos,corridas where pronosticos.cor_id=corridas.id AND valores_prono_num.prono_id=pronosticos.id and pronosticos.cor_id=$1 " + date_filter + " ON CONFLICT DO NOTHING",[cor_id])
			await client.query("COMMIT")
		} catch (e) {
			await client.query("ROLLBACK")
			return Promise.reject("crud.guardarCorrida:" +e.toString())
		}
		finally {
			client.release()
		}
		return this.getCorridasGuardadas(cor_id)
	}
	

	static async getCorridasGuardadas(cor_id,cal_id,forecast_timestart,forecast_timeend,forecast_date,timestart,timeend,qualifier,estacion_id,var_id,includeProno=false,isPublic,series_id,series_metadata,cal_grupo_id,group_by_qualifier) {
		// console.log({includeProno:includeProno, isPublic: isPublic})
		var public_filter = (isPublic) ? "AND calibrados.public=true" : ""
		var grupo_filter = (cal_grupo_id) ? "AND calibrados.grupo_id=" + parseInt(cal_grupo_id) : ""
		var cor_id_filter = (cor_id) ? (Array.isArray(cor_id)) ? " AND corridas_guardadas.id IN (" + cor_id.join(",") + ")" : " AND corridas_guardadas.id=" + cor_id : ""
		var cor_id_filter2 = (cor_id) ? (Array.isArray(cor_id)) ? " AND pronosticos_guardados.cor_id IN (" + cor_id.join(",") + ")" : " AND pronosticos_guardados.cor_id=" + cor_id : ""
		var pronosticos = []
		return global.pool.query("SELECT corridas_guardadas.id,\
		corridas_guardadas.date,\
		corridas_guardadas.cal_id\
			FROM corridas_guardadas, calibrados\
			WHERE corridas_guardadas.cal_id=coalesce($1,corridas_guardadas.cal_id)\
			AND corridas_guardadas.date>=coalesce($2::timestamptz,'1970-01-01'::date)\
			AND corridas_guardadas.date<=coalesce($3::timestamptz,'2100-01-01'::date)\
			AND corridas_guardadas.date=coalesce($4::timestamptz,corridas_guardadas.date)\
			AND corridas_guardadas.cal_id=calibrados.id\
			" + public_filter + "\
			" + grupo_filter + "\
			" + cor_id_filter + "\
			ORDER BY corridas_guardadas.cal_id, corridas_guardadas.date",[cal_id,forecast_timestart,forecast_timeend,forecast_date])
			.then(result=>{
				if(!result.rows) {
					return
				}
				if(!includeProno) {
					return result.rows.map(r=>{
						return {cor_id:r.id,cal_id:r.cal_id,forecast_date:r.date}
					})
				}
				return Promise.all(result.rows.map(r=>{
					return global.pool.query("SELECT series.id series_id,\
												   series.estacion_id,\
												   series.var_id,\
												   to_char(pronosticos_guardados.timestart::timestamptz at time zone 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') timestart,\
												   to_char(pronosticos_guardados.timeend::timestamptz at time zone 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') timeend,\
												   valores_prono_num_guardados.valor,\
												   pronosticos_guardados.qualifier\
											from pronosticos_guardados,valores_prono_num_guardados,series  \
											where valores_prono_num_guardados.prono_id=pronosticos_guardados.id\
											and pronosticos_guardados.timestart>=coalesce($2::timestamptz,'1970-01-01'::date)\
											and pronosticos_guardados.timeend<=coalesce($3::timestamptz,'2100-01-01'::date)\
											AND pronosticos_guardados.qualifier=coalesce($4,pronosticos_guardados.qualifier)\
											and series.id=pronosticos_guardados.series_id\
											AND series.estacion_id=coalesce($5,series.estacion_id)\
											AND series.var_id=coalesce($6,series.var_id)\
											AND series.id=coalesce($7,series.id)\
											AND pronosticos_guardados.cor_id=$1\
											ORDER BY pronosticos_guardados.series_id,pronosticos_guardados.timestart",[r.id,timestart,timeend,qualifier,estacion_id,var_id,series_id])
				.then(result=>{
					if(result.rows) {
						var series = {}
						if(group_by_qualifier) {   // one series element for each series_id+qualifier combination
							result.rows.forEach(p=>{
								var key = p.series_id + "_" + p.qualifier
								if(!series[key]) {
									series[key] = {series_id:p.series_id,estacion_id:p.estacion_id,var_id:p.var_id,qualifier:p.qualifier,pronosticos:[]}
								}
								series[key].pronosticos.push({timestart:p.timestart,timeend:p.timeend,valor:p.valor}) // cor_id:r.cor_id,series_id:p.series_id,
							})
						} else {    // one series element for each series_id, regardless of qualifier (results in mixed qualifier series)
							result.rows.forEach(p=>{
								if(!series[p.series_id]) {
									series[p.series_id] = {series_id:p.series_id,estacion_id:p.estacion_id,var_id:p.var_id,pronosticos:[]}
								}
								series[p.series_id].pronosticos.push({timestart:p.timestart,timeend:p.timeend,valor:p.valor,qualifier:p.qualifier}) // cor_id:r.cor_id,series_id:p.series_id,
							})
						}
						var series_data = Object.keys(series).sort().map(k=>series[k]) 
						var corrida = {cor_id:r.id,cal_id:r.cal_id,forecast_date:r.date,series:series_data}
						if(series_metadata) {
							var promises = []
							corrida.series.forEach(serie=>{
								promises.push(this.getSerie("puntual",serie.series_id)
								.then(result=>{
									serie.metadata = result
									return
								}))
							})
							return Promise.all(promises)
							.then(()=>{
								return corrida
							})
						} else {
							return corrida
						}
					} else {
						return
					}
				})
			}))
		})
	}

	
	
	static async upsertPronostico(client,pronostico) {
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		// console.log([pronostico.series_id,pronostico.cor_id,pronostico.timestart,pronostico.timeend,pronostico.qualifier].join(","))
		var timeend = (pronostico.timeend) ? pronostico.timeend : pronostico.timestart
		if(pronostico.series_table && pronostico.series_table == "series_areal") {
			const queryText = "INSERT INTO pronosticos_areal (series_id,cor_id,timestart,timeend,qualifier,valor)\
			VALUES ($1,$2,$3::timestamptz,$4::timestamptz,$5,$6)\
			ON CONFLICT (series_id,cor_id,timestart,timeend,qualifier)\
			DO update set timestart=excluded.timestart\
			RETURNING *"
			// console.log([pronostico.series_id,pronostico.cor_id,pronostico.timestart,timeend,pronostico.qualifier,pronostico.valor])
			const result = await client.query(queryText,[pronostico.series_id,pronostico.cor_id,pronostico.timestart,timeend,pronostico.qualifier,pronostico.valor])
			if(!result.rows.length) {
				console.error("Nothing insterted into pronosticos_areal")
			} else {
				// console.log("inserted row: "+ JSON.stringify([pronostico.series_id,pronostico.cor_id,pronostico.timestart,timeend,pronostico.qualifier,pronostico.valor]) + " into pronosticos_areal")
			}
			if(release_client) {
				client.release()
			}
			return new internal.pronostico({...result.rows[0],series_table:"series_areal"})
		} else {
			const queryText = "INSERT INTO pronosticos (series_id,cor_id,timestart,timeend,qualifier)\
				VALUES ($1,$2,$3::timestamptz,$4::timestamptz,$5)\
				ON CONFLICT (series_id,cor_id,timestart,timeend,qualifier)\
				DO update set timestart=excluded.timestart\
				RETURNING *"
			var result = await client.query(queryText,[pronostico.series_id,pronostico.cor_id,pronostico.timestart,timeend,pronostico.qualifier])
			if(result.rows.length == 0) {
				console.error("No se inserto observacion")
				if(release_client) {
					client.release()
				}
				return Promise.reject("No se inserto observacion")
				//~ return client.query('ROLLBACK')
				//~ .then( res=> {
					//~ client.release()
					//~ return
				//~ })
			} else {
				var prono = result.rows[0]
				const insertValorText = "INSERT INTO valores_prono_num (prono_id,valor)\
					VALUES ($1,$2)\
					ON CONFLICT (prono_id)\
					DO UPDATE SET valor=excluded.valor\
					RETURNING *"
				result = await client.query(insertValorText, [prono.id, pronostico.valor])
				prono.valor=result.rows[0].valor
				prono.series_table = "series"
				if(release_client) {
					client.release()
				}
				return new internal.pronostico(prono)
			}
		}
	}
	
	static async upsertPronosticos(client,pronosticos) {
		var release_client = false
		if(!client) {
			client = await global.pool.connect()
			release_client = true
		} 
		// filter by series_table
		var values = [] 
		var values_areal = [] 
		var values_rast = []
		for (var [i, p] of pronosticos.entries()) {
			var timeend = (p.timeend) ? p.timeend : p.timestart
			// console.debug([p.series_id,p.cor_id,p.timestart.toISOString(),timeend.toISOString(),p.qualifier,p.valor])
			if(p.series_table && p.series_table == "series_areal") {
				const args = [
					p.series_id, 
					p.cor_id, 
					p.timestart.toISOString(),
					timeend.toISOString(),
					p.qualifier,
					parseFloat(p.valor)
				]
				try {
					control_query_args(
						[
							{name: "series_id", type:"integer"},
							{name: "cor_id", type:"integer"},
							{name: "timestart", type:"string"},
							{name: "timeend", type:"string"},
							{name: "qualifier", type:"string"},
							{name: "valor", type:"float"}
						],
						args
					)
				} catch(e) {
					throw(new Error("Invalid values at index [" + i + "]: " + e.toString()))
				}
				values_areal.push(vsprintf("(%d,%d,'%s'::timestamptz,'%s'::timestamptz,'%s',%f)", args))
			} else if(p.series_table && p.series_table == "series_rast") {
				const args = [
					p.series_id, 
					p.cor_id, 
					p.timestart.toISOString(), 
					timeend.toISOString(), 
					p.qualifier, 
					// new Buffer.from(p.valor).toString('hex')
					(p.valor instanceof Buffer) ?  "\\x" + p.valor.toString('hex') : p.valor
				]
				try {
					control_query_args(
						[
							{name: "series_id", type:"integer"},
							{name: "cor_id", type:"integer"},
							{name: "timestart", type:"string"},
							{name: "timeend", type:"string"},
							{name: "qualifier", type:"string"},
							{name: "valor", type:"string"}
						],
						args
					)
				} catch(e) {
					throw(new Error("Invalid values at index [" + i + "]: " + e.toString()))
				}
				values_rast.push(vsprintf("(%d,%d,'%s'::timestamptz,'%s'::timestamptz,'%s',ST_FromGDALRaster('%s'))", args))
			} else {
				const args = [
					p.series_id, 
					p.cor_id, 
					p.timestart.toISOString(), 
					timeend.toISOString(), 
					p.qualifier, 
					parseFloat(p.valor)
				]
				try {
					control_query_args(
						[
							{name: "series_id", type:"integer"},
							{name: "cor_id", type:"integer"},
							{name: "timestart", type:"string"},
							{name: "timeend", type:"string"},
							{name: "qualifier", type:"string"},
							{name: "valor", type:"float"}
						],
						args
					)
				} catch(e) {
					throw(new Error("Invalid values at index [" + i + "]: " + e.toString()))
				}	
				values.push(vsprintf("(%d,%d,'%s'::timestamptz,'%s'::timestamptz,'%s',%f)", args))
			}
		}
		var pronosticos_result = []
		if(values_areal.length) {
			var stmt = `INSERT INTO pronosticos_areal (series_id,cor_id,timestart,timeend,qualifier,valor) \
			VALUES ${values_areal.join(",")} ON CONFLICT (series_id,cor_id,timestart,timeend,qualifier)\
			DO UPDATE SET valor=excluded.valor\
			RETURNING id,series_id,'series_areal' as series_table,cor_id,timestart,timeend,qualifier,valor`
			const result_areal = await client.query(stmt)
			pronosticos_result.push(...result_areal.rows)
		}
		if(values_rast.length) {
			var stmt = `INSERT INTO pronosticos_rast (series_id,cor_id,timestart,timeend,qualifier,valor) \
			VALUES ${values_rast.join(",")} ON CONFLICT (series_id,cor_id,timestart,qualifier)\
			DO UPDATE SET valor=excluded.valor, timeend=excluded.timeend\
			RETURNING id,series_id,'series_rast' as series_table,cor_id,timestart,timeend,qualifier,valor`
			const result_rast = await client.query(stmt)
			pronosticos_result.push(...result_rast.rows)
		}
		if(values.length) {
			try {
				var stmt = "CREATE TEMPORARY TABLE prono_tmp (series_id int,cor_id int,timestart timestamp,timeend timestamp,qualifier varchar,valor real);\
				INSERT INTO prono_tmp (series_id,cor_id,timestart,timeend,qualifier,valor)\
				VALUES " + values + ";"
				await client.query(stmt)
				await client.query("INSERT INTO pronosticos (series_id,cor_id,timestart,timeend,qualifier)\
					SELECT series_id,cor_id,timestart,timeend,qualifier \
					FROM prono_tmp \
					ON CONFLICT (series_id,cor_id,timestart,timeend,qualifier)\
					DO update set timestart=excluded.timestart;")
				const result = await client.query("WITH inserted_valores AS (\
					INSERT INTO valores_prono_num (prono_id,valor)\
						SELECT pronosticos.id,prono_tmp.valor\
						FROM pronosticos,prono_tmp\
						WHERE pronosticos.series_id=prono_tmp.series_id\
						AND pronosticos.timestart=prono_tmp.timestart\
						AND pronosticos.timeend=prono_tmp.timeend\
						AND pronosticos.qualifier=prono_tmp.qualifier\
						AND pronosticos.cor_id=prono_tmp.cor_id\
					ON CONFLICT (prono_id)\
					DO UPDATE SET valor=excluded.valor\
					RETURNING *\
				) SELECT pronosticos.id,pronosticos.series_id,'series' as series_table,pronosticos.cor_id,pronosticos.timestart,pronosticos.timeend,pronosticos.qualifier,inserted_valores.valor\
					FROM pronosticos,inserted_valores\
					WHERE pronosticos.id=inserted_valores.prono_id\
					ORDER BY pronosticos.series_id,pronosticos.qualifier,pronosticos.timestart;")
				console.log("upserted " + result.rows.length + " pronosticos rows")
				await client.query("DROP TABLE prono_tmp")
				pronosticos_result.push(...result.rows)
			} catch(e) {
				return Promise.reject(e)
			}
		}
		console.log("upserted " + pronosticos_result.length + " pronosticos")
		if(release_client) {
			client.release(true)
		}
		return Promise.resolve(pronosticos_result)
	}
	
	static async getSeriesBySiteAndVar(
		estacion_id,
		var_id,
		startdate,
		enddate,
		includeProno=true,
		regular=false,
		dt="1 days",
		proc_id,
		isPublic,
		forecast_date,
		series_id,
		tipo="puntual",
		from_view=true,
		get_cal_stats
	) {
		var stmt
		var params
		if(series_id) {
			if(tipo == "areal") {
				stmt = `SELECT
					'area' AS tipo,
					series_areal.id,
					fuentes.public
				FROM series_areal
				JOIN fuentes ON fuentes.id=series_areal.fuentes_id
				WHERE series_areal.id = $1`
				params = [ series_id ]
			} else if (tipo == "raster") {
				stmt = `SELECT 
					'raster' AS tipo,
					series_rast.id,
					fuentes.public
				FROM series_rast
				JOIN escenas ON series_rast.escena_id = escenas.id
				JOIN fuentes ON fuentes.id=series_rast.fuentes_id
				WHERE series.id = $1`
				params = [ series_id ]
			}else {
				stmt = `SELECT 
					'puntual' AS tipo,
					series.id,
					redes.public
				FROM series
				JOIN estaciones ON series.estacion_id = estaciones.unid
				JOIN redes ON estaciones.tabla = redes.tabla_id
				WHERE series.id = $1`
				params = [ series_id ]
			}
		} else {
			proc_id = (proc_id) ? proc_id : (var_id == 4) ? 2 : 1
			stmt = "SELECT series.id,redes.public from series,estaciones,redes where estacion_id=$1 and var_id=$2 and proc_id=$3 and series.estacion_id=estaciones.unid AND estaciones.tabla=redes.tabla_id "
			params = [ estacion_id, var_id, proc_id ]
		}
		// console.log("getSeriesBySiteAndVar at "  + Date()) 
		const result = await global.pool.query(stmt, params)
		// console.log("got series at " + Date())
		if(!result.rows) {
			// console.log("No series rows returned")
			return 
		}
		if(result.rows.length == 0) {
			// console.log("0 series rows returned")
			return 
		}
		if(isPublic) {
			if (result.rows[0].public == false) {
				// console.log("series not public")
				throw("El usuario no está autorizado para acceder a esta serie")
			}
		}
		// console.log("get serie tipo" + tipo + " id " + result.rows[0].id)
		const serie = await this.getSerie(tipo,result.rows[0].id,startdate,enddate,{asArray:true,regular: regular, dt: dt})  // (tipo,id,timestart,timeend,options)
		// console.log("got serie at " + Date())
		if(includeProno) {
			const series_sim = await this.getSeries(tipo,{estacion_id: serie.estacion.id, var_id: serie.var.id, unit_id: serie.unidades.id, fuentes_id: (serie.fuente) ? serie.fuente.id : undefined},{fromView:from_view})
			if(!series_sim.length) {
				console.log("No series sim found with id " + series_id + ", tipo " + tipo)
				serie.pronosticos = []
				return serie
			}
			// console.log(JSON.stringify({series_sim:series_sim.map(s=>s.id)}))
			const calibrados = await this.getCalibrados(estacion_id,var_id,true,startdate,enddate,undefined,undefined,undefined,isPublic,undefined,undefined,undefined,forecast_date,undefined,series_sim.map(s=>s.id),undefined,serie.tipo)
			serie.pronosticos = calibrados
			if(get_cal_stats) {
				for(const calibrado of serie.pronosticos) {
					if(calibrado.corrida == undefined || calibrado.corrida.series == undefined || !calibrado.corrida.series.length) { continue }
					calibrado.cal_stats = {}
					for(const serie_prono of calibrado.corrida.series) {
						calibrado.cal_stats[serie_prono.qualifier ?? "main"] = getCalStats(
							serie.observaciones, 
							serie_prono.pronosticos,
							calibrado.dt
						)
					}
				}
			}
			if(serie.pronosticos) {
				console.log("getSeriesBySiteAndVar with prono of length "  + serie.pronosticos.
			length)
			} else {
				console.log("getSeriesBySiteAndVar with no pronosticos")
			}
			return serie
		} else {
			//~ console.log("getSeriesBySiteAndVar done at "  + Date())
			return serie
		}
	}

	static async getMonitoredVars(tipo="puntual",GeneralCategory) {
		var filter_string = internal.utils.control_filter2(
			{
				tipo: {type: "string"},
				"GeneralCategory": {type: "string", table: "var"}
			},
			{tipo:tipo,GeneralCategory:GeneralCategory},
			"monitored_vars"
		)
		return global.pool.query(`SELECT 
			monitored_vars.tipo,
			monitored_vars.id,
			monitored_vars.nombre,
			monitored_vars.tipo2,
			var."GeneralCategory" 
		FROM monitored_vars
		JOIN var ON var.id=monitored_vars.id 
		${filter_string}`)
		.then(result=>{
			if(result.rows) {
				return result.rows
			} else {
				return []
			}
		})
		.catch(e=>{
			if(config.verbose) {
				console.error(e)
			} else {
				console.error(e.toString())
			}
		})
	}

	static async getMonitoredFuentes(tipo="puntual",var_id,isPublic) {
		var public_filter = (isPublic) ? " AND public=true" : ""
		return global.pool.query("SELECT tipo,var_id,fuentes_id,nombre,public from monitored_fuentes where tipo=$1 AND var_id=$2" + public_filter,[tipo,var_id])
		.then(result=>{
			if(result.rows) {
				return result.rows
			} else {
				return []
			}
		})
		.catch(e=>{
			if(config.verbose) {
				console.error(e)
			} else {
				console.error(e.toString())
			}
		})
	}
	
	
	// tabprono
	
	static async insertTabprono(tabprono_geojson,insert_obs=true) {
		var promises = []
		tabprono_geojson.features.forEach(f=>{
			promises.push(global.pool.query("INSERT INTO tabprono_parana (unid, estacion_nombre, geom, fecha_hoy, altura_hoy, mes, altura_media_mes, nivel_de_alerta, nivel_de_evacuacion, fecha_pronostico, altura_pronostico_min, altura_pronostico, altura_pronostico_max, estado_pronostico, fecha_tendencia, altura_tendencia_min, altura_tendencia, altura_tendencia_max, estado_tendencia,valor) VALUES\
			($1, $2, st_setsrid(st_point($3,$4),4326), $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, null)\
			ON CONFLICT (unid) DO UPDATE SET estacion_nombre=excluded.estacion_nombre, geom=excluded.geom, fecha_hoy=excluded.fecha_hoy, altura_hoy=excluded.altura_hoy, mes=excluded.mes, altura_media_mes=excluded.altura_media_mes, nivel_de_alerta=excluded.nivel_de_alerta, nivel_de_evacuacion=excluded.nivel_de_evacuacion, fecha_pronostico=excluded.fecha_pronostico, altura_pronostico_min=excluded.altura_pronostico_min, altura_pronostico=excluded.altura_pronostico,altura_pronostico_max=excluded.altura_pronostico_max, estado_pronostico=excluded.estado_pronostico, fecha_tendencia=excluded.fecha_tendencia, altura_tendencia_min=excluded.altura_tendencia_min, altura_tendencia=excluded.altura_tendencia, altura_tendencia_max=excluded.altura_tendencia_max, estado_tendencia=excluded.estado_tendencia, valor=excluded.valor\
			RETURNING *",[f.properties.estacion_id, f.properties.estacion_nombre, f.geometry.coordinates[0], f.geometry.coordinates[1], f.properties.nivel_hoy[0], f.properties.nivel_hoy[1], f.properties["altura_media_mensual(1994-2018)"][0], f.properties["altura_media_mensual(1994-2018)"][1], f.properties.nivel_de_alerta, f.properties.nivel_de_evacuacion, f.properties.pronostico[0], f.properties.pronostico[1], f.properties.pronostico[2], f.properties.pronostico[3], f.properties.estado_pronostico, f.properties.tendencia[0], f.properties.tendencia[1], f.properties.tendencia[2], f.properties.tendencia[3], f.properties.estado_tendencia]))
		})
		return Promise.all(promises)
		.then(result=>{
			global.pool.query("insert into tabprono_parana_historia (unid, estacion_nombre, geom, fecha_hoy, altura_hoy, mes, altura_media_mes, nivel_de_alerta, nivel_de_evacuacion, fecha_pronostico, altura_pronostico, estado_pronostico, fecha_tendencia, altura_tendencia, estado_tendencia) select unid, estacion_nombre, geom, fecha_hoy, altura_hoy, mes, altura_media_mes, nivel_de_alerta, nivel_de_evacuacion, fecha_pronostico, altura_pronostico, estado_pronostico, fecha_tendencia, altura_tendencia, estado_tendencia from tabprono_parana ON CONFLICT (unid,fecha_hoy) do nothing")
			.then(()=>{
				console.log("Insert into tabprono_parana_historia OK")
			}).catch(e=>{
				if(config.verbose) {
					console.error(e)
				} else {
					console.error(e.toString())
				}
			})
			if(insert_obs) {
				// upsert pronosticos
				var pronos=[]
				const prono_sid = {19: 1541, 20: 3381, 23: 3382, 24: 3383, 26: 3384, 29: 3385, 30: 1543, 34: 3387} // id de serie var_id=2 proc_id=8
				result.forEach(r=>{
					r = r.rows[0]
					//~ console.log(r)
					pronos.push(new internal.observacion({tipo: "puntual", series_id: prono_sid[r.unid], timestart: r.fecha_hoy, timeend: r.fecha_hoy, valor: r.altura_hoy}))
					pronos.push(new internal.observacion({tipo: "puntual", series_id: prono_sid[r.unid], timestart: r.fecha_pronostico, timeend: r.fecha_pronostico, valor: r.altura_pronostico}))
					pronos.push(new internal.observacion({tipo: "puntual", series_id: prono_sid[r.unid], timestart: r.fecha_tendencia, timeend: r.fecha_tendencia, valor: r.altura_tendencia}))
				})
				return this.upsertObservaciones(pronos)
			} else {
				return result.map(r=>r.rows[0])
			}
		})
	}
	
	// tools
	
	static async points2rast(points,metadata={},options={},upsert) {
		var outputdir = (options.outputdir) ? path.resolve(options.outputdir) : path.resolve(this.config.pp_cdp.outputdir)
		var nmin=(options.nmin) ? options.nmin : this.config.pp_cdp.nmin;
		var radius1= (options.radius1) ? options.radius1 : this.config.pp_cdp.radius1;
		var radius2= (options.radius2) ? options.radius2 : this.config.pp_cdp.radius2;
		var out_x =  (options.out_x) ? options.out_x : this.config.pp_cdp.out_x;
		var out_y =  (options.out_y) ? options.out_y : this.config.pp_cdp.out_y;
		var nullvalue = (options.nullvalue) ? options.nullvalue : this.config.pp_cdp.nullvalue;
		if(!metadata.series_id) {
			metadata.series_id = this.config.pp_cdp.series_id
		}
		var method_ = (options.method) ? options.method : this.config.pp_cdp.method;
		var method
		{
			switch (method_.toLowerCase()) {
				case "invdist":
					method = "invdist:radius1=" + radius1 + ":radius2=" + radius2 + ":max_points=4:min_points=1:nodata=" + nullvalue;
					break;
				case "nearest":
					method = "nearest:radius1=" + radius1 + ":radius2=" + radius2 + ":angle=0:nodata=" + nullvalue;
					break;
				case "linear":
					method = "linear:radius=" + radius1 + ":nodata=" + nullvalue;
					break;
				deafult:
					return Promise.reject("Método incorrecto. Válidos: invdist, nearest, linear");
			}
		}
		var target_extent = (options.target_extent) ? options.target_extent : this.config.pp_cdp.target_extent
		var roifile = (options.roifile) ? options.roifile : path.resolve(this.config.pp_cdp.roifile)
		var srs = (options.srs) ? parseInt(srs) : this.config.pp_cdp.srs
		var makepng = (options.makepng) ? options.makepng : this.config.pp_cdp.makepng 
		var rand = sprintf("%08d",Math.random()*100000000)
		var geojsonfile= (options.geojsonfile) ? path.resolve(options.geojsonfile) : "/tmp/points_"+rand+".geojson"
		var rasterfile="/tmp/grid_"+rand+".tif";
		var rasternonull="/tmp/grid_nonull_"+rand+".tif";
		var tempresultfile="/tmp/grid_nonull_crop_"+rand+".tif";
		var warpedfile= "/tmp/grid_nonull_crop_warped_"+rand+".tif";
		var rules_file = path.resolve( (options.tipo) ? (options.tipo == "diario") ? this.config.pp_cdp.rules_file_diario : this.config.pp_cdp.rules_file_semanal : this.config.pp_cdp.rules_file )
		var zfield = (options.zfield) ? options.zfield : this.config.pp_cdp.zfield
		if(options.output) {
			if(!validFilename(options.output)) {
				return Promise.reject("invalid output filename")
			}
		}
		var resultfile= (options.output) ? outputdir + "/" + options.output : path.resolve(this.config.pp_cdp.outputdir) + "/rast_" + method_ + "_" + radius1 + "_" + radius2 + "_" + out_x + "_" + out_y + ".tif" 
		var pngfile= resultfile.replace(/\.tif$/,".png"); // "/home/alerta5/13-SYNOP/mapas_semanales_gdal/pp_semanal_idw_$label_date.png";

		return global.pool.query("with p as (\
			select st_transform(st_setsrid(st_point($1, $2),4326),$5::int) bl,\
			       st_transform(st_setsrid(st_point($3, $4),4326),$5::int) tr )\
			select st_x(p.bl),st_y(p.bl),st_x(p.tr),st_y(p.tr) from p",[target_extent[0][0],target_extent[0][1],target_extent[1][0],target_extent[1][1],srs])
		.then(result=>{
			if(result.rows.length==0) {
				throw "extent reprojection error"
			}
			return ogr2ogr(points).format("GeoJSON").options(['-t_srs','EPSG:'+srs]).promise()
		}).then(data=>{
			return fs.writeFile(geojsonfile,JSON.stringify(data))
		}).then(()=> {
			return pexec("gdal_grid -txe " + target_extent[0][0] + " " + target_extent[1][0] + " -tye " + target_extent[0][1] + " " + target_extent[1][1] + " -outsize " + out_x + " " + out_y + " -zfield " + zfield + " -ot Float32 -a " + method + " " + geojsonfile + " " + rasterfile)
		}).then(result=>{
			if(result.stdout && config.verbose) {
				console.log("crud.points2rast: stdout: " + result.stdout)	
			}
			return pexec("gdal_translate -a_nodata " + nullvalue + " " + rasterfile + " " + rasternonull)
		}).then(result=>{
			if(result.stdout && config.verbose) {
				console.log("crud.points2rast: stdout: " + result.stdout)
			}
			if(result.stderr && config.verbose) {
				console.error(result.stderr)
			}
			return pexec("gdalwarp -dstnodata " + nullvalue + " -cutline " + roifile + " " + rasternonull + " " + tempresultfile) //("gdal_calc.py --overwrite  -A " + rasternonull + " -B " + roifile + " --outfile=" + tempresultfile + " --NoDataValue=-9999 --calc=\"A*B\"")
		}).then(result=>{
			if(result.stdout && config.verbose) {
				console.log("crud.points2rast: stdout: " + result.stdout)
			}
			if(result.stderr && config.verbose) {
				console.error("crud.points2rast: stderr: " + result.stderr)
			}
			return pexec("gdal_translate -a_nodata NAN " + tempresultfile + " " + resultfile)
		}).then(result=>{
			if(result.stdout  && config.verbose) {
				console.log("crud.points2rast: stdout: " + result.stdout)
			}
			if(result.stderr  && config.verbose) {
				console.error(result.stderr)
			}
			return pexec("gdalwarp -t_srs EPSG:" + srs + " -srcnodata -9999 -dstnodata nan -ot Float32 -overwrite " + tempresultfile + " " + resultfile);
		}).then(result=>{
			if(result.stdout  && config.verbose) {
				console.log("crud.points2rast: stdout: " + result.stdout)
			}
			if(result.stderr  && config.verbose) {
				console.error(result.stderr)
			}
			return pexec("gdalwarp -t_srs EPSG:4326 -dstnodata -9999 -overwrite " + tempresultfile + " " + warpedfile)
		}).then(result=>{
			if(result.stdout  && config.verbose) {
				console.log("crud.points2rast: stdout: " + result.stdout)
			}
			if(result.stderr  && config.verbose) {
				console.error(result.stderr)
			}
			if(makepng) {
				return pexec("gdaldem color-relief " + tempresultfile + " " + rules_file + " " + pngfile + " -of PNG -alpha")
			} else {
				return Promise.resolve()
			}
		}).then(result=>{
			if(result.stdout && config.verbose) {
				console.log("crud.points2rast: stdout: " + result.stdout)
			}
			if(result.stderr && config.verbose) {
				console.error("crud.points2rast: stderr: " + result.stderr)
			}
			return fs.readFile(warpedfile)
		}).then(data=>{
			return {tipo:"rast",series_id:metadata.series_id,timeupdate:metadata.timeupdate,timestart: metadata.timestart, timeend: metadata.timeend, valor: data} 
		}).then(obs=>{
			if(upsert) {
				if(!metadata.series_id) {
					throw("Falta series_id")
				} else {
					//~ console.log("to_update: ts:" + obs.timestart + ", te:" + obs.timeend + ", series_id:" + obs.series_id)
					return this.upsertObservacion(obs)
				}
			} else {
				return obs
			}
		})
	} 
	
	static async getObservacionesPuntuales2Rast(filter={},options={}) {
		var raster_format = (options.format) ? options.format : "GTiff"
		options.format="GeoJSON"
		return this.getObservacionesTimestart(filter,options)
		.then(observaciones=>{
			if(observaciones.length == 0) {
				throw("no observaciones found")
			}
			var metadata = {
				series_id: options.output_series_id,
				timeupdate: observaciones[0].timeupdate,
				timestart: observaciones[0].timestart,
				timeend: observaciones[0].timeend
			}
			return this.points2rast(observaciones,metadata,options)
		})
	}
	
	static async get_pp_cdp_diario(fecha,filter={},options={},upsert) {
		const used = process.memoryUsage();
		for (let key in used) {
		  console.log(`${key} ${Math.round(used[key] / 1024 / 1024 * 100) / 100} MB`);
		}

		var timestart = (fecha) ? new Date(fecha) : new Date(new Date().getTime() - 1000*3600*35)
		if(timestart.toString() == "Invalid Date") {
			return Promise.reject("fecha: Invalid Date")
		}
		timestart = new Date(timestart.getUTCFullYear(),timestart.getUTCMonth(),timestart.getUTCDate(),9)
		var timeend = new Date(timestart.getTime() + 24*3600*1000)
		console.log({ts:timestart,te:timeend})
		filter.estacion_id = (filter.estacion_id) ? filter.estacion_id : this.config.pp_cdp.estacion_ids
		return this.getCampo(1,timestart,timeend,filter,options)
		.then(campo=>{
			if(!options.skip_count_control) {
				var count_synop = campo.series.reduce((count,s)=> count + ((s.estacion.tabla == 'stations') ? 1 : 0),0)
				var count_synop_cdp = campo.series.reduce((count,s)=> count + ((s.estacion.tabla == 'stations_cdp') ? 1 : 0),0)
				console.log({count_synop:count_synop,count_synop_cdp:count_synop_cdp})
				if(count_synop == 0 || count_synop_cdp == 0) {
					throw("Faltan registros SYNOP")
				}
			}
			options.output = "pp_diaria_" + timestart.toISOString().substring(0,10).replace(/-/g,"") + "_nearest.tif"
			if(!fs.existsSync(path.resolve(sprintf("%s/%04d", this.config.pp_cdp.outputdir, timestart.getUTCFullYear())))) {
				fs.mkdirSync(path.resolve(sprintf("%s/%04d", this.config.pp_cdp.outputdir, timestart.getUTCFullYear())))
			}
			if(!fs.existsSync(path.resolve(sprintf("%s/%04d/%02d", this.config.pp_cdp.outputdir, timestart.getUTCFullYear(), timestart.getUTCMonth()+1)))) {
				fs.mkdirSync(path.resolve(sprintf("%s/%04d/%02d", this.config.pp_cdp.outputdir, timestart.getUTCFullYear(), timestart.getUTCMonth()+1)))
			}
			options.outputdir = path.resolve(sprintf("%s/%04d/%02d", this.config.pp_cdp.outputdir, timestart.getUTCFullYear(), timestart.getUTCMonth()+1))
			var csv_file = path.resolve(options.outputdir + "/" + "pp_diaria_" + timestart.toISOString().substring(0,10).replace(/-/g,"") + ".csv")
			// escribe archivo CSV
			fs.writeFile(csv_file,campo.toCSV())
			.catch(e=>{
				console.error(e)
			})
			//~ options.geojsonfile = path.resolve(options.outputdir + "/" + options.output.replace(/\.tif$/,".json"))
			var surf_file = path.resolve(options.outputdir + "/" + "pp_diaria_" + timestart.toISOString().substring(0,10).replace(/-/g,"") + "_surf.tif")
			var png_file =  path.resolve(options.outputdir + "/" + "pp_diaria_" + timestart.toISOString().substring(0,10).replace(/-/g,"") + "_surf.png")
			var geojson_file = path.resolve(options.outputdir + "/" + "pp_diaria_" + timestart.toISOString().substring(0,10).replace(/-/g,"") +".json")
			var nearest_file = path.resolve(options.outputdir + "/" + options.output)
			// genera raster x vecino más próximo y escribe archivo geojson
			return this.points2rast(campo.toGeoJSON(),{series_id:this.config.pp_cdp.series_id,timestart:timestart,timeend:timeend},{...options,geojsonfile:geojson_file,tipo:"diario"},upsert)
			.then(result=>{
				if(upsert && !options.no_update_areales) {
					// calcula medias areales
					this.rast2areal(this.config.pp_cdp.series_id,timestart,timeend,"all")
					.then(result=>{
						console.log("upserted " + result.length + " into series_areal, series_id:" + this.config.pp_cdp.series_id)
					})
					.catch(e=>{
						console.error(e)
					})
				}
				// genera raster x splines
				var surf_parameters = {...this.config.grass,maskfile: path.resolve(this.config.pp_cdp.maskfile),timestart:timestart,timeend:timeend,res:0.1}
				return printMap.surf(geojson_file,surf_file,surf_parameters)
				.then(output=>{
					console.log(output)
					var parameters = {...this.config.grass, timestart: timestart,timeend: timeend, title: "precipitaciones diarias campo interpolado [mm]"}
					parameters.render_file = undefined
					// imprime mapa splines
					return printMap.print_pp_cdp_diario(surf_file,png_file,parameters,geojson_file)
				})
				.then(output=>{
					console.log(output)
					if(options.no_send_data) {
						return {type:"pp_cdp_diario",timestart:result.timestart,timeend:result.timeend,files:{points_geojson:geojson_file,points_csv:csv_file,nearest_tif:nearest_file,nearest_png:nearest_file.replace(/\.tif$/,".png"),surf_tif:surf_file,surf_png:png_file}}
					} else {
						return result
					}
				})
			})
		})
	}
	static async get_pp_cdp_semanal(fecha,filter={},options={}) {
		// toma fecha inicial, si falta, por defecto es hoy - 8 días 
		var timestart = (fecha) ? new Date(fecha) : new Date(new Date().getTime() - 1000*3600*(35 + 7*24))
		if(timestart.toString() == "Invalid Date") {
			return Promise.reject("fecha: Invalid Date")
		}
		timestart = new Date(timestart.getUTCFullYear(),timestart.getUTCMonth(),timestart.getUTCDate(),9)
		// toma fecha final = fecha incial + 7 días
		var timeend = new Date(timestart.getTime() + 7*24*3600*1000)
		console.log({ts:timestart,te:timeend})
		// toma filtro de estaciones del archivo de configuración
		filter.estacion_id = (filter.estacion_id) ? filter.estacion_id : this.config.pp_cdp.estacion_ids
		// obtiene campo de precipitaciones puntuales
		return this.getCampo(1,timestart,timeend,filter,options)
		.then(campo=>{
			// elimina estaciones con menos de 6 registros
			campo.series = campo.series.filter(s=>s.count >= 6)
			// controla que haya estaciones SYNOP con 7 registros
			if(!options.skip_count_control) {
				var count_synop = campo.series.reduce((count,s)=> count + ((s.estacion.tabla == 'stations' && s.count >= 7) ? 1 : 0),0)
				var count_synop_cdp = campo.series.reduce((count,s)=> count + ((s.estacion.tabla == 'stations_cdp' && s.count >= 7) ? 1 : 0),0)
				console.log({count_synop:count_synop,count_synop_cdp:count_synop_cdp})
				if(count_synop == 0 || count_synop_cdp == 0) {
					throw("Faltan registros SYNOP")
				}
			}
			// genera nombre de archivo raster y crea directorios YYYY/MM si no existen
			options.output = "pp_semanal_" + timestart.toISOString().substring(0,10).replace(/-/g,"") + "_nearest.tif"
			if(!fs.existsSync(path.resolve(sprintf("%s/%04d", this.config.pp_cdp.outputdir_semanal, timestart.getUTCFullYear())))) {
				fs.mkdirSync(path.resolve(sprintf("%s/%04d", this.config.pp_cdp.outputdir_semanal, timestart.getUTCFullYear())))
			}
			if(!fs.existsSync(path.resolve(sprintf("%s/%04d/%02d", this.config.pp_cdp.outputdir_semanal, timestart.getUTCFullYear(), timestart.getUTCMonth()+1)))) {
				fs.mkdirSync(path.resolve(sprintf("%s/%04d/%02d", this.config.pp_cdp.outputdir_semanal, timestart.getUTCFullYear(), timestart.getUTCMonth()+1)))
			}
			options.outputdir = path.resolve(sprintf("%s/%04d/%02d", this.config.pp_cdp.outputdir_semanal, timestart.getUTCFullYear(), timestart.getUTCMonth()+1))
			var csv_file = path.resolve(options.outputdir + "/" + "pp_semanal_" + timestart.toISOString().substring(0,10).replace(/-/g,"") + ".csv")
			// escribe archivo CSV
			fs.writeFile(csv_file,campo.toCSV())
			.catch(e=>{
				console.error(e)
			})
			//~ options.geojsonfile = path.resolve(options.outputdir + "/" + options.output.replace(/\.tif$/,".json"))
			var surf_file = path.resolve(options.outputdir + "/" + "pp_semanal_" + timestart.toISOString().substring(0,10).replace(/-/g,"") + "_surf.tif")
			var png_file =  path.resolve(options.outputdir + "/" + "pp_semanal_" + timestart.toISOString().substring(0,10).replace(/-/g,"") + "_surf.png")
			var geojson_file = path.resolve(options.outputdir + "/" + "pp_semanal_" + timestart.toISOString().substring(0,10).replace(/-/g,"") + ".json")
			var nearest_file = path.resolve(options.outputdir + "/" + options.output)
			// genera raster x vecino más próximo y escribe archivo geojson
			return this.points2rast(campo.toGeoJSON(),{timestart:timestart,timeend:timeend},{...options,geojsonfile: geojson_file,tipo:"semanal"})
			.then(result=>{
				var surf_parameters = {...this.config.grass,maskfile: path.resolve(this.config.pp_cdp.maskfile),timestart:timestart,timeend:timeend,res:0.1}
				surf_parameters.tension = 80
				// genera raster x splines
				return printMap.surf(geojson_file,surf_file,surf_parameters)
				.then(output=>{
					console.log(output)
					var parameters = {...this.config.grass, timestart: timestart,timeend: timeend, title: "precipitaciones semanales campo interpolado [mm]"}
					parameters.render_file = undefined
					// imprime mapa splines
					return printMap.print_pp_cdp_semanal(surf_file,png_file,parameters,geojson_file)
				})
				.then(output=>{
					console.log(output)
					if(options.no_send_data) {
						return {type:"pp_cdp_semanal",timestart:result.timestart,timeend:result.timeend,files:{points_geojson:geojson_file,points_csv:csv_file,nearest_tif:nearest_file,nearest_png:nearest_file.replace(/\.tif$/,".png"),surf_tif:surf_file,surf_png:png_file}}
					} else {
						return result
					}
				})
			})
		})
	}
	static async get_pp_cdp_batch(timestart,timeend,filter={},options={},upsert) {
		var timestart_diario = (timestart) ? timeSteps.DateFromDateOrInterval(timestart) : new Date(new Date().getTime() - 1000*3600*(35 + 14*24))
		if(timestart_diario.toString() == "Invalid Date") {
			return Promise.reject("fecha: Invalid Date")
		}
		var timestart_semanal = timestart_diario
		var timeend_diario = (timeend) ? timeSteps.DateFromDateOrInterval(timeend) : new Date(new Date().getTime() - 1000*3600*35)
		var timeend_semanal = new Date(timeend_diario.getTime() - 1000*3600*24*6)
		var fechas_diario = []
		for(var i=timestart_diario.getTime();i<=timeend_diario.getTime();i=i+1000*3600*24) {
			fechas_diario.push(new Date(i))
		}
		if(fechas_diario.length == 0) {
			return Promise.reject("intervalo de fechas nulo")
		}
		var fechas_semanal = []
		for(var i=timestart_semanal.getTime();i<=timeend_semanal.getTime();i=i+1000*3600*24) {
			fechas_semanal.push(new Date(i))
		}
		var all_results = []
		var all_errors = []
		options.no_send_data = true
		return Promise.allSettled(
			fechas_diario.map(fecha => this.get_pp_cdp_diario(fecha,filter,options,upsert))
		).then(result=>{
			result.forEach(r=>{
				if(r.status == 'fulfilled') {
					all_results.push(r.value)
				} else {
					all_errors.push(r.reason)
					console.error(r.reason)
				}
			})
			return Promise.allSettled(
				fechas_semanal.map(fecha => this.get_pp_cdp_semanal(fecha,filter,options))
			)
		}).then(result=>{
			result.forEach(r=>{
				if(r.status == 'fulfilled') {
					all_results.push(r.value)
				} else {
					all_errors.push(r.reason)
					console.error(r.reason)
				}
			})
			if(all_results.length == 0) {
				throw(all_errors)
			} else {
				return all_results
			}
		})
	}
	
	static async checkAuth(user,params={}) {
		switch (user.role) {
			case "admin":
				return true
				break;
			case "writer":
				var series_table = (params.tipo) ? (params.tipo == "puntual") ? "series" : (params.tipo == "areal") ? "series_areal " : (params.tipo == "rast" || params.tipo == "raster") ? "series_rast" : "series" : "series"
				var observaciones_table = (params.tipo) ? (params.tipo == "puntual") ? "observaciones" : (params.tipo == "areal") ? "observaciones_areal" : (params.tipo == "rast" || params.tipo == "raster") ? "observaciones_rast" : "observaciones" : "observaciones"
				var query_promise
				if(params.redId) {
					query_promise = global.pool.query("SELECT 1 FROM redes WHERE user_id=$1 AND id=$2",[user.id,params.redId])
				} else if(params.estacion_id) {
					query_promise = global.pool.query("SELECT 1 FROM estaciones,redes WHERE redes.tabla_id=estaciones.tabla AND redes.user_id=$1 AND estaciones.unid=$2",[user.id,params.estacion_id])
				} else if(params.series_id) {
					query_promise = global.pool.query("SELECT 1 FROM " + series_table + ",estaciones,redes WHERE " + series_table + ".estacion_id=estaciones.unid AND redes.tabla_id=estaciones.tabla AND redes.user_id=$1 AND " + series_table + ".id=$2",[user.id,params.series_id])
				} else if(params.observacion_id) {
					query_promise = global.pool.query("SELECT 1 FROM " + observaciones_table + "," + series_table + ",estaciones,redes WHERE " + observaciones_table + ".series_id=" + series_table + ".id AND " + series_table + ".estacion_id=estaciones.unid AND redes.tabla_id=estaciones.tabla AND redes.user_id=$1 AND " + observaciones_table + ".id=$2",[user.id,params.observaciones_id])
				}
				return query_promise
				.then(result=>{
					if(result.rows.length > 0) {
						return true
					} else {
						return false
					}
				})
				.catch(e=>{
					console.error(e)
					return false
				})
				break;
			case "reader","public":
				return false
				break;
			default:
				return false
		}
	}

	// RALEO (THIN) SERIES - by SERIES_ID OR FUENTES_ID
	static async thinObs(tipo,filter, options,client) { // filter:series_id,timestart,timeend; options: interval={'hours':1},deleteSkipped=false,returnSkipped=false) {
		if(!filter.timestart || !filter.timeend) {
			return Promise.reject("crud.thinObs: Missing filter.timestart filter.timeend")
		}
		var release_client = false
		if(!client) {
			client = await global.pool.connect()
			release_client = true
		}
		options.interval = (options.interval) ? options.interval : {'hours':1}
		if(typeof options.interval == "string") {
			options.interval = parsePGinterval(options.interval)
		}
		// CHECK INTERVAL LOWER LIMIT
		if(timeSteps.interval2epochSync(options.interval) < config.thin.interval_lower_limit) {
			return Promise.reject("crud.thinObs: interval lower limit is " + config.thin.interval_lower_limit)
		}
		// GENERATE TIME SEQUENCE
		var seq = timeSteps.dateSeq(filter.timestart,filter.timeend,options.interval)
		// CASE SINGLE SERIES_ID 
		if(filter.series_id && typeof filter.series_id == "number") {
			var result = await this.thinSeries(tipo,{series_id:filter.series_id,timestart:filter.timestart,timeend:filter.timeend},{interval:options.interval,deleteSkipped:options.deleteSkipped,returnSkipped:options.returnSkipped},seq,client)
			if(release_client) {
				client.release()
			}
			return [
				{
					id:filter.series_id,
					observaciones: result
				}
			]
		// CASE MULTIPLE SERIES_ID OR FUENTES_ID/RED_ID
		} else {
			if(!(tipo == "puntual" && filter.red_id) && !(tipo!="puntual" && filter.fuentes_id) && !filter.series_id) {
				if(release_client) {
					client.release()
				}
				return Promise.reject("Missing filter.series_id OR filter.fuentes_id/red_id")
			}
			filter.id = (filter.series_id) ? filter.series_id : filter.id
			var filter_get_series = {...filter}
			delete filter_get_series.timestart
			delete filter_get_series.timeend
			var series = await this.getSeries(tipo,filter_get_series,{no_metadata:true},client)
			if(series.length == 0) {
				if(release_client) {
					client.release()
				}
				console.log("crud.thinObs: no series found")
				return
			}
			var results = []
			for(var i in series) {
				try {
					var result = await this.thinSeries(tipo,{series_id:series[i].id,timestart:filter.timestart,timeend:filter.timeend},options,seq,client)
				} catch (e) {
					console.error("crud.thinObs: " + e.toString())
					results.push({error:e})
					break
				}
				results.push({values:result})
			}
			var i=-1
			if(release_client) {
				client.release()
			}
			return results.map(r=>{
				i++
				if (r.values) {
					return {
						id: series[i].id,
						observaciones: r.values
					}
				} else {
					return {
						id: series[i].id,
						observaciones: null,
						message: r.error.toString()
					}
				}
			})
		}
	}
	
	static async thinSeries(tipo='puntual',filter={},options={},seq,client) {
		// console.log({options:options})
		if(!filter.series_id || !filter.timestart || !filter.timeend || !options.interval) {
			return Promise.reject("Missing filter.series_id, filter.timestart, filter.timeend, options.interval")
		}
		if(!seq) {
			if(typeof options.interval == "string") {
				options.interval = parsePGinterval(options.interval)
			}
			// CHECK INTERVAL LOWER LIMIT
			if(timeSteps.interval2epochSync(options.interval) < config.thin.interval_lower_limit) {
				return Promise.reject("crud.thinObs: interval lower limit is " + config.thin.interval_lower_limit)
			}
			// console.log("interval:" + JSON.stringify(interval))
			seq = timeSteps.dateSeq(filter.timestart,filter.timeend,options.interval) 
		} 
		return this.getObservaciones(tipo,filter,undefined,client)
		.then(observaciones=>{
			// console.log("got " + observaciones.length + " observaciones.")
			// console.log(observaciones)
			var i = 0
			var result = []
			var skipped = []
			seqLoop:
			for(var j=0;j<seq.length-1;j++) {
				// console.log(seq[j])
				obsLoop:
				for(var k=i;k<observaciones.length;k++) {
					if(observaciones[k].timestart>=seq[j]) {
						if(observaciones[k].timestart < seq[j+1]) {
							result.push(observaciones[k])
							i = k + 1
							break obsLoop
						} else {
							i = k
							break obsLoop
						}
					} else {
						skipped.push(observaciones[k])
						// if(options.deleteSkipped) {
							// console.log(observaciones[k].timestart.toISOString() + ", id:" + observaciones[k].id)
						// 	this.deleteObservacion(tipo,observaciones[k].id)
						// }
					}
				}
			}
			if(options.deleteSkipped) {
				if(skipped.length == 0) {
					console.log("crud/thinObs: nothing to delete")
					return []
				}
				var obs_ids = skipped.map(o=>o.id) 
				// console.log(obs_ids)
				return this.deleteObservaciones(tipo,{id:obs_ids},undefined,client)
				.then(deleted=>{
					if(options.returnSkipped) {
						return deleted
					} else {
						return result
					}
				})
			}
			if(options.returnSkipped) {
				return skipped
			} else {
				return result
			}
		})
	}

	// 	 SERIES OBS - by SERIES_ID OR FUENTES_ID
	static async pruneObs(tipo,filter, options={},client) { // filter:series_id,timestart,timeend; options: no_send_data=false) {
		if(!filter.timestart || !filter.timeend) {
			return Promise.reject("crud.pruneObs: Missing filter.timestart filter.timeend")
		}
		
		// CASE SINGLE SERIES_ID 
		if(filter.series_id && typeof filter.series_id == "number") {
			return this.deleteObservaciones(tipo,{series_id:filter.series_id,timestart:filter.timestart,timeend:filter.timeend},{no_send_data:options.no_send_data},client)
			.then(result=>{
				return [
					{
						id:filter.series_id,
						observaciones: result
					}
				]
			})
		// CASE MULTIPLE SERIES_ID OR FUENTES_ID/RED_ID
		} else {
			if(!(tipo == "puntual" && filter.red_id) && !(tipo!="puntual" && filter.fuentes_id) && !filter.series_id) {
				return Promise.reject("crud.pruneObs: Missing filter.series_id OR filter.fuentes_id/red_id")
			}
			filter.id = (filter.series_id) ? filter.series_id : filter.id
			return this.getSeries(tipo,filter,{no_metadata:true},client)
			.then(async series=>{
				if(series.length == 0) {
					console.log("crud.pruneObs: no series found")
					return
				}
				series = series.filter(s=>s.date_range && s.date_range.timestart && s.date_range.timestart < new Date(filter.timeend) && s.date_range.timeend && s.date_range.timeend >= new Date(filter.timestart))
				var results = []
				var accum_deletes = 0
				console.log("about to prune " + series.length + " series")
				for(var i=0;i<series.length;i++) {
					var series_id = series[i].id
					console.log("index:" + i + ", series_id:" + series_id)
					try {
						var result = await this.deleteObservaciones(tipo,{series_id:series_id,timestart:filter.timestart,timeend:filter.timeend},{no_send_data:options.no_send_data},client)
					} catch (e) {
						console.error("crud.pruneObs: " + e.toString())
						results.push({error:e})
						break
					}
					if(options.no_send_data) {
						accum_deletes += result
						console.log("accum deletes:" + accum_deletes)
						results.push({
							series_id: series[i].id,
							observaciones: result
						})
					} else {
						results.push({values:result})
					}
				}
				if(options.no_send_data) {
					return results
				} else {
					var i=-1
					return results.map(r=>{
						i++
						if (r.values) {
							return {
								id: series[i].id,
								observaciones: r.values
							}
						} else {
							return {
								id: series[i].id,
								observaciones: null,
								message: r.error.toString()
							}
						}
					})
				}
			})
		}
	}
	static async prunePartedByYear(tipo,filter,options,client) {
		if(!filter.timestart || !filter.timeend) {
			throw("Missing timestart and/or timeend")
		}
		filter.timestart = new Date(filter.timestart)
		filter.timeend = new Date(filter.timeend)
		var startYear = filter.timestart.getUTCFullYear()
		var endYear = filter.timeend.getUTCFullYear()
		var results = []
		var release_client = false
		if(!client) {
			release_client = true
			client = await global.pool.connect()
		}
		for(var y=startYear;y<=endYear;y++) {
			var timestart = new Date(y,0,1)
			if(timestart < filter.timestart) {
				timestart = filter.timestart
			}
			var timeend = new Date(y+1,0,1)
			if(timeend > filter.timeend) {
				timeend = filter.timeend
			}
			var this_filter = {...filter}
			this_filter.timestart = timestart
			this_filter.timeend = timeend
			// console.log(JSON.stringify(this_filter))
			try {
				var result = await this.pruneObs(tipo,this_filter,options,client)
				results.push(result)
			} catch(e) {
				console.error(e)
			}
		}
		return results
	}

	// pais
	static async getPais(id,name) {
		var stmt
		if(!id) {
			if(!name) {
				return Promise.reject("Missing id or name")
			}
			stmt = internal.utils.pasteIntoSQLQuery("SELECT id,nombre,abrev,wmdr_name,wmdr_notation,wmdr_uri FROM paises WHERE lower(nombre)=lower($1)",[name])
		} else {
			stmt = internal.utils.pasteIntoSQLQuery(("SELECT id,nombre,abrev,wmdr_name,wmdr_notation,wmdr_uri FROM paises WHERE id=$1",[parseInt(id)]))
		}
		return global.pool.query(stmt)
		.then(result=>{
			if(!result.rows.length) {
				throw("pais not found")
			}
			return result.rows[0]
		})
	}
	
	static async getPaises(filter={}) {
		var filter_string = internal.utils.control_filter2({id:{type:"integer"},nombre:{type:"string",case_insensitive:true},abrev:{type:"string",case_insensitive:true},wmdr_name:{type:"string"},wmdr_notation:{type:"string"},wmdr_uri:{type:"string",case_insensitive:true}},filter,"paises")
		return global.pool.query(`SELECT id,nombre,abrev,wmdr_name,wmdr_notation,wmdr_uri FROM paises WHERE 1=1 ${filter_string} ORDER BY nombre`)
		.then(result=>{
			return result.rows
		})
	}

	static async getRegionesOMM(filter={}) {
		var filter_string = internal.utils.control_filter2({id:{type:"integer"},name:{type:"string",case_insensitive:true},notation:{type:"string",case_insensitive:true},uri:{type:"string",case_insensitive:true}},filter,"regiones_omm")
		return global.pool.query(`SELECT id,name,notation,uri FROM regiones_omm WHERE 1=1 ${filter_string}`)
		.then(result=>{
			return result.rows
		})
	}

	static async getProcessTypeWaterml2(filter={}) {
		var filter_string = internal.utils.control_filter2({proc_id:{type:"integer"},name:{type:"string",case_insensitive:true},notation:{type:"string",case_insensitive:true},uri:{type:"string",case_insensitive:true}},filter,"process_type_waterml2")
		return global.pool.query(`SELECT proc_id,name,notation,uri FROM process_type_waterml2 WHERE 1=1 ${filter_string}`)
		.then(result=>{
			return result.rows
		})
	}

	static async getDataTypes(filter={}) {
		var filter_string = internal.utils.control_filter2({id:{type:"integer"},term:{type:"string",case_insensitive:true},in_waterml1_cv:{type:"boolean"},waterml2_code:{type:"string",case_insensitive:true},waterml2_uri:{type:"string",case_insensitive:true}},filter,"datatypes")
		return global.pool.query(`SELECT id,term,in_waterml1_cv,waterml2_code,waterml2_uri FROM datatypes WHERE 1=1 ${filter_string}`)
		.then(result=>{
			return result.rows
		})
	}

	// static async series2waterml2(series) {
	// 	return series2waterml2.convert(series)
	// }

	static async getTipoEstaciones(filter={}) {
		var filter_string = internal.utils.control_filter2({id:{type:"integer"},tipo:{type:"string",case_insensitive:true},nombre:{type:"string", case_insensitive:true}},filter,"tipo_estaciones")
		return global.pool.query(`SELECT tipo,id,nombre FROM tipo_estaciones WHERE 1=1 ${filter_string} ORDER BY tipo`)
		.then(result=>{
			return result.rows
		})
	}
}

internal.utils = {
	makeGetSeriesNextPageUrl: function(tipo="puntual",next_offset,req,filter={},options={}) {
		if(next_offset === undefined || next_offset == null) {
			return undefined
		}
		var query_arguments = (req) ? {...req.query} : {...filter,...options}
		query_arguments.offset = next_offset
		return (config.rest && config.rest.url) ? `${config.rest.url}/obs/${tipo}/series?${querystring.stringify(query_arguments)}` : (req) ? `${req.protocol}://${req.get('host')}${req.path}?${querystring.stringify(query_arguments)}` : `obs/${tipo}/series?${querystring.stringify(query_arguments)}`
	},
	getPageProperties: function(limit,offset,rows) {
		if(!limit) {
			return [undefined,undefined,undefined]
		}
		offset = (offset) ? offset : 0 
		var total = (rows.length) ? rows[0].total : 0
		var is_last_page = (rows.length < limit || parseInt(offset) + parseInt(limit) >= total) ? true : false
		var next_offset = (is_last_page) ? undefined : parseInt(offset) + parseInt(limit)
		return [total, is_last_page, next_offset]
	},
	get_data_availability_string: function(data_availability,has_obs) {
		var filter_string = ""
		if(data_availability && ["h","n","c","r"].indexOf(data_availability) >= 0) {
			has_obs = true
		}
		if(has_obs) {
			if(data_availability) {
				switch(data_availability.toLowerCase().substring(0,1)) {
					case "r":
						filter_string += " AND now() - date_range.timeend < '1 days'::interval"
						break;
					case "n":
						filter_string +=  " AND now() - date_range.timeend < '3 days'::interval"
						break;
					case "c":
						filter_string += " AND (date_range.timestart <= coalesce($2,now())) and (date_range.timeend >= coalesce($1,now()-'90 days'::interval))"
						break;
					case "h":
						break;
					default:
						break;
				}
			}
			var series_range_join = ""
		} else {
			var series_range_join = "LEFT OUTER"
		}
		return [filter_string,series_range_join]
	},
	validate_with_model: function (instance,model) {
		if(!schemas.hasOwnProperty(model)) {
			throw("model " + model + " no encontrado en schema")
		}
		var result = g.validate(instance,schemas[model])
		if(result.errors.length) {
			console.error(result.toString())
			return { "valid": false, "reason": result.toString() } 
		}
		return { "valid": true}
	},	
	control_filter: function (valid_filters, filter={}, tablename) {
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
					if(! filter[key] instanceof internal.geometry) {
						console.error("Invalid geometry object")
						control_flag++
					} else {
						filter_string += "  AND ST_Distance(st_transform(" + fullkey + ",4326),st_transform(" + filter[key].toSQL() + ",4326)) < 0.001" 
					}
				} else if (valid_filters[key] == "timestart") {
					var ldate = (filter[key] instanceof Date)  ? filter[key].toISOString() : timeSteps.parseDateString(filter[key]).toISOString()
					filter_string += " AND " + fullkey + "::timestamptz>='" + ldate + "'"
				} else if (valid_filters[key] == "timeend") {
					var ldate = (filter[key] instanceof Date) ? filter[key].toISOString() : timeSteps.parseDateString(filter[key]).toISOString()
					filter_string += " AND " + fullkey + "::timestamptz<='" + ldate + "'"
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
	},
	build_order_by_clause: function (valid_fields, first, default_table, default_fields, order="asc") {
		if(first) {
			var index = default_fields.indexOf(first)
			if(index >= 0) {
				default_fields.sort((x,y) => x == first ? -1 : y == first ? 1 : 0)
			} else {
				default_fields.unshift(first)
			}
		}
		const sort_by_fields = []
		for(var key of default_fields) {
			if(valid_fields[key]) {
				const order_string = (key == first) ? (order == "desc") ? " DESC" : "" : ""
				if(valid_fields[key].function) {
					sort_by_fields.push(`${valid_fields[key].function}${order_string}`)
				} else {
					var table = (valid_fields[key].table !== undefined) ? valid_fields[key].table : default_table
					var column = valid_fields[key].column ?? key
					if(table) {
						sort_by_fields.push(`"${table}"."${column}"${order_string}`)
					} else {
						sort_by_fields.push(`"${column}"${order_string}`)
					}
				}
			}
		}
		if(sort_by_fields.length) {
			return  `ORDER BY ${sort_by_fields.join(", ")}`
		} else {
			return ""
		}
	},
	alert_error: function(message, throw_=false) {
		if(throw_) {
			throw(new Error(message))
		} else {
			console.error(message)
		}
	},
	control_filter2: function (valid_filters, filter, default_table, throw_if_invalid=false) {
		// valid_filters = { column1: { table: "table_name", type: "data_type", required: bool, column: "column_name"}, ... }  
		// filter = { column1: "value1", column2: "value2", ....}
		// default_table = "table"
		var filter_string = " "
		var control_flag = 0
		loop1: for(var filter_key of Object.keys(valid_filters)) {
			var filter_def = valid_filters[filter_key]
			var key = new String(filter_key.toString())
			if(typeof filter[key] == "undefined" || filter[key] === null) {
				if(filter_def.alias && filter[filter_def.alias]  != undefined) {
					key = new String(filter_def.alias.toString())
				} else {
					if (filter_def.required) {
						internal.utils.alert_error(
							"Missing value for mandatory filter key " + key,
							throw_if_invalid
						)
						control_flag++
					}
					continue
				}
			}

			var table_prefix = (filter_def.table) ? '"' + filter_def.table + '".' :  (default_table) ? '"' + default_table + '".' : ""
			var column_name = (filter_def.column) ? '"' + filter_def.column + '"' : '"' + key + '"'
			var fullkey = table_prefix + column_name


			if(Array.isArray(filter[key])) {
				for(var f of filter[key]) {
					if(/[';]/.test(f)) {
						internal.utils.alert_error(
							"Invalid filter value: illegal characters found in key " + key,
							throw_if_invalid)
						control_flag++
						break loop1	
					}	
				}
			} else {
				if(/[';]/.test(filter[key])) {
					internal.utils.alert_error(
						"Invalid filter value: illegal characters found in key " + key,
						throw_if_invalid)
					control_flag++
					break loop1
				}
			}
			if(filter_def.type == "regex_string") {
				var regex = filter[key].replace('\\','\\\\')
				filter_string += " AND " + fullkey  + " ~* '" + filter[key] + "'"
			} else if(filter_def.type == "string") {
				if(filter_def.case_insensitive) {
					if(Array.isArray(filter[key])) {
						if(filter[key].length) {
							filter_string += ` AND lower(${fullkey}) IN (${filter[key].map(v=>`lower('${v}')`).join(",")})`
						} else if(filter_def.required) {
							internal.utils.alert_error(
								"Falta valor para filtro obligatorio " + key + " (array vacío)",
								throw_if_invalid)
							control_flag++
						}
					} else {
						filter_string += ` AND lower(${fullkey})=lower('${filter[key]}')`
					}
				} else {
					if(Array.isArray(filter[key])) {
						if(filter[key].length) {
							filter_string += ` AND ${fullkey} IN (${filter[key].map(v=>`'${v}'`).join(",")})`
						} else if(filter_def.required) {
							internal.utils.alert_error(
								"Falta valor para filtro obligatorio " + key + " (array vacío)",
								throw_if_invalid)
							control_flag++
						}
					} else {
						filter_string += " AND "+ fullkey + "='" + filter[key] + "'"
					}
				}
			} else if (filter_def.type == "boolean") {
				var boolean = (/^[yYtTvVsS1]/.test(filter[key])) ? "true" : "false"
				filter_string += " AND "+ fullkey + "=" + boolean + ""
			} else if (filter_def.type == "boolean_only_true") {
				if (/^[yYtTvVsS1]/.test(filter[key])) {
					filter_string += " AND "+ fullkey + "=true"
				} 
			} else if (filter_def.type == "boolean_only_false") {
				if (!/^[yYtTvVsS1]/.test(filter[key])) {
					filter_string += " AND "+ fullkey + "=false"
				} 
			} else if (filter_def.type == "geometry") {
				if(! filter[key] instanceof internal.geometry) {
					internal.utils.alert_error(
						"Invalid geometry object at filter key " + key,
						throw_if_invalid)
					control_flag++
				} else if (filter[key].type == "Point"){
					filter_string += "  AND ST_Distance(st_transform(" + fullkey + ",4326),st_Buffer(st_transform(" + filter[key].toSQL() + ",4326),0.001)) < 0.000001" 
				} else {
					filter_string += "  AND ST_Distance(st_transform(" + fullkey + ",4326),st_transform(" + filter[key].toSQL() + ",4326)) < 0.001" 
				}
			} else if (filter_def.type == "date") {
				let d
				if(filter[key] instanceof Date) {
					d = filter[key]
				} else {
					d = new Date(filter[key])
				}
				if(filter_def.trunc != undefined) {
					assertValidDateTruncField(filter_def.trunc)
					filter_string += ` AND date_trunc('${filter_def.trunc}',${fullkey})::timestamptz = date_trunc('${filter_def.trunc}', '${d.toISOString()}'::timestamptz)`	
				} else {
					filter_string += " AND " + fullkey + "::timestamptz='" + d.toISOString() + "'::timestamptz"
				}
			} else if (filter_def.type == "timestart") {
				var ldate = (filter[key] instanceof Date) ? filter[key].toISOString() : timeSteps.parseDateString(filter[key]).toISOString()		
				filter_string += " AND " + fullkey + "::timestamptz>='" + ldate + "'"
			} else if (filter_def.type == "timeend") {
				var ldate = (filter[key] instanceof Date) ? filter[key].toISOString() : timeSteps.parseDateString(filter[key]).toISOString()
				filter_string += " AND " + fullkey + "::timestamptz<='" + ldate + "'"
			} else if (filter_def.type == "numeric_interval") {
				if(Array.isArray(filter[key])) {
					if(filter[key].length < 2) {
						internal.utils.alert_error(
							"numeric_interval must have at least 2 items at filter key " + key,
							throw_if_invalid)
						control_flag++
					} else {
						filter_string += " AND " + fullkey + ">=" + parseFloat(filter[key][0]) + " AND " + key + "<=" + parseFloat(filter[key][1])
					}
				} else {
						filter_string += " AND " + fullkey + "=" + parseFloat(filter[key])
				}
			} else if(filter_def.type == "numeric_min") {
				filter_string += " AND " + fullkey + ">=" + parseFloat(filter[key])
			} else if(filter_def.type == "numeric_max") {
				filter_string += " AND " + fullkey + "<=" + parseFloat(filter[key])
			} else if (filter_def.type == "integer") {
				if(Array.isArray(filter[key])) {
					if(filter[key].length) {
						var values = filter[key].map(v=>parseInt(v)).filter(v=>v.toString()!="NaN")
						if(!values.length) {
							internal.utils.alert_error(
								"Invalid integer at filter key " + key,
								throw_if_invalid
							)
							control_flag++
						} else {
							filter_string += " AND "+ fullkey + " IN (" + values.join(",") + ")"
						}
					} else if(filter_def.required) {
						internal.utils.alert_error(
							"Missing value at mandatory filter key " + key + " (empty array)",
							throw_if_invalid
						)
						control_flag++
					}
				} else {
					var value = parseInt(filter[key])
					if(value.toString() == "NaN") {
						internal.utils.alert_error(
							"Invalid integer at filter key " + key,
							throw_if_invalid
						)
						control_flag++
					} else {
						filter_string += " AND "+ fullkey + "=" + value + ""
					}
				}
			} else if (filter_def.type == "number" || filter_def.type == "float") {
				if(Array.isArray(filter[key])) {
					if(filter[key].length) {
						var values = filter[key].map(v=>parseFloat(v)).filter(v=>v.toString()!="NaN")
						if(!values.length) {
							internal.utils.alert_error(
								"Invalid float at filter key " + key,
								throw_if_invalid
							)
							control_flag++
						} else {
							filter_string += " AND "+ fullkey + " IN (" + values.join(",") + ")"
						}
					} else if(filter_def.required) {
						internal.utils.alert_error(
							"Missing value for mandatory filter key " + key + " (empty array)",
							throw_if_invalid
						)
						control_flag++
					}
				} else {
					var value = parseFloat(filter[key])
					if(value.toString() == "NaN") {
						internal.utils.alert_error(
							"Invalid integer at filter key " + key,
							throw_if_invalid
						)
						control_flag++
					} else {
						filter_string += " AND "+ fullkey + "=" + value + ""
					}
				}
			} else if (filter_def.type == "interval") {
				var value = timeSteps.createInterval(filter[key])
				if(!value) {
					throw("invalid interval filter: " + filter[key])
				}
				if(value.toPostgres() == '0') {
					filter_string += ` AND (${fullkey}='${value.toPostgres()}'::interval OR ${fullkey} IS NULL)`	
				} else {
					filter_string += ` AND ${fullkey}='${value.toPostgres()}'::interval`
				}
			} else if (filter_def.type == 'json_array') {
				if(Array.isArray(filter[key])) {
					if(filter[key].length) {
						filter_string += " AND " + fullkey + "::jsonb ?& array[" + filter[key].map(v=>`'${v}'`).join(",") + "]"
					} else if(filter_def.required) {
						internal.utils.alert_error(
							"Missing value for mandatory filter key " + key + " (empty array)",
							throw_if_invalid
						)
						control_flag++
					}
				} else {
					filter_string += " AND "+ fullkey + "::jsonb ?& array['" + filter[key] + "']"
				}
			} else if(filter_def.type == "search") {
				if(filter_def.columns) {
					var concat_fields = filter_def.columns.map(column=> {
						var table = column.table ?? filter_def.table ?? default_table
						if(table) {
							return `"${table}"."${column.name}"::text`
						} else {
							return `"${column.name}"::text`
						}
					})
					if(filter_def.case_insensitive) {
						fullkey = `lower(concat(${concat_fields.join(",")}))`
						filter_string += " AND " + fullkey  + " ~* lower('" + filter[key] + "')"
					} else {
						fullkey = `concat(${concat_fields.join(",")}))`
						filter_string += " AND " + fullkey  + " ~* '" + filter[key] + "'"
					}
				} else {
					filter_string += " AND " + fullkey  + " ~* '" + filter[key] + "'"
				}
			// qualifiers::jsonb ?& array['1']
			} else if(filter_def.type == "data_availability") {
				var d_a_values = ["H","C","NRT","RT"]
				var apply_filter = false
				if(filter[key].toLowerCase() == "r" || filter[key].toLowerCase() == "rt") {
					d_a_values = ["RT"]
					apply_filter = true
				} else if(filter[key].toLowerCase() == "n" || filter[key].toLowerCase() == "nrt") {
					d_a_values = ["NRT","RT"]
					apply_filter = true
				} else if(filter[key].toLowerCase() == "c") {
					d_a_values = ["C","NRT","RT"]
					apply_filter = true
				} else if(filter[key].toLowerCase() == "h") {
					d_a_values = ["H","C","NRT","RT"]
					apply_filter = true
				} 
				if(apply_filter) {
					var d_a_values_string = d_a_values.map(v=>`'${v}'`).join(",")
					filter_string += ` AND ${fullkey} IN (${d_a_values_string})`
				}
			} else {
				if(Array.isArray(filter[key])) {
					if(filter[key].length) {
						filter_string += " AND "+ fullkey + " IN (" + filter[key].join(",") + ")"
					} else if(filter_def.required) {
						internal.utils.alert_error(
							"Missing value for mandatory filter key " + key + " (empty array)",
							throw_if_invalid
						)
						control_flag++
					}
				} else {
					filter_string += " AND "+ fullkey + "=" + filter[key] + ""
				}
			}
		}
		if(control_flag > 0) {
			return null
		} else {
			return filter_string
		}
	},
	control_filter_json: function (valid_filters, filter, default_table) {
		// valid_filters = { filter1: { path: "path->to->>attribute", type: "data_type", required: bool, table: "table_name"}, ... }  
		// filter = { filter1: "value1", filter2: "value2", ....}
		// default_table = "table"
		var filter_string = " "
		var control_flag = 0
		Object.keys(valid_filters).forEach(key=>{
			var table_prefix = (valid_filters[key].table) ? '"' + valid_filters[key].table + '".' :  (default_table) ? '"' + default_table + '".' : ""
			if(Array.isArray(valid_filters[key].path)) {
				var path 
				var index = -1
				for(var attr of valid_filters[key].path) {
					index = index + 1
					if(index == 0) {
						path = `"${attr}"`
					} else if (index < valid_filters[key].path.length - 1) {
						path = `${path}->'${attr}'`
					} else {
						path = `${path}->>'${attr}'`
					}
				}
			} else if (valid_filters[key].path) {
				path = valid_filters[key].path
			} else {
				path = '"' + key + '"'
			}
			var fullpath = table_prefix + path
			if(typeof filter[key] != "undefined" && filter[key] !== null) {
				if(/[';]/.test(filter[key])) {
					console.error("Invalid filter value")
					control_flag++
				}
				if(valid_filters[key].type == "regex_string") {
					var regex = filter[key].replace('\\','\\\\')
					filter_string += " AND " + fullpath  + " ~* '" + filter[key] + "'"
				} else if(valid_filters[key].type == "string") {
					if(valid_filters[key].case_insensitive) {
						filter_string += ` AND lower(${fullpath})=lower('${filter[key]}')`
					} else {
						filter_string += " AND "+ fullpath + "='" + filter[key] + "'"
					}
				} else if (valid_filters[key].type == "boolean") {
					var boolean = (/^[yYtTvVsS1]/.test(filter[key])) ? "true" : "false"
					filter_string += " AND ("+ fullpath + ")::bool = " + boolean + ""
				} else if (valid_filters[key].type == "boolean_only_true") {
					if (/^[yYtTvVsS1]/.test(filter[key])) {
						filter_string += " AND ("+ fullpath + ")::bool = true"
					} 
				} else if (valid_filters[key].type == "boolean_only_false") {
					if (!/^[yYtTvVsS1]/.test(filter[key])) {
						filter_string += " AND ("+ fullpath + ")::bool = false"
					} 
				} else if (valid_filters[key].type == "geometry") {
					if(! filter[key] instanceof internal.geometry) {
						console.error("Invalid geometry object")
						control_flag++
					} else {
						filter_string += "  AND ST_Distance(st_transform(st_geomfromgeojson(" + fullpath + "),4326),st_transform(" + filter[key].toSQL() + ",4326)) < 0.001" 
					}
				} else if (valid_filters[key].type == "date") {
					let d
					if(filter[key] instanceof Date) {
						d = filter[key]
					} else {
						d = new Date(filter[key])
					}
					filter_string += " AND (" + fullpath + ")::timestamptz='" + d.toISOString() + "'::timestamptz"
				} else if (valid_filters[key].type == "timestart") {
					var ldate = (filter[key] instanceof Date) ? filter[key].toISOString() : timeSteps.parseDateString(filter[key]).toISOString()		
					filter_string += " AND (" + fullpath + ")::timestamptz>='" + ldate + "'"
				} else if (valid_filters[key].type == "timeend") {
					var ldate = (filter[key] instanceof Date) ? filter[key].toISOString() : timeSteps.parseDateString(filter[key]).toISOString()
					filter_string += " AND (" + fullpath + ")::timestamptz<='" + ldate + "'"
				} else if (valid_filters[key].type == "numeric_interval") {
					if(Array.isArray(filter[key])) {
						if(filter[key].length < 2) {
							console.error("numeric_interval debe ser de al menos 2 valores")
							control_flag++
						} else {
							filter_string += " AND (" + fullpath + ")::numeric >= " + parseFloat(filter[key][0]) + " AND (" + fullpath + ")::numeric <= " + parseFloat(filter[key][1])
						}
					} else {
						 filter_string += " AND (" + fullpath + ")::numeric = " + parseFloat(filter[key])
					}
				} else if(valid_filters[key].type == "numeric_min") {
					filter_string += " AND (" + fullpath + ")::numeric >= " + parseFloat(filter[key])
				} else if(valid_filters[key].type == "numeric_max") {
					filter_string += " AND (" + fullpath + ")::numeric <= " + parseFloat(filter[key])
				} else if (valid_filters[key].type == "integer") {
					if(Array.isArray(filter[key])) {
						var values = filter[key].map(v=>parseInt(v)).filter(v=>v.toString()!="NaN")
						if(!values.length) {
							console.error("Invalid integer")
							control_flag++
						} else {
							filter_string += " AND (" + fullpath + ")::int IN (" + values.join(",") + ")"
						}
					} else {
						var value = parseInt(filter[key])
						if(value.toString() == "NaN") {
							console.error("Invalid integer")
							control_flag++
						} else {
							filter_string += " AND ("+ fullpath + ")::int = " + value + ""
						}
					}
				} else if (valid_filters[key].type == "number" || valid_filters[key].type == "float") {
					if(Array.isArray(filter[key])) {
						var values = filter[key].map(v=>parseFloat(v)).filter(v=>v.toString()!="NaN")
						if(!values.length) {
							console.error("Invalid float")
							control_flag++
						} else {
							filter_string += " AND (" + fullpath + ")::numeric IN (" + values.join(",") + ")"
						}
					} else {
						var value = parseFloat(filter[key])
						if(value.toString() == "NaN") {
							console.error("Invalid integer")
							control_flag++
						} else {
							filter_string += " AND (" + fullpath + ")::numeric = " + value + ""
						}
					}
				} else if (valid_filters[key].type == "interval") {
					var value = timeSteps.createInterval(filter[key])
					if(!value) {
						throw("invalid interval filter: " + filter[key])
					}
					filter_string += ` AND (${fullpath})::interval = '${value.toPostgres()}'::interval`
				} else if (valid_filters[key].type == 'json_array') {
					if(Array.isArray(filter[key])) {
						filter_string += " AND (" + fullpath + ")::jsonb ?& array[" + filter[key].map(v=>`'${v}'`).join(",") + "]"
					} else {
						filter_string += " AND (" + fullpath + ")::jsonb ?& array['" + filter[key] + "']"
					}
	
				// qualifiers::jsonb ?& array['1']
				} else {
					if(Array.isArray(filter[key])) {
						filter_string += " AND ("+ fullpath + ")::numeric IN (" + filter[key].join(",") + ")"
					} else {
						filter_string += " AND (" + fullpath + ")::numeric = " + filter[key] + ""
					}
				}
			} else if (valid_filters[key].required) {
				console.error("Falta valor para filtro obligatorio " + key)
				control_flag++
			}
		})
		if(control_flag > 0) {
			return null
		} else {
			return filter_string
		}
	},	
	build_group_by_clause: function (valid_columns,columns=[],default_table) {
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
	},
	pasteIntoSQLQuery: function (query,params) {
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
	},
	build_read_query: function(model_name,filter,table_name,options) {
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
		var filter_string = internal.utils.control_filter3(model,filter,table_name)
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
			order_by_clause = " ORDER BY " + order_by.map(key=>internal.utils.getFullKey(model,key,table_name)).join(",")
		}
		return {
			query: "SELECT " + selected_columns.map(key=> internal.utils.getFullKey(model,key,table_name)).join(", ") + " FROM " + '"' + table_name + '" WHERE 1=1 ' + filter_string + order_by_clause,
			child_tables: child_tables,
			meta_tables: meta_tables,
			table_name: table_name
		}
	},	
	getFullKey: function(model,key,default_table) {
		return (model.table_name) ? "\"" + model.table_name + "\".\"" + key + "\"" : (default_table) ? "\"" + default_table + "\".\"" + key + "\"" : "\"" + key + "\""
	},	
	control_filter3: function (model, filter, default_table) {
		var filter_string = " "
		var control_flag = 0
		for(const key of Object.keys(model.properties)) {
			const property = model.properties[key]
			if(property.type == 'array' && property.items && property.items["$href"]) {
				continue
			} else if (!property.type && property.$ref) {
				property.type = property.$ref.split("/").pop()
			}
			var fullkey = internal.utils.getFullKey(model,key,default_table)
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
					if(!value instanceof internal.geometry) { // value.constructor && value.constructor.name == "geometry") {
						geometry = new internal.geometry(value)
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
	},	
	changeRef: function(object,key) {
		if(key == "$ref") {
			object[key] = "/" + object[key].split("/").pop()
		}
	},	
	traverse: function(o,func) {
		for (var i in o) {
			func.apply(this,[o,i]);  
			if (o[i] !== null && typeof(o[i])=="object") {
				//going one step down in the object tree!!
				internal.utils.traverse(o[i],func);
			}
		}
	},
	/**
	 * converts accumulated to pulses
	 * assigns timestamp of the start of the accumulation period
	 * returns n - 1 observations
	**/
	acum2pulses: function (observaciones) {
		observaciones = observaciones.sort((a,b)=>{
			return new Date(a.timestart).getTime() - new Date(b.timestart).getTime()
		})
		const observaciones_pulse = []
		for(var i=0;i<observaciones.length - 1; i++) {
			const observacion_pulse = new internal.observacion(observaciones[i])
			observacion_pulse.id = undefined
			observacion_pulse.valor = (observaciones[i+1].valor < observaciones[i].valor) ? observaciones[i+1].valor : observaciones[i+1].valor - observaciones[i].valor
			observaciones_pulse.push(observacion_pulse)
		}
		return observaciones_pulse
	},
	getLimitString(limit,offset) {
		if(limit) {
			var page_limit = parseInt(limit)
			var pagination = true
			var limit_string = `LIMIT ${page_limit}`
			var page_offset = (offset) ? parseInt(offset) : 1
			if(offset) {
				limit_string += ` OFFSET ${page_offset}`
			}
			return [page_limit,pagination,page_offset,limit_string]
		} else {
			return [undefined,false,undefined,""]
		}
	}
}

/**
 * execute pg query
 * @param {string} query_string 
 * @param {Array|undefined} query_args 
 * @param {Client|undefined} client 
 * @param {Boolean} [release_client=false] - if true, when the query fails it releases the provided client before throwing error
 * @returns {Promise<Object[]>} promise to an array representing the query result rows
 */
async function executeQueryReturnRows(query_string,query_args,client,release_client) {
	if(!query_string) {
		throw("missing query string")
	}
	if(client)  {
		if(query_args) {
			try {
				var result = await client.query(query_string,query_args)
			} catch(e) {
				if(release_client) {
					client.release()
				}
				throw(e)
			}
		} else {
			try {
				var result = await client.query(query_string)
			} catch(e) {
				if(release_client) {
					client.release()
				}
				throw(e)
			}
		}
		if(release_client) {
			client.release()
		}
	} else {
		if(query_args) {
			var result = await global.pool.query(query_string,query_args)
		} else {
			var result = await global.pool.query(query_string)
		}
	}
	return result.rows
}

function getPercentile(values, percentile) {
	var rank =  Math.round(percentile * (values.length + 1))
	rank = (rank == 0) ? 1 : (rank > values.length) ? values.length : rank
	return values[rank-1]
}

function isNumeric(str) {
	if (typeof str != "string") return false // we only process strings!  
	return !isNaN(str) && // use type coercion to parse the _entirety_ of the string (`parseFloat` alone does not do this)...
		   !isNaN(parseFloat(str)) // ...and ensure strings of whitespace fail
}

module.exports = internal
