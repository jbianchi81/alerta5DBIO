const CRUD = require('./CRUD')
const wkx = require('wkx')
const turf = require('@turf/turf')
const csv = require('csv-parser')
const path = require('path');
var fs = require('fs')
var fsPromises = require('promise-fs')
var sprintf = require('sprintf-js').sprintf
const { Transform } = require('stream');
const zlib = require('zlib')
// var pexec = require('child-process-promise').exec;
const shapefile = require('shapefile');
const { corrida } = require('./CRUD');
const timeSteps = require('./timeSteps')

const internal = {}

internal.hmfs = class {
	// config.basins_path 	the path to geojson file of HMFS basins
	// config.data_path     the base path of HMFS data directory
	// config.tabla         the source string identifier of the dataset (i.e. "hmfs")
	// config.fuentes_id    the source integer identifier of the dataset (from PK of table "fuentes", i.e.  ) 
	constructor(config={}) {
		this.config = {}
		this.config.basins_path = config.basins_path
		this.config.data_path = config.data_path
		this.config.tabla = config.tabla
		this.config.fuentes_id = config.fuentes_id
		this.config.variable_map = config.variable_map
		this.config.cal_id_map = config.cal_id_map
	}

	variable_map = {
		"asmt": 20,
		"flow": 4
	}

	cal_id_map = {
		"s2sep": 5
	}

	series_map = {
		"asmt": {},
		"flow": {}
	}

	flow_stations = [
		1006402500, 1005704848, 1006003208,
		1006002693, 1006002519, 1006002516,
		1006006537, 1005902536, 1005902464,
		1005902463, 1005906888, 1005906905,
		1006702819, 1005704953, 1005704850,
		1005902464, 1006003423, 1006003343,
		1006004174, 1006003227, 1006003249,
		1006003295, 1006004326, 1006004306,
		1006002582, 1006004208, 1006004333,
		1006310008, 1005907361, 1006005227,
		1006005237, 1005804919, 1005806418,
		1005905111, 1005905171, 1005905117,
		1005906866, 1006702982, 1005708330,
		1006702820, 1005705104, 1006003488,
		1006005981
	]

	async createAccessor() {
		// read if fuente exists
		const fuentes = await CRUD.fuente.read({nombre:"hmfs"})
		if(fuentes.length) {
			var fuente = fuentes[0]
		} else {
			// create fuente (for series areales)
			var fuente = new CRUD.fuente({
				nombre: "hmfs",
				def_proc_id: 4,
				public: false
			})
			await fuente.create()
		}
		// set/overwrite fuentes_id
		this.config.fuentes_id = fuente.id
		// create accessor
		const acc = new CRUD.accessor({
			class: "hmfs",
			name: "hmfs",
			config: this.config,
			title: "Output of HRC HMFS-Plata hydrologic modeling"
		})
		await acc.create()
		// create red (for series puntuales)
		const red = new CRUD.red({
			tabla_id: this.config.tabla,
			nombre: "hmfs",
			public: false
		})
		await red.create()
		// create model
		const hrc_model = new CRUD.modelo({
			nombre: "hmfs",
			tipo: "P-Q",
			def_var_id: 4,
			def_unit_id: 10
		})
		await hrc_model.create()
		// check if calibrados exists
		var cal0 = await CRUD.calibrado.read({
			model_id: hrc_model.id,
			nombre: "hmfs_15d"
		})
		if(!cal0.length) {
			// create calibrado 15d
			cal0 = new CRUD.calibrado({
				model_id: hrc_model.id,
				nombre: "hmfs_15d",
				activar: true,
				selected: true,
				dt: "06:00:00"
		  	})
			await cal0.create()
		}
		var cal1 = await CRUD.calibrado.read({
			model_id: hrc_model.id,
			nombre: "hmfs_s2s"
		})
		if(!cal1.length) {
			// create calibrado s2s		
			cal1 = new CRUD.calibrado({
				model_id: hrc_model.id,
				nombre: "hmfs_s2s",
				activar: true,
				selected: true,
				dt: "06:00:00"
		  	})
			await cal1.create()
		}
		// create data directory
		if (!fs.existsSync(this.config.data_path)){
			fs.mkdirSync(this.config.data_path);
		}
		const dirs = [
			path.resolve(this.config.data_path),
			path.resolve(this.config.data_path,"FCST"),
			path.resolve(this.config.data_path,"FCST/S2SEP"),
			path.resolve(this.config.data_path,"FCST/S2SEP/BASIN"),
			path.resolve(this.config.data_path,"FCST/S2SEP/BASIN/ASMT"),
			path.resolve(this.config.data_path,"FCST/S2SEP/BASIN/ASMT/UNIMPAIRED"),
			path.resolve(this.config.data_path,"FCST/S2SEP/STREAM"),
			path.resolve(this.config.data_path,"FCST/S2SEP/STREAM/FLOW"),
			path.resolve(this.config.data_path,"FCST/S2SEP/STREAM/FLOW/UNIMPAIRED")
		]
		for(var dir of dirs) {
			if (!fs.existsSync(dir)){
				fs.mkdirSync(dir);
			}
		}
		return acc
	}

	async updateSeries(filter={},options={}) {
		const sites = await this.updateSites(filter,options)
		const series = await this.getSeries(filter,options)
		for(var serie of series) {
			await serie.create()
		}
		return series
	}

	async updateSites(filter={},options={}) {
		const sites = await this.getSites(filter,options)
		// first create estaciones
		for(var site of sites) {
			if(site instanceof CRUD.estacion) {
				await site.create()
			}
		}
		// then create areas
		for(var site of sites) {
			if(site instanceof CRUD.area) {
				await site.create()
			}
		}
		return sites
	}

	async getSeries(filter={},options={}) {
		const sites = await this.getSites(filter,options)
		const series = []
		for(var site of sites) {
			if(site instanceof CRUD.area) {
				// serie SMC areal
				series.push(new CRUD.serie({
					tipo: "areal",
					area_id: site.id,
					var_id: 20,
					proc_id: 4,
					unit_id: 23,
					fuentes_id: this.config.fuentes_id
				}))
				// serie SMC areal media mensual
				series.push(new CRUD.serie({
					tipo: "areal",
					area_id: site.id,
					var_id: 90,
					proc_id: 4,
					unit_id: 23,
					fuentes_id: this.config.fuentes_id
				}))
				// serie SMC areal media diaria
				series.push(new CRUD.serie({
					tipo: "areal",
					area_id: site.id,
					var_id: 119,
					proc_id: 4,
					unit_id: 23,
					fuentes_id: this.config.fuentes_id
				}))
			} else if(site instanceof CRUD.estacion) {
				// serie Q
				series.push(new CRUD.serie({
					tipo: "puntual",
					estacion_id: site.id,
					var_id: 4,
					proc_id: 4,
					unit_id: 10
				}))
				// serie Q medio mensual
				series.push(new CRUD.serie({
					tipo: "puntual",
					estacion_id: site.id,
					var_id: 48,
					proc_id: 4,
					unit_id: 10
				}))
			}
		}
		for(var valid_filter of ["tipo","var_id","proc_id","unit_id"]) {
			if(filter.hasOwnProperty(valid_filter)) {
				series = series.filter(f=> f[valid_filter] == filter[valid_filter])
			}
		}
		return series
	}

	async getSites(filter={},options={}) {
		const sites = await this.parseBasinsShapefile(this.config.basins_path,filter.id,filter.geom)
		return [...sites.areas, ...sites.estaciones]
	}

	async parseBasinsShapefile(basins_path,id,geom,limit=10,begin_position=0) {
		const basins_string = await fsPromises.readFile(basins_path,'utf-8')
		const basins_geojson = JSON.parse(basins_string)
		const areas = []
		const estaciones = []
		var i = -1
		for(var basin_geojson of basins_geojson.features) {
			i = i + 1
			if(i < begin_position) {
				continue
			}
			if(limit && i >= begin_position + limit) {
				break
			}
			if(id && parseInt(basin_geojson.properties.VALUE) != id) {
				continue
			}
			const [basin, centroid] = this.basinFromGeojson(basin_geojson)
			if(geom) {
				const point = turf.point(centroid.geom.coordinates)
				const polygon = turf.polygon(basin.geom.coordinates)
				if(!turf.booleanPointInPolygon(point,polygon)) {
					console.log("Centroid outside provided bounding box, skipping")
					continue
				}
			}
			areas.push(basin)
			estaciones.push(centroid)
		}
		return {
			areas: areas,
			estaciones: estaciones
		}
	}
	basinFromGeojson(basin) {
		const b = new CRUD.area({
			id: parseInt(basin.properties.VALUE),
			geom: basin.geometry,
			exutorio: this.WKBtoGeom(basin.properties.CENTROID),
			exutorio_id: parseInt(basin.properties.VALUE)
		})
		const c = new CRUD.estacion({
			id: parseInt(basin.properties.VALUE),
			id_externo: parseInt(basin.properties.VALUE),
			tabla: "hmfs",
			geom: this.WKBtoGeom(basin.properties.CENTROID)
		})
		return [b, c]
	}
	WKBtoGeom(wkb) {
		// Split WKB into array of integers (necessary to turn it into buffer)
		var hexAry = wkb.match(/.{2}/g);
		var intAry = [];
		for (var i in hexAry) {
			intAry.push(parseInt(hexAry[i], 16));
		}

		// Generate the buffer
		const buf = Buffer.from(intAry);
		const wkx_geom = wkx.Geometry.parse(buf)
		return wkx_geom.toGeoJSON()
	}
	
	// DATA

	getDataPaths(base_path=this.config.data_path) {
		return {
			s2sep: {
				asmt: path.resolve(base_path, "FCST/S2SEP/BASIN/ASMT/UNIMPAIRED"),
				flow: path.resolve(base_path, "FCST/S2SEP/STREAM/FLOW/UNIMPAIRED")
			}
		}
	}

	file_patterns = {
		s2sep: {
			asmt: "{{year}}{{month}}{{date}}-{{hour}}00_hmfs_prod_fcst_s2sep_asmt_06hr_m{{ensemble_member}}_unimp_plata.csv.gz",
			flow: "{{year}}{{month}}{{date}}-{{hour}}00_hmfs_prod_fcst_s2sep_flow_01hr_m{{ensemble_member}}_unimp_{{basin_id}}.csv.gz"
		}
	}

	listProductFiles(product="s2sep",variable="asmt",begin_date,end_date,forecast_hours,ensemble_member,basin_id,data_path=this.config.data_path) {
		if(!this.file_patterns.hasOwnProperty(product)) {
			throw("Bad product name:" + product)
		}
		const product_data_dir = this.getDataPaths(data_path)[product][variable]
		if(!product_data_dir) {
			throw("Data directory not found for given variable name:" + variable)
		}
		const file_pattern = this.file_patterns[product][variable]
		const years = fs.readdirSync(product_data_dir)
		const results = []
		// const dirs = []
		// const files = []
		for(var year of years) {
			if(begin_date && year < begin_date.getUTCFullYear()) {
				continue
			}
			if(end_date && year > end_date.getUTCFullYear()) {
				continue
			}
			const months = fs.readdirSync(`${product_data_dir}/${year}`)
			for(var month of months) {
				// TODO skip if date is outside range
				const days = fs.readdirSync(`${product_data_dir}/${year}/${month}`)
				for(var day of days) {
					// TODO skip if date is outside range
					const date = new Date(Date.UTC(parseInt(year),parseInt(month)-1,parseInt(day)))
					if(begin_date && date < begin_date) {
						continue
					}
					if(end_date && date > end_date) {
						continue
					}
					// dates.push(date)
					const dir = `${product_data_dir}/${year}/${month}/${day}`
					// dirs.push(dir)
					var day_files = fs.readdirSync(dir)
					var p = file_pattern.replace("{{year}}",sprintf("%04d",year)).replace("{{month}}",sprintf("%02d",month)).replace("{{date}}",sprintf("%02d",day))
					if(forecast_hours) {
						p = p.replace("{{hour}}",sprintf("%02d",forecast_hours))
					} else {
						p = p.replace("{{hour}}","\\d{2}")
					}
					if(ensemble_member && !Array.isArray(ensemble_member)) {
						p = p.replace("{{ensemble_member}}",sprintf("%02d",ensemble_member))
					} else {
						p = p.replace("{{ensemble_member}}","\\d{2}")
					}
					if(basin_id  && !Array.isArray(basin_id)) {
						p = p.replace("{{basin_id}}",sprintf("%02d",basin_id))
					} else {
						p = p.replace("{{basin_id}}","\\d+")
					}
					const re = new RegExp(p)
					// console.log(re)
					// console.log("Found " + day_files.length + " files before filtering")
					day_files = day_files.filter(f => re.test(f))
					// console.log("Found " + day_files.length + " files after filtering")
					day_files = day_files.map(f=>`${dir}/${f}`)
					var files = day_files.map(f=>this.getFileMetadata(f))
					if(ensemble_member && Array.isArray(ensemble_member)) {
						files = files.filter(f=>ensemble_member.indexOf(f.ensemble_member) >= 0)
					}
					if(basin_id && Array.isArray(basin_id)) {
						files = files.filter(f=>{
							if(f.basin_id) {
								if(basin_id.indexOf(f.basin_id) >= 0) {
									return true
								} else {
									return false
								}
							} else {
								return true
							}
						})
					}
					// console.log(files.map(f=>f.filename))
					results.push({
						date: date,
						dir: dir,
						files: files
					})
				}	
			}
		}
		return results // {dates: dates, dirs: dirs, files: files}
	}

	getFileMetadata(fullpath) {
		const filename = fullpath.replace(/^.*[\\\/]/, '')
		var matches = filename.match(/^(\d{4})(\d{2})(\d{2})-(\d{2})00_hmfs_prod_fcst_s2sep_([a-z]{4})_/)
		if(!matches) {
			throw("File " + fullpath + " not matching required pattern")
		}
		const variable = matches[5]
		if(variable=="asmt") {
			return this.getAsmtFileMetadata(filename)
		} else if(variable=="flow") {
			return this.getFlowFileMetadata(filename)
		} else {
			throw("Bad variable: " + variable)
		}
	}
	
	getAsmtFileMetadata(filename) {
		// YYYYMMDD-HH00_hmfs_prod_fcst_s2sep_asmt_06hr_m<EM>_unimp_plata.csv.gz
		var matches = filename.match(/^(\d{4})(\d{2})(\d{2})-(\d{2})00_hmfs_prod_fcst_s2sep_asmt_06hr_m(\d{2})_unimp_plata.csv.gz/)
		return {
			filename: filename.toString(),
			variable: "asmt",
			date: new Date(Date.UTC(matches[1],matches[2]-1,matches[3],matches[4])),
			ensemble_member: parseInt(matches[5]),
			time_step: {hours: 6}
		}
	}

	getFlowFileMetadata(filename) {
		// YYYYMMDD-HH00_hmfs_prod_fcst_s2sep_flow_01hr_mEM_unimp_1005802104.csv.gz
		var matches = filename.match(/^(\d{4})(\d{2})(\d{2})-(\d{2})00_hmfs_prod_fcst_s2sep_flow_01hr_m(\d{2})_unimp_(\d+).csv.gz/)
		return {
			filename: filename.toString(),
			variable: "flow",
			date: new Date(Date.UTC(matches[1],matches[2]-1,matches[3],matches[4])),
			ensemble_member: parseInt(matches[5]),
			basin_id: parseInt(matches[6]),
			time_step: {hours: 1}
		}
	}

	async parseProductFile(file_path,begin_date,end_date,ensemble_member,basin_id,create=false,corrida,max_rows_for_insert=20000) {
		// console.log("file_path:" + file_path)
		const file_metadata = this.getFileMetadata(file_path)
		if(!corrida) {
			corrida = new CRUD.corrida({
			cal_id: this.config.cal_id,
			forecast_date:file_metadata.date})
			if(create) {
				await corrida.create()
			}
		}
		if(ensemble_member && !Array.isArray(ensemble_member)) {
			ensemble_member = [ensemble_member]
		}
		if(basin_id && !Array.isArray(basin_id)) {
			basin_id = [basin_id]
		}
		const client = await global.pool.connect()
		return new Promise((resolve,reject)=>{
			if(!fs.existsSync(file_path)) {
				reject(new Error("File " + file_path + " not found"))
			}
			const transform = getTransformStream()
			const read_stream = fs.createReadStream(file_path)
			.pipe(zlib.createGunzip())
			.pipe(transform)
			.pipe(csv())
			const results = []
			const partial_results = []
			// var chunk_index = -1
			read_stream.on("data",async (chunk) => {
				// chunk_index = chunk_index + 1
				// console.log(chunk)
				const prono = this.parseDataObj(chunk,file_metadata.variable,corrida.cor_id)
				if(begin_date && prono.timestart < begin_date) {
					// console.log("skipped: timestart predates begin_date")
					return
				}
				if(end_date && prono.timestart > end_date) {
					// console.log("skipped: timestart postdates end_date")
					return
				}
				if(ensemble_member && ensemble_member.indexOf(parseInt(prono.qualifier)) < 0) {
					// console.log("skipped: qualifier " + prono.qualifier + " not selected")
					return
				}
				if(basin_id && basin_id.indexOf(prono.estacion_id) < 0) {
					// console.log("skipped: basin_id " + prono.estacion_id + " not selected")
					return
				}
				if(create) {
					partial_results.push(prono)
					if(partial_results.length==max_rows_for_insert) {
						read_stream.pause()
						console.log("upserting " + partial_results.length + " pronosticos")
						await CRUD.CRUD.upsertPronosticos(client,partial_results)
						partial_results.length = 0
						read_stream.resume()
					}
					// var key = parseInt(chunk_index / 2500)
					// if(!partial_results[key]) {
					// 	partial_results[key] = [prono]
					// } else {
					// 	partial_results[key].push(prono)
					// }
					// if(partial_results[key].length >= 2500) {
					// 	console.log("push partial_results. index: " + chunk_index)
					// 	// promises.push(
					// 	await CRUD.CRUD.upsertPronosticos(undefined,partial_results[key])
					// 	delete partial_results[key] //.splice(0,partial_results.length)
					// 	if (global.gc) {global.gc();}
					// 	// if(promises.length >= 10) {
					// 	// 	console.log("await promises")
					// 	// 	await Promise.all(promises)
					// 	// 	promises = []
					// 	// }
					// }
				} else {
					results.push(prono)
				}
			})
			read_stream.on("end",async () => {
				// await CRUD.CRUD.upsertPronosticos(undefined,partial_results)
				console.log("upserting " + partial_results.length + " pronosticos")
				await CRUD.CRUD.upsertPronosticos(client,partial_results)
				partial_results.length = 0
				if(!create) {
					console.log("file parsing produced " + results.length + " records")
					resolve(results)
				} else {
					resolve()
				}
			})
		})
	}

	parseDataObj(data_obj,variable,cor_id) {
		const var_id = this.variable_map[variable]
		var series_table = (this.series_map[variable][parseInt(data_obj.id_location)]) ? this.series_map[variable][parseInt(data_obj.id_location)].table : undefined
		var series_id = (this.series_map[variable][parseInt(data_obj.id_location)]) ? this.series_map[variable][parseInt(data_obj.id_location)].id : undefined
		return {
			series_table: series_table,
			series_id: series_id,
			estacion_id: parseInt(data_obj.id_location),
			var_id: var_id,
			forecast_date: new Date(data_obj.date_product),
			timestart: new Date(data_obj.date_valid),
			valor: parseFloat(data_obj.value),
			qualifier: parseInt(data_obj.id_member).toString(),
			cor_id: cor_id
		}
	}

    async updatePronostico(filter={},options={}) {
        return this.update(filter,options)
    }

    async update(filter={},options={}) {
        const variable = (filter.var_id) ? this.getVariableById(filter.var_id) : undefined
        const result = await this.upsertS2SepProno(filter.forecast_date,filter.timestart,filter.timeend,filter.estacion_id,filter.qualifier,undefined,filter.cal_id,variable)
		var date_offset = (filter.forecast_date) ? filter.forecast_date.getUTCDate() - 1 : 0
		if(!filter.var_id || filter.var_id == 20 || filter.var_id == 90) {
			await this.getMonthlyMean(this.cal_id_map["s2sep"],filter.forecast_date,filter.estacion_id,undefined,undefined,20,90,undefined,filter.timestart,filter.timeend,date_offset, true) // asmt_monthly
		}
		if(!filter.var_id || filter.var_id == 4 || filter.var_id == 48) {
			await this.getMonthlyMean(this.cal_id_map["s2sep"],filter.forecast_date,filter.estacion_id,undefined,undefined,4,48,undefined,filter.timestart,filter.timeend,date_offset, true) // flow_monthly
		}
		if(options.refresh_date_range) {
			console.log("refreshing series prono date range")
			await CRUD.CRUD.refreshSeriesPronoDateRange()
			await CRUD.CRUD.refreshSeriesPronoDateRangeByQualifier()
		}
		return result
    }

    async get(filter={},options={}) {
        const variable = (filter.var_id) ? this.getVariableById(filter.var_id) : undefined
        return this.getS2SepProno(filter.forecast_date,filter.timestart,filter.timeend,filter.estacion_id,filter.qualifier,undefined,filter.cal_id,variable)
    }

    getVariableById(var_id) {
        for(var key of Object.keys(this.variable_map)) {
            if(this.variable_map[key] == var_id) {
                return key
            }
        }
        return
    }

	async upsertS2SepProno(forecast_date,timestart,timeend,basin_id,ensemble_member,data_path=this.config.data_path,cal_id=this.cal_id_map["s2sep"],variable) {
		await this.getS2SepProno(forecast_date,timestart,timeend,basin_id,ensemble_member,data_path,cal_id,variable,true)
		return 
	}

	async getS2SepProno(forecast_date,timestart,timeend,basin_id,ensemble_member,data_path=this.config.data_path,cal_id=this.cal_id_map["s2sep"],variable,create=false) {
		if(create) {
			console.log("create empty corrida")
			var corrida = new CRUD.corrida({
				cal_id: cal_id,
				forecast_date: forecast_date
			})
			await corrida.create()
			console.log("created corrida cor_id " + corrida.cor_id)
		}
		// set series map
		console.log("get series map")
		await this.setSeriesMap()
		console.log("got series map")

		if(variable && !Array.isArray(variable)) {
			variable = [variable]
		}

		var [begin_date, forecast_hours] = this.separateDateHours(forecast_date)
		var end_date = begin_date
		// fetch asmt products
		console.log("get product file list")
		var asmt_products = this.listProductFiles("s2sep","asmt",begin_date,end_date,forecast_hours,ensemble_member,basin_id,data_path)
		console.log("got product file list")
		if(!asmt_products.length) {
			throw("No forecast found")
		}
		// select last date
		asmt_products.sort((a,b)=> a - b)
		const last_asmt_product = asmt_products[asmt_products.length-1]
		var last_date = last_asmt_product.files.map(f=>f.date).reduce((a,b) => (a > b) ? a : b, last_asmt_product.files[0].date)
		last_asmt_product.files = last_asmt_product.files.filter(f=>f.date.getTime() == last_date.getTime())

		const pronosticos = []
		if(!variable || variable.indexOf("asmt") >= 0) {
			// parse asmt files
			console.log("fetch asmt products")
			for(var file of last_asmt_product.files) {
				const fullpath = `${last_asmt_product.dir}/${file.filename}`
				console.log("Parsing file: " + fullpath)
				const pronos = await this.parseProductFile(fullpath,timestart,timeend,file.ensemble_member,basin_id,create,corrida)
				if(!create) {
					console.log("Found " + pronos.length + " records")
					pronosticos.push(...pronos)
				}
			}
		}
		if(!variable || variable.indexOf("flow") >= 0) {
			// fetch flow products
			console.log("fetch flow products: last_date " + last_date.toISOString());
			[begin_date, forecast_hours] = this.separateDateHours(last_date)
			end_date = begin_date
			console.log(["s2sep","flow",begin_date,end_date,forecast_hours,ensemble_member,basin_id,data_path])
			const flow_products = this.listProductFiles("s2sep","flow",begin_date,end_date,forecast_hours,ensemble_member,basin_id,data_path)
			const last_flow_product = (flow_products.length) ? flow_products[0] : undefined

			if(last_flow_product) {
				// parse flow files
				console.log("last flow product found: " + last_flow_product.dir)
				for(var file of last_flow_product.files) {
					const fullpath = `${last_flow_product.dir}/${file.filename}`
					console.log("Parsing file: " + fullpath)
					const pronos = await this.parseProductFile(fullpath,timestart,timeend,file.ensemble_member,file.basin_id,create,corrida)
					if(!create) {
						console.log("Found " + pronos.length + " records")
						pronosticos.push(...pronos)
					}
				}
			}
		}
		if(!create) {
			const series = this.groupPronosBySeries(pronosticos)

			return new CRUD.corrida({
				forecast_date: last_date,
				cal_id: cal_id,
				series: series
			})
		} else {
			await corrida.updateSeriesDateRange({estacion_id:basin_id,qualifier: ensemble_member})
			return
		}
	}

	groupPronosBySeries(pronosticos) {
		const res = []
		const m = {}
		let i, j, curr
		for (i = 0, j = pronosticos.length; i < j; i++) {
			curr = pronosticos[i];
			var key = `${curr.series_table}.${curr.series_id}`
			if (!(key in m)) {
				m[key] = {
					series_table: curr.series_table,
					series_id: curr.series_id,
					pronosticos: []
				}
				res.push(m[key])
			}
			m[key].pronosticos.push({
				timestart: curr.timestart,
				valor: curr.valor,
				qualifier: curr.qualifier
			})
		}
   		return res
	}

	separateDateHours(date) {
		const date_part = (date) ? new Date(Date.UTC(date.getUTCFullYear(),date.getUTCMonth(),date.getUTCDate())) : undefined
		const hours = (date) ? date.getUTCHours() : undefined
		return [date_part, hours]
	}

	async setSeriesMap() {
		this.series_map = await this.getSeriesMap()
	}

	async getSeriesMap() {
		const series_asmt = await CRUD.serie.read({
			tipo: "areal",
			var_id: this.variable_map["asmt"],
			proc_id: 4,
			fuentes_id: this.config.fuentes_id
		},{
			no_metadata: true
		})
		const series_asmt_map = {}
		for(var serie of series_asmt) {
			series_asmt_map[serie.area_id] = {
				tipo: serie.tipo,
				table: "series_areal",
				id: serie.id
			}
		}
		const series_flow = await CRUD.serie.read({
			tipo: "puntual",
			var_id: this.variable_map["flow"],
			proc_id: 4,
			tabla_id: this.config.tabla
		},{
			no_metadata: true
		})
		const series_flow_map = {}
		for(var serie of series_flow) {
			series_flow_map[serie.estacion_id] = {
				tipo: serie.tipo,
				table: "series",
				id: serie.id
			}
		}
		return {
			"asmt": series_asmt_map,
			"flow": series_flow_map
		}
	}

	// async getSitesFromShp(basins_path=this.config.basins_path,create=false,max_parallel_promises=16,max_features_per_extraction=100) {
	// 	const basin_ids = await getBasinIdsFromShp(basins_path)
	// 	console.log ("Found " + basin_ids.length + " basin ids")
	// 	const sites = []
	// 	var promises = []
	// 	var promise_count = 0
	// 	var index = 0
	// 	while(index < basin_ids.length) {
	// 	// for(var id of basin_ids) {
	// 		const ids_to_extract = basin_ids.slice(index,Math.min(index+max_features_per_extraction,basin_ids.length-1))
	// 		index = index + 1
	// 		promise_count = promise_count + 1
	// 		if(promise_count >= max_parallel_promises) {
	// 			console.log("reached max parallel promises. Awaiting resolution")
	// 			await Promise.all(promises)
	// 			promises = []
	// 			promise_count = 0
	// 		}
	// 		console.log("reading from basin id " + ids_to_extract[0] + " to " + ids_to_extract[-1])
	// 		promises.push(extractBasinById(basins_path,ids_to_extract).then(async basins=>{
	// 			if(!basins.features.length) {
	// 				console.error("Basins not found. skipped")
	// 				return
	// 			}
	// 			for(var feature of basins.features) {
	// 				console.log("read basin id " + feature.properties.VALUE)
	// 				const [area, exutorio] = this.basinFromGeojson(feature)
	// 				if(create) {
	// 					await exutorio.create()
	// 					await area.create()
	// 					console.log("created exutorio and area")
	// 				}
	// 			}
	// 			sites.push(area)
	// 			sites.push(exutorio)
	// 			return
	// 		}))
	// 	}
	// 	await Promise.all(promises)
	// 	return sites
	// }

	async getSitesFromShp (filename=this.config.basins_path,create=false) {
		// const sites= []
		const source = await shapefile.open(filename)
		while (true) {
			const result = await source.read()
			if(result.done) {
				return // sites
			}
			// console.log(result.value.properties.VALUE)
			if(parseInt(result.value.properties.VALUE) > 2147483647) {
				console.error("Bad id, must be <= 2147483647")
				continue
			}
			const [area, exutorio] = this.basinFromGeojson(result.value)
			if(create) {
				await exutorio.create()
				await area.create()
				// console.log("created exutorio and area")
			}
			// sites.push(exutorio)
			// sites.push(area)
		}
	}

	async createFakeMonthlyRun(forecast_date=new Date(),horizon={months:3},cal_id=this.cal_id_map["s2sep"],area_id,flow_stations=this.flow_stations) {
		if(!area_id) {
			var series_smm = await CRUD.serie.read({tipo:"areal",tabla_id:"hmfs",var_id:90,proc_id:4},{no_metadata:true})	
		} else {
			var series_smm = await CRUD.serie.read({tipo: "areal", area_id: area_id,var_id:90,proc_id:4},{no_metadata:true})	
		}
		var horizon = timeSteps.createInterval(horizon)
		var end_date = timeSteps.advanceInterval(forecast_date,horizon)
		var dt = timeSteps.createInterval({months:1})
		var time_index = timeSteps.dateSeq(forecast_date,end_date,dt)
		const corrida = new CRUD.corrida({
			forecast_date: forecast_date,
			cal_id: cal_id,
			series: series_smm.map(serie=>{
				return {
					series_table: "series_areal",
					series_id: serie.id,
					qualifier: "main",
					pronosticos: time_index.map(time=>{
						return {
							timestart: time,
							timeend: timeSteps.advanceInterval(time,dt),
							valor: Math.random()
						}
					})
				}
			})
		})
		var series_qmm = await this.getFlowSeries(flow_stations,48)
		for(var serie of series_qmm) {
			corrida.series.push({
				series_table: "series",
				series_id: serie.id,
				qualifier: "main",
				pronosticos: time_index.map(time=>{
					return {
						timestart: time,
						timeend: timeSteps.advanceInterval(time,dt),
						valor: Math.random() * 10000
					}
				})
			})
		}
		return corrida
	}

	async getPolygonByPoint(point_geom) {
		const areas = await CRUD.CRUD.getAreas({geom:point_geom},{no_geom:true})
		if(!areas.length) {
			console.error("Polygon not found")
			return
		}
		return areas[0]
	}

	async setFlowStationsFromEstaciones(estaciones,var_id=48) {
		if(typeof estaciones == 'string') {
			estaciones = await CRUD.estacion.readFile(estaciones)
		}
		const flow_stations = []
		for(var estacion of estaciones) {
			const area = await this.getPolygonByPoint(estacion.geom)
			if(!area) {
				continue
			}
			const flow_station = await CRUD.estacion.read({
				id: area.id,
				tabla: "hmfs" 
			})
			if(flow_station.length) {
				flow_stations.push(flow_station[0])
			}
		}
		this.flow_stations = flow_stations.map(s=>s.id)
		return flow_stations
	}
	async getFlowSeries(flow_stations=this.flow_stations,var_id=48) {
		const series_qmm = await CRUD.serie.read({tipo:"puntual", estacion_id:flow_stations, tabla_id:"hmfs", var_id:var_id, proc_id:4})
		return series_qmm
	}

	async getMonthlyMean(cal_id=this.cal_id_map["s2sep"],forecast_date,basin_id,source_series_id,dest_series_id,source_var_id=20,dest_var_id=90,qualifier,timestart,timeend,date_offset=0,create=true) {
		var corridas = await CRUD.corrida.read({cal_id:cal_id,var_id:source_var_id,forecast_date:forecast_date,estacion_id:basin_id,series_id:source_series_id,qualifier:qualifier},{group_by_qualifier:true})	
		var series_dest = await CRUD.serie.read({tipo:(dest_var_id==90) ? "areal":"puntual", estacion_id:basin_id,var_id:dest_var_id,series_id:dest_series_id,fuentes_id:(dest_var_id==90)?this.config.fuentes_id:undefined},{no_metadata:true})
		const client = await global.pool.connect()
		for(var corrida of corridas) {
			corrida.series_mensuales = []
			var pronos_to_create = []
			for(var serie of corrida.series) {
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
				const c = await CRUD.corrida.read({cor_id:corrida.id, tipo: (dest_var_id == 90) ? "areal" : "puntual",series_id:serie.series_id, timestart: timestart, timeend:timeend, qualifier: serie.qualifier},{group_by_qualifier:true, includeProno:true})
				// console.log("got " + c[0].series[0].pronosticos.length + " pronosticos from cor_id: " + corrida.id + ", series_id: " + serie.series_id + ", qualifier: " + serie.qualifier)
				if(!c[0].series[0].pronosticos.length) {
					continue
				}
				const s  = new CRUD.serie({
					series_table: serie.series_table,
					series_id: serie.series_id
				})
				s.setObservaciones(c[0].series[0].pronosticos)
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
				// if(create) {
				// 	await 
				// }

				corrida.series_mensuales.push(result)
				if(create) {
					pronos_to_create.push(...result.pronosticos.map(p=>{
						p.series_id = result.series_id
						p.series_table = result.series_table
						p.qualifier = result.qualifier
						p.cor_id = corrida.id
						return p
					}))
				}
				if(pronos_to_create.length >= 5000) {
					await CRUD.CRUD.upsertPronosticos(client,pronos_to_create)
					pronos_to_create.length = 0
				} 
			}
			if(pronos_to_create.length) {
				await CRUD.CRUD.upsertPronosticos(client,pronos_to_create)
			} 
		}
		return corridas
	}

	async getQuantiles(filter={},quantiles,labels,create,client) {
		const corridas = await CRUD.corrida.read(
			{
				cal_id:filter.cal_id ?? this.cal_id_map["s2sep"],
				cor_id: filter.cor_id,
				var_id:filter.var_id,
				forecast_date: filter.forecast_date,
				estacion_id: filter.basin_id ?? filter.area_id ?? filter.estacion_id,
				tipo: filter.tipo,
				series_id: filter.series_id,
				tabla: filter.tabla,
				qualifier: filter.qualifier
			},{
				group_by_qualifier:false,
				includeProno: false
			})
		client = client ?? await global.pool.connect()
		for(var corrida of corridas) {
			for(var serie of corrida.series) {
				// console.log("set pronosticos. series_id:" + serie.series_id)
				await serie.setPronosticos({
					timestart: filter.timestart,
					timeend: filter.timeend
				},undefined,client)
				// const initial_length = serie.pronosticos.length
				// console.log("got " + initial_length + " pronosticos")
				await serie.getQuantileSeries(quantiles,labels,create,client)
				// console.log("got " + serie.pronosticos.length + " pronosticos after getQuantileSeries")
				if(create) {
					serie.pronosticos.length = 0
				}
			}
		}
		if(!create) {
			return corridas
		} else {
			CRUD.CRUD.refreshSeriesPronoDateRange()
			CRUD.CRUD.refreshSeriesPronoDateRangeByQualifier()
		}
		return
	}
}

const getTransformStream = () => {
	const transform = new Transform({
		transform: (chunk, encoding, next) => {
			next(null, chunk);
		},
	});
 	return transform;
}

// const getBasinIdsFromShp = async filename => {
// 	return pexec(`ogrinfo ${filename} -dialect sqlite -sql "select VALUE from hrc_plata_daqcs_hmfs_basin_geom" | grep VALUE | cut -d" " -f6`)
// 	.then(result=>{
// 		var stdout = result.stdout
// 		var stderr = result.stderr
// 		if(stdout) {
// 			var basin_ids = stdout.split("\n").map(s=>parseInt(s)).filter(i=>(i.toString() != "NaN"))
// 		}
// 		if(stderr) {
// 			throw new Error(stderr)
// 		}
// 		return basin_ids
// 	})
// }

// // reads shp and integer, returns geojson
// const extractBasinById = async (filename,basin_id) => {
// 	const query = (Array.isArray(basin_id)) ? basin_id.map(id=>`VALUE=${id}`).join(" OR ") : `VALUE=${basin_id}`
// 	return pexec(`ogr2ogr -f GeoJSON -where "${query}" /vsistdout/ ${filename}`)
// 	.then(result=>{
// 		var stdout = result.stdout
// 		var stderr = result.stderr
// 		if(stdout) {
// 			var basins = JSON.parse(stdout)
// 		}
// 		if(stderr) {
// 			throw new Error(stderr)
// 		}
// 		return basins
// 	})
// }


module.exports = internal
