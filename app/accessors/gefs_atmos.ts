import { AbstractAccessorEngine} from "./abstract_accessor_engine"
import {exec as pexec} from 'child-process-promise'
import {serie as CrudSerie, observacion as CrudObservacion, fuente as CrudFuente, escena as CrudEscena, corrida as CrudCorrida, pronostico as CrudPronostico} from "../CRUD"
import axios, { AxiosInstance } from "axios"
import {sprintf} from 'sprintf-js'
import { existsSync, mkdirSync, createWriteStream } from "fs"
import { downloadAndWriteStream, grib2obs, flatten, VariableMap, groupBySeriesIdAndQualifier } from './accessor_utils'

interface Config {
	url: string
	// files_url: string
	data_dir?: string
	bbox?: { 
		leftlon: number, 
		rightlon: number, 
		toplat: number, 
		bottomlat: number
	}
	start_hour?: number
	end_hour?: number
	dt?: number
	levels?: string[]
	variables?: string[]
	variable_map?: VariableMap
	ens?: number
	[x : string] : unknown
	cal_id: number
}


export class Client extends AbstractAccessorEngine {

	connection : AxiosInstance

	config : Config

	default_variable_map : VariableMap = {
		"APCP06": {
			name: "apcp",
			var_id: 91,
			proc_id:4,
			unit_id: 9,
			series_id: 55
		}
	}

	default_config : Config = {
		url: "https://nomads.ncep.noaa.gov/cgi-bin/filter_gefs_atmos_0p25s.pl",
		// files_url: "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gens/prod/",
		data_dir: "/../../data/gefs_atmos/",
		bbox: { leftlon: -70, rightlon: -40, toplat: -10, bottomlat:-40},
		start_hour: 6,
		end_hour: 241,
		dt: 6,
		levels: [ "surface"],
		variables: [ "APCP" ],
		variable_map: this.default_variable_map,
		ens: 30,
		cal_id: 721
	}

	static fuente : CrudFuente = new CrudFuente({
		nombre: "gefs_atmos",
		id: 55,
		data_table: "",
		data_column: "",
		tipo: "QPF",
		def_proc_id: 4,
		def_dt: "06:00:00",
		hora_corte: "00:00:00",
		def_unit_id: 9,
		def_var_id: 91,
		fd_column: "",
		mad_table: "",
		scale_factor: 0,
		data_offset: 0,
		def_pixel_height: 0.25,
		def_pixel_width: 0.25,
		def_pixeltype: "64BF",
		def_srid: 4326,
		def_extent: {"type":"Polygon","coordinates":[[[-70,-10],[-40,-10],[-40,-40],[-70,-40],[-70,-10]]]},
		date_column: "",
		abstract: "GEFS ATMOS",
		source: "https://nomads.ncep.noaa.gov/cgi-bin/filter_gefs_atmos_0p25s.pl",
		public: false
	})

	static escena : CrudEscena = new CrudEscena({
		id: 25,
		nombre: "cdp", 
		geom: {"type":"Polygon","coordinates":[[[-70,-10],[-40,-10],[-40,-40],[-70,-40],[-70,-10]]]}
	})

	static serie : CrudSerie = new CrudSerie({
		tipo: "raster",
		id: 55,
		estacion: this.escena,
		var: {
			id: 91
		},
		procedimiento: {
			id: 4
		},
		unidades: {
			id: 9
		},
		fuente: this.fuente
	})

	path?: string
	start_hour: number
	end_hour: number
	dt: number
	max_hour: number = 240
	ens: number
	url: string
	variable_map : VariableMap
	forecast_date: Date
	default_forecast_date: Date
	default_qualifiers: string[]

	constructor(config : Config) {
		super(config)
		this.connection = axios.create()
		this.config = {...this.default_config, ...config}
		this.url = this.config.url
		this.start_hour = this.config.start_hour || 6
		this.end_hour = this.config.end_hour || 241
		this.dt = this.config.dt || 6
		this.ens = this.config.ens || 30
		this.variable_map = this.config.variable_map || this.default_variable_map
		this.default_forecast_date = new Date()
		this.default_forecast_date.setHours(this.default_forecast_date.getHours() - 6)		
		this.default_forecast_date.setMinutes(0, 0, 0)
		this.forecast_date = this.default_forecast_date
		this.default_qualifiers = ["gec00", "geavg", "gespr"]
		var member = 1
		while(member <= this.ens) {
			this.default_qualifiers.push(sprintf("gep%02d", member))
			member = member + 1
		}
	}

	async createSerie() : Promise<CrudSerie> {
		const fuente = await Client.fuente.create()
		const escena = await Client.escena.create()
		const series = await CrudSerie.create([Client.serie])
		if(!series.length) {
			throw new Error("Nothing created")
		}
		return series[0]
	}
	
	async test() {
		try {
			await this.connection.get(this.config.url)
		} catch(e) {
			console.error("accessor test failed")
			return false
		}
		console.info("accessor test ok")
		return true
	}

	async get(
		filter : {
			forecast_date?: string | Date,
    		timestart?: string | Date,
    		timeend?: string | Date,
			qualifiers?: string[],
			qualifier?: string
		}={},
		options={}
	) : Promise<CrudPronostico[]> {
		var dates = this.getDates(filter) 
		this.forecast_date = dates.forecast_date
		var forecast_date = dates.forecast_date
		var timestart = dates.timestart
		var timeend = dates.timeend
		var dates_dir = dates.dates_dir
		var times_dir = dates.times_dir
		var forecast_date_path = dates.forecast_date_path
		var forecast_time_path = dates.forecast_time_path
		if(! existsSync(forecast_date_path)) {
			mkdirSync(forecast_date_path)
		}
		if(! existsSync(forecast_time_path)) {
			mkdirSync(forecast_time_path)
		}
		console.info("accessors.gefs_wave.get: path: " + this.path)
		var hours = []
		for(var i : number=this.start_hour;i<=this.end_hour;i=i+this.dt) {
			if(i>this.max_hour) {
				continue
			}
			const forecast_time = new Date(forecast_date)
			forecast_time.setHours(forecast_time.getHours() + i)
			// SKIPS DATES OUT OF RANGE
			if(timestart && forecast_time < timestart) {
				continue
			}
			if(timeend && forecast_time > timeend) {
				continue
			}
			hours.push(i)
		}
		const results : CrudPronostico[][] = []
		const qualifiers = (filter.qualifier) ? [filter.qualifier] : (filter.qualifiers) ? filter.qualifiers : this.default_qualifiers
		for(const qualifier of qualifiers) {
		// while(member <= this.ens) {
			for(const i of hours) {
				var file = sprintf ("%s.t%02dz.pgrb2s.0p25.f%03d", qualifier, forecast_date.getUTCHours(), i)
				console.debug(`file: ${file}`)
				var params : Record<string, any> = {
					file: file,
					subregion: "",
					dir: "/" + dates_dir + times_dir + "atmos/pgrb2sp25"
				}
				if(this.config.bbox) {
					params = {...params, ...this.config.bbox}
				}
				if(this.config.levels) {
					this.config.levels.forEach(level=>{
						params["lev_"+level] = "on"
					})
				}
				if(this.config.variables) {
					this.config.variables.forEach(variable=>{
						params["var_"+variable] = "on"
					})
				}
				var localfilepath = `${__dirname}${this.config.data_dir}${dates_dir}${times_dir}${file}.grib2`
				//~ console.log({localfilepath:localfilepath})
				await downloadAndWriteStream(
					this.url,
					params,
					localfilepath,
					this.connection
				)
				
				results.push(
					await grib2obs(
						localfilepath,
						this.variable_map,
						(this.config.bbox) ? [this.config.bbox.leftlon, this.config.bbox.toplat, this.config.bbox.rightlon, this.config.bbox.bottomlat] : undefined,
						"milímetros",
						true,
						qualifier
					)
				)
			}
		}

		var pronosticos = flatten(results)
		return pronosticos
	}

	async getPronostico(
		filter : {
			forecast_date?: string | Date,
    		timestart?: string | Date,
    		timeend?: string | Date,
			qualifiers?: string[],
			qualifier?: string
		}={},
		options: {}={}
	) : Promise<CrudCorrida> {
		const pronosticos = await this.get(filter,options)
		if(!pronosticos.length) {
			throw new Error("No se encontraron pronosticos")
		}
		return new CrudCorrida({
			cal_id: this.config.cal_id,
			forecast_date: this.forecast_date,
			series: groupBySeriesIdAndQualifier(pronosticos, undefined, "series_rast")
		})
	}

	async updatePronostico(
		filter : {
			forecast_date?: string | Date,
    		timestart?: string | Date,
    		timeend?: string | Date,
			qualifiers?: string[],
			qualifier?: string
		}={},
		options={}
	) : Promise<CrudCorrida> {
		const corrida = await this.getPronostico(filter,options)
		const created = await corrida.create()
		if(!created) {
			console.error("Corrida no insertada en base de datos")
			return corrida
		}
		return created
	}
	
	getDates(filter : {
		forecast_date?: Date|string,
		timestart?: Date|string,
		timeend?: Date|string
	}) : {
			forecast_date: Date,
			timestart?: Date,
			timeend?: Date,
			dates_dir: string,
			times_dir: string,
			forecast_date_path: string,
			forecast_time_path: string
		}
	{
		var forecast_date = (filter.forecast_date) ? new Date(filter.forecast_date) : this.default_forecast_date
		if(forecast_date.toString() == "Invalid Date") {
			throw new Error("Invalid forecast date")
		}
		forecast_date.setMinutes(0,0,0)
		var timestart, timeend
		if(filter.timestart) {
			timestart = new Date(filter.timestart)
			if(timestart.toString() == "Invalid Date") {
				throw new Error("Invalid timestart")
			}
		}
		if(filter.timeend) {
			timeend = new Date(filter.timeend)
			if(timeend.toString() == "Invalid Date") {
				throw new Error("Invalid timeend")
			}
		}
		// 	SET TIME TO MULTIPLE OF 6 //
		forecast_date.setUTCHours(forecast_date.getUTCHours() - forecast_date.getUTCHours()%6)
		var dates_dir = sprintf ("gefs.%04d%02d%02d/",forecast_date.getUTCFullYear(),forecast_date.getUTCMonth()+1,forecast_date.getUTCDate())
		var times_dir = sprintf ("%02d/", forecast_date.getUTCHours())
		// console.log({forecast_date: forecast_date.toISOString(),times_dir:times_dir, dates_dir:dates_dir})
		var forecast_date_path = __dirname + this.config.data_dir + dates_dir.replace(/\/$/,"")
		var forecast_time_path = __dirname + this.config.data_dir + dates_dir + times_dir.replace(/\/$/,"")
		this.path = forecast_time_path
		return {
			forecast_date: forecast_date,
			timestart: timestart,
			timeend: timeend,
			dates_dir: dates_dir,
			times_dir: times_dir,
			forecast_date_path: forecast_date_path,
			forecast_time_path: forecast_time_path
		}
	}
	
	async printMaps(forecast_date: Date) {
		const dates = this.getDates({forecast_date:forecast_date})
		return this.callPrintMaps(dates.forecast_date_path)
	}

	async callPrintMaps(path : string, skip_print?: boolean, location?: string) {
		var mapset = sprintf("%04d",Math.floor(Math.random()*10000))

		if(!location) {
			if(!(global as any).config?.grass?.location) {
				throw new Error("global.config.grass.location no definido")
			}
			var location_mapset = sprintf("%s/%s",(global as any).config.grass.location,mapset) // sprintf("%s/GISDATABASE/WGS84/%s",process.env.HOME,mapset)
		} else {
			var location_mapset = sprintf("%s/%s",location,mapset)
		}
		var batchjob = sprintf("%s/../py/print_wind_map.py",__dirname)
		if(path) {
			console.debug("callPrintWindMap: path: " + path)
			process.env.gefs_run_path = path
		}
		if(skip_print) {
			process.env.skip_print = "True"
		}
		var command = sprintf("grass %s -c --exec %s", location_mapset, batchjob)
		const result = await pexec(command)
		console.debug("batch job called")
		var stdout = result.stdout
		var stderr = result.stderr
		if(stdout) {
			console.log(stdout)
		}
		if(stderr) {
			console.error(stderr)
		}
		process.env.gefs_run_path = undefined
	}
}
