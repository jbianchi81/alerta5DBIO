const CRUD = require('../CRUD')
var fs = require('fs')
var fsPromises = require('promise-fs')
var axios = require("axios")
const https = require('https')
const timeSteps = require('../timeSteps')
const { serie } = require('../CRUD')


const internal = {}

internal.client = class {
	constructor(config) {
		if(config) {
			this.config = config
		} else {
			this.config = config.snih
		}
	}
	get (filter={}, options) {
		var codigoVariable, codigoEstacion, fechaDesde, fechaHasta
		if(!filter.series_id) {
			if(!filter.estacion_id) {
				return Promise.reject("falta estacion_id")
			} 
			if(!filter.var_id) {
				return Promise.reject("falta var_id")
			}
		} else {
			filter.id = filter.series_id
		}
		if(!filter.timestart || !filter.timeend) {
			return Promise.reject("Falta timestart y/o timeend")
		}
		fechaDesde = filter.timestart
		fechaHasta = filter.timeend
		delete filter.timestart
		delete filter.timeend
		delete filter.series_id
		filter.proc_id = [1,2]
		return CRUD.serie.read({tipo:"puntual",...filter})
		.then(series=>{
			if(!series) {
				throw new Error("serie no encontrada")
			}
            var serie
			if(Array.isArray(series)) {
                if(series.length==0) {
				    throw new Error("serie no encontrada")
                }
                serie = series[0]
			} else {
                serie = series
            }

			Object.keys(this.config.variable_map).forEach(key=> {
				if(this.config.variable_map[key].var_id == serie.var.id) {
					codigoVariable = key
				}
			})
			//~ console.log({codigoEstacion:codigoEstacion, codigoVariable: codigoVariable})
			return fsPromises.readFile(this.config.asociaciones,{encoding:"utf8"})
			.then(data=>{
				const asociaciones = JSON.parse(data).filter(a=> {
					var id_externo = a.Estacion % 10000 // codigoEstacion = 10000*a.Red + parseInt(series[0].estacion.id_externo) 
					return (serie.estacion.id_externo == id_externo && a.Codigo == codigoVariable)
				})
				if(asociaciones.length == 0) {
					throw ("No se encontraron asociaciones")
				}
				codigoEstacion = asociaciones[0].Estacion
				return this.getDatosHistoricos({timestart:fechaDesde,timeend:fechaHasta,estacion:codigoEstacion,codigo:codigoVariable,output:"/tmp/historicos.json"})
			})
			.then(listaDatosHistoricos=>{
				if(!listaDatosHistoricos) {
					throw "No se obtuvieron resultados"
				}
				return this.parseHistoricos(listaDatosHistoricos,{series_id:serie.id,"var":serie.var})
			})
		})
	}
	getDatosHistoricos(options={}) {
		const instance = axios.create({
		  httpsAgent: new https.Agent({  
			rejectUnauthorized: false
		  })
		});

		if(!options.timestart || ! options.timeend || !options.estacion || !options.codigo) {
			return Promise.reject("Faltan parametros")
		}
		var body = {"fechaDesde":options.timestart,"fechaHasta":options.timeend,"estacion":options.estacion,"codigo":options.codigo,"validados":(options.validados) ? options.validados : true}
		// console.log({AccessorRequestBody:body})
		return instance.post("https://snih.hidricosargentina.gob.ar/MuestraDatos.aspx/LeerDatosHistoricos",JSON.stringify(body),{headers:{Accept: 'application/json, text/javascript, */*; q=0.01', 'Content-Type': 'application/json; charset=utf-8'}})
		.then(response=>{
			if(!response.data) {
				return
			}
			//~ var listaDatosHistoricos = response.data.d
			if(options && options.output) {
				fs.writeFile(options.output,JSON.stringify(response.data.d.Mediciones,null,2),{encoding:"utf8"},(err) => {
				  if (err) throw err;
				  console.log('The file ' + options.output + ' has been saved!');
				})
			}
			return response.data.d.Mediciones
		})
	}
	parseHistoricos(listaDatosHistoricos,options) {
		// {"FechaHora":"/Date(473425200000)/","Medicion":1.38,"Calificador":" ","Validado":true}
		const observaciones = new CRUD.observaciones(
			listaDatosHistoricos.map(d=>{
				var timestart =  new Date(new Date().setTime(d.FechaHora.match(/\-?\d+/)[0]))
				var timeend = new Date(new Date().setTime(d.FechaHora.match(/\-?\d+/)[0]))
				//~ console.log({timestart_:timestart.toISOString(), timeend_: timeend.toISOString(),timeSupport: options.var.timeSupport, timeSupport_e: timeSteps.interval2epochSync(options.var.timeSupport)})
				if(options && options.var && options.var.timeSupport && timeSteps.interval2epochSync(options.var.timeSupport) != 0) {
					//~ console.log("timeSupport not 0/null")
					if(options.var.def_hora_corte) {
						timestart = timeSteps.getPreviousTimeStep(timestart,options.var.def_hora_corte,options.var.timeSupport)
					} else {
						timestart = timeSteps.getPreviousTimeStep(timestart,null,options.var.timeSupport)
					}
					timeend = timeSteps.advanceTimeStep(timestart,options.var.timeSupport)
				}
				var observacion = {
					timestart: timestart,
					timeend: timeend,
					valor: d.Medicion,
					descripcion: "calificador:" + d.Calificador + ",validado:" + d.Validado
				}
				//~ console.log({timestart:timestart.toISOString(), timeend: timeend.toISOString()})
				if(options && options.series_id) {
					observacion.series_id = options.series_id
				}
				return observacion
			})
		)
		return observaciones.removeDuplicates()
	}
	
	update(filter,options) {
		return this.get(filter,options)
		.then(result=>{
			return CRUD.observaciones.create(result) // .upsertObservaciones(result)
		})
	}
	
	getSites(filter) {
		return this.getEstacionesSNIH()
		.then(listaEstaciones=>{
			if(filter && filter.id_externo) {
				if(!Array.isArray(filter.id_externo)) {
					filter.id_externo = [filter.id_externo]
				}
				//~ var Codigos = filter.id_externo.map(i=>parseInt(i) + 10000)
				listaEstaciones = listaEstaciones.filter(e=> {
					var Codigos = filter.id_externo.map(i=> e.Red*10000 + parseInt(i))
					//~ console.log(Codigos)
					return (Codigos.indexOf(e.Codigo) >= 0)
				})
			} 
			if(filter && filter.red_id) {
				if(!Array.isArray(filter.red_id)) {
					filter.red_id = [filter.red_id]
				}
				listaEstaciones = listaEstaciones.filter(e=> filter.red_id.indexOf(e.Red) >= 0)
			} 
			if(filter && filter.nombre) {
				listaEstaciones = listaEstaciones.filter(e=> {
					var matches = e.Descripcion.match(filter.red_id)
					if(matches) {
						return true
					} else {
						return false
					}
				})
			} 
			return this.parseEstaciones(listaEstaciones)
		})
		.then(estaciones=>{
			if(filter && filter.estacion_id) {
				filter.id = filter.estacion_id
			}
			//~ var valid_filters = ["id"] // ,"id_externo"]
			//~ for(var key of valid_filters) {
				//~ if(filter[key]) {
					//~ estaciones = estaciones.filter(e=> (e[key] == filter[key]))
				//~ }
			//~ }
			if(filter && filter.id) {
				if(!Array.isArray(filter.id)) {
					filter.id = [filter.id]
				}
				estaciones = estaciones.filter(e=> filter.id.indexOf(e.id) >= 0)
			}
			return estaciones
		})
	}
	
	getEstacionesSNIH() {
		const instance = axios.create({
		  httpsAgent: new https.Agent({  
			rejectUnauthorized: false
		  })
		});
		return instance.post("https://snih.hidricosargentina.gob.ar/Filtros.aspx/LeerEstaciones",'',{headers:{Accept: 'application/json, text/javascript, */*; q=0.01', 'Content-Type': 'application/json; charset=utf-8'}})
		.then(response=>{
			if(!response.data) {
				return
			}
			return response.data.d
		})
	}
	parseEstaciones(estaciones) {
		return Promise.all(estaciones.map(e=>{
			var tipo = (/HM/.test(e.Tipo)) ? "A" : (/H/.test(e.Tipo)) ? "H" : (/M/.test(e.Tipo)) ? "M" : undefined 
			var estacion = {
				id_externo: (e.Codigo % 10000).toString(),
				tabla: "alturas_bdhi",
				nombre: e.Descripcion,
				rio: e.Rio,
				localidad: e.Poblado,
				pais: "Argentina",
				distrito: this.config.provincias_map[e.Provincia],
				cero_ign: (e.CeroEscala != -999) ? e.CeroEscala : null,
				geom: {
					type: "Point",
					coordinates: [ -1 * e.Longitud, -1 * e.Latitud ]
				},
				altitud: e.Cota,
				automatica: (e.Transmision == 'T' || e.Transmision == 'A') ? true : false,
				tipo: tipo,
				has_obs: true,
				habilitar: e.Habilitada,
				propietario: "snih:" + e.Red,
				real: true
			}
			estacion = new CRUD.estacion(estacion)
			return estacion.getEstacionId(global.pool)
			.then(()=>{
				//~ console.log(estacion.toString())
				return estacion
			})
		}))
	}
	
	updateSites(filter,options) {
		return this.getSites(filter)
		.then(estaciones=>{
			console.log("got " + estaciones.length + " estaciones")
			return CRUD.estacion.create(estaciones) // crud.upsertEstaciones(estaciones)
		})
	}
	//~ "Codigo": 13449,
    //~ "Descripcion": "San Javier",
    //~ "Regional": 2,
    //~ "Sistema": 2,
    //~ "Cuenca": 39,
    //~ "Red": 1,
    //~ "Subcuenca": "Río de la Plata",
    //~ "Provincia": 18,
    //~ "Rio": "Uruguay",
    //~ "Lugar": "San Javier",
    //~ "Poblado": "San Javier",
    //~ "Area": 95200,
    //~ "Cota": 79.77,
    //~ "Latitud": 27.8690833333333,
    //~ "Longitud": 55.13025,
    //~ "MesHidrologico": 1,
    //~ "NivelPsicrometrico": 1003.75,
    //~ "Ventilador": false,
    //~ "Alta": "/Date(-1459630800000)/",
    //~ "Baja": "/Date(7289578800000)/",
    //~ "CeroEscala": 79.77,
    //~ "SistemaCota": 5,
    //~ "AfluenteDe": "Río de la Plata",
    //~ "EsNavegable": "SI",
    //~ "Departamento": "San Javier",
    //~ "DistanciaDesembocadura": "960",
    //~ "Habilitada": true,
    //~ "Tipo": "HA",
    //~ "Transmision": "M",
    //~ "ModoDeLlegar": "Desde Corrientes por RN Nº 12 hasta el empalme con RN Nº 120. Continuar por la RN Nº 120, hasta el empalme con la RN Nº 14. Por esta última, hasta San José, y desde San José por la RP Nº 1 hasta la localidad de Azara. Desde Azara, tomar la R P Nº 2 hasta San Javier. Los caminos del recorrido, no presentan inconvenientes para acceder a la estación. \nTotal Recorrido: 460 Km.\n",
    //~ "Actual": "S",
    //~ "RegistroValidoHasta": "/Date(7289578800000)/",
    //~ "Autor": 115,
    //~ "Registro": "/Date(1578670814000)/"

	async getSeries(filter={},options={},client) {
		var series_filter = {...filter}
		if(filter && filter.series_id) {
			const serie = await CRUD.serie.read({id: filter.series_id})
			if(!serie) {
				console.error("Serie id " + filter.series_id + " not found")
				return []
			}
			series_filter.id_externo = [serie.estacion.id_externo]
			series_filter.estacion_id = [serie.estacion.id]
			series_filter.var_id = [serie.var.id]
			series_filter.unit_id = [serie.unidades.id]
		} else {
			for(var key of ["id_externo", "estacion_id", "var_id", "unit_id"]) {
				if(series_filter[key] && !Array.isArray(series_filter[key])) {
					if(key == "id_externo") {
						series_filter[key] = [series_filter[key].toString()]
					} else {
						series_filter[key] = [parseInt(series_filter[key])]
					}
				}
			}
		}
		await this.getEstacionMap(client)
		fs.writeFile("/tmp/estacion_map.json",JSON.stringify(this.config.estacion_map,null,2),{encoding:"utf8"},err=>{
			if(err) throw err
			console.log("Se escribió el archivo /tmp/estacion_map.json")
		})
		const listaAsociaciones = await this.getAsociaciones(options)
		const series = []
		for (var a of listaAsociaciones) {
			var id_externo = (a.Estacion % 10000).toString()
			//~ console.log(series_filterid_externo:id_eseries_filter,filter_id_externo:filter.id_externo})
			if(series_filter.id_externo && series_filter.id_externo.indexOf(id_externo)<0) {
				continue
			}
			if(!this.config.estacion_map[id_externo]) {
				console.warn("no existe estacion_map." + id_externo + ". skipping.")
				continue
			}
			var estacion_id = this.config.estacion_map[id_externo].id
			if(series_filter.estacion_id && series_filter.estacion_id.indexOf(estacion_id)<0) {
				continue
			}
			if(!this.config.variable_map[a.Codigo]) {
				// console.error("no existe variable_map." + a.Codigo)
				continue
			}
			var var_id = this.config.variable_map[a.Codigo].var_id
			if(series_filter.var_id && series_filter.var_id.indexOf(var_id)<0) {
				continue
			}
			var unit_id = this.config.variable_map[a.Codigo].unit_id
			if(series_filter.unit_id && series_filter.unit_id.indexOf(unit_id)<0) {
				continue
			}
			//~ return crud.getEstaciones({tabla:"alturas_bdhi",id_externo:id_externo})
			//~ .then(estaciones=>{
				//~ if(estaciones.length==0) {
					//~ console.error("No existe la estación " + id_externo)
					//~ return
				//~ }
				//~ if(filter.estacion_id && estaciones[0].id != filter.estacion_id) {
					//~ return
				//~ }
			const serie = new CRUD.serie({
				tipo: "puntual",
				var_id: var_id,
				proc_id: 1,
				unit_id: unit_id,
				estacion_id: estacion_id // estaciones[0].id
			})
			await serie.getId(false)
			series.push(serie)
			//~ })
		}
		return series
			
					//~ const valid_filters = ["var_id","proc_id","unit_id","estacion_id","id_externo"]
					//~ for(var key of valid_filters) {
						//~ if(filter[key]) {
							//~ series = series.filter(s=> (s[key] == filter[key]))
						//~ }
					//~ }
					//~ return series
				//~ })
	}
	
	async upsertSeries(filter,options={},client) {
		return this.getSeries(filter,options,client)
		.then(series=>{
			return CRUD.serie.create(series,{all:options.all}) // crud.upsertSeries(series,options.all,undefined,undefined)
		})
	}

	async updateSeries(filter,options={}) {
		return this.upsertSeries(filter,options)
	}
	//~ "__type": "Compartido.Contenedor.CAsociacionesCN",
    //~ "Estacion": 13858,
    //~ "Codigo": 206,
    //~ "Desde": "/Date(1464800707000)/",
    //~ "Hasta": "/Date(7289578800000)/",
    //~ "Minimo": 14,
    //~ "Maximo": 30,
    //~ "TipoValidacion": 11,
    //~ "Actual": "S",
    //~ "RegistroValidoHasta": "/Date(7289578800000)/",
    //~ "Autor": 3,
    //~ "Registro": "/Date(1508267336000)/"
	
	getAsociaciones(options) {
		const instance = axios.create({
		  httpsAgent: new https.Agent({  
			rejectUnauthorized: false
		  })
		});
		return instance.post("https://snih.hidricosargentina.gob.ar/MuestraDatos.aspx/LeerListaAsociaciones",'',{headers:{Accept: 'application/json, text/javascript, */*; q=0.01', 'Content-Type': 'application/json; charset=utf-8'}})
		.then(response=>{
			if(!response.data) {
				return
			}
			//~ listaAsociaciones = response.data.d
			if(options && options.output) {
				fs.writeFile(options.output,JSON.stringify(response.data.d,null,2),{encoding:"utf8"},(err) => {
				  if (err) throw err;
				  console.log('The file ' + options.output + ' has been saved!');
				})
			}
			return response.data.d
		})
	}
	
	async getEstacionMap(client) {
		return CRUD.estacion.read({tabla:"alturas_bdhi"}) // crud.getEstaciones({tabla:"alturas_bdhi"},undefined,client)
		.then(estaciones=>{
			this.config.estacion_map = {}
			estaciones.forEach(e=>{
				if(e.id_externo == undefined) {
					return
				}
				this.config.estacion_map[e.id_externo] = {
					id: e.id,
					nombre: e.nombre
				}
			})
			return
		})
	}
}

module.exports = internal