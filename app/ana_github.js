const internal = {}
const axios = require('axios')
const fs = require('fs')
const path = require('path')
const CRUD = require('./CRUD')

internal.ana_github = class {
    constructor(config) {
        this.config = (config) ? config : {}
		this.config.sites_local_file = (this.config.sites_local_file) ? (fs.existsSync(path.resolve(this.config.sites_local_file))) ? path.resolve(this.config.sites_local_file) : path.resolve(__dirname,"../data/ana/ListaEstacoesTelemetricas.xml") : path.resolve(__dirname,"../data/ana/ListaEstacoesTelemetricas.xml")
	}
	async test() {
        try {
            var response = await axios.get(this.config.listing_url)
        } catch(e) {
            console.error(e)
            return false
        }
        if(response.statusText == 'OK') {
            return true
        } else {
            return false
        }
    }
    async get(filter={},options={}) {
        await this.getAvailableSitesAll()
        if(filter && filter.series_id && !Array.isArray(filter.series_id)) {
			const serie = await CRUD.serie.read({id:filter.series_id, tipo:"puntual"},options)
            try {
    			var result = await this.getObservaciones(serie.estacion,[serie],filter.timestart,filter.timeend,false,options)
            } catch(e) {
                console.log("Request failed for estacion " + serie.estacion.id)
                console.error(e.toString())
                return []
            }
            return result
		}
		options.update=false
		return this.getDataBatch(filter,options)
    }
    async update(filter,options={}) {
        await this.getAvailableSitesAll()
		if(filter && filter.series_id && !Array.isArray(filter.series_id)) {
			const serie = await CRUD.serie.read({id:filter.series_id, tipo:"puntual"})
			return this.getObservaciones(serie.estacion,[serie],filter.timestart,filter.timeend,true,options)
		}
		options.update=true
		return this.getDataBatch(filter,options)
	}
    async getObservaciones(estacion,series,timestart,timeend,update=false,options) {
		var series_id = {}
		for(var serie of series) {
			if(serie.var.id==2) {
					series_id.Nivel = serie.id
			} else if(serie.var.id==4) {
				series_id.Vazao = serie.id
			} else if(serie.var.id==27 && this.config.precip_estacion_ids.indexOf(serie.estacion.id) >= 0) {
				console.log("estacion: " + serie.estacion.id + " precip series:id:" + serie.id)
				series_id.Chuva = serie.id
			}
		}
        try {
    		var obs = await this.getData(estacion.id_externo,timestart,timeend,series_id)
        } catch(e) {
            throw(e)
        }
		console.log("got " + obs.length + " observaciones from station " + estacion.id)
		if(update) {
			var upserted = await CRUD.CRUD.upsertObservaciones(obs,"puntual",undefined,undefined)
			var length = upserted.length
			console.log("upserted " + length + " registros for station " + estacion.id)
			upserted=""
			obs=""
			if(options.run_asociaciones) {
				var result = await CRUD.CRUD.runAsociaciones({estacion_id:estacion.id,source_var_id:27,source_proc_id:1,timestart:timestart,timeend:timeend},{inst:true,no_send_data:true})
				if(!result) {
					console.error("No records created from estacion_id="+estacion.id+" var_id=27 for asoc")
				} else {																		//~ return [...upserted,...result]
					length+=result.length
				}
				result = await CRUD.CRUD.runAsociaciones({estacion_id:estacion.id,source_var_id:31,source_proc_id:1,timestart:timestart,timeend:timeend},{no_send_data:true})
				if(!result) {
					console.error("No records created from estacion_id="+estacion.id+" var_id=31 for asoc")
				} else {
					//~ return [...upserted,...result]
					length+=result.length
				}
				// result = await crud.runAsociaciones({estacion_id:estacion.id,source_var_id:4,source_proc_id:1,timestart:timestart,timeend:timeend},{no_send_data:true})
				// if(!result) {
				// 	console.error("no records created from estacion_id="+estacion.id+" var_id=4 for asoc")
				// } else {
				// 	length+= result.length
				// }
				result=""
				return length
			} else {
				return length
			}
		} else {
			return obs
		}
	}
    async getData(id_externo,timestart,timeend,series_id,var_columns=this.config.var_columns,url=this.config.url,dir=this.config.remote_data_dir) {
        // console.log(series_id)
        if(! this.available_sites) {
            throw("Available sites is not defined")
        }
        if(Object.keys(this.available_sites).indexOf(id_externo) < 0) {
            throw("id_externo not present in available sites")
        }
        var abs_url = `${url}/${this.available_sites[id_externo].path}`
        try {
            var response = await axios({
                method: "get",
                url: abs_url,
                responseType: "text",
                transformResponse: undefined
            })
        } catch(e) {
            if(e.response) {
                throw(new Error(e.response.data))
            } else if(e.request) {
                throw(new Error(e.request))
            } else {
                throw(new Error(e.message))
            }
            // console.error(e.config)
            // return
        }
        var csv = response.data
        var data = csv.split("\n")
        var header = data.shift()
        data = data.map(row=>{            
            // console.log(row)
            return row.split(";").map(e=>e.replace(/\"/g,"")) // JSON.parse("[" + row + "]")
        })
        const observaciones = []
        data.forEach((row,i)=>{
            const datetime = row[0].split(" ")
            if(datetime.length <2) {
                console.log("invalid date: " + row[0] + "Skipping.")
                return
            }
            const date = datetime[0].split("/")
            const time = datetime[1].split(":")
            const timestamp = new Date(date[2],date[1]-1,date[0],time[0],time[1],time[2])
            if(timestamp.toString() == "Invalid Date") {
                console.error("Invalid Date: " + row[0] + ". Skipping row")
                return
            }
            if(timestart && timestamp < timestart) {
                // skip row
                return
            }
            if(timeend && timestamp > timeend) {
                // skip row
                return
            }
            if(series_id.Nivel) {
                if(!var_columns.Nivel) {
                    console.warn("var_columns.Nivel not set")
                } else {
                    const column = var_columns.Nivel
                    if(/^\s*$/.test(row[column])) {
                        // console.warn("Nivel column of row " + i + " is empty")
                    } else {
                        const valor = parseFloat(row[column].replace(",","."))
                        if(valor.toString() == "NaN") {
                            console.error("Invalid float: " + row[column] + ". Skipping")
                        } else {
                            observaciones.push(new CRUD.observacion({
                                series_id: series_id.Nivel,
                                tipo: "puntual",
                                timestart: timestamp,
                                timeend: timestamp,
                                valor: valor * 0.01 // <= cm to m
                            }))
                        }
                    }
                }
            }
            if(series_id.Chuva) {
                if(!var_columns.Chuva) {
                    console.warn("var_columns.Chuva not set")
                } else {
                    const column = var_columns.Chuva
                    if(/^\s*$/.test(row[column])) {
                        // console.warn("Chuva column of row " + i + " is empty")
                    } else {
                        const valor = parseFloat(row[column].replace(",","."))
                        if(valor.toString() == "NaN") {
                            console.error("Invalid float: " + row[column] + ". Skipping")
                        } else {
                            observaciones.push(new CRUD.observacion({
                                series_id: series_id.Chuva,
                                tipo: "puntual",
                                timestart: timestamp,
                                timeend: timestamp,
                                valor: valor
                            }))
                        }
                    }
                }
            }
            if(series_id.Vazao) {
                if(!var_columns.Vazao) {
                    console.warn("var_columns.Vazao not set")
                } else {
                    const column = var_columns.Vazao
                    if(/^\s*$/.test(row[column])) {
                        // console.warn("Chuva column of row " + i + " is empty")
                    } else {
                        const valor = parseFloat(row[column].replace(",","."))
                        if(valor.toString() == "NaN") {
                            console.error("Invalid float: " + row[column] + ". Skipping")
                        } else {
                            observaciones.push(new CRUD.observacion({
                                series_id: series_id.Chuva,
                                tipo: "puntual",
                                timestart: timestamp,
                                timeend: timestamp,
                                valor: valor
                            }))
                        }
                    }
                }
            }
        })
        return observaciones
    }
    async getDataBatch(filter,options) {
		//~ console.log({filter:filter})
		var getestacionesfilter = {tabla:"red_ana_hidro"}
		if(filter.estacion_id) {
			getestacionesfilter.unid = filter.estacion_id
		}
		var timestart=new Date()
		timestart.setTime(timestart.getTime() - 7*24*3600*1000)
		var timeend=new Date()
		if(filter.timestart) {
			timestart = new Date(filter.timestart)
		}
		if(filter.timeend) {
			timeend = new Date(filter.timeend)
		}
        try {
    		var estaciones = await CRUD.CRUD.getEstaciones(getestacionesfilter,undefined)
        } catch (e) {
            throw(e)
        }
        if(estaciones.length==0) {
            console.error("no estaciones found")
            throw new Error("no estaciones found")
        }
        var observaciones = []
        var estaciones_with_data_count = 0
        for(var i=0;i<estaciones.length;i++) {
            var e = estaciones[i]
            if(!e.id_externo) {
                console.log("missing id_externo for estacion_id:"+e.id)
                continue
            }
            //~ console.log({id_externo:e.id_externo})
            var series = await CRUD.CRUD.getSeries('puntual',{estacion_id:e.id,proc_id:1,var_id:filter.var_id},undefined)
            try {
                var obs = await this.getObservaciones(e,series,timestart,timeend,options.update,options)
            } catch(error) {
                console.error("Request failed for estacion " + e.id)
                console.error(error.toString())
                continue
            }
            if(obs.length) {
                estaciones_with_data_count++
                observaciones.push(...obs)
            }
        }
        if(options.update) {
            console.log("upserted "+ observaciones.length + " registros")
            return observaciones
        } else if(observaciones.length==0) {
            console.error("no data found")
            return []
        }
        console.log("got " + observaciones.length + " observaciones from " + estaciones_with_data_count + " estaciones") 
        return observaciones 
	}
    async getAvailableSitesAll() {
        console.log(this.config)
        // dados
        const sites = await this.getAvailableSites(this.config.listing_url)
        console.log("sites: " + Object.keys(sites).length)
        Object.keys(sites).forEach(key=>{
            sites[key].var_columns = this.config.var_columns
            sites[key].dir = this.config.remote_data_dir
        })
        // dados_setor_eletrico
        const sites_sector_electrico = await this.getAvailableSites(this.config.sector_electrico_listing_url)
        console.log("sites_sector_electrico: " + Object.keys(sites_sector_electrico).length)
        Object.keys(sites_sector_electrico).forEach(key=>{
            sites_sector_electrico[key].var_columns = this.config.var_columns_sector_electrico
            sites_sector_electrico[key].dir = this.config.remote_data_dir_sector_electrico
        })
        Object.assign(sites,sites_sector_electrico)
        this.available_sites = sites
    }
    async getAvailableSites(url=this.config.listing_url) {
        console.log("get listing from " + url)
        try {
            var response = await axios.get(url,{
                headers: {
                    "Host": "github.com",
                    "Accept": "*/*",
                    "user-agent": "curl/7.81.0"
                }
            })
        } catch(e) {
            throw(e)
        }
        const listing = response.data
        // return listing
        const available_sites = {}
        listing.payload.tree.items.forEach((item,i)=>{
            if(item.name) {
                var name = item.name.match(/^\d+/)
                if(!name || !name.length) {
                    console.error("Invalid file name: " + item.name + ". Skipping")
                    return
                }
                const id_externo = parseInt(name[0])
                if(id_externo.toString() == "NaN") {
                    console.error("Invalid id_externo: " + name[0])
                    return
                }
                available_sites[id_externo.toString()] = item
            }
        })
        return available_sites
    }
    // async getAvailableSitesSectorElectrico() {
    //     return this.getAvailableSites(this.config.sector_electrico_listing_url)
    // }
    // async getSitesSectorElectrico(filter={},url=this.config.sector_electrico_listing_url) {
    //     return this.getSites(filter,url)
    // }
    async getSites(filter={}) {
        await this.getAvailableSitesAll()
        var available_sites = Object.keys(this.available_sites)
        console.log("Available sites: " + available_sites.length)
        if(filter.id_externo) {
            if(!Array.isArray(filter.id_externo)) {
                if(available_sites.indexOf(filter.id_externo) < 0) {
                    console.error("filter.id_externo: " + filter.id_externo + " not found in directory listing")
                    return []
                }
            } else {
                const found_ids = []
                filter.id_externo.forEach((id,i)=>{
                    if(available_sites.indexOf(id) < 0) {
                        console.error("filter.id_externo: " + id + " not found in directory listing")
                    } else {
                        found_ids.push(id)
                    }
                })
                filter.id_externo = found_ids
            }
        } else {
            filter.id_externo = available_sites
        }
        filter.tabla = "red_ana_hidro"
        console.log(filter)
        const sites = await CRUD.estacion.read(filter)
        console.log("Found sites: " + sites.length)
        return sites
    }
}

module.exports = internal.ana_github