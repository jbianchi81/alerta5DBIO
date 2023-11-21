var PromiseFtp = require("promise-ftp")
const {serie: Serie, observacion: Observacion} = require('./CRUD')
// const CSV = require('csv-string')
const { parse: CSVparse } = require('csv-parse/sync')
const fs = require('fs')
const path = require('path')

const internal = {}

internal.Client = class {

    static _get_is_multiseries = true

    default_config = {
        url: "genica.com.ar",
        path: "/",
        local_path: "../data/genica",
        series_map: {},
        tabla: "alturas_genica"
    }
    /**
     * Genica accessor - gets csv files ftp server, parses date-time from Fecha and Hora columns assuming local time and value from Alturas column which is converted from cm to m. series_id must be assigned manually on config.series_map: {id_externo:{estacion_id: int, series_id: int}, ...}
     * @param {Object} config
     * @param {string} config.url
     * @param {string} config.user
     * @param {string} config.password
     * @returns {internal.Client} 
     */
    constructor(config={}) {
        this.config = {}
        Object.assign(this.config,this.default_config)
        Object.assign(this.config,config)
        this.ftp = new PromiseFtp();
    }
    async connect() {
        return this.ftp.connect(
            {
                host: this.config.url, 
                user: this.config.user, 
                password: this.config.password
            }
        )
    }
    async test() {
		return this.connect()
		.then( serverMessage=>{
			console.log('serverMessage:' + serverMessage)
			this.ftp.end()
			return true
		}).catch(e=>{
			console.error(e)
			this.ftp.end()
			return false
		})
	}
    filterById(filter={},id) {
        id = id.toString()
        if(filter.id_externo) {
            if(Array.isArray(filter.id_externo)) {
                if(filter.id_externo.indexOf(id) < 0) {
                    return true
                }
            } else {
                if(filter.id_externo != id) {
                    return true
                }
            }

        }
        if(filter.estacion_id) {
            if(!this.config.series_map[id]) {
                return true
            }
            var estacion_id = this.config.series_map[id].estacion_id
            if(Array.isArray(filter.estacion_id)) {
                if(filter.estacion_id.indexOf(estacion_id) < 0) {
                    return true
                }
            } else {
                if(filter.estacion_id != estacion_id) {
                    return true
                }
            }
        }
        if(filter.series_id) {
            if(!this.config.series_map[id]) {
                return true
            }
            var series_id = this.config.series_map[id].series_id
            if(Array.isArray(filter.series_id)) {
                if(filter.series_id.indexOf(series_id) < 0) {
                    return true
                }
            } else {
                if(filter.series_id != series_id) {
                    return true
                }
            }
        }
        return false
    }
    filterByDateRange(serie,timestart,timeend) {
        if(timestart) {
            serie.observaciones = serie.observaciones.filter(o=> o.timestart >= timestart)
        }
        if(timeend) {
            serie.observaciones = serie.observaciones.filter(o=> o.timestart <= timeend)
        }
    }
    async get(filter={}) {
        await this.connect()
        const list = await this.ftp.list(this.config.path)
        const results = []
        console.log("Got list of " + list.length + " files")
        for(var file of list) {
            // console.log(file.name)
            const match = file.name.match(/^file_(\d+)\.csv$/)
            if(!match) {
                console.log("Skip file " + file.name)
                continue
            }
            const id = parseInt(match[1])
            if(this.filterById(filter,id)) {
                console.log("Filter out file " + file.name)
                continue
            }
            console.log("Get file: " + file.name)
            const local_copy = path.resolve(__dirname, this.config.local_path,`${id}.csv`)
            await this.getFile(file.name,local_copy)
            const serie = this.parseFile(local_copy,id)
            this.filterByDateRange(serie,filter.timestart,filter.timeend)
            results.push(serie)
        }
        await this.ftp.end()
        return results
    }
    async getFile(filename,local_copy) {
        return this.ftp.get(filename)
        .then( stream => {
            return new Promise(function (resolve, reject) {
                stream.once('close', resolve);
                stream.once('error', reject);
                stream.pipe(fs.createWriteStream(local_copy));
            })
        })
    }
    parseFile(file,id) {
        const observaciones = CSVparse(
            fs.readFileSync(file,'utf-8'),
            {
                columns: true,
                delimiter: ";",
                trim: true,
                record_delimiter: "\n"
            }
        )
        return new Serie({
            tipo: "puntual",
            id: (this.config.series_map[id]) ? this.config.series_map[id].series_id : undefined,
            observaciones: observaciones.map(o=> this.parseObservacion(o))
        })
    }
    parseObservacion(o) {
        var date = o.Fecha.split("/").map(d=>parseInt(d))
        var time = o.Hora.split(":").map(d=>parseInt(d))
        const timestart = new Date(date[2],date[1]-1,date[0],time[0],time[1])
        const valor = parseInt(o.Altura) * 0.01
        // console.log("timestart: " + timestart.toISOString() + ", valor:" + valor.toString())
        return new Observacion({
            timestart: timestart,
            timeend: timestart,
            valor: valor
        })
    }
    async update(filter={}) {
        const series = await this.get(filter)
        for(var serie of series) {
            if(!serie.id) {
                console.warn("Missing id of series. Skipping")
                continue
            }
            console.log("createObservaciones series_id: " + serie.id)
            await serie.createObservaciones()
        }
        return series
    }
    async readSeries(filter={}) {
        const series_id = Object.keys(this.config.series_map).forEach(key=> this.config.series_map[key].series_id)
        var series = await Serie.read({id:series_id})
        series = series.filter(s=>s.filterSerie(filter))
        return series
    }
    async getSeries(filter={}) {
        return Object.keys(this.config.series_map).map(key=> {
            return new Serie({
                tipo: "puntual",
                id: this.config.series_map.series_id,
                estacion: {
                    id: this.config.series_map.estacion_id,
                    id_externo: key,
                    tabla: this.config.tabla
                },
                var: {
                    id: 2
                },
                unidades: {
                    id: 11
                },
                procedimiento: {
                    id: 1
                }
            })
        })
    }
    /**
     * Will fail if estaciones are not found in database (Must create manually). This service is missing fundamental metadata (i.e. station coordinates) 
     * @param {Object} filter 
     * @returns 
     */
    async updateSeries(filter={}) {
        const series = await this.getSeries(filter)
        await Serie.create(series)
        return series
    }
}

module.exports = internal