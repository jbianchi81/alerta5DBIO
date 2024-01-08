const {serie: Serie, observacion: Observacion, observaciones: Observaciones, serie} = require('./CRUD')
const axios = require('axios')

const internal = {}

internal.Client = class {

    static _get_is_multiseries = false

    default_config = {
        url: "http://69.28.90.79:5000/api",
        tabla: "prevenir",
        tipo: "puntual",
        proc_id: 1,
        var_id: 2,
        unit_id: 11
    }
    /**
     * prevenir accessor - Requests from FdX web API '/rango' method which returns a json array of objects, from each object it reads timestart from 'time' property assuming UTC time and value from 'Nivel' property assuming meters units. 'topic' parameter of request is taken from 'id_externo' property of estaciones plus '/nivel'. 'fecha_inicio' and 'fecha_fin' query parameters are interpreted as UTC and accept the format YYYY-MM-DD hh:mm:ss or YYYY-MM-DD. Example request: http://69.28.90.79:5000/api/rango?topic=telemetria_10/nivel&fecha_inicio=2024-01-05&fecha_fin=2024-01-05%2012:00:00
     * @param {Object} config
     * @param {string} config.url
     * @param {string} config.tabla
     * @returns {internal.Client} 
     */
    constructor(config={}) {
        this.config = {}
        Object.assign(this.config,this.default_config)
        Object.assign(this.config,config)
    }
    async test() {
        try {
			const response = await axios.get(`${this.config.url}/rango`)
            const {statusCode} = response
            if(statusCode != 200) {
                console.error(`Accessor test failed. Status code ${statusCode}`)
                return false
            }
			return true
		}
        catch(e) {
			console.error(e)
			this.ftp.end()
			return false
		}
	}
    async get(filter={}) {
        // if(!filter.timestart || !filter.timeend || !filter.series_id) {
        //     throw("Missing filter.timestart and/or filter.timeend and/of filter.series_id")
        // }
        // if(!filter.series_id) {
        //     if(!filter.estacion_id) {
        //         if(!filter.id_externo)  { 
        //             throw("Missing filter.series_id or filter.estacion_id or filter.id_externo")
        //         }
            // } 
        // } 
        // console.debug({filter:filter})
        const serie = await Serie.read({tipo:"puntual",id:filter.series_id}) // this.readSeries(filter)
        if(!serie) {
            console.warn("accessor.get: no series found")
            return []
        }
        // console.debug(series[0].estacion)
        const id_externo = serie.estacion.id_externo
        const series_id = serie.id
        const topic = `${id_externo}/nivel`
        const timestart = new Date(filter.timestart).toISOString().replace("T"," ")
        const timeend = new Date(filter.timeend).toISOString().replace("T"," ")
        const result = await this.requestRange(topic,timestart,timeend)
        const observaciones = []
        result.forEach((item,index)=> {
            try {
                var observacion = this.parseObservacion(item,series_id)
            } catch(e) {
                console.error(`Parse error at item ${index} of accessor.get response: ${e.toString()}`)
                return
            }
            observaciones.push(observacion)
        })
        var o = new Observaciones(observaciones).removeDuplicates()
        return o
    }
    async requestRange(topic,timestart,timeend) {
        // console.debug([topic,timestart,timeend])
        try {
            var response = await axios({
                method: "get",
                url: `${this.config.url}/rango`,
                params: {
                    topic: topic,
                    fecha_inicio: timestart,
                    fecha_fin: timeend
                },
                responseType: "json"
            })
            // console.debug(response)
            if(response.status != 200) {
                console.error(`accessor.requestRange: query error. status code ${response.status}, message: ${response.statusText}`)
                return []
            }
        } catch(e) {
            console.error(`accessor.requestRange: query error. ${e.toString()}`)
            console.error(e)
            return []
        }
        return response.data
    }
    parseObservacion(o,series_id) {
        if(!o.time) {
            throw("Missing time property")
        }
        const timestart = new Date(o.time.replace(" ","T") + ".000Z")
        if(timestart.toString() == "Invalid Date") {
            throw("Invalid date")
        }
        const valor = parseFloat(o.Nivel)
        if(valor.toString() == "NaN") {
            throw("Invalid value")
        }
        return new Observacion({
            timestart: timestart,
            timeend: timestart,
            valor: valor,
            series_id: series_id
        })
    }
    async update(filter={}) {
        const observaciones = await this.get(filter)
        if(!observaciones.length) {
            console.warn("accessor.update: no observaciones found")
            return []
        }
        return Observaciones.create(observaciones)
    }
    async readSeries(filter={}) {
        const series_filter = {}
        Object.assign(series_filter,filter)
        series_filter.tipo = this.config.tipo
        series_filter.tabla_id = this.config.tabla
        series_filter.proc_id = this.config.proc_id
        series_filter.var_id = this.config.var_id
        series_filter.unit_id = this.config.unit_id
        series_filter.id = series_filter.series_id
        delete series_filter.timestart
        delete series_filter.timeend
        // console.debug("series_filter: " + JSON.stringify(series_filter))
        var series = await Serie.read(series_filter)
        return series
    }
    async getSeries(filter={}) {
        delete filter.timestart
        delete filter.timeend
        // console.debug({filter:filter})
        if(!this.config.estaciones || !this.config.estaciones.length) {
            throw("estaciones list missing from accessor config")
        }
        var series = []
        for(var estacion of this.config.estaciones) {
            const serie = new Serie({
                tipo: this.config.tipo,
                estacion: estacion,
                var: {
                    id: this.config.var_id
                },
                procedimiento: {
                    id: this.config.proc_id
                },
                unidades: {
                    id: this.config.unit_id
                }
            })
            await serie.estacion.getEstacionId()
            await serie.getId()
            series.push(serie)
        }
        series = series.filter(s=>{
            const filtered = s.filterSerie(filter)
            // console.debug({
            //     series_id: s.id,
            //     id_externo: s.estacion.id_externo,
            //     filtered: filtered
            // })
            return filtered
        })
        // console.debug("length: " + series.length)
        return series
    }
    /**
     * Reads config.estaciones list and creates one Serie for each. Will fail if coordinates are missing
     * @param {Object} filter 
     * @returns {Serie[]} series
     */
    async updateSeries(filter={}) {
        const series = await this.getSeries(filter)
        await Serie.create(series,{upsert_estacion:true})
        return series
    }
}

module.exports = internal