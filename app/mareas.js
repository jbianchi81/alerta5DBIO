'use strict'

require('./setGlobal')
const internal = {};

internal.CRUD = class {
	// constructor(pool,config){
    //     this.pool = pool
	// 	if(config) {
	// 		this.config = config
    //     }
    // }
    static async getAlturasMareaFull(filter={}) {
        var timestart = (filter.timestart) ? new Date(filter.timestart) : new Date(new Date().getTime() - 2*24*3600*1000)
        var timeend = (filter.timeend) ? new Date(filter.timeend) : new Date(new Date().getTime() + 4*24*3600*1000)
        var filter_string = "WHERE timestart>=$1::timestamptz::timestamp AND timestart<=$2::timestamptz::timestamp"
        var params = [timestart,timeend]
        var placeholders = ["$3","$4"]
        if(filter.forecast_date) {
            var fecha_emision = new Date(filter.forecast_date)
            filter_string += " AND fecha_emision=" + placeholders.shift() + "::timestamptz::timestamp"
            params.push(fecha_emision)
        }
        if(filter.estacion_id) {
            if(Array.isArray(filter.estacion_id)) {
                filter_string += " AND estacion_id IN (" + filter.estacion_id.map(i=>parseInt(i)).join(",") + ")"

            } else {
                filter_string += " AND estacion_id=" + placeholders.shift()
                params.push(parseInt(filter.estacion_id))
            }
        }
        try {
            var result = await global.pool.query("SELECT  estacion_id, timestart, fecha_emision,\
                            altura_meteo, altura_astro, altura_suma, altura_suma_corregida\
                    FROM alturas_marea_full \
                    " + filter_string + " \
                    ORDER BY estacion_id,timestart,fecha_emision"
                ,params)              
        } catch(e) {
            throw(e)
        }
        return result.rows
    }
}

module.exports = internal