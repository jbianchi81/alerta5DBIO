'use strict'

const moment = require('moment-timezone')

const internal = {}
var parsePGinterval = require('postgres-interval')

function isJson(str) {
    try {
        JSON.parse(str);
    } catch (e) {
        return false;
    }
    return true;
}

internal.createInterval = function(value) {
	if(!value) {
		return //  parsePGinterval()
	}
	if(value.constructor && value.constructor.name == 'PostgresInterval') {
		var interval = parsePGinterval()
		Object.assign(interval,value)
		return interval
	}
	if(value instanceof Object) {
		var interval = parsePGinterval()
		Object.keys(value).map(k=>{
			switch(k) {
				case "milliseconds":
				case "millisecond":
					interval.milliseconds = value[k]
					break
				case "seconds":
				case "second":
					interval.seconds = value[k]
					break
				case "minutes":
				case "minute":
					interval.minutes = value[k]
					break
				case "hours":
				case "hour":
					interval.hours = value[k]
					break
				case "days":
				case "day":
					interval.days = value[k]
					break
				case "months":
				case "month":
				case "mon":
					interval.months = value[k]
					break
				case "years":
				case "year":
					interval.years = value[k]
					break
				default:
					break
			}
		})
		return interval
	}
	if(typeof value == 'string') {
		if(isJson(value)) {
			var interval = parsePGinterval()
			Object.assign(interval,JSON.parse(value))
			return interval
		} else {
			return parsePGinterval(value)
		}
	} else {
		console.error("timeSteps.createInterval: Invalid value")
		return
	}
}

internal.interval2iso8601String = function(interval) {
	return moment.duration(interval).toString()
}

internal.interval2string = function(interval) {
	if(!interval) {
		return "00:00:00"
	}
	if(interval instanceof Object) {
		if(Object.keys(interval).length == 0) {
			return "00:00:00"
		} else {
			var string = ""
			Object.keys(interval).forEach(key=>{
				string += interval[key] + " " + key + " "
			})
			return string.replace(/\s$/,"")
		}
	} else {
		return interval.toString()
	}
}

internal.interval2epochSync = function(interval) {
	if(!interval) {
		return 0
	}
	if(!interval instanceof Object) {
		console.error("interval must be an postgresInterval object")
		return
	}
	var seconds = 0
	Object.keys(interval).map(k=>{
		switch(k) {
			case "milliseconds":
			case "millisecond":
				seconds = seconds + interval[k] * 0.001
				break
			case "seconds":
			case "second":
				seconds = seconds + interval[k]
				break
			case "minutes":
			case "minute":
				seconds = seconds + interval[k] * 60
				break
			case "hours":
			case "hour":
				seconds = seconds + interval[k] * 3600
				break
			case "days":
			case "day":
				seconds = seconds + interval[k] * 86400
				break
			case "weeks":
			case "week":
				seconds = seconds + interval[k] * 86400 * 7
				break
			case "months":
			case "month":
			case "mon":
				seconds = seconds + interval[k] * 86400 * 31
				break
			case "years":
			case "year":
				seconds = seconds + interval[k] * 86400 * 365
				break
			default:
				break
		}
	})
	return seconds
}	



internal.getPreviousTimeStep = function(timestamp,def_hora_corte,timeSupport) {
	var timeSupport_e = internal.interval2epochSync(timeSupport) * 1000
	var def_hora_corte_e = internal.interval2epochSync(def_hora_corte) * 1000
	console.log({timeSupport_e:timeSupport_e,def_hora_corte_e:def_hora_corte_e})
	if(timeSupport_e >= 86400000) {
		timestamp = new Date(timestamp.getFullYear(),timestamp.getMonth(),timestamp.getDate(),0,0,0,0) // timestamp % (24 * 60 * 60 * 1000)
		timestamp.setTime(timestamp.getTime() + def_hora_corte_e)
	} else {
		var timestamp_d = new Date(timestamp.getFullYear(),timestamp.getMonth(),timestamp.getDate(),0,0,0,0)
		console.log({timestamp_d:timestamp_d})
		var time = timestamp.getTime() -  timestamp_d.getTime() //% (24 * 60 * 60 * 1000)
		console.log({time:time})
		timestamp -= time
		if(time < def_hora_corte_e) {
			timestamp += def_hora_corte_e
			timestamp -= timeSupport_e
		} else {
			time -= def_hora_corte_e
			time -= time % timeSupport_e
			timestamp += time + def_hora_corte_e
		}
	}
	console.log({timestamp:timestamp})
	return new Date(timestamp)
}
	
internal.advanceTimeStep = function(start_timestamp,timeSupport) {
	var timestamp = new Date(start_timestamp) 
	Object.keys(timeSupport).forEach(key=>{
		switch(key.toLowerCase()) {
			case "milliseconds":
			case "millisecond":
				timestamp.setMilliseconds(timestamp.getMilliseconds()+timeSupport[key])
				break
			case "seconds":
			case "second":
				timestamp.setSeconds(timestamp.getSeconds()+timeSupport[key])
				break
			case "minutes":
			case "minute":
				timestamp.setMinutes(timestamp.getMinutes()+timeSupport[key])
				break
			case "hours":
			case "hour":
				timestamp.setHours(timestamp.getHours()+timeSupport[key])
				break
			case "days":
			case "day":
				timestamp.setDate(timestamp.getDate()+timeSupport[key])
				break
			case "weeks":
			case "week":
				timestamp.setDate(timestamp.getDate()+timeSupport[key]*7)
				break
			case "months":
			case "month":
			case "mon":
				timestamp.setMonth(timestamp.getMonth()+timeSupport[key])
				break
			case "years":
			case "year":
				timestamp.setYear(timestamp.getFullYear()+timeSupport[key])
				break
			default:
				break
		}
	})
	return timestamp
}

internal.advanceInterval = function(date,interval={hours:1}) {
	if(!interval instanceof Object) {
		console.error("interval must be a postgresInterval object")
		return
	}
	var new_date = new Date(date)
	Object.keys(interval).map(k=>{
		switch(k) {
			case "milliseconds":
			case "millisecond":
				new_date.setUTCMilliseconds(new_date.getUTCMilliseconds() + interval[k])
				break
			case "seconds":
			case "second":
				new_date.setUTCSeconds(new_date.getUTCSeconds() + interval[k])
				break
			case "minutes":
			case "minute":
				new_date.setUTCMinutes(new_date.getUTCMinutes() + interval[k])
				break
			case "hours":
			case "hour":
				new_date.setUTCHours(new_date.getUTCHours() + interval[k])
				break
			case "days":
			case "day":
				new_date.setUTCDate(new_date.getUTCDate() + interval[k])
				break
			case "weeks":
			case "week":
				new_date.setUTCDate(new_date.getUTCDate() + interval[k]*7)
				break
			case "months":
			case "month":
			case "mon":
				new_date.setUTCMonth(new_date.getUTCMonth() + interval[k])
				break
			case "years":
			case "year":
				new_date.setUTCFullYear(new_date.getUTCFullYear() + interval[k])
				break
			default:
				break
		}
	})
	return new_date
}

internal.dateSeq = function (timestart,timeend,interval) {
	var seq = []
	var date = new Date(timestart)
	var end = new Date(timeend)
	while(date<=end) {
		seq.push(new Date(date))
		date = internal.advanceInterval(date,interval)
	}
	return seq
}

internal.date2tste = function(date) {
	var ts = new Date(date)
	ts = new Date(ts.getUTCFullYear(),ts.getUTCMonth(),ts.getUTCDate())
	var te = new Date(ts)
	te.setDate(te.getDate() + 1)
	return [ts, te]
}

internal.setTOffset = function(date,t_offset) {
	if(!t_offset) {
		t_offset = {"days":1}
	}
	t_offset = internal.createInterval(t_offset)
	var new_date = new Date(date.getUTCFullYear(),date.getUTCMonth(),date.getUTCDate())
	while(new_date < date) {
		var next_date = internal.advanceInterval(new_date,t_offset)
		if(next_date > date) {
			break
		}
		new_date = next_date
	}
	return new_date
}

// internal.string2interval(string=>{
// 	if(!string) {
// 		return {}
// 	}
// 	if(typeof string == "string") {
// 		var a = string.split(/\s+/)
// 		for(var i=0;i<a.length;i=i+2) {
// 			switch
// 		}
// 	}
// })

internal.doy2month = function(doy) {
    var date = new Date(2022,0,1)
    date.setUTCDate(doy)
    return date.getUTCMonth()
}

internal.doy2date = function(doy) {
    var date = new Date(2022,0,1)
    date.setUTCDate(doy)
    return date.getUTCDate()
}

internal.assertRealTime = function(observaciones,time_interval={"days":1},options={}) {
    // options: {count: int, count_series_id: int}

    if(!observaciones || !observaciones.length) {
        throw("No se encontraron observaciones")
    }
    time_interval = internal.createInterval(time_interval)
    if(!time_interval) {
        throw("time_interval inválido")
    }
    var date_limit = internal.advanceInterval(new Date(),time_interval)
    var max_date = observaciones[0].timestart
    var observaciones_rt = observaciones.filter(o=>{
        max_date = (o.timestart > max_date) ? o.timestart : max_date
        return o.timestart >= date_limit
    })
    if(!observaciones_rt.length) {
        throw(`No se encontraron observaciones a tiempo real (límite: ${date_limit.toISOString()}, fecha máxima encontrada: ${max_date.toISOString()}`)
    }
    if(options.count && observaciones_rt.length < options.count) {
        throw(`No se encontraron suficientes observaciones a tiempo real (solicitado: ${options.count}, encontrado: ${observaciones_rt.length})`)
    }
    if(options.count_series_id) {
        var series_id_set = new Set(observaciones_rt.map(o=>o.series_id))
        if(series_id_set.size < options.count_series_id) {
            throw(`No se encontraron suficientes series_id distintos a tiempo real (solicitado: ${options.count_series_id}, encontrado: ${series_id_set.size})`)
        }
    }
}

internal.daysInMonth = function(month, year) {
    return new Date(year, month, 0).getDate();
}

internal.roundDateTo = function(date,roundTo="hour",truncate=false,m=1,utc=true) {
	var new_date = new Date(date)
	if (roundTo == "month" || roundTo == "mon" || roundTo == "months") {
		if(!truncate) {
			var days_in_month = internal.daysInMonth(new_date.getMonth(),new_date.getFullYear())
			new_date.setMonth(new_date.getMonth() + Math.round(new_date.getDate()/days_in_month))
		}
		new_date.setDate(1)
		new_date.setHours(0,0,0,0)
		if(m>1) {
			new_date.setMonth(new_date.getMonth() - new_date.getMonth()%m)
		}
	} else if (roundTo == "day" || roundTo == "date" || roundTo == "days") {
		if(!truncate) {
			new_date.setDate(new_date.getDate() + Math.round(new_date.getHours()/24))
		}
		new_date.setHours(0,0,0,0)
		if(m>1) {
			new_date.setDate(1 + new_date.getDate() - new_date.getDate()%m)
		}
	} else if (roundTo == "hour"  || roundTo == "hours") {
		if(!truncate) {
			new_date.setHours(new_date.getHours() + Math.round(new_date.getMinutes()/60))
		}
		new_date.setMinutes(0,0,0)
		if(m>1) {
			if(utc) {
				new_date.setUTCHours(new_date.getUTCHours() - new_date.getUTCHours()%m)
			} else {
				new_date.setHours(new_date.getHours() - new_date.getHours()%m)
			}
		}
	} else if (roundTo == "minute" || roundTo == "minutes") {
		if(!truncate) {
			new_date.setMinutes(new_date.getMinutes() + Math.round(new_date.getSeconds()/60))
		}
		new_date.setSeconds(0,0)
		if(m>1) {
			new_date.setMinutes(new_date.getMinutes() - new_date.getMinutes()%m)
		}
	} else {
		throw("roundDateTo: illegal value for roundTo: use months,days,hours, minutes")
	}
	return new_date
}

internal.DateFromInterval = function(interval,date=new Date(),roundTo,truncate=false,m) {
	var new_interval = internal.createInterval(interval)
	var new_date = internal.advanceInterval(date, new_interval)
	if(roundTo) {
		return internal.roundDateTo(new_date,roundTo,truncate.valueOf,m)
	} else if (interval.roundTo) {
		// roundTo = internal.createInterval(interval.roundTo)
		roundTo = interval.roundTo
		if(Object.keys(roundTo).length < 1) {
			throw("DateFromInterval: Invalid roundTo interval")
		}
		// var roundToUnits = Object.keys(roundTo)[0]
		// var roundToValue = roundTo[roundToUnits]
		for( const [key,value] of Object.entries(roundTo)) {
			console.log("roundToUnits: " + key + ", roundToValue: " + value)
			new_date = internal.roundDateTo(new_date,key,truncate.valueOf,value)
		}
		return new_date
	} else {
		return new_date
	}

}

internal.parseDateString = function(date) {
	if(date instanceof Date) {
		return date
	}
	if(/^\d{4}-\d{1,2}-\d{1,2}$/.test(date)) {
		var utc_date = new Date(date)
		return new Date(utc_date.getUTCFullYear(),utc_date.getUTCMonth(),utc_date.getUTCDate())
	} else {
		return new Date(date)
	}
}

internal.DateFromDateOrInterval = function(date,reference_date,roundTo,truncate,m) {
	if(!date) {
		return
	}
	if(date instanceof Date) {
		// date = date
	} else if (typeof date === 'string' || date instanceof String) {
		date = internal.parseDateString(date)
	} else if (date instanceof Object) {
		date = internal.DateFromInterval(date,reference_date,roundTo,truncate,m)
	}
	if(date.toString() == "Invalid Date") {
		throw("Invalid Date")
	}
	return date
}

internal.getMonthlyTimeseries = function(timestart,timeend,date_offset=0, utc=false) {
	var result = []
	if(utc) {
		var ts = new Date(Date.UTC(timestart.getUTCFullYear(),timestart.getUTCMonth(),1 + date_offset))
	} else {
		var ts = new Date(timestart.getFullYear(),timestart.getMonth(),1 + date_offset)
	}
	while(ts<timeend) {
		var te = new Date(ts)
		if(utc) {
			te.setUTCMonth(te.getUTCMonth() + 1)
			te = new Date(Date.UTC(te.getUTCFullYear(),te.getUTCMonth(),1 + date_offset))
		} else {
			te.setMonth(te.getMonth() + 1)
			te = new Date(te.getFullYear(),te.getMonth(),1)
		}
		result.push({
			timestart: ts,
			timeend: te
		})
		ts = new Date(te)
	}
	return result
}

internal.getDailyTimeseries = function(timestart,timeend, hours_offset=0, utc=false) {
	var result = []
	if(utc) {
		var ts = new Date(Date.UTC(timestart.getUTCFullYear(),timestart.getUTCMonth(),timestart.getUTCDate(),hours_offset))
	} else {
		var ts = new Date(timestart.getFullYear(),timestart.getMonth(),timestart.getDate(),hours_offset)
	}
	while(ts<timeend) {
		var te = new Date(ts)
		if(utc) {
			te.setUTCDate(te.getUTCDate() + 1)
			te = new Date(Date.UTC(te.getUTCFullYear(),te.getUTCMonth(),te.getUTCDate(),hours_offset))
		} else {
			te.setDate(te.getDate() + 1)
			te = new Date(te.getFullYear(),te.getMonth(),te.getDate(),hours_offset)
		}
		result.push({
			timestart: ts,
			timeend: te
		})
		ts = new Date(te)
	}
	return result
}

internal.getHourlyTimeseries = function(timestart,timeend,inst=false) {
	var result = []
	var ts = new Date(timestart.getFullYear(),timestart.getMonth(),timestart.getDate(),timestart.getHours())
	timeend = (timeend) ? timeend : new Date()
	while(ts<timeend) {
		var te = new Date(ts)
		te.setHours(te.getHours() + 1)
		te = new Date(te.getFullYear(),te.getMonth(),te.getDate(),te.getHours())
		result.push({
			timestart: ts,
			timeend: (inst) ? ts : te
		})
		ts = new Date(te)
	}
	return result
}

internal.isoDurationToHours = function(aggregationDuration) {
    // ACcording to: ISO 8601 (https://tc39.es/proposal-temporal/docs/duration.html)
    var duration = moment.duration(aggregationDuration)
    return duration._data
}


module.exports = internal


// 

//~ var def_hora_corte = null
//~ var timeSupport = {years: 1}
//~ var ts = new Date(725857200000) // new Date('1993-01-01T03:00:00.000Z')
//~ var pts = getPreviousTimeStep(ts,def_hora_corte,timeSupport)
//~ console.log(pts)
//~ console.log(pts.toISOString())
