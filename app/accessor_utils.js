var pexec = require('child-process-promise').exec;
var fsPromises = require('promise-fs')
var fs = require('fs')
const axios = require('axios')
const Observacion = require('./CRUD').observacion
const { booleanPointInPolygon } = require('@turf/boolean-point-in-polygon')

// accessors aux functions 

const internal = {}

internal.grib2obs = async function(config={}) { // LEE 1 GRIB, GENERA GTIFFs  // config={filepath:string, variable_map:{"key":{var_id:int,proc_id:int,unit_id:int,series_id:int},...},bbox:{leftlon:number,toplat:number, rightlon:number,bottomlat:number}, units: string}
	if(!config.filepath) {
		return Promise.reject("Falta filepath")
	}
	if(!config.variable_map) {
		return Promise.reject("Falta variable_map")
	}
    const units = config.units ?? "meters_per_second"
	const gdalinfo_result = await pexec('gdalinfo -json ' + config.filepath)
    var stdout = gdalinfo_result.stdout
    var stderr = gdalinfo_result.stderr
    var gdalinfo = JSON.parse(stdout)
    //~ var time_update = new Date(parseInt(gdalinfo.bands[0].metadata[""].GRIB_REF_TIME.split(/\s/)[0])*1000)
    const observaciones = []
	for(var band of gdalinfo.bands) {
        // var ref_time = new Date(parseInt(band.metadata[""].GRIB_REF_TIME.split(/\s/)[0])*1000)
        // var valid_time = new Date(parseInt(band.metadata[""].GRIB_VALID_TIME.split(/\s/)[0])*1000)
        var var_index  =  Object.keys(config.variable_map).indexOf(band.metadata[""].GRIB_ELEMENT)
        if(var_index < 0) {
            console.warn("band not mapped:"+band.metadata[""].GRIB_ELEMENT)
            continue
        } 
        var variable = config.variable_map[band.metadata[""].GRIB_ELEMENT]
        var gtiff_filename = config.filepath.replace(/\.grib2$/,"." + variable.name.replace(new RegExp(/\s/g),"") + ".tif")
        await pexec('gdal_translate -b ' + band.band + ' -a_srs EPSG:4326 -a_ullr ' + config.bbox.leftlon + ' ' + config.bbox.toplat + ' ' + config.bbox.rightlon + ' ' + config.bbox.bottomlat + ' -of GTiff ' + config.filepath + ' "' + gtiff_filename + '"')
        await pexec(`gdal_edit.py -mo "UNITS=${units}" ` + gtiff_filename)
        observaciones.push(await internal.rast2obs(gtiff_filename,variable.series_id))
	}
    console.log("got " + observaciones.length + " observaciones")
    return observaciones
}


internal.rast2obs = async function(filename,series_id) { // LEE GTIFF , GENERA observación  // filename, series_id, 
	var observacion = {}
	const gdalinfo_result = await pexec('gdalinfo -json '+filename)
    var stdout = gdalinfo_result.stdout
    var stderr = gdalinfo_result.stderr
    if(stderr) {
        console.error(stderr)
    }
    var gdalinfo = JSON.parse(stdout)
    var band = gdalinfo.bands[0]
    var ref_time = new Date(parseInt(band.metadata[""].GRIB_REF_TIME.split(/\s/)[0])*1000)
    var valid_time = new Date(parseInt(band.metadata[""].GRIB_VALID_TIME.split(/\s/)[0])*1000)
    observacion = {
        tipo: "rast",
        timeupdate: ref_time,
        timestart: new Date(valid_time), 
        timeend: new Date(valid_time)
    }
    if(series_id) {
        observacion.series_id = series_id
    }
    const data = await fsPromises.readFile(filename, 'hex')
    observacion.valor = '\\x' + data
    return new Observacion(observacion)
}

internal.roundTo = function (value,precision) {
	value = parseFloat(value.toString().replace(/\s.*$/,"").replace(",","."))
	var regexp = new RegExp("^(\\d+\\." + "\\d".repeat(precision) + ")\\d+$")
	return parseFloat((value+.5/10**precision).toString().replace(regexp,"$1"))
}

internal.delay = async function(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

internal.promiseFromChildProcess = async function (child,filename) {
	return new Promise(function (resolve, reject) {
		child.addListener("error", reject);
		child.addListener("exit", resolve(filename));
		child.stdout.on('data', function(data) {
			console.log('stdout: ' + data);
		});
		child.stderr.on('data', function(data) {
			console.log('stderr: ' + data);
		});
	});
}
	
internal.toDate = function(d) {
	if(d instanceof Date) {
		return d
	} else {
		var d2;
		if(/^\d{4}-\d{2}-\d{2}\s*$/.test(d)) {
			d2 = d.replace(/\s+/,"");
			d2 = d2 + "T00:00:00.000Z";
		} else if(/^\d{4}-\d{2}-\d{2}[\s|T]\d{2}(:\d{2})?(:\d{2})?$/.test(d)) {
			d2 = d.replace(/\s/,"T") + ":00:00.000Z".substring(d.length-13);
		} else {
			d2=d;
		}			
		return new Date(d2);
	}
}

internal.flatten = function(arr) {
  return arr.reduce(function (flat, toFlatten) {
    return flat.concat(Array.isArray(toFlatten) ? internal.flatten(toFlatten) : toFlatten);
  }, []);
}

internal.printAxiosGetError = function (error) {
	if(error && error.response) {
		console.error(error.toString() + ". " + error.response.statusText + ": " + error.request.res.responseUrl)	
	} else {
		console.error(error.toString())
	}

}

internal.filterSites = function(sites=[],params={}) {
	return sites.filter(s=>{
        return (
            [
                internal.filterByParam(params.name, s.name),
                internal.filterByParam(params.nombre, s.nombre),
                internal.filterByParam(params.id_externo, s.id_externo),
                internal.filterByParam(params.estacion_id, s.id),
                internal.filterByParam(params.id, s.id),
                internal.filterByParam(params.geom, s.geom, internal.pointInPolygon)
            ].indexOf(false) < 0
        )
	})
}

/**
 * 
 * @param {Geometry} filter_geom - filter geometry (polygon)
 * @param {Geometry} item_geom - item geometry (point)
 * @returns {boolean}
 */
internal.pointInPolygon = function(filter_geom,item_geom) {
	return booleanPointInPolygon(item_geom, filter_geom)
}


/**
 * @param {any} filter_value
 * @param {any} item_value
 * @param {Function(any, any) : boolean} func
 * 
 * If filter_value is undefined or an empty array -> return true
 * 
 * else if func is defined -> return func(filter_value, item_value) : boolean
 * 
 * else if item_value is undefined -> return false
 * 
 * else if filter_value in an array -> return true if item_value exists in filter_value
 * 
 * Else                        -> return true if item_value equals filter_value
 */
internal.filterByParam = function(filter_value, item_value, func) {
    if(filter_value == undefined || ( Array.isArray(filter_value) && filter_value.length == 0 )) {
        return true
    }
    if(func) {
        return func(filter_value, item_value)
    } else if (item_value == undefined) {
        return false
    }
    if(Array.isArray(filter_value)) {
        if(filter_value.indexOf(item_value) >= 0) {
            return true
        }
    } else if(item_value == filter_value) {
        return true
    }
    return false
}

internal.filterSeries = function(series=[],params={}) {
	return series.filter(serie => {
        return (
            [
                internal.filterByParam(params.estacion_id, serie.estacion.id),
                internal.filterByParam(params.var_id, serie.var.id),
                internal.filterByParam(params.unit_id, serie.unidades.id),
                internal.filterByParam(params.id_externo, serie.estacion.id_externo),
                internal.filterByParam(params.series_id, serie.id),
                internal.filterByParam(params.id, serie.id),
                internal.filterByParam(params.tipo, serie.tipo)
            ].indexOf(false) < 0
        )
		
	})
}

/**
 * Fetch url with querystring params and write stream into localfilepath. Resolve with callback on write stream finish
 * @param {string} url - url
 * @param {Object} params - querystring params
 * @param {string} localfilepath - where to save response
 * @param {function} callback - run this on write stream finish
 * @param {number} delay - milliseconds to wait before callback execution
 * @returns
 */
internal.fetch = async function(
    url,
    params,
    localfilepath,
    callback,
    delay=1000
    ) {
    var writer = fs.createWriteStream(localfilepath)
    // console.debug({
    //     url: url,
    //     params: params
    // })
    const response = await axios.get(
        url,
        {
            params: params,
            responseType: 'stream'
        }
    )
    response.data.pipe(writer)
    return new Promise((resolve, reject) => {
        writer.once('finish', ()=>{
            setTimeout(()=>{
                resolve(callback)
            },delay)
        })
        writer.on('error', reject)
        response.data.on('error', (e) =>{
            reject("file:" + localfilepath + " write failed, error:" + e.toString())
        })
    })
}

internal.createUrlParams = function(params) {
    const url_params = new URLSearchParams()
    for(const [key, value] of Object.entries(params)) {
        if(Array.isArray(value)) {
            for(const item of value) {
                url_params.append(key, item)
            }
        } else if(value) {
            url_params.append(key, value)
        }
    }
    return url_params
}

module.exports = internal