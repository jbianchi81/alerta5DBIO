const sprintf = require('sprintf-js').sprintf
const accessor_utils = require('../accessor_utils')
const path = require('path');
const {observaciones, corrida} = require('../CRUD')
const AbstractAccessorEngine = require('./abstract_accessor_engine').AbstractAccessorEngine

// URL=
// "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t06z.pgrb2.0p25.f000&lev_surface=on&var_ACPCP=on&subregion=&leftlon=-70&rightlon=-40&toplat=-10&bottomlat=-40&dir=%2Fgfs.20240610%2F06%2Fatmos"

const internal = {}

internal.Client = class extends AbstractAccessorEngine {

    default_config = {
        "url": "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl",
        "file_pattern": "gfs.t%02dz.pgrb2.0p25.f%03d",
        "bbox": {
            "leftlon": -70, 
            "rightlon": -40, 
            "toplat": -10, 
            "bottomlat": -40
        },
        "level": "surface",
        "var": "APCP",
        "latency": 4,
        "start_hour": 6,
        "end_hour": 384,
        "step_hour": 6,
        "localfilepath": "data/nomads_gfs/nomads_gfs.grib2",
        "localfile_pattern": "nomads_gfs_%03d.grib2",
        "units": "milímetros",
        "variable_map": {
            "APCP06": {
                "name": "precipitación 6 horaria",
                "series_id": 5
            }
        },
        "data_dir": "data/nomads_gfs",
        "cal_id": 676
    }

    async getFile(
        date,
        hour,
        bbox,
        level,
        variable,
        localfilepath) {
        if(!date) {
            var date = new Date()
            date.setUTCHours(date.getUTCHours() - this.config.latency)
            date.setUTCHours(date.getUTCHours() - date.getUTCHours() % 6)
            date.setMinutes(0, 0, 0)
        }
        const directory = sprintf("/gfs.%04d%02d%02d/%02d/atmos", date.getUTCFullYear(), date.getUTCMonth() + 1, date.getUTCDate(), date.getUTCHours())
        var file = sprintf(this.config.file_pattern, date.getUTCHours(), hour)
        var params = {
                file: file,
                subregion: "",
                leftlon: bbox.leftlon,
                rightlon: bbox.rightlon,
                toplat: bbox.toplat,
                bottomlat: bbox.bottomlat,
                dir: directory
        }
        params[`lev_${level}`] = "on"    
        params[`var_${variable}`] = "on"
        await accessor_utils.fetch(
            this.config.url,
            params,
            localfilepath
        )
        const o = await accessor_utils.grib2obs(
            {
                filepath: localfilepath,
                variable_map: this.config.variable_map,
                bbox: bbox,
                units: this.config.units
            }
        )
        if(o.length) {
            return o[0]
        } else {
            throw("Observation extraction from raster file failed")
        }
    }

    async get(filter={},options={}) {
        var forecast_date = filter.forecast_date
        if(!forecast_date) {
            forecast_date = new Date()
            forecast_date.setUTCHours(forecast_date.getUTCHours() - this.config.latency)
            forecast_date.setUTCHours(forecast_date.getUTCHours() - forecast_date.getUTCHours() % 6)
            forecast_date.setMinutes(0, 0, 0)
        }
        const o = []
        for(var i=this.config.start_hour;i<=this.config.end_hour;i=i+this.config.step_hour) {
            const timestart = new Date(forecast_date)
            timestart.setUTCHours(timestart.getUTCHours() + i - this.config.step_hour)
            if(filter.timestart && filter.timestart.getTime() > timestart.getTime()) {
                // console.debug(`timestart ${timestart.toISOString()} predates filter.timestart ${filter.timestart.toISOString()}. Skipping`)
                continue
            }
            if(filter.timeend && filter.timeend.getTime() < timestart.getTime()) {
                // console.debug(`timestart ${timestart.toISOString()} exceeds filter.timeend ${filter.timeend.toISOString()}. Skipping`)
                continue
            }
            var filepath = path.resolve(this.config.data_dir, sprintf(this.config.localfile_pattern, i))
            const observacion = await this.getFile(filter.forecast_date, i, this.config.bbox, this.config.level, this.config.var, filepath)
            observacion.timestart.setUTCHours(observacion.timestart.getUTCHours() - this.config.step_hour)
            o.push(observacion)
        }
        return new observaciones(o)
    }

    async update(filter={},options={}) {
        const o = await this.get(filter, options)
        return o.create()
    }

    async getPronostico(filter={},options={}) {
        const pronosticos = await this.get(filter,options)
        if(!pronosticos.length) {
            throw("Pronostico not found")
        }
        const series = []
        for(var p of pronosticos) {
            const series_index = series.map(s=>s.series_id).indexOf(p.series_id)
            if(series_index < 0) {
                series.push({
                    series_id: p.series_id,
                    series_table: "series_rast",
                    qualifier: "main",
                    pronosticos: [p]
                })
            } else {
                series[series_index].pronosticos.push(p)
            }
        }
        return new corrida({
            cal_id: this.config.cal_id,
            forecast_date: pronosticos[0].timeupdate,
            series: series
        })
    }

    async updatePronostico(filter={},options={}) {
        const c  = await this.getPronostico(filter, options)
        return c.create()
    }
}

module.exports = internal