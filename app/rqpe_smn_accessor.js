const fs = require("fs")
var sprintf = require('sprintf-js').sprintf
const path = require('path');
const { execSync } = require('child_process')
// const config = require("config")
// const {Pool} = require("pg")
// const pool = new Pool(config.database)
const CRUD = require('./CRUD')
const crud = CRUD.CRUD // new CRUD.CRUD(pool,config)

rqpe_smn = class {
    constructor(config) {
        this.default_config = {
            data_dir: path.resolve(__dirname,"..","data","qpe_sinarame"),
            series_id: 16,
            rma: 2
        }
        this.config = this.default_config
		if(config) {
			Object.keys(config).forEach(key=>{
				this.config[key] = config[key]
			})
		}
    }

    getBounds(input) {
        // get lon bounds
        var gdalinfo_lon = JSON.parse(execSync(`gdalinfo -json -sd 2 -stats ${input}`))
        // get lat bounds    
        var gdalinfo_lat = JSON.parse(execSync(`gdalinfo -json -sd 3 -stats ${input}`))
        return {
            "ul": [gdalinfo_lon["bands"][0]["metadata"][""]["STATISTICS_MINIMUM"], gdalinfo_lat["bands"][0]["metadata"][""]["STATISTICS_MAXIMUM"]],
            "lr": [gdalinfo_lon["bands"][0]["metadata"][""]["STATISTICS_MAXIMUM"], gdalinfo_lat["bands"][0]["metadata"][""]["STATISTICS_MINIMUM"]]
        }
    }

    nc2gtiff (input, output) {
        // get lon bounds
        var gdalinfo_lon = JSON.parse(execSync(`gdalinfo -json -sd 2 -stats ${input}`))
        // get lat bounds    
        var gdalinfo_lat = JSON.parse(execSync(`gdalinfo -json -sd 3 -stats ${input}`))
        // convert to GTiff
        const bounds = this.getBounds(input)
        execSync(`gdal_translate -of GTiff -a_ullr ${bounds.ul[0]} ${bounds.ul[1]} ${bounds.lr[0]} ${bounds.lr[1]} ${input} ${output}`)
    }

    gtiff2valor(input) {
        var data = fs.readFileSync(input, 'hex')
        return '\\x' + data
    }

    nc2valor(input,output) {
        this.nc2gtiff(input,output)
        return this.gtiff2valor(output)
    }

    createYearMonthDateDirs(base_dir,date) {
        const yyyy = date.getUTCFullYear()
        const mm = date.getUTCMonth()
        const dd = date.getUTCDate()
        const yyyy_dir = path.resolve(base_dir,sprintf("%04d",yyyy))
        if(!fs.existsSync(yyyy_dir)) {
            fs.mkdirSync(yyyy_dir)
        }  
        const mm_dir = path.resolve(this.config.data_dir,sprintf("%04d/%02d",yyyy,mm+1))
        if(!fs.existsSync(mm_dir)) {
            fs.mkdirSync(mm_dir)
        }  
        const dd_dir = path.resolve(this.config.data_dir,sprintf("%04d/%02d/%02d",yyyy,mm+1,dd))
        if(!fs.existsSync(dd_dir)) {
            fs.mkdirSync(dd_dir)
        }
        return
    }

    rast2obs(input,output,series_id=this.default_config.series_id,upsert=false) {
        var filename = input.replace(/^.*[\\\/]/, '')
        var sp = filename.split("_")
        var rma = parseInt(sp[0].substring(3,4))
        var yyyy = parseInt(sp[1].substring(0,4))
        var mm = parseInt(sp[1].substring(4,6))
        var dd = parseInt(sp[1].substring(6,8))
        var hh = parseInt(sp[2].substring(0,2))
        var MM = parseInt(sp[2].substring(2,4))
        const timeend = new Date(Date.UTC(yyyy,mm-1,dd,hh,MM))
        var time_support = parseInt(sp[3]) // minutes
        const timestart = new Date(timeend.getTime() - time_support * 60 * 1000)
        const suffix = sp[4]
        if(!output) {
            output =  path.resolve(this.config.data_dir,sprintf ("%04d/%02d/%02d/%s.GTiff", yyyy, mm, dd, filename))
            this.createYearMonthDateDirs(this.config.data_dir,timeend)
        }  
        const valor = this.nc2valor(input,output)
        const obs = new CRUD.observacion({
            tipo: "rast",
            series_id: series_id,
            timestart: timestart,
            timeend: timeend,
            valor: valor,
            timeupdate: new Date()
        })
        if(upsert) {
            return this.upsertObs(obs)
        } else {
            return obs
        }
    }
    
    upsertObs(obs) {
        return crud.upsertObservacion(obs)
    }

    getEscena(input,upsert=false) {
        const bounds = this.getBounds(input)
        const escena = new CRUD.escena("rqpe_smn",{type:"BOX",coordinates:[...bounds.ul,...bounds.lr]})
        if(upsert) {
            return crud.upsertEscena(escena)
        } else {
            return escena
        }
    }
}

module.exports = {rqpe_smn: rqpe_smn}