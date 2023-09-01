'use strict'

var fs =require("promise-fs")
const accessors = require('../app/accessors.js')
var sprintf = require('sprintf-js').sprintf, vsprintf = require('sprintf-js').vsprintf
var pexec = require('child-process-promise').exec;
const program = require('commander')
const config = require('config')

const internal = {}

internal.update_gefs_wave = function(forecast_date,options={}) {
    if(!forecast_date) {
        forecast_date= new Date()
        forecast_date.setTime(forecast_date.getTime() - 5*3600*1000)
    }
    return accessors.new("gefs_wave")
    .then(accessor=>{
        var update_options = {no_send_data:true}
        return accessor.engine.update({forecast_date:forecast_date},update_options)
    })
    .catch(e=>{
        console.error(e)
    })
}

 internal.callPrintWindMap = function(path,skip_print) {
    var mapset = sprintf("%04d",Math.floor(Math.random()*10000))
    var location = sprintf("%s/%s",config.grass.location,mapset) // sprintf("%s/GISDATABASE/WGS84/%s",process.env.HOME,mapset)
    var batchjob = sprintf("py/print_wind_map.py",process.env.HOME)
    if(path) {
        console.log("callPrintWindMap: path: " + path)
        process.env.gefs_run_path = path
    }
    if(skip_print) {
        process.env.skip_print = "True"
    }
    var command = sprintf("grass %s -c --exec %s", location, batchjob)
    return pexec(command)
    .then(result=>{
        console.log("batch job called")
        var stdout = result.stdout
		var stderr = result.stderr
        if(stdout) {
            console.log(stdout)
        }
        if(stderr) {
            console.error(stderr)
        }
        process.env.gefs_run_path = undefined
    })
}

internal.getDates = function(forecast_date) {
    return accessors.new("gefs_wave")
    .then(accessor=>{
        return accessor.engine.getDates({forecast_date:forecast_date})
    })
}

internal.run = function(forecast_date,options={}) {
    var update_options = {}
    if(options.skip_download) {
        return internal.getDates(forecast_date)
        .then(dates=>{
            return internal.callPrintWindMap(dates.forecast_time_path,options.skip_print)
        })
    }
    return internal.update_gefs_wave(forecast_date,update_options)
    .then(result=>{
        console.log({result:result})
        return internal.callPrintWindMap(result.path,options.skip_print)
        .then(()=>{
            if(options.output) {
                return fs.writeFile(options.output,result)
            } else {
                return
            }
        })
    })
}

module.exports = internal

///////////////////////////////////////////////////////////////////////////////////

if (typeof require !== 'undefined' && require.main === module) {
    program
    .version('0.0.1')
    .description('update gefs wave');

    program
    .command('run')
    .alias('r')
    .description('descarga gefs wave, actualiza base de datos y genera mapas')
    .option('-d, --forecast_date <value>', 'fecha de emisión')
    .option('-o, --output <value>', 'output to file')
    .option('-S, --skip_download','no descargar')
    .action(options => {
        if(options.forecast_date) {
            options.forecast_date = new Date(options.forecast_date)
            if(options.forecast_date.toString() == "Invalid Date") {
                console.error("invalid forecast_date")
                process.exit(1)
            }
        }
        var run_opts = {}
        internal.run(options.forecast_date,options)
        .catch(e=>{
            console.error(e)
            process.exit(1)
        })

    })

    if(process.argv) {
    program.parse(process.argv);
    }
}
// run(new Date("2021-09-08 06:00:00"),{output:"responses/update_gefs_wave.json",skip_download:false,skip_print:false})
// run()
// .catch(e=>{
//     console.error(e)
// })

// path="/home/alerta5/44-NODEJS_APIS/alerta5DBIO/data/gefs_wave/gefs.20210908/06"
// callPrintWindMap(path).then(()=>console.log("Finished")).catch(e=>console.error(e))


// curl -b /tmp/a5cookie -X POST "http://localhost:3005/accessors/gefs_wave/update?forecast_date=$date_format&no_send_data=true" -o upd_gefs.json