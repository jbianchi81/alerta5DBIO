'use strict'

require('./setGlobal')
// const { Pool, Client } = require('pg')
// const global.config = require('config');
// const global.pool = new Pool(config.database)
const CRUD = require('./CRUD')
const crud = CRUD.CRUD // new CRUD.CRUD(global.pool,global.config)
const fs = require('promise-fs')
const path = require('path');
const cron = require('node-cron')
const nodemailer = require('nodemailer')
var accessors = require('./accessors')
const update_gefs_wave = require("./update_gefs_wave")
const timeSteps = require('./timeSteps');
const { assert } = require('console');
const winston = require('winston')

const logfile = path.resolve(global.config.cronjobs.logfile)
fs.writeFileSync(logfile,`${new Date().toISOString()}: Starting cronjobs\n`)

const myLogFormat = winston.format.combine(
    winston.format.label({label: 'cronjobs'}),
    winston.format.timestamp({ format: 'HH:mm:ss.SSSSS'}),
    winston.format.printf(log => {
        return `${log.timestamp} [${log.label}] ${log.level}: ${log.message}`
    })
)

const logger = winston.loggers.add('cronjob-logger',{
    transports: [
        new winston.transports.Console({
            format: myLogFormat
        }),
        new winston.transports.File({
            filename:global.config.cronjobs.logfile,
            format: myLogFormat
        })
    ]
})

var transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
      user: global.config.cronjobs.email.address,
      pass: global.config.cronjobs.email.password
    }
});

function sendAlert(message) {
    var mailOptions = {
        from: global.config.cronjobs.email.address,
        to: global.config.cronjobs.email.destination_address,
        subject: 'Alerta de cronjobs de A5',
        text: `${new Date().toISOString()}: ${message}`
    };
    transporter.sendMail(mailOptions, function(error, info){
        if (error) {
            logger.error(error);
        } else {
            logger.verbose('Email sent: ' + info.response);
        }
    });
}

// const logging = function(level,message) {
//     fs.appendFileSync(logfile,`${new Date().toISOString()} ${level.toUpperCase()}: ${message}\n`) 
//     if(level.toLowerCase() == "log" || level.toLowerCase() == "info" || level.toLowerCase() == "verbose" || level.toLowerCase() == "debug") {
//         console.log(`${new Date().toISOString()} ${level.toUpperCase()}: ${message}`)
//     } else if(level.toLowerCase() == "warn" || level.toLowerCase() == "error") {
//         console.error(`${new Date().toISOString()} ${level.toUpperCase()}: ${message}`)
//     } else if(level.toLowerCase() == "alert") {
//         console.error(`${new Date().toISOString()} ${level.toUpperCase()}: ${message}`)
//         var mailOptions = {
//             from: config.cronjobs.email.address,
//             to: config.cronjobs.email.destination_address,
//             subject: 'Alerta de cronjobs de A5',
//             text: `${new Date().toISOString()}: ${message}`
//         };
//         transporter.sendMail(mailOptions, function(error, info){
//             if (error) {
//               logger.error(error);
//             } else {
//               logger.verbose('Email sent: ' + info.response);
//             }
//         }); 
//     } else {
//         logger.error("Bad log level")
//     }
// }

const cronjob = class {
    constructor(name,cron_expression,func,options,alert) {
        this.alert = alert
        this.name = name
        this.cron_expression = cron_expression
        this.func = func
        // this.func = function () {
        //     return this.task_func.then(()=>{
        //         return true
        //     }).catch((e)=>{
        //         logging("alert",`Nueva alerta de Cronjobs de A5. JOB NAME: ${this.name}MESSAGE: ${e.toString()}`)
        //         return false
        //     })}
        this.options = options
        if(!this.validate()) {
            throw(this.name + ": Invalid cron expression")
        }
        this.schedule()
    }
    schedule() {
        this.task = cron.schedule(
            this.cron_expression,
            async() => {
                logger.info("Performing cronjob task " + this.name)
                try {
                    await this.func()          
                } catch(e) {
                    if(this.alert) {
                        logger.error(e)
                        sendAlert(`Nueva alerta de Cronjobs de A5. JOB NAME: ${this.name}. MESSAGE: ${e.toString()}.`)
                    } 
                    // logger.error(e)
                    console.error(e)
                }
            },
            this.options
        )
    }
    validate() {
        return cron.validate(this.cron_expression)
    }
    start() {
        this.task.start()
    }
    stop() {
        this.task.stop()
    }
}

const internal = {}

internal.my_tasks = [
    {
        "name": "Prueba_dummy",
        "cron_expression": "* * * * *",
        "func": async function() {
            const loggerTest = require("../tmp/loggerTest.js")
            loggerTest.run()
            return Promise.resolve()
        },
        "options": {
            "scheduled": true
        },
        "alert": true
    },
    {
        "name": "Prueba_error",
        "cron_expression": "* * * * *",
        "func": async function() {
            return Promise.reject("Error test")
        },
        "options": {
            "scheduled": false
        },
        "alert": true
    },
    {
        "name": "runAsocACUMAR",
        "cron_expression": "*/10 * * * *",
        "options": {
            "scheduled": false
        },
        "alert": true,
        "func": async function() {
            var timestart = new Date()
            timestart.setTime(timestart.getTime() - 4*24*3600*1000)
            timestart = new Date(timestart.getFullYear(),timestart.getMonth(),timestart.getDate(),21)
            var timeend = new Date()
            const acc = new accessors.acumar()
            return acc.update({}).then(o=>{
                // if(!o.length) {
                //     throw("runAsocACUMAR: no se encontraron observaciones")
                // }
                assertRealTime(o,{hours:-6},{count_series_id:8})
                return crud.getEstaciones({tabla:"red_acumar"})
            }).then(estaciones=>{
                var estacion_ids = estaciones.map(e=>e.id)
                if(!estacion_ids.length) {
                    throw("runAsocACUMAR: no se encontraron estaciones")
                }
                return crud.runAsociaciones({source_tipo:"puntual",estacion_id:estacion_ids,source_var_id:27,timestart:timestart,timeend:timeend})
                .then((results)=>{
                    logger.verbose("runAsocACUMAR: DONE native to hourly")
                    if(!results.length) {
                        throw("runAsocACUMAR: no se realizaron asociaciones nativo a horario")
                    }
                    return crud.runAsociaciones({source_tipo:"puntual",estacion_id:estacion_ids,source_var_id:31,timestart:timestart,timeend:timeend})
                })
                .then((results)=>{
                    logger.verbose("runAsocACUMAR: DONE hourly to 3-hourly")
                    if(!results.length) {
                        throw("runAsocACUMAR: no se realizaron asociaciones horario a 3-horario")
                    }
                    return crud.runAsociaciones({source_tipo:"puntual",estacion_id:estacion_ids,source_var_id:34,timestart:timestart,timeend:timeend})
                })
                .then((results)=>{
                    if(!results.length) {
                        throw("runAsocACUMAR: no se realizaron asociaciones 3-horario a diario")
                    }
                    logger.verbose("runAsocACUMAR: DONE 3-hourly to daily")
                    return
                })
            })
        }
    },
    {
        "name": "deleteCorridas",
        "cron_expression": "0 5 * * *",
        "alert": true,
        "options": {
            "scheduled": false
        },
        "func": async function () {
            return crud.batchDeleteCorridas({n:10,skip_cal_id:[288,289,376,377,379,308,391,400,439,440,441,442,432,433,439,440,441,442,443,444,445,446,447,454,457,455,456,458,459,460,461]})
            .then(result=>{
                if(!result.length) {
                    throw("deleteCorridas: No se eliminaron corridas")
                }
                return fs.writeFile("tmp/deletedCorridas.json",JSON.stringify(result,null,2))
            })
        }
    },
    {
        "name": "updateMonthlyStats",
        "cron_expression": "0 0 * * 7",
        "options": {
            "schedule": false
        },
        "alert": true,
        "func": async function() {
            const daily_stats_dir = path.resolve(__dirname,"../data/DAILYSTATS")
            const timestart = new Date("1995-01-01")
            const timeend = new Date("2022-01-01")
            const range = 15
            var series_ids = [1,2,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,96,99,100,101,102,103,105,106,107,109,112,118,153,154,119,121,122,125,126,127,128,130,133,135,136,137,139,140,153,155,156,157,159,170,215,319,1535,3041,3280,3309,3312,3314,3327,3357,3358,6131,6135,6144,6147,6148,6157,6164,6167,6173,6183,6186,6189,6192,6193,6194,6197,6203,6206,6211,6215,6218,6221,6230,6232,6235,6244,6245,6247,6252,6253,6254,6260,6270,6273,6276,6277,6290,6291,6292,6297,6298,6320,6321,6331,6334,6340,6343,6346,6350,6356,6362,6368,6374,6377,6382,6385,6389,6392,6397,6400,6401,6404,6405,6406,6408,6411,6414,6422,6429,6432,6433,6435,6439,6442,6448,6451,6453,6460,6463,6470,6473,6476,6479,6481,6484,6490,6493,6495,6497,6498,6499,6500,6506,6507,6510,6511,6519,6521,6524,6529,6547,6552,6555,6557,6561,6565,6571,6574,6577,6586,6602,6605,6611,6625,6628,6633]
            try {
                var q_series = await crud.getSeries("puntual",{var_id:4,proc_id:2,date_range_before:new Date("2001-01-01"),count:3650})
                series_ids.push(...q_series.map(s=>s.id))
            } catch(e) {
                throw e
            }
            const valid_results = []
            for(var id of series_ids) {
                console.log("update cuantiles series_id:" + id)
                try{
                    var dailyStats = await crud.getCuantilesDiariosSuavizados("puntual",id,timestart,timeend,range,"00:00:00",2)
                    valid_results.push(dailyStats)
                } catch(e) {
                    logger.error(e)
                    continue
                }
                try {
                    var dailyStatsList = await crud.upsertDailyDoyStats(dailyStats)
                    valid_results.push(dailyStatsList)
                } catch(e) {
                    logger.error(e)
                    continue
                }
                fs.writeFileSync(`${daily_stats_dir}/DailyStats_${id}.csv`,dailyStatsList.toCSV())
                try {
                    var mensuales = await crud.upsertCuantilesMensuales(tipo,id)
                    valid_results.push(mensuales)
                }catch(e) {
                    logger.error(e)
                    continue
                }
            }
            if(!valid_results.length) {
                throw("No se obtuvieron stats mensuales")
            }
        }
    },
    {
        "name":"ANAupdateAll",
        "cron_expression": "20  10,22 * * *",
        "options":{
            "scheduled": false
        },
        "alert": true,
        "func": async function() {
            var timestart = new Date()
            timestart.setTime(timestart.getTime() - 7*24*3600*1000)
            var timeend = new Date()
            return accessors.new("ana")
            .then(accessor=>{
                return accessor.engine.updateAll({timestart:timestart,timeend:timeend},{no_send_data:false})
            }).then(results=>{
                // if(!results.length) {
                //     throw("ANAupdateAll: no se encontraron observaciones")
                // }
                assertRealTime(results,{days:-1},{count_series_id:400})
                return
            })
        }        
    },
    {
        "name": "updateConaeAPI",
        "cron_expression": "20 10,11,12 * * *",
        "options": {
            "scheduled": false
        },
        "alert": true,
        "func": async function() {
            var timestart = new Date()
            timestart.setTime(timestart.getTime() - 8*24*3600*1000)
            timestart = new Date(timestart.getFullYear(),timestart.getMonth(),timestart.getDate(),21)
            // var timestart2 = new Date(timestart.getTime() - 24*3600*1000)
            var timeend = new Date(timestart.getTime() + 7*24*3600*1000)
            return accessors.new("conae_api")
            .then(accessor=>{
                return accessor.engine.update({timestart:timestart},{no_send_data:true})
            }).then(()=>{
                return crud.rast2areal(9,timestart,timeend,"all") 
                // return crud.rastExtractByArea(9,timestart,timeend,"all",{no_send_data:true})
            })
        }
    },
    {
        "name": "update_conae_hem",
        "cron_expression": "* * * * *", // "20 10,11,12 * * *",
        "options": {
            "scheduled": false
        },
        "alert": true,
        "func": async function() {
            return accessors.new("conae_hem")
            .then(accessor=>{
                return accessor.engine.get(undefined,{no_send_data:true})
            })
        }
    },
    {
        "name": "update_prefe_hmm",
        "cron_expression": "0 15 1,2,3 * *",
        "options": {
            "scheduled": false
        },
        "alert": true,
        "func": async function() {
            var timestart = new Date()
            timestart = new Date(timestart.getFullYear()-3,0,1,0).toISOString().substring(0,10)
            var timeend = new Date().toISOString().substring(0,10)
            return crud.getEstaciones({tabla:"alturas_prefe"})
            .then(async estaciones=>{
                results = []
                for(estacion of estaciones) {
                    for (var_id of [33,48,49,50,51,52]) {
                        try {
                            results.push(await crud.runAsociaciones({estacion_id: estacion.id, dest_var_id: var_id, run:true, timestart: timestart, timeend: timeend}))
                        } catch(e) {
                            throw(e)
                        }
                    }
                }
                if(!results.length) {
                    throw("update_prefe_hmm: No se encontraron asociaciones")
                }
                return global.pool.query("refresh materialized view series_stats_25")
            })
        }
    },
    {
        "name": "update_emcwf",
        "cron_expression": "0 0,12 13,14,15,16 * *",
        "options": {
            "scheduled": false
        },
        "alert": true,
        "func": async function() {
            return accessors.new("ecmwf_mensual")
            .then(accessor=>{
                return accessor.engine.update({date:new Date().toISOString().substring(0,10)},{no_send_data:true})
            })
        }
    },
    {
        "name": "update_gefs_wave",
        "cron_expression": "0 0,12 13,14,15,16 * *",
        "options": {
            "scheduled": false
        },
        "alert": true,
        "func": async function() {
            return update_gefs_wave.run()
        }
    },
    {
        "name": "update_mch_py",
        "cron_expression": "10 */6 * * *",
        "options": {
            "scheduled": false
        },
        "alert": true,
        "func": async function() {
            var timestart = new Date(new Date().getTime() - 20*24*3600*1000).toISOString().substring(0,10)
            var timeend = new Date().toISOString().substring(0,10)
            return accessors.new("mch_py")
            .then(accessor=>{
                return accessor.engine.updateAll({timestart:timestart,timeend:timeend})
            })
            .then(observaciones=>{
                assertRealTime(observaciones,{"days": -2},{count_series_id:10})
                return
            })
        }
    },
    {
        "name": "pp_cdp_batch",
        "cron_expression": "*/20 11,12 * * *",
        "options": {
            "scheduled": false
        },
        "alert": true,
        "func": async function() {
            return	crud.get_pp_cdp_batch(undefined,undefined,undefined,undefined,true)
        }
    },{
        "name": "update_sarws",
        "cron_expression": "* * * * *", // "43 */6 * * *",
        "options": {
            "scheduled": false
        },
        "alert": true,
        "func": async function() {
            var timestart = new Date(new Date().getTime() - 20*24*3600*1000).toISOString().substring(0,10)
            var timeend = new Date().toISOString().substring()
            return accessors.new("sarws")
            .then(accessor=>{
                return accessor.engine.update({timestart:timestart,timeend:timeend})
            })
            .then(observaciones=>{
                assertRealTime(observaciones,{"days": -2},{count_series_id:80})
                return
            })
        }
    }
]


internal.scheduled_tasks = []

for (var task of internal.my_tasks) {
    logger.verbose("adding task " + task.name)
    internal.scheduled_tasks.push(new cronjob(task.name,task.cron_expression,task.func,task.options,task.alert))
}

// aux functions

function assertRealTime(observaciones,time_interval={"days":1},options={}) {
    // options: {count: int, count_series_id: int}

    if(!observaciones || !observaciones.length) {
        throw("No se encontraron observaciones")
    }
    time_interval = timeSteps.createInterval(time_interval)
    if(!time_interval) {
        throw("time_interval inválido")
    }
    var date_limit = timeSteps.advanceInterval(new Date(),time_interval)
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

module.exports = internal
