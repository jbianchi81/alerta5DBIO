import { AbstractAccessorEngine, Config} from "./abstract_accessor_engine"
import {spawn} from 'child-process-promise'
import {serie as CrudSerie, observacion as CrudObservacion, fuente as CrudFuente, escena as CrudEscena, corrida as CrudCorrida, pronostico as CrudPronostico} from "../CRUD"
import path from "path"
import fs from "fs"
import {spawnAsync} from "./accessor_utils"

interface CosmoConfig extends Config {
    url: string,
    dir_1d: string,
    dir_3h: string,
    series_id_1d: number,
    series_id_3h: number,
    cal_id: number
}

export class Client extends AbstractAccessorEngine {

    config : CosmoConfig

    constructor(config : CosmoConfig) {
        super(config)
        this.config = config
    }
    
    async getPronostico(
		filter : {
			forecast_date?: string | Date,
    		timestart?: string | Date,
    		timeend?: string | Date
		}={},
		options: {}={}
	) : Promise<CrudCorrida> {
        // agrega cosmo
        if(!(global as any).config?.agrega_cosmo_exec) {
			throw new Error("agrega_cosmo_exec not set in config")
		}
        const params : any = {}
        if(filter.forecast_date) {
            var forecast_date = new Date(filter.forecast_date)
            params.emision = forecast_date.toISOString().slice(0,10).replace(/-/g,"")
            params.inicializacion = forecast_date.toISOString().slice(11,13)
        } else {
            var forecast_date = new Date()
            forecast_date.setUTCHours(0)
            forecast_date.setUTCMinutes(0)
            forecast_date.setUTCSeconds(0)
            forecast_date.setUTCMilliseconds(0)
            params.emision = forecast_date.toISOString().slice(0,10).replace(/-/g,"")
            params.inicializacion = "00"
        }
        params.host = this.config.url
        params.host_1d = this.config.dir_1d
        params.host_3h = this.config.dir_3h

        await spawnAsync(
            (global as any).config.agrega_cosmo_exec,
            params
        )

        // lee .tif
        // diario
        const dir_1d = path.resolve(
            this.config.dir_1d, 
            forecast_date.toISOString().substring(0,4), 
            forecast_date.toISOString().substring(5,7)
        )
        
        if(!fs.existsSync(path.resolve(dir_1d, "index.csv"))) {
            throw new Error("Falta index.csv en " + dir_1d)
        }

        const files_1d = fs.readFileSync(path.resolve(dir_1d, "index.csv"), {encoding: "utf-8"}).split("\n")

        // const pronosticos_1d = fs.readdirSync(dir_1d).filter(f => f.endsWith(".tif")).map(f => {
        const pronosticos_1d = files_1d.map(f => {
            if(!fs.existsSync(path.resolve(dir_1d, f))) {
                throw new Error("Missing file: " + dir_1d + "/" + f)
            }
            const match = f.match(/PLATA_(\d{10})_(\d{10})\.tif/)
            if(!match) return null
            const timeend = new Date(parseYYYYMMDDHH(match[2]))
            timeend.setUTCHours(timeend.getUTCHours() + 1)
            const timestart = new Date(timeend)
            timestart.setUTCDate(timestart.getUTCDate() - 1)
            return {
                filename: match[0],
                forecast_date: forecast_date,
                timestart: timestart,
                timeend: timeend,
                valor: fs.readFileSync(path.resolve(dir_1d, f)),
                qualifier: "main"
            }
        }).filter((v): v is NonNullable<typeof v> => !!v)

        // 3 horario
        const dir_3h = path.resolve(this.config.dir_3h, forecast_date.toISOString().substring(0,4), forecast_date.toISOString().substring(5,7))

        if(!fs.existsSync(path.resolve(dir_3h, "index.csv"))) {
            throw new Error("Falta index.csv en " + dir_3h)
        }

        const files_3h = fs.readFileSync(path.resolve(dir_3h, "index.csv"), {encoding: "utf-8"}).split("\n")

        // const pronosticos_3h = fs.readdirSync(dir_3h).filter(f => f.endsWith(".tif")).map(f => {
        const pronosticos_3h = files_3h.map(f => {
            if(!fs.existsSync(path.resolve(dir_3h, f))) {
                throw new Error("Missing file: " + dir_3h + "/" + f)
            }
            const match = f.match(/PLATA_(\d{10})_(\d{10})\.tif/)
            if(!match) return null
            const timeend = new Date(parseYYYYMMDDHH(match[2]))
            timeend.setUTCHours(timeend.getUTCHours() + 1)
            const timestart = new Date(timeend)
            timestart.setUTCHours(timestart.getUTCHours() - 3)
            return {
                filename: match[0],
                forecast_date: forecast_date,
                timestart: timestart,
                timeend: timeend,
                valor: fs.readFileSync(path.resolve(dir_3h, f))
            }
        }).filter((v): v is NonNullable<typeof v> => !!v)
        
        // devuelve corrida
        return new CrudCorrida({
            forecast_date: forecast_date,
            cal_id: this.config.cal_id,
            series: [
                {
                    series_table: "series_rast",
                    series_id: this.config.series_id_1d,
                    pronosticos: pronosticos_1d
                },
                {
                    series_table: "series_rast",
                    series_id: this.config.series_id_3h,
                    pronosticos: pronosticos_3h
                }
            ]
        })
    }

    async updatePronostico(
        filter : {
			forecast_date?: string | Date,
    		timestart?: string | Date,
    		timeend?: string | Date
		}={},
		options: {}={}
	) : Promise<CrudCorrida> {
        const corrida = await this.getPronostico(filter, options)
        await corrida.create()
        return corrida
    }
}

function parseYYYYMMDDHH(dateString: string): Date {
    const year = parseInt(dateString.substring(0, 4), 10)
    const month = parseInt(dateString.substring(4, 6), 10) - 1
    const day = parseInt(dateString.substring(6, 8), 10)
    const hour = parseInt(dateString.substring(8, 10), 10)
    return new Date(Date.UTC(year, month, day, hour))
}