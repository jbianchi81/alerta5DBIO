import { AbstractAccessorEngine, AccessorEngine, ObservacionesFilter, SeriesFilter, SitesFilter, Config } from "./abstract_accessor_engine"
import { Observacion, Serie, Interval, Location, Escena } from '../a5_types'
import get from 'axios'
import path from 'path'
import { unlinkSync, readFileSync, writeFileSync } from 'fs'
import { fetch, filterSeries } from '../accessor_utils'
import { advanceInterval, Interval_pg } from '../timeSteps'
import { exec } from 'child-process-promise'
import { getRegularSeries, rastExtract, escena as crud_escena, observaciones as crud_observaciones, serie as crud_serie} from '../CRUD'
import { print_rast_series } from '../print_rast'
import { sprintf } from 'sprintf-js'
import { print_rast } from '../print_rast'
import { Geometry } from "../geometry_types"
import {Client as FtpClient, FileInfo} from "basic-ftp";

class Escena_ extends crud_escena implements Escena  {
  id ? : number
  nombre : string
  geom : Geometry
  [x : string] : unknown

  constructor(fields : {}) {
    super(fields)
  }
}

interface HidroestimadorConfig extends Config {
    url: string,
    user: string,
    password: string,
    remote_path : string,
    local_path: string,
    series_id: number,
    escena_id: number,
    file_pattern: string,
    t_offset: number // hours
}

/**
 * Docs: http://repositorio.smn.gob.ar/handle/20.500.12160/2972
 * 
 * Available date range for daily product: last year
 * 
 * Time offset of daily product: 12Z
 * 
 * Latency approx. 4:30 hours
 */
export class Client extends AbstractAccessorEngine implements AccessorEngine {

    config: HidroestimadorConfig

    downloaded_files : Array<string> 

    constructor(config: HidroestimadorConfig) {
        super(config)
        this.setConfig(config)
    }

    default_config: HidroestimadorConfig = {
        url: "ftp.smn.gob.ar",
        user: "user",
        password: "password",
        remote_path: "SQPE",
        local_path: "data/hidroestimador",
        series_id: 16,
        escena_id: 24,
        file_pattern: "^Ajuste_(\d{8}).tif$",
        t_offset: 9
    }

    file_pattern : RegExp = new RegExp(/^Ajuste_(\d{8})\.tif$/)

    ftp_client : FtpClient

    async getSites(filter : SitesFilter) : Promise<Array<Location>> {
      const escenas = [
        new Escena_({
          "id": this.config.escena_id,
          "nombre": "hidroestimador",
          "geom": {"type":"Polygon","coordinates":[[[-76.099995422,-19.899998856],[-48.900000763,-19.899998856],[-48.900000763,-56.099999237],[-76.099995422,-56.099999237],[-76.099995422,-19.899998856]]]}
        })
      ]
      return escenas
    }

    async updateSites(filter : SitesFilter) {
        const sites = (await this.getSites(filter))
        const upserted = await crud_escena.create(sites)
        return upserted.map((site : Escena) => new Escena_(site))
    }

    async getSeries(filter: SeriesFilter) : Promise<Array<Serie>> {
        const series = [
            {
                "id": this.config.series_id,
                "tipo": "raster",
                "estacion": {
                  "id": 24,
                  "nombre": "hidroestimador",
                  "geom": {"type":"Polygon","coordinates":[[[-76.099995422,-19.899998856],[-48.900000763,-19.899998856],[-48.900000763,-56.099999237],[-76.099995422,-56.099999237],[-76.099995422,-19.899998856]]]}
                },
                "var": {"id":1,"var":"P","nombre":"precipitación diaria 12Z","abrev":"precip_diaria_met","type":"num","datatype":"Succeeding Total","valuetype":"Field Observation","GeneralCategory":"Climate","VariableName":"Precipitation","SampleMedium":"Precipitation","def_unit_id":"22","timeSupport":{"years":0,"months":0,"days":1,"hours":0,"minutes":0,"seconds":0,"milliseconds":0},"def_hora_corte":{"years":0,"months":0,"days":0,"hours":9,"minutes":0,"seconds":0,"milliseconds":0}},
                "procedimiento": {
                  "id": 5,
                  "nombre": "Estimado",
                  "abrev": "est",
                  "descripcion": "Estimado a partir de observaciones indirectas"
                },
                "unidades": {"id":22,"nombre":"milímetros por día","abrev":"mm/d","UnitsID":305,"UnitsType":"velocity"},
                "fuente": {"id":11,"nombre":"hidroestimador_diario","data_table":"hidroestimador_diario_table","data_column":"rast","tipo":"QPE","def_proc_id":5,"def_dt":{"years":0,"months":0,"days":1,"hours":0,"minutes":0,"seconds":0,"milliseconds":0},"hora_corte":{"years":0,"months":0,"days":0,"hours":9,"minutes":0,"seconds":0,"milliseconds":0},"def_unit_id":22,"def_var_id":1,"fd_column":null,"mad_table":null,"scale_factor":null,"data_offset":null,"def_pixel_height":0.1,"def_pixel_width":0.1,"def_srid":4326,"def_extent":{"type":"Polygon","coordinates":[[[-76.0999954223633,-19.8999988555908],[-48.9000007629395,-19.8999988555908],[-48.9000007629395,-56.0999992370605],[-76.0999954223633,-56.0999992370605],[-76.0999954223633,-19.8999988555908]]]},"date_column":"date","def_pixeltype":"16BUI","abstract":"Estimación satelital de la precipitación a paso diario de la misión GOES, producto Hidrestimador. Fuente: SMN","source":"http://www.smn.gob.ar","public":false}
            }
        ] as unknown as Array<Serie>
        return filterSeries(series.map(s => new crud_serie(s)), filter)
    }

    async accessServer() : Promise<void> {
        this.ftp_client = new FtpClient()
        this.ftp_client.ftp.verbose = false
        const response = await this.ftp_client.access(
            {
                host: this.config.url, 
                user: this.config.user, 
                password: this.config.password
            })
        console.debug(response.message)
    }

    async getFilesList(filter: ObservacionesFilter) : Promise<FileInfo[]> {
        return this.ftp_client.list(this.config.remote_path)
    }

    async get(filter: ObservacionesFilter, options: Object): Promise<Array<Observacion>> {
        if (!filter.timestart) {
            filter.timestart = new Date(new Date().getTime() - 8 * 24 * 3600 * 1000)
        } else {
            filter.timestart = new Date(filter.timestart)
            if (filter.timestart.toString() == "Invalid Date") {
                throw new Error("Invalid timestart")
            }
        }
        if (!filter.timeend) {
            filter.timeend = new Date()
        } else {
            filter.timeend = new Date(filter.timeend)
            if (filter.timeend.toString() == "Invalid Date") {
                throw new Error("Invalid timeend")
            }
        }
        if(!this.ftp_client) {
            try {
                await this.accessServer()
            } catch(e) {
                throw(e)
            }
        }
        var files_list = await this.getFilesList(filter)
        files_list = files_list.filter(f => f.isFile).filter(f=> this.file_pattern.test(f.name))
        
        if (!files_list || !files_list.length) {
            console.error("accessors/hidroestimador: No files found")
            return []
        }
        var remote_paths = []
        for (var item of files_list) {
            remote_paths.push(`${this.config.remote_path}/${item.name}`)
        }
        const downloaded_files = await this.downloadFiles(remote_paths)
        const observaciones = await this.rast2obsList(downloaded_files)
        for (const file of downloaded_files) {
            unlinkSync(file)
        }
        return observaciones
    }

    parseDateFromFilename(filename : string) : Date { 
        const d = filename.match(this.file_pattern)[1]
        if(!d.length) {
            throw new Error("File pattern not matched in filename: " + filename)
        }
        return new Date(parseInt(`${d[0]}${d[1]}${d[2]}${d[3]}`), parseInt(`${d[4]}${d[5]}`) - 1, parseInt(`${d[6]}${d[7]}`),this.config.t_offset)
    }

    async update(filter : ObservacionesFilter, options : {}) : Promise<Array<Observacion>> {
        const observaciones = await this.get(filter, options)
        if (!observaciones || observaciones.length == 0) {
            console.error("accessors/hidroestimador/update: Nothing retrieved")
            return []
        }
        const result = await crud_observaciones.create(observaciones) //, "raster", this.config.series_id)
        // const result_diario = await this.getDiario(filter, options)
        // writeFileSync(this.config.tmpfile_json, JSON.stringify(result_diario, null, 4))
        return result
    }

    async test() : Promise<boolean> {
        try {
          this.accessServer()
        } catch (e) {
          console.error(e)
          return false
        }
        return true
    }

    // Downloads files to this.config.local_path
    async downloadFiles(product_urls: Array<string>) : Promise<Array<string>> {
        const downloaded_files : Array<string> = []
        this.downloaded_files = downloaded_files
        if(!this.ftp_client) {
            await this.accessServer()
        }
        for (var u of product_urls) {
            var local_filename = path.join(this.config.local_path, path.basename(u))
            console.debug("accessors/hidroestimador: downloading: " + u + " into: " + local_filename)
            await this.ftp_client.downloadTo(local_filename, u)
            downloaded_files.push(local_filename)
        }
        return downloaded_files
    }

    async rast2obsList(filenames: Array<string>) : Promise<Array<Observacion>> {
        // console.log(JSON.stringify(filenames))
        var observaciones = []
        for (const filename of filenames) {
            const base = path.basename(filename)
            const timestart = this.parseDateFromFilename(base)
            if(timestart.toString() == "Invalid Date") {
                throw("Invalid Date at filename: " + base)
            }
            var timeend : Date = advanceInterval(timestart, {days: 1})
            try {
                var data = readFileSync(filename, 'hex')
            } catch (e) {
                console.error(e)
                continue
            }
            observaciones.push(
              {
                tipo: "raster", 
                series_id: this.config.series_id, 
                timestart: timestart, 
                timeend: timeend,
                valor: '\\x' + data 
              })
        }
        return observaciones
    }

    // printMaps(timestart: Date, timeend: Date) {
    //     return this.callPrintMaps(timestart, timeend, false)
    // }

    // async callPrintMaps(timestart: Date, timeend: Date, skip_print: boolean) {
    //     var mapset = sprintf("%04d", Math.floor(Math.random() * 10000))
    //     var location = sprintf("%s/%s", global.config.grass.location, mapset) // sprintf("%s/GISDATABASE/WGS84/%s",process.env.HOME,mapset)
    //     var batchjob = path.resolve(__dirname, "../py/print_precip_map.py")
    //     if (timestart) {
    //         // console.log("callPrintMaps: timestart: " + timestart.toISOString().replace("Z",""))
    //         process.env.timestart = timestart.toISOString().replace("Z", "")
    //     }
    //     if (timeend) {
    //         // console.log("callPrintMaps: timeend: " + timeend.toISOString().replace("Z",""))
    //         process.env.timeend = timeend.toISOString().replace("Z", "")
    //     }
    //     if (skip_print) {
    //         process.env.skip_print = "True"
    //     }
    //     var command = sprintf("grass %s -c --exec %s", location, batchjob)
    //     const result = await exec(command)
    //     // console.log("batch job called")
    //     var stdout = result.stdout
    //     var stderr = result.stderr
    //     if (stdout) {
    //         console.log(stdout)
    //     }
    //     if (stderr) {
    //         console.error(stderr)
    //     }
    //     process.env.timestart = undefined
    //     process.env.timeend = undefined
    //     process.env.skip_print = undefined
    // }

    // async printMapSemanal(timestart: Date, timeend: Date) {
    //     const location = path.resolve("data/gpm/semanal") // this.config.mes_local_path
    //     var ts = new Date(timestart.getTime())
    //     var te = new Date(timestart.getTime())
    //     te.setUTCDate(te.getUTCDate() + 7)
    //     var results = []
    //     while (te <= timeend) {
    //         console.log({ ts: ts.toISOString(), te: te.toISOString() })
    //         try {
    //             var serie = await rastExtract(13, ts, te, { funcion: "SUM", min_count: 7 })
    //             if (!serie.observaciones || !serie.observaciones.length) {
    //                 throw ("No se encontraron suficientes observaciones")
    //             }
    //             var result = await print_rast(
    //                 {
    //                     prefix: "",
    //                     location: location,
    //                     patron_nombre: "gpm_semanal.YYYYMMDD.HHMMSS.tif"
    //                 },
    //                 undefined,
    //                 serie.observaciones[0]
    //             )
    //             results.push(result)
    //         } catch (e) {
    //             console.error(e)
    //         }
    //         ts.setUTCDate(ts.getUTCDate() + 1)
    //         te.setUTCDate(te.getUTCDate() + 1)
    //     }
    //     var new_timeend = new Date(timeend)
    //     new_timeend.setDate(new_timeend.getDate() - 7)
    //     await this.printSemanalPNG(timestart, new_timeend, false)
    //     return results
    // }
    // async printSemanalPNG(timestart: Date, timeend: Date, skip_print: boolean) {
    //     var mapset = sprintf("%04d", Math.floor(Math.random() * 10000))
    //     var location = sprintf("%s/%s", global.config.grass.location, mapset) // sprintf("%s/GISDATABASE/WGS84/%s",process.env.HOME,mapset)
    //     var batchjob = path.resolve(__dirname, "../py/print_precip_map.py")
    //     if (timestart) {
    //         // console.log("callPrintMaps: timestart: " + timestart.toISOString().replace("Z",""))
    //         process.env.timestart = timestart.toISOString().replace("Z", "")
    //     }
    //     if (timeend) {
    //         // console.log("callPrintMaps: timeend: " + timeend.toISOString().replace("Z",""))
    //         process.env.timeend = timeend.toISOString().replace("Z", "")
    //     }
    //     if (skip_print) {
    //         process.env.skip_print = "True"
    //     }
    //     process.env.base_path = "data/gpm/semanal" // this.config.local_path
    //     process.env.type = "semanal"
    //     var command = sprintf("grass %s -c --exec %s", location, batchjob)
    //     const result = await exec(command)
    //     // console.log("batch job called")
    //     var stdout = result.stdout
    //     var stderr = result.stderr
    //     if (stdout) {
    //         console.log(stdout)
    //     }
    //     if (stderr) {
    //         console.error(stderr)
    //     }
    //     process.env.timestart = undefined
    //     process.env.timeend = undefined
    //     process.env.skip_print = undefined
    // }
}
