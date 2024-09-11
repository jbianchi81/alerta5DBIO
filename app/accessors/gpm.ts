import { AbstractAccessorEngine, AccessorEngine, ObservacionesFilter, SeriesFilter, SitesFilter, Config } from "./abstract_accessor_engine"
import { Observacion, Serie, Interval, Location, Escena } from '../a5_types'
import get from 'axios'
import path from 'path'
import { unlinkSync, readFileSync, writeFileSync } from 'fs'
import { fetch, filterSeries } from '../accessor_utils'
import { advanceInterval } from '../timeSteps'
import { exec } from 'child-process-promise'
import { getRegularSeries, rastExtract, escena as crud_escena, observaciones as crud_observaciones, serie as crud_serie} from '../CRUD'
import { print_rast_series } from '../print_rast'
import { sprintf } from 'sprintf-js'
import { print_rast } from '../print_rast'
import { Geometry } from "../geometry_types"

class Escena_ extends crud_escena implements Escena  {
  id ? : number
  nombre : string
  geom : Geometry
  [x : string] : unknown

  constructor(fields : {}) {
    super(fields)
  }
}

interface SearchParams {
    q: "precip_1d" | "precip_3h",
    lat: number,
    lon: number,
    limit: number,
    area: number,
    startTime?: string, // YYYY-MM-DD 
    endTime?: string, // YYYY-MM-DD 
}

interface GPMConfig extends Config {
    url: string,
    local_path: string,
    dia_local_path: string,
    search_params: SearchParams,
    bbox: Array<number>,
    tmpfile: string,
    tmpfile_json : string,
    series_id: number,
    scale: number
    dia_series_id: number,
    escena_id: number
}

/**
 * Docs: https://pmmpublisher.pps.eosdis.nasa.gov/docs
 */
export class Client extends AbstractAccessorEngine implements AccessorEngine {

    config: GPMConfig

    downloaded_files : Array<string> 

    subset_files : Array<string> 

    constructor(config: GPMConfig) {
        super(config)
        this.setConfig(config)
    }

    default_config: GPMConfig = {
        url: "https://pmmpublisher.pps.eosdis.nasa.gov/opensearch",
        local_path: "data/gpm/3h/",
        dia_local_path: "data/gpm/dia",
        search_params:
        {
            q: "precip_1d",
            lat: -25,
            lon: -45,
            limit: 64,
            area: 0.25
        },
        bbox: [-70, -10, -40, -40],
        tmpfile: "/tmp/gpm_transformed.tif",
        tmpfile_json: "/tmp/gpm_dia.json",
        series_id: 4,
        scale: 0.1,
        dia_series_id: 13,
        escena_id: 11
    }

    async getSites(filter : SitesFilter) : Promise<Array<Location>> {
      const escenas = [
        new Escena_({
          "id": this.config.escena_id,
          "nombre": "pp_gpm_3h",
          "geom": {
            "type": "Polygon",
            "coordinates": [
              [
                [
                  -90,
                  10
                ],
                [
                  -30,
                  10
                ],
                [
                  -30,
                  -60
                ],
                [
                  -90,
                  -60
                ],
                [
                  -90,
                  10
                ]
              ]
            ]
          }
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
                  "id": 11,
                  "nombre": "pp_gpm_3h",
                  "geom": {
                    "type": "Polygon",
                    "coordinates": [
                      [
                        [
                          -90,
                          10
                        ],
                        [
                          -30,
                          10
                        ],
                        [
                          -30,
                          -60
                        ],
                        [
                          -90,
                          -60
                        ],
                        [
                          -90,
                          10
                        ]
                      ]
                    ]
                  }
                },
                "var": {
                  "id": 34,
                  "var": "P3h",
                  "nombre": "precipitación 3 horaria",
                  "abrev": "precip_3h",
                  "type": "num",
                  "datatype": "Cumulative",
                  "valuetype": "Field Observation",
                  "GeneralCategory": "Climate",
                  "VariableName": "Precipitation",
                  "SampleMedium": "Precipitation",
                  "def_unit_id": 9,
                  "timeSupport": {
                    "hours": 3
                  },
                  "def_hora_corte": {}
                },
                "procedimiento": {
                  "id": 5,
                  "nombre": "Estimado",
                  "abrev": "est",
                  "descripcion": "Estimado a partir de observaciones indirectas"
                },
                "unidades": {
                  "id": 9,
                  "nombre": "milímetros",
                  "abrev": "mm",
                  "UnitsID": 54,
                  "UnitsType": "Length"
                },
                "fuente": {
                  "id": 13,
                  "nombre": "pp_gpm_3h",
                  "data_table": "pp_gpm_3h",
                  "data_column": "rast",
                  "tipo": "QPE",
                  "def_proc_id": 5,
                  "def_dt": {
                    "hours": 3
                  },
                  "hora_corte": {},
                  "def_unit_id": 9,
                  "def_var_id": 34,
                  "fd_column": null,
                  "mad_table": "pmad_gpm_3h",
                  "scale_factor": 1,
                  "data_offset": 0,
                  "def_pixel_height": 0.1,
                  "def_pixel_width": 0.1,
                  "def_srid": 4326,
                  "def_extent": {
                    "type": "Polygon",
                    "coordinates": [
                      [
                        [
                          -90,
                          10
                        ],
                        [
                          -30,
                          10
                        ],
                        [
                          -30,
                          -60
                        ],
                        [
                          -90,
                          -60
                        ],
                        [
                          -90,
                          10
                        ]
                      ]
                    ]
                  },
                  "date_column": "date",
                  "def_pixeltype": "32BF",
                  "abstract": "Estimación satelital de la precipitación a paso 3 horario de la misión GPM. Fuente NASA",
                  "source": "ftp://jsimpson.pps.eosdis.nasa.gov/data/imerg/early/",
                  "public": true
                },
                "date_range": {
                  "timestart": "2021-07-31T00:00:00.000Z",
                  "timeend": "2024-09-09T21:00:00.000Z",
                  "count": "3836"
                }
            },
            {
                "id": this.config.dia_series_id,
                "tipo": "raster",
                "estacion": {
                  "id": 11,
                  "nombre": "pp_gpm_3h",
                  "geom": {
                    "type": "Polygon",
                    "coordinates": [
                      [
                        [
                          -90,
                          10
                        ],
                        [
                          -30,
                          10
                        ],
                        [
                          -30,
                          -60
                        ],
                        [
                          -90,
                          -60
                        ],
                        [
                          -90,
                          10
                        ]
                      ]
                    ]
                  }
                },
                "var": {
                  "id": 1,
                  "var": "P",
                  "nombre": "precipitación diaria 12Z",
                  "abrev": "precip_diaria_met",
                  "type": "num",
                  "datatype": "Succeeding Total",
                  "valuetype": "Field Observation",
                  "GeneralCategory": "Climate",
                  "VariableName": "Precipitation",
                  "SampleMedium": "Precipitation",
                  "def_unit_id": 22,
                  "timeSupport": {
                    "days": 1
                  },
                  "def_hora_corte": {
                    "hours": 9
                  }
                },
                "procedimiento": {
                  "id": 5,
                  "nombre": "Estimado",
                  "abrev": "est",
                  "descripcion": "Estimado a partir de observaciones indirectas"
                },
                "unidades": {
                  "id": 22,
                  "nombre": "milímetros por día",
                  "abrev": "mm/d",
                  "UnitsID": 305,
                  "UnitsType": "velocity"
                },
                "fuente": {
                  "id": 6,
                  "nombre": "gpm",
                  "data_table": "pp_gpm",
                  "data_column": "rast",
                  "tipo": "QPE",
                  "def_proc_id": 5,
                  "def_dt": {
                    "days": 1
                  },
                  "hora_corte": {
                    "hours": 9
                  },
                  "def_unit_id": 22,
                  "def_var_id": 1,
                  "fd_column": null,
                  "mad_table": "pmad_gpm",
                  "scale_factor": null,
                  "data_offset": null,
                  "def_pixel_height": 0.1,
                  "def_pixel_width": 0.1,
                  "def_srid": 4326,
                  "def_extent": {
                    "type": "Polygon",
                    "coordinates": [
                      [
                        [
                          -90,
                          10
                        ],
                        [
                          -30,
                          10
                        ],
                        [
                          -30,
                          -60
                        ],
                        [
                          -90,
                          -60
                        ],
                        [
                          -90,
                          10
                        ]
                      ]
                    ]
                  },
                  "date_column": "date",
                  "def_pixeltype": "32BF",
                  "abstract": "Estimación satelital de la precipitación a paso diario de la misión GPM. Fuente NASA",
                  "source": "ftp://jsimpson.pps.eosdis.nasa.gov/data/imerg/early/",
                  "public": true
                },
                "date_range": {
                  "timestart": "2021-07-31T12:00:00.000Z",
                  "timeend": "2024-02-29T12:00:00.000Z",
                  "count": 380
                }
            }
        ] as Array<Serie>
        return filterSeries(series.map(s => new crud_serie(s)), filter)
    }

    async getFilesList(filter: ObservacionesFilter) {
        const params = { ...this.config.search_params }
        // q: "precip_3hr",
        // lat: "-25",
        // lon: "-45",
        // limit: "56",
        params.startTime = filter.timestart.toISOString().substring(0, 10)
        params.endTime = filter.timeend.toISOString().substring(0, 10)
        console.debug({ url: this.config.url, params: params })
        return get(this.config.url, { params: params })
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
        const response = await this.getFilesList(filter)
        // console.log(response.data)
        if (!response.data || !response.data.items || !response.data.items.length) {
            console.error("accessors/gpm: No products found")
            return []
        }
        var product_urls = []
        var product_ids = []
        for (var item of response.data.items) {
            if (item["@type"] != "geoss:precipitation") {
                continue
            }
            for (var j in item.action) {
                var action = item.action[j]
                if (action["@type"] != "ojo:download") {
                    continue
                }
                for (var k in action.using) {
                    var using = action.using[k]
                    if (using.mediaType == "image/tiff") {
                        product_urls.push(using.url)
                        product_ids.push(using["@id"])
                    }
                }
            }
        }
        const local_path = (this.config.search_params.q == "precip_1d") ? this.config.dia_local_path : this.config.local_path
        var local_filenames = product_ids.map(id => {
            return path.resolve(local_path, id)
        })
        const downloaded_files = await this.downloadFiles(product_urls, local_filenames)
        const dt = (this.config.search_params.q == "precip_1d") ? {days: 1} : {hours: 3}
        const observaciones = await this.rast2obsList(downloaded_files, dt)
        for (const file of downloaded_files) {
            unlinkSync(file)
        }
        return observaciones
    }

    async update(filter : ObservacionesFilter, options : {}) : Promise<Array<Observacion>> {
        const observaciones = await this.get(filter, options)
        if (!observaciones || observaciones.length == 0) {
            console.error("accessors/gpm/update: Nothing retrieved")
            return []
        }
        const result = await crud_observaciones.create(observaciones) //, "raster", this.config.series_id)
        // const result_diario = await this.getDiario(filter, options)
        // writeFileSync(this.config.tmpfile_json, JSON.stringify(result_diario, null, 4))
        return result
    }

    async test() : Promise<boolean> {
        const params = { ...this.config.search_params }
        params.startTime = new Date().toISOString().substring(0, 10)
        params.endTime = new Date().toISOString().substring(0, 10)
        try {
          var response = await get(this.config.url, { params: params })
        } catch (e) {
          console.error(e)
          return false
        }
        if (response.status <= 299) {
            return true
        } else {
            return false
        }
    }

    async downloadFiles(product_urls: Array<string>, local_filenames: Array<string>) {
        const downloaded_files : Array<string> = []
        this.downloaded_files = downloaded_files
        for (var u in product_urls) {
            var filename = local_filenames[u]
            console.log("accessors/gpm: downloading: " + product_urls[u] + " into: " + filename)
            try {
                await fetch(product_urls[u], undefined, filename)
            } catch (e) {
                console.error(e)
                continue
            }
            downloaded_files.push(filename)
        }
        return downloaded_files
    }

    async rast2obsList(filenames: Array<string>, dt: Interval = { hours: 3 }) {
        // console.log(JSON.stringify(filenames))
        var observaciones = []
        this.subset_files = []
        for (var filename of filenames) {
            var filename2 = filename.replace(/\.tif$/, "_subset.tif")
            var base = path.basename(filename)
            var b = base.split(".")
            if (b.length == 4) {
                // has time
                var timestart = new Date(
                    Date.UTC(
                        parseInt(b[1].substring(0, 4)),
                        parseInt(b[1].substring(4, 6)) - 1,
                        parseInt(b[1].substring(6, 8)),
                        parseInt(b[2].substring(0, 2))
                    ) - 2 * 3600 * 1000
                )
            } else {
                // only date
                var timestart = new Date(
                    Date.UTC(
                        parseInt(b[1].substring(0, 4)),
                        parseInt(b[1].substring(4, 6)) - 1,
                        parseInt(b[1].substring(6, 8))
                    )
                )
            }
            if(timestart.toString() == "Invalid Date") {
                throw("Invalid Date: " + b[1])
            }
            var timeend = advanceInterval(timestart, dt)
            var scale = this.config.scale
            // return new Promise( (resolve, reject) => {
            try {
                await exec('gdal_translate -projwin ' + this.config.bbox.join(" ") + ' -a_nodata 9999 ' + filename + ' ' + filename2) // ('gdal_translate -a_scale 0.1 -unscale -projwin ' + this.config.bbox.join(" ") + ' ' + filename + ' ' + filename2)  this.config.tmpfile)  // ulx uly lrx lrt
            } catch (e) {
                console.error(e)
                continue
            }
            try {
                var data = readFileSync(filename2, 'hex')
            } catch (e) {
                console.error(e)
                continue
            }
            this.subset_files.push(filename2)

            observaciones.push(
              {
                tipo: "raster", 
                series_id: (this.config.search_params.q == "precip_1d") ? this.config.dia_series_id : this.config.series_id, 
                timestart: timestart, 
                timeend: timeend, 
                scale: scale, 
                valor: '\\x' + data 
              })
        }
        return observaciones
    }

    async getDiario(filter: ObservacionesFilter, options = {}) {
        // console.log({config:this.config})
        options["insertSeriesId"] = this.config.dia_series_id
        options["t_offset"] = "12:00:00"
        const result = await getRegularSeries(
            "raster",
            this.config.series_id,
            { days: 1 },
            filter.timestart,
            filter.timeend,
            options
        )
        const dia_location = path.resolve(this.config.dia_local_path)
        return print_rast_series(
            {
                id: 13,
                observaciones: result
            }, {
            location: dia_location,
            format: "GTiff",
            patron_nombre: "gpm_dia.YYYYMMDD.HHMMSS.tif"
        }
        )
    }

    printMaps(timestart: Date, timeend: Date) {
        return this.callPrintMaps(timestart, timeend, false)
    }

    async callPrintMaps(timestart: Date, timeend: Date, skip_print: boolean) {
        var mapset = sprintf("%04d", Math.floor(Math.random() * 10000))
        var location = sprintf("%s/%s", global.config.grass.location, mapset) // sprintf("%s/GISDATABASE/WGS84/%s",process.env.HOME,mapset)
        var batchjob = path.resolve(__dirname, "../py/print_precip_map.py")
        if (timestart) {
            // console.log("callPrintMaps: timestart: " + timestart.toISOString().replace("Z",""))
            process.env.timestart = timestart.toISOString().replace("Z", "")
        }
        if (timeend) {
            // console.log("callPrintMaps: timeend: " + timeend.toISOString().replace("Z",""))
            process.env.timeend = timeend.toISOString().replace("Z", "")
        }
        if (skip_print) {
            process.env.skip_print = "True"
        }
        var command = sprintf("grass %s -c --exec %s", location, batchjob)
        const result = await exec(command)
        // console.log("batch job called")
        var stdout = result.stdout
        var stderr = result.stderr
        if (stdout) {
            console.log(stdout)
        }
        if (stderr) {
            console.error(stderr)
        }
        process.env.timestart = undefined
        process.env.timeend = undefined
        process.env.skip_print = undefined
    }

    async printMapSemanal(timestart: Date, timeend: Date) {
        const location = path.resolve("data/gpm/semanal") // this.config.mes_local_path
        var ts = new Date(timestart.getTime())
        var te = new Date(timestart.getTime())
        te.setUTCDate(te.getUTCDate() + 7)
        var results = []
        while (te <= timeend) {
            console.log({ ts: ts.toISOString(), te: te.toISOString() })
            try {
                var serie = await rastExtract(13, ts, te, { funcion: "SUM", min_count: 7 })
                if (!serie.observaciones || !serie.observaciones.length) {
                    throw ("No se encontraron suficientes observaciones")
                }
                var result = await print_rast(
                    {
                        prefix: "",
                        location: location,
                        patron_nombre: "gpm_semanal.YYYYMMDD.HHMMSS.tif"
                    },
                    undefined,
                    serie.observaciones[0]
                )
                results.push(result)
            } catch (e) {
                console.error(e)
            }
            ts.setUTCDate(ts.getUTCDate() + 1)
            te.setUTCDate(te.getUTCDate() + 1)
        }
        var new_timeend = new Date(timeend)
        new_timeend.setDate(new_timeend.getDate() - 7)
        await this.printSemanalPNG(timestart, new_timeend, false)
        return results
    }
    async printSemanalPNG(timestart: Date, timeend: Date, skip_print: boolean) {
        var mapset = sprintf("%04d", Math.floor(Math.random() * 10000))
        var location = sprintf("%s/%s", global.config.grass.location, mapset) // sprintf("%s/GISDATABASE/WGS84/%s",process.env.HOME,mapset)
        var batchjob = path.resolve(__dirname, "../py/print_precip_map.py")
        if (timestart) {
            // console.log("callPrintMaps: timestart: " + timestart.toISOString().replace("Z",""))
            process.env.timestart = timestart.toISOString().replace("Z", "")
        }
        if (timeend) {
            // console.log("callPrintMaps: timeend: " + timeend.toISOString().replace("Z",""))
            process.env.timeend = timeend.toISOString().replace("Z", "")
        }
        if (skip_print) {
            process.env.skip_print = "True"
        }
        process.env.base_path = "data/gpm/semanal" // this.config.local_path
        process.env.type = "semanal"
        var command = sprintf("grass %s -c --exec %s", location, batchjob)
        const result = await exec(command)
        // console.log("batch job called")
        var stdout = result.stdout
        var stderr = result.stderr
        if (stdout) {
            console.log(stdout)
        }
        if (stderr) {
            console.error(stderr)
        }
        process.env.timestart = undefined
        process.env.timeend = undefined
        process.env.skip_print = undefined
    }
}
