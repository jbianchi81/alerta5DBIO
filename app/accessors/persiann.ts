import { AbstractAccessorEngine, AccessorEngine, ObservacionesFilter, SeriesFilter, SitesFilter, Config, ObservacionesOptions } from "./abstract_accessor_engine"
import { Observacion, Serie, Interval, Location, Escena } from '../a5_types'
import get from 'axios'
import path from 'path'
import { unlinkSync, readFileSync, writeFileSync, createWriteStream } from 'fs'
import { writeFile, readFile, unlink } from "fs/promises";
import { fetch, filterSeries } from '../accessor_utils'
import { advanceInterval, Interval as Interval_pg } from '../timeSteps'
import { exec } from 'child-process-promise'
import { getRegularSeries, rastExtract, escena as crud_escena, observaciones as crud_observaciones, serie as crud_serie, observacion as crud_observacion } from '../CRUD'
import { print_rast_series } from '../print_rast'
import { sprintf } from 'sprintf-js'
import { print_rast } from '../print_rast'
import { Geometry } from "../geometry_types"
import { PromiseFTP } from "promise-ftp"
import { promisify } from "util";
import zlib from "zlib";
import { pipeline } from "stream/promises";
import { spawn } from "child_process";

// import * as gdal from "@gdal/warp"; // Node-GDAL bindings

class FileItem {
    name : string
}

class Escena_ extends crud_escena implements Escena {
    id?: number
    nombre: string
    geom: Geometry
    [x: string]: unknown

    constructor(fields: {}) {
        super(fields)
    }
}

// interface SearchParams {
//     q: "precip_1d" | "precip_3h",
//     lat: number,
//     lon: number,
//     limit: number,
//     area: number,
//     startTime?: string, // YYYY-MM-DD 
//     endTime?: string, // YYYY-MM-DD 
// }

interface PERSIANNConfig extends Config {
    url: string,
    file_pattern: string;
    input_dir: string,
    output_dir: string,
    geojson_path: string,
    pixelsize: number,
    xs: number,
    ys: number
    originx: number,
    originy: number,
    nodata_value: number,
    escena_id: number,
    series_id: number
}

/**
 * Docs: https://pmmpublisher.pps.eosdis.nasa.gov/docs
 * 
 * Available date range for daily product: last 2 months
 * 
 * Time offset of daily product: 00Z
 * 
 * Latency approx. 12 hours
 */
export class Client extends AbstractAccessorEngine implements AccessorEngine {

    config: PERSIANNConfig

    downloaded_files: Array<string>

    subset_files: Array<string>

    ftp: PromiseFTP

    escena: Location

    constructor(config: PERSIANNConfig) {
        super(config)
        this.setConfig(config)
        this.ftp = new PromiseFTP();
    }

    default_config: PERSIANNConfig = {
        url: "http://persiann.eng.uci.edu/CHRSdata/PERSIANN/daily/ms6s4_d{code}.bin.gz",
        file_pattern: "ms6s4_d{year_code:02d}{julian_day:03d}.bin.gz",
        input_dir: "descargas_persiann",
        output_dir: "persiann_cdp",
        geojson_path: "cca_CDP.geojson",
        pixelsize: 0.25,
        xs: 1440,
        ys: 400,
        originx: -180,
        originy: 50,
        nodata_value: -9999,
        escena_id: 776,
        series_id: 21
    }

    async getSites(filter: SitesFilter): Promise<Array<Location>> {
        const escenas = [
            new Escena_({
                "id": this.config.escena_id,
                "nombre": "persiann",
                "geom": { "type": "Polygon", "coordinates": [[[-67, -14], [-43.5, -14], [-43.5, -38.25], [-67, -38.25], [-67, -14]]] }
            })
        ]
        return escenas
    }

    async updateSites(filter: SitesFilter) {
        const sites = (await this.getSites(filter))
        const upserted = await crud_escena.create(sites)
        return upserted.map((site: Escena) => new Escena_(site))
    }

    async getSeries(filter: SeriesFilter): Promise<Array<Serie>> {
        const escena: Location = await this.getSites({})[0]
        const series = [
            {
                "id": this.config.series_id,
                "tipo": "raster",
                "estacion": escena,
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
                    "timeSupport": new Interval_pg({
                        "days": 1
                    }),
                    "def_hora_corte": new Interval_pg({
                        "hours": 9
                    })
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
                    "id": 51, 
                    "nombre": "persiann", 
                    "data_table": "persiann", 
                    "data_column": "rast", 
                    "tipo": "PA", 
                    "def_proc_id": 6, 
                    "def_dt": "1 days", 
                    "hora_corte": "9 hours", 
                    "def_unit_id": 22, 
                    "def_var_id": 1, 
                    "fd_column": null, 
                    "mad_table": null, 
                    "scale_factor": null, 
                    "data_offset": null, 
                    "def_pixel_height": 0.25, 
                    "def_pixel_width": 0.25, 
                    "def_srid": 4326, 
                    "def_extent": { "type": "Polygon", "coordinates": [[[-67, -14], [-43.5, -14], [-43.5, -38.25], [-67, -38.25], [-67, -14]]] }, 
                    "date_column": "timestart", 
                    "def_pixeltype": "32BF", 
                    "abstract": "The current operational PERSIANN (Precipitation Estimation from Remotely Sensed Information using Artificial Neural Networks) system developed by the Center for Hydrometeorology and Remote Sensing (CHRS) at the University of California, Irvine (UCI) uses neural network function classification/approximation procedures to compute an estimate of rainfall rate at each 0.25° x 0.25° pixel of the infrared brightness temperature image provided by geostationary satellites. An adaptive training feature facilitates updating of the network parameters whenever independent estimates of rainfall are available. The PERSIANN system was based on geostationary infrared imagery and later extended to include the use of both infrared and daytime visible imagery. The PERSIANN algorithm used here is based on the geostationary longwave infrared imagery to generate global rainfall. Rainfall product covers 60°S to 60°N globally.", "source": "https://chrsdata.eng.uci.edu/",
                    "public": true 
                }
            }
        ] as Array<Serie>
        return filterSeries(series.map(s => new crud_serie(s)), filter)
    }

    async getFilesList() : Promise<string[]> {
        const files_list : Array<FileItem> = await this.ftp.list(this.config.url)
        return files_list.map(f => f.name).filter(name => (name.match(/^file_(\d+)\.bin\.gz$/)))
    }

    async getFile(filename : string,local_copy : string) {
        const stream = await this.ftp.get(filename)
        return new Promise(function (resolve, reject) {
            stream.once('close', resolve);
            stream.once('error', reject);
            stream.pipe(createWriteStream(local_copy));
        })        
    }

    parseDateFromFilename(filename :string) : Date | null {
        const matches = filename.match(/ms6s4_d(\d{2})(\d{3})\.bin\.gz/)
        if(!matches) {
            return null
        }
        const year = 2000 + parseInt(matches[1])
        const doy = parseInt(matches[2])
        return new Date(year, 0, doy, 9)
    }

    filterByDate(filename : string, timestart : Date, timeend : Date) : boolean {
        const date = this.parseDateFromFilename(filename)
        if(!date) {
            return false
        }
        if(date < timestart) {
            return false
        }
        if(date > timeend) {
            return false
        }
        return true
    }

    async get(filter: ObservacionesFilter, options: ObservacionesOptions): Promise<Array<Observacion>> {
        if (!filter.timestart) {
            filter.timestart = new Date(new Date().getTime() - 9 * 24 * 3600 * 1000)
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
        const filenames = await this.getFilesList()
        // filter by date
        const filtered_filenames = filenames.filter(f => this.filterByDate(f, filter.timestart, filter.timeend))
        console.debug({filtered_filenames})
        if (!filtered_filenames.length) {
            console.error("accessors/persiann: No products found for the specified dates")
            return []
        }
        const downloaded_files = await this.downloadFiles(filtered_filenames)
        const observaciones = await this.rast2obsList(downloaded_files, options.update)
        for (const file of downloaded_files) {
            unlinkSync(file)
        }
        return observaciones
    }

    async update(filter: ObservacionesFilter, options: {}): Promise<Array<Observacion>> {
        const observaciones = await this.get(filter, {...options, update: true})
        if (!observaciones || observaciones.length == 0) {
            console.error("accessors/gpm/update: Nothing retrieved")
            return []
        }
        return observaciones
    }

    async test(): Promise<boolean> {
        const fileslist = await this.getFilesList()
        if (fileslist.length) {
            return true
        } else {
            return false
        }
    }

    async downloadFiles(filenames: Array<string>) {
        const downloaded_files: Array<string> = []
        this.downloaded_files = downloaded_files
        for (var filename of filenames) {
            const local_filename = path.resolve(this.config.output_dir, filename)
            const remote_filename = `${this.config.url}/${filename}`
            console.debug("accessors/persiann: downloading: " + remote_filename + " into: " + local_filename)
            try {
                await this.getFile(remote_filename, local_filename)
            } catch (e) {
                console.error(e)
                continue
            }
            downloaded_files.push(filename)
        }
        return downloaded_files
    }

    async decompressGz(gzPath: string, binPath: string): Promise<void> {
        const gunzip = promisify(zlib.gunzip);
        const compressed = await readFile(gzPath);
        const decompressed = await gunzip(compressed as unknown as Uint8Array);
        await writeFile(binPath, decompressed as Uint8Array);
    }

    /** Run a shell command and wait for it to finish */
    async runCommand(cmd: string, args: string[]): Promise<void> {
        return new Promise((resolve, reject) => {
            const child = spawn(cmd, args, { stdio: "inherit" });
            child.on("close", code => {
            if (code === 0) resolve();
            else reject(new Error(`${cmd} failed with code ${code}`));
            });
        });
    }


    async binToTif(binPath: string, tifPath: string): Promise<void> {

        // Read binary file into Float32Array
        const buffer = await readFile(binPath);
        const numElements = buffer.byteLength / 4;
        const data = new Float32Array(numElements);

        for (let i = 0; i < numElements; i++) {
            data[i] = buffer.readFloatBE(i * 4);
            if (data[i] < 0) data[i] = this.config.nodata_value;
        }

        const flipped = new Float32Array(numElements);
        for (let row = 0; row < this.config.ys; row++) {
            const srcStart = row * this.config.xs;
            const dstStart = (this.config.ys - row - 1) * this.config.xs;
            flipped.set(data.subarray(srcStart, srcStart + this.config.xs), dstStart);
        }

        // Write raw float32 binary file for gdal_translate
        const rawPath = binPath + ".float32";
        await writeFile(rawPath, Buffer.from(flipped.buffer)  as Uint8Array);

        const translateArgs = [
            "-of", "GTiff",
            "-ot", "Float32",
            "-a_srs", "EPSG:4326",
            "-a_ullr", this.config.originx.toString(),
            this.config.originy.toString(),
            (this.config.originx + this.config.xs * this.config.pixelsize).toString(),
            (this.config.originy - this.config.ys * this.config.pixelsize).toString(),
            rawPath,
            tifPath
        ];

        await this.runCommand("gdal_translate", translateArgs);

        await unlink(rawPath); // cleanup
    }

    async recortarTif(tifPath: string, outputPath: string, geom?: string): Promise<void> {
        if(!geom) {
            if(!this.escena) {
                this.escena = await this.getSites({})[0]
            }
            geom = this.escena.geom.toString()
        }
        const warpArgs = [
            "-cutline", geom,
            "-crop_to_cutline",
            "-dstnodata", this.config.nodata_value.toString(),
            tifPath,
            outputPath
        ];

        await this.runCommand("gdalwarp", warpArgs);
    }

    async procesarArchivo(filename: string, output_filename : string): Promise<void> {
        if (!(filename.endsWith(".gz") && filename.startsWith("persiann_"))) return;

        const datecode = filename.substring(9, 17);
        const gzPath = path.join(this.config.input_dir, filename);
        const binPath = path.join(this.config.input_dir, `${datecode}.bin`);
        const tifPath = path.join(this.config.input_dir, `${datecode}.tif`);
        const recortadoPath = path.join(this.config.output_dir, output_filename);

        console.log(`\nProcesando ${filename}...`);

        try {
            await this.decompressGz(gzPath, binPath);
            await this.binToTif(binPath, tifPath);
            await this.recortarTif(tifPath, recortadoPath);

            await unlink(binPath);
            await unlink(tifPath);

            console.log(`Guardado: ${recortadoPath}`);
        } catch (err) {
            console.error(`Error procesando ${filename}:`, err);
        } finally {
            global.gc?.(); // optional GC trigger
        }
    }

    async rast2obsList(filenames: Array<string>, update_ : boolean=false) : Promise<Observacion[]> {
        // console.log(JSON.stringify(filenames))
        this.subset_files = []
        for (var filename of filenames) {
            var timestart = this.parseDateFromFilename(filename)
            var timeend = new Date(timestart)
            timeend.setDate(timestart.getDate() + 1)
            const output_filename = filename.replace(/\.bin\.gz$/, "_cdp.tif")
            try {
                await this.procesarArchivo(filename, output_filename) 
            } catch (e) {
                console.error(e)
                continue
            }
            this.subset_files.push(output_filename)
        }
        var observaciones = []
        for(const file of this.subset_files) {
            const data = readFileSync(path.join(this.config.output_dir, file), 'hex')
            const obs = new crud_observacion({
                tipo: "raster",
                series_id: this.config.series_id,
                timestart: timestart,
                timeend: timeend,
                valor: '\\x' + data
            })
            if(update_) {
                const created_obs : Observacion = await obs.create()
                observaciones.push(created_obs)
            } else {
                observaciones.push(obs)
            }
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
