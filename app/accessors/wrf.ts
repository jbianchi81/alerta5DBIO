import { AbstractAccessorEngine } from "./abstract_accessor_engine"
import { corrida as CrudCorrida, pronostico as CrudPronostico, SerieTemporalSimDict } from "../CRUD"
import axios from "axios"
import { exec as pexec } from "child-process-promise"
import { mkdtemp, readFile, rm, writeFile } from "fs/promises"
import { createWriteStream } from "fs"
import { tmpdir } from "os"
import { join } from "path"
import PromiseFtp from "promise-ftp"

interface Config {
    url: string
    host: string
    user: string
    password: string
    filepath: string
    series_id: number
    cal_id: number
    available_files? : {path: string, utc_time: number}[]
    [x: string]: unknown
}

interface GdalBand {
    metadata?: Record<string, Record<string, string>>
}

interface GdalInfo {
    metadata?: Record<string, Record<string, string>>
    bands?: GdalBand[]
}

export class Client extends AbstractAccessorEngine {
    config: Config
    ftp: PromiseFtp

    constructor(config: Config) {
        super(config)
        this.config = config
        this.ftp = new PromiseFtp()
    }

    async connect() {
		try {
			var serverMessage = await this.ftp.connect({host: this.config.host, user: this.config.user, password: this.config.password})
			console.log('serverMessage:' + serverMessage)
			return
		} catch (e) {
			throw e
		}
	}

    private findMetadataValue(info: GdalInfo, key: string, band?: GdalBand): string {
        const metadata = [band?.metadata, info.metadata]

        for (const domains of metadata) {
            for (const values of Object.values(domains ?? {})) {
                const entry = Object.entries(values).find(
                    ([name]) => name.toUpperCase() === key
                )
                if (entry) return entry[1]
            }
        }

        throw new Error(`GRIB metadata ${key} not found`)
    }

    private parseGribDate(value: string): Date {
        const epoch = parseInt(value)
        const date = new Date(epoch * 1000)

        if (Number.isNaN(date.getTime())) {
            throw new Error(`Invalid GRIB date: ${value}`)
        }
        return date
    }

    private async writeStreamToFile(stream : NodeJS.ReadableStream, output : string) {
            return new Promise(function (resolve, reject) {
                stream.once('close', resolve)
                stream.once('error', reject)
                stream.pipe(createWriteStream(output))
            });
        }

    async getPronostico(filter:{forecast_date?: Date}={}, options:any={}): Promise<CrudCorrida> {
        const workDir = await mkdtemp(join(tmpdir(), "wrf-"))
        const gribPath = join(workDir, "input.grib")

        var filepath = this.config.filepath
        if(filter.forecast_date && this.config.available_files) {
            const utc_time = filter.forecast_date.getUTCHours()
            for(const f of this.config.available_files) {
                if(f.utc_time == utc_time) {
                    filepath = f.path
                }
            }
        }

        try {
            await this.connect()
            // const response = await axios.get<ArrayBuffer>(this.config.url, {
            //     responseType: "arraybuffer"
            // })
            console.debug("conectado a ftp")
            const stream = this.ftp.get(filepath)
            console.debug("Descargando " + filepath)
            await this.writeStreamToFile(await stream,gribPath)
            console.debug("se escribió " + gribPath)

            await this.ftp.end()
            // await writeFile(gribPath, Buffer.from(response.read()))

            const { stdout } = await pexec(`gdalinfo -json "${gribPath}"`)
            const info = JSON.parse(stdout) as GdalInfo
            const bands = info.bands ?? []

            if (!bands.length) {
                throw new Error("GRIB file contains no raster bands")
            }
            console.debug("se leyeron " + bands.length + " bandas")

            const forecastDate = this.parseGribDate(
                this.findMetadataValue(info, "GRIB_REF_TIME", bands[0])
            )

            const corrida = new CrudCorrida({
                cal_id: this.config.cal_id,
                forecast_date: forecastDate,
                series: []
            })

            if(options.update) {
                await corrida.create()
                console.debug("se creó la corrida " + corrida.id)
            }

            const serie : SerieTemporalSimDict = {
                series_table: "series_rast",
                series_id: this.config.series_id,
                pronosticos: []
            }

            for (let index = 0; index < bands.length; index++) {
                const band = bands[index]
                const validTime = this.parseGribDate(
                    this.findMetadataValue(info, "GRIB_VALID_TIME", band)
                )
                const bandPath = join(workDir, `band-${index + 1}.tif`)

                await pexec(
                    `gdal_translate -q -of GTiff -b ${index + 1} "${gribPath}" "${bandPath}"`
                )

                const pronostico = {
                    timestart: validTime,
                    valor: await readFile(bandPath),
                    series_id: this.config.series_id,
                    series_table: "series_rast",
                    srid: 990001
                }
                if(options.update) {
                    await CrudPronostico.create([pronostico], {tipo : "raster", cor_id: corrida.id, no_send_data: true})
                    serie.pronosticos.push({timestart: validTime})
                    console.debug("se creó el pronóstico " + validTime.toISOString())
                } else {
                    serie.pronosticos.push(pronostico)                  
                }
            }

            console.debug("fin de la corrida")
            corrida.series.push(serie)
            return corrida
        } finally {
            await rm(workDir, { recursive: true, force: true })
        }
    }

    async updatePronostico(
		filter : any={},
		options : any={}
	) : Promise<CrudCorrida> {
		return this.getPronostico(filter, {update: true})
    }
}