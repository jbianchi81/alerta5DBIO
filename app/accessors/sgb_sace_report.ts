import {parseMarkdownTable, readIfExists, parseDDMMYYYY} from '../utils2'
import {AbstractAccessorEngine, AccessorEngine} from './abstract_accessor_engine'
import { Pronostico } from '../a5_types'

type ClientConfig = {
    url : string // not used
    series_id_map : Record<string, number>
    station_name_column: string
    file : string
    null_value : string
    forecast_column_map : Record<string, number> // {"column name": forecast_horizon (days), ...}
    date_column: string
    cal_id: number
    scale: number // convert to meters
    precision: number // round to
}

type SerieProno = {
    series_table: "series" | "series_areal" | "series_rast"
    series_id : number
    cor_id?: number
    pronosticos: Pronostico[]
}

type Corrida = {
    forecast_date : Date
    series: SerieProno[]
    cal_id: number

}

export class Client extends AbstractAccessorEngine implements AccessorEngine {

    example_md_table = `| Estação Fluviométrica | Dia Atual  | Cota Atual (cm) | Dia +7 (cm) | Dia +14 (cm) | Dia +21 (cm) | Dia +28 (cm) |
        | --------------------- | ---------- | --------------- | ----------- | ------------ | ------------ | ------------ |
        | BARRA DO BUGRES       | 25/02/2026 | 301             | 250         | 225          | NA           | NA           |
        | CUIABÁ                | 25/02/2026 | 450             | 352         | 333          | NA           | NA           |
        | CÁCERES               | 25/02/2026 | 338             | 349         | 360          | 360          | 357          |
        | LADÁRIO               | 25/02/2026 | 112             | 119         | 130          | 135          | 140          |
        | FORTE COIMBRA         | 25/02/2026 | -1              | 11          | 19           | 27           | 34           |
        | PORTO MURTINHO        | 25/02/2026 | 223             | 229         | 238          | 252          | 267          |
    `

    default_config : ClientConfig = {
        url: "https://sgb.gov.br/sace/index_bacias_monitoradas.php?getbacia=bparaguaiboletim",
        series_id_map: {     // Ids de serie de altura diaria simulada de estaciones ANA
            "BARRA DO BUGRES": 43910,
            "CUIABÁ": 43912,
            "CÁCERES": 43913,
            "LADÁRIO": 43914,
            "FORTE COIMBRA": 43916,
            "PORTO MURTINHO": 43917
        },
        station_name_column: "Estação Fluviométrica",
        file: "../public/planillas/sace_boletin_paraguai.md",
        null_value: "NA",
        forecast_column_map: {
            "Cota Atual (cm)": 0,
            "Dia +7 (cm)": 7,
            "Dia +14 (cm)": 14,
            "Dia +21 (cm)": 21,
            "Dia +28 (cm)": 28
        },
        date_column: "Dia Atual",
        cal_id: 712,
        scale: 0.01,
        precision: 2
    }

    config : ClientConfig

    constructor(config: ClientConfig) {
        super(config)
        this.config = this.default_config
        Object.assign(this.config,config)
    }

    parseTableRow(row : any) : {serie: SerieProno, forecast_date: Date} {
        if(!(this.config.station_name_column in row)) {
            throw new Error(`Missing column ${this.config.station_name_column} in md table`)
        }
        const station_name = row[this.config.station_name_column]
        if(!(this.config.date_column in row)) {
            throw new Error(`Missing column ${this.config.date_column} in md table`)
        }
        const forecast_date = parseDDMMYYYY(row[this.config.date_column])
        if(!(station_name in this.config.series_id_map)) {
            console.error(`Missing station name ${station_name} in config.series_id_map`)
            var series_id = 0
        } else {
            var series_id = this.config.series_id_map[station_name]
        }
        const pronosticos : Pronostico[] = []
        for(const col of Object.keys(this.config.forecast_column_map)) {
            const forecast_horizon_days = this.config.forecast_column_map[col]
            if(col in row) {
                if(row[col] == this.config.null_value) {
                    console.debug(`Skipping null value at station ${station_name}, forecast horizon ${forecast_horizon_days} days`)
                    continue
                }
                const timestart = new Date(forecast_date)
                timestart.setDate(timestart.getDate() + forecast_horizon_days)
                const timeend = new Date(timestart)
                timeend.setDate(timeend.getDate() + 1)
        
                const valor = parseFloat((parseFloat(row[col]) * this.config.scale).toFixed(this.config.precision))
                pronosticos.push({
                    timestart: timestart,
                    timeend: timeend,
                    valor: valor
                })
            }
        }
        return {
            forecast_date: forecast_date,
            serie: {
                series_table: "series",
                series_id: series_id,
                pronosticos: pronosticos
            }
        }
    }

    async readMdTableFile(path : string) : Promise<Corrida> {
        const md_data = await readIfExists(path)
        const data = parseMarkdownTable(md_data)
        const series = data.map(row => this.parseTableRow(row))
        if(!series.length) {
            throw new Error("No valid rows found in .md file")
        }
        return {
            forecast_date: series[0].forecast_date,
            series: series.map(s => s.serie),
            cal_id: this.config.cal_id
        }
    }

    async getPronostico(filter : {}) : Promise<Corrida> {
        return this.readMdTableFile(this.config.file)
    }
}
