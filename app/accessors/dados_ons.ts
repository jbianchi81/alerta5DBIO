import { AbstractAccessorEngine } from './abstract_accessor_engine'
import { Database, RowData } from "duckdb-async"
import { fetch } from '../accessor_utils'

type DadosHidrologicosRecord = {
    id_subsistema : string,
    nom_subsistema: string,
    tip_reservatorio: string,
    nom_bacia: string,
    nom_ree: string,
    id_reservatorio: string,
    nom_reservatorio: string,
    num_ordemcs: number,
    cod_usina: number,
    din_instante: Date,
    val_nivelmontante: number,
    val_niveljusante: number,
    val_volumeutilcon: number,
    val_vazaoafluente: number,
    val_vazaoturbinada: number,
    val_vazaovertida: number,
    val_vazaooutrasestruturas: number,
    val_vazaodefluente: number,
    val_vazaotransferida: number,
    val_vazaonatural: number,
    val_vazaoartificial: number,
    val_vazaoincremental: number,
    val_vazaoevaporacaoliquida: number,
    val_vazaousoconsuntivo: number
}

type Observacion = {
    timestart : Date,
    timeend ? : Date,
    valor : number,
    series_id ? : number
}

type AccessorFilter = {
    timestart : Date,
    timeend : Date,
    series_id ? : number|Array<number>,
    estacion_id ? : number|Array<number>,
    var_id ? : number|Array<number>
}

type AccessorFilterWithArrays = {
    timestart : Date,
    timeend : Date,
    series_id ? : Array<number>,
    estacion_id ? : Array<number>,
    var_id ? : Array<number>
}

type Config = {
    url : string,
    file_pattern : string,
    output_file : string,
    sites_map : Array<SiteMap>,
    var_map : Array<VariableMap>,
    series_map : Array<SerieMap>
}

type SiteMap = {
    id_reservatorio : string,
    estacion_id : number
}

type VariableMap = {
    field_name: string,
    var_id: number
}

type SerieMap = {
    estacion_id : number,
    var_id : number,
    series_id : number
}

export class Client extends AbstractAccessorEngine {

    static _get_is_multiseries : boolean = true

    config : Config

    default_config : Config = {
        url : "https://ons-aws-prod-opendata.s3.amazonaws.com/",
        file_pattern: "dataset/dados_hidrologicos_di/DADOS_HIDROLOGICOS_RES_%YYYY%.parquet",
        output_file: "/tmp/dados_ons.parquet",
        sites_map: [],
        var_map: [
            { field_name: "val_volumeutilcon", var_id: 26},
            { field_name: "val_vazaoafluente", var_id: 22},
            { field_name: "val_vazaovertida", var_id: 24},
            { field_name: "val_vazaodefluente", var_id: 23}
        ],
        series_map: []
    }

    static async readParquetFile(filename : string, limit : number = 1000000, offset : number = 0) : Promise<Array<DadosHidrologicosRecord>> {
        const db : Database = await Database.create(":memory:");
        const rows : Array<RowData> = await db.all(`SELECT * FROM READ_PARQUET('${filename}') LIMIT ${limit} OFFSET ${offset}`)
        // console.log(rows);
        return rows.map(r => r as DadosHidrologicosRecord)
    }

    static parseDadosHidrologicosRecord(record : DadosHidrologicosRecord, field : string, series_id ? : number) : Observacion {
        var end_date = new Date(record.din_instante)
        end_date.setUTCDate(end_date.getUTCDate() + 1)
        return {
            timestart: record.din_instante,
            timeend: end_date, 
            valor: record[field],
            series_id: series_id
        } as Observacion
    }

    getEstacionId(id_reservatorio : string) : number|undefined {
        for(const estacion of this.config.sites_map) {
            if( estacion.id_reservatorio == id_reservatorio ) {
                return estacion.estacion_id
            }
        }
        return
    }

    getSeriesId(estacion_id : number, var_id : number) : number|undefined {
        for(const serie of this.config.series_map) {
            if( serie.estacion_id == estacion_id && serie.var_id == var_id ) {
                return serie.series_id
            }
        }
        return
    }

    static setFilterValuesToArray(filter : AccessorFilter) : AccessorFilterWithArrays {
        const filter_ = Object.assign({},filter)
        for(const key of ["series_id", "estacion_id", "var_id"]) {
            if(filter_[key] != undefined) {
                if(!Array.isArray(filter_[key])) {
                    filter_[key] = [filter_[key]]
                }
            }
        }
        return filter_ as AccessorFilterWithArrays
    }


    async get (filter : AccessorFilter) : Promise<Array<Observacion>> {
        if(!filter || !filter.timestart || !filter.timeend) {
            throw("Missing timestart and/or timeend")
        }
        const filter_ = Client.setFilterValuesToArray(filter)
        const observaciones : Array<Observacion> = [] 
        for(var year = filter_.timestart.getUTCFullYear(); year <= filter_.timeend.getUTCFullYear(); year++) {
            const filepath = this.config.file_pattern.replace("%YYYY%", year.toString())
            const url = `${this.config.url}${filepath}`
            await fetch(url, undefined, this.config.output_file, () => null)
            const records = await Client.readParquetFile(this.config.output_file)
            for(const record of records) {
                if(record.din_instante < filter_.timestart || record.din_instante > filter_.timeend) {
                    continue
                }
                const estacion_id = this.getEstacionId(record.id_reservatorio)
                var series_id : number|undefined
                if(!estacion_id) {
                    console.warn(`estacion_id not found for id_reservatorio '${record.id_reservatorio}`)
                } else {
                    if(filter_.estacion_id && filter_.estacion_id.indexOf(estacion_id) < 0 ) {
                        continue
                    }
                    for(const variable of this.config.var_map) {
                        series_id = this.getSeriesId(estacion_id, variable.var_id)
                        observaciones.push(Client.parseDadosHidrologicosRecord(record, variable.field_name, series_id))
                    }
                }
            }
        }

        return observaciones
    } 
}
