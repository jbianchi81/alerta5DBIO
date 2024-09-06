import { AbstractAccessorEngine, AccessorEngine, ObservacionesFilter, ObservacionesFilterWithArrays, SeriesFilter, SitesFilter, SitesFilterWithArrays, SeriesFilterWithArrays } from './abstract_accessor_engine'
import { Database, RowData } from "duckdb-async"
import { fetch, filterSites } from '../accessor_utils'
import { Estacion, Observacion, Procedimiento, Serie, SerieOnlyIds, Unidades, Variable } from '../a5_types'
import {estacion as crud_estacion, var as crud_var, procedimiento as crud_proc, unidades as crud_unidades, serie as crud_serie, serie} from '../CRUD'

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

type ReservatorioRecord = {
    nom_reservatorio: string,
    tip_reservatorio: string,
    cod_resplanejamento: number,
    cod_posto: number,
    nom_usina: string,
    ceg: string,
    id_subsistema: string,
    nom_subsistema: string,
    nom_bacia: string,
    nom_rio: string,
    nom_ree: string,
    dat_entrada: string,
    val_cotamaxima: string,
    val_cotaminima: string,
    val_volmax: string,
    val_volmin: string,
    val_volutiltot: string,
    val_produtibilidadeespecifica: number,
    val_produtividade65volutil: number,
    val_tipoperda: string,
    val_perda: number,
    val_latitude: number,
    val_longitude: number,
    id_reservatorio: string
}

type Config = {
    url : string,
    sites_file : string,
    file_pattern : string,
    output_file : string,
    sites_output_file : string,
    sites_map : Array<SiteMap>,
    var_map : Array<VariableMap>,
    series_map : Array<SerieMap>,
    unit_map : Array<UnitMap>,
    tabla : string,
    pais : string,
    propietario : string,
    proc_id : number
}

type SiteMap = {
    id_reservatorio : string,
    estacion_id : number
}

interface LoadedSiteMap extends SiteMap {
    estacion : Estacion
}

type VariableMap = {
    field_name: string,
    var_id: number
}

interface LoadedVariableMap extends VariableMap {
    variable : Variable
}

type SerieMap = {
    estacion_id : number,
    var_id : number,
    series_id : number
}

interface LoadedSerieMap extends SerieMap {
    serie : Serie
}

type UnitMap = {
    var_id : number,
    unit_id : number
}

interface LoadedUnitMap extends UnitMap {
    unidades : Unidades 
}


export class Client extends AbstractAccessorEngine implements AccessorEngine {

    static _get_is_multiseries : boolean = true

    config : Config

    constructor(config : Object = {}) {
        super(config)
        this.setConfig(config)
    }

    default_config : Config = {
        url : "https://ons-aws-prod-opendata.s3.amazonaws.com/",
        sites_file: "dataset/reservatorio/RESERVATORIOS.parquet",
        file_pattern: "dataset/dados_hidrologicos_di/DADOS_HIDROLOGICOS_RES_%YYYY%.parquet",
        output_file: "/tmp/dados_ons.parquet",
        sites_output_file: "/tmp/reservatorios_ons.parquet",
        sites_map: [],
        var_map: [
            { field_name: "val_volumeutilcon", var_id: 26},
            { field_name: "val_vazaoafluente", var_id: 22},
            { field_name: "val_vazaovertida", var_id: 24},
            { field_name: "val_vazaodefluente", var_id: 23},
            { field_name: "val_vazaotransferida", var_id: 25},
            { field_name: "val_vazaoturbinada", var_id: 92},
            { field_name: "val_nivelmontante", var_id: 93},
            { field_name: "val_niveljusante", var_id: 94}
        ],
        series_map: [],
        unit_map: [
            {
                var_id: 26,
                unit_id: 15
            },
            {
                var_id: 22,
                unit_id: 10
            },
            {
                var_id: 24,
                unit_id: 10
            },
            {
                var_id: 23,
                unit_id: 10
            },
            {
                var_id: 25,
                unit_id: 10
            },
            {
                var_id: 92,
                unit_id: 10
            },
            {
                var_id: 93,
                unit_id: 11
            },
            {
                var_id: 94,
                unit_id: 11
            }
        ],
        tabla: "dados_ons",
        pais: "Brasil",
        propietario: "ONS",
        proc_id: 1
    }

    procedimiento : Procedimiento

    var_map : Array<LoadedVariableMap> = []

    unit_map : Array<LoadedUnitMap> = []

    sites_map : Array<LoadedSiteMap> = []

    series_map : Array<LoadedSerieMap> = []

    static async readParquetFile(filename : string, limit : number = 1000000, offset : number = 0 , output : string|undefined = undefined) : Promise<Array<Object>> {
        const db : Database = await Database.create(":memory:");
        const rows : Array<RowData> = await db.all(`SELECT * FROM READ_PARQUET('${filename}') LIMIT ${limit} OFFSET ${offset}`)
        // console.log(rows);
        // return rows.map(r => r as DadosHidrologicosRecord)
        if(output) {
            await db.all(`COPY (SELECT * FROM READ_PARQUET('${filename}') LIMIT ${limit} OFFSET ${offset}) TO '${output}' (HEADER, DELIMITER ',')`)
        }
        return rows
    }

    static parseDadosHidrologicosRecord(record : DadosHidrologicosRecord, field : string, series_id ? : number) : Observacion {
        // assumes daily timestep
        var start_date = new Date(record.din_instante.getUTCFullYear(),record.din_instante.getUTCMonth(),record.din_instante.getUTCDate())
        var end_date = new Date(start_date)
        end_date.setUTCDate(end_date.getUTCDate() + 1)
        return {
            timestart: start_date,
            timeend: end_date, 
            valor: record[field],
            series_id: series_id
        } as Observacion
    }

    getEstacionId(id_reservatorio : string) : number|undefined {
        for(const estacion of this.sites_map) {
            if( estacion.id_reservatorio == id_reservatorio ) {
                return estacion.estacion_id
            }
        }
        return
    }

    getSeriesId(estacion_id : number, var_id : number) : number|undefined {
        for(const serie of this.series_map) {
            if( serie.estacion_id == estacion_id && serie.var_id == var_id ) {
                return serie.series_id
            }
        }
        console.warn("Series id for estacion_id=" + estacion_id + ", var id=" + var_id + " not found in series_map. Please run updateSeries")
        return
    }

    static setFilterValuesToArray(filter : Object) : Object {
        const filter_ = Object.assign({},filter)
        for(const key of ["series_id", "estacion_id", "var_id", "id_externo"]) {
            if(filter_[key] != undefined) {
                if(!Array.isArray(filter_[key])) {
                    filter_[key] = [filter_[key]]
                }
            } else {
                filter_[key] = []
            }
        }
        return filter_
    }

    getSerieById(series_id : number) : Serie {
        for(const serie_map of this.series_map) {
            if(serie_map.series_id == series_id) {
                return serie_map.serie
            }
        }
        throw(new Error("Series id " + series_id + " not found in series_map"))
    }


    async get(
        filter : ObservacionesFilter,
        options : {
            return_series ? : boolean
        } = {}) : Promise<Array<Observacion>|Array<Serie>> {
        if(!filter || !filter.timestart || !filter.timeend) {
            throw("Missing timestart and/or timeend")
        }
        if(!this.sites_map.length) {
            await this.loadSitesMap()
        }
        if(!this.series_map.length) {
            await this.loadSeriesMap()
        }
        const filter_ = Client.setFilterValuesToArray(filter) as ObservacionesFilterWithArrays
        const observaciones : Array<Observacion> = [] 
        for(var year = filter_.timestart.getUTCFullYear(); year <= filter_.timeend.getUTCFullYear(); year++) {
            const filepath = this.config.file_pattern.replace("%YYYY%", year.toString())
            const url = `${this.config.url}${filepath}`
            await fetch(url, undefined, this.config.output_file, () => null)
            const records = (await Client.readParquetFile(this.config.output_file)).map(r => r as DadosHidrologicosRecord)
            for(var record of records) {
                record = record as DadosHidrologicosRecord
                const record_timestamp = new Date(record.din_instante.getUTCFullYear(),record.din_instante.getUTCMonth(),record.din_instante.getUTCDate())
                if(record_timestamp < filter_.timestart || record_timestamp > filter_.timeend) {
                    continue
                }
                // filter by id_externo
                if(filter_.id_externo.length && filter_.id_externo.indexOf(record.id_reservatorio) < 0) {
                    continue
                }
                const estacion_id = this.getEstacionId(record.id_reservatorio)
                var series_id : number|undefined
                if(!estacion_id) {
                    // console.warn(`estacion_id not found for id_reservatorio ${record.id_reservatorio}`)
                    continue
                } else {
                    // filter by estacion_id
                    if(filter_.estacion_id.length && filter_.estacion_id.indexOf(estacion_id) < 0 ) {
                        continue
                    }
                    for(const variable of this.config.var_map) {
                        //filter by var_id
                        if(filter_.var_id.length && filter_.var_id.indexOf(variable.var_id) < 0 ) {
                            continue
                        }
                        // filter out nulls
                        if(record[variable.field_name] == undefined || parseFloat(record[variable.field_name]).toString() == 'NaN') {
                            continue
                        }
                        // foreach var, find series_id and push obs into array
                        series_id = this.getSeriesId(estacion_id, variable.var_id)
                        observaciones.push(Client.parseDadosHidrologicosRecord(record, variable.field_name, series_id))
                    }
                }
            }
        }
        if(options.return_series) {
            // classify observaciones into series. Warning: observaciones with missing series_id will be ignored
            const series = {}
            for(const observacion of observaciones) {
                if(!observacion.series_id) {
                    continue
                }
                if(series[observacion.series_id]) {
                    series[observacion.series_id].observaciones.push(observacion)
                } else {
                    const serie = this.getSerieById(observacion.series_id)
                    serie.observaciones = [observacion]
                    series[observacion.series_id] = serie
                }
            }
            return Object.keys(series).map(series_id=> {
                return series[series_id]
            })
        }
        return observaciones
    }

    async update(
        filter : ObservacionesFilter,
        options : {
            return_series ? : boolean
        } = {}) : Promise<Array<Observacion>|Array<Serie>> {
        const series = await this.get(filter, {...options, return_series: true})
        const updated : Array<Serie> = []
        for(var serie of series) {
            const c_serie = new crud_serie(serie)
            await c_serie.createObservaciones()
            updated.push(c_serie as Serie)
        } 
        return updated
    }
    
    parseReservatorioRecord(record : ReservatorioRecord, estacion_id ? : number, url ? : string) : Estacion {
        if(!record.nom_reservatorio) {
            throw(new Error("Invalid site: missing name (nom_reservatorio"))
        }
        if(!record.id_reservatorio) {
            throw(new Error("Invalid site: missing id (id_reservatorio"))
        }
        if(!record.val_latitude || !record.val_longitude) {
            throw(new Error("Invalid site: missing latitude or longitude (val_latitude, val_longitude)"))
        }
        return {
            id: estacion_id,
            nombre: record.nom_reservatorio,
            id_externo: record.id_reservatorio,
            tabla: this.config.tabla,
            geom: {
                type: "Point",
                coordinates: [
                    record.val_longitude,
                    record.val_latitude
                ]
            },
            pais: this.config.pais,
            rio: record.nom_rio,
            has_obs: true,
            tipo: "E",
            automatica: false,
            habilitar: true,
            propietario: this.config.propietario,
            abreviatura: record.id_reservatorio,
            URL: url,
            real: true,
            public: true
        }

    }

    async getSites (filter : SitesFilter) : Promise<Array<Estacion>> {
        await this.loadSitesMap()
        const filter_ = Client.setFilterValuesToArray(filter) as SitesFilterWithArrays
        var estaciones : Array<Estacion> = [] 
        const url = `${this.config.url}${this.config.sites_file}`
        await fetch(url, undefined, this.config.sites_output_file, () => null)
        var records = (await Client.readParquetFile(this.config.sites_output_file)).map(r => r as ReservatorioRecord)
        if(filter_.id_externo.length) {
            records = records.filter(r => filter_.id_externo.indexOf(r.id_reservatorio) >= 0)
        }
        for(const record of records) {
            var estacion_id = this.getEstacionId(record.id_reservatorio)
            if(filter_.estacion_id.length) {
                if(!estacion_id) {
                    continue
                }
                if(filter_.estacion_id.indexOf(estacion_id) < 0 ) {
                    continue
                }
            }
            try {
                var estacion = this.parseReservatorioRecord(record, estacion_id, url)
            } catch(e) {
                console.error("parseReservatorioRecord error: " + e.toString())
                continue
            }
            estaciones.push(estacion)
        }
        if(filter_.geom) {
            estaciones = filterSites(estaciones, {geom: filter_.geom} )
        }
        return estaciones.map(e => new crud_estacion(e) as Estacion)
    }

    getUnidades(var_id : number) : Unidades {
        for(var unit of this.unit_map) {
            if(unit.var_id == var_id) {
                return unit.unidades
            }
        }
        throw(new Error("Unidades for var_id=" + var_id + " not found"))
    }

    async loadSitesMap() {
        const estaciones = await crud_estacion.read({
            tabla: this.config.tabla
        }) as Array<Estacion>
        this.sites_map = estaciones.map(estacion => {
            return {
                estacion_id: estacion.id,
                id_reservatorio: estacion.id_externo,
                estacion: estacion
            } as LoadedSiteMap
        })
    }

    async loadSeriesMap() {
        const series = await crud_serie.read({
            tabla_id: this.config.tabla
        }) as Array<Serie>
        this.series_map = series.map(serie => {
            return {
                series_id: serie.id,
                estacion_id: serie.estacion.id,
                var_id: serie.var.id,
                serie: serie
            } as LoadedSerieMap
        })
    }

    /** Loads variables defined in config.var_map from database and sets this.var_map */
    async loadVarMap() {
        const variables = await crud_var.read({
            id: this.config.var_map.map(v=>v.var_id)
        }) as Array<Variable>
        for(var mapped_var of this.config.var_map) {
            const i = variables.map(v=>v.id).indexOf(mapped_var.var_id)
            if(i < 0) {
                throw(new Error("Variable with id=" + mapped_var.var_id + " not found in database"))
            }
            this.var_map.push({
                variable: variables[i],
                ...mapped_var
            })
        }
    }

    async loadProc() {
        const proc = await crud_proc.read({
            id: this.config.proc_id
        })
        if(!proc) {
            throw(new Error("Procedimiento with id=" + this.config.proc_id + " not found in database"))
        }
        this.procedimiento = proc
    }

    /** Loads units defined in config.unit_map from database and sets this.unit_map */
    async loadUnitMap() {
        const units = await crud_unidades.read({
            id: this.config.unit_map.map(u=>u.unit_id)
        }) as Array<Unidades>
        for(var mapped_unit of this.config.unit_map) {
            const i = units.map(u=>u.id).indexOf(mapped_unit.unit_id)
            if(i < 0) {
                throw(new Error("Unidades with id=" + mapped_unit.unit_id + " not found in database"))
            }
            this.unit_map.push({
                unidades: units[i],
                ...mapped_unit
            })
        }
    }
    
    async getSeries(filter : SeriesFilter = {}) : Promise<Array<Serie>> {
        await this.loadSitesMap()
        await this.loadVarMap()
        await this.loadProc()
        await this.loadUnitMap()
        const filter_ = Client.setFilterValuesToArray(filter) as SeriesFilterWithArrays
        const estaciones = await this.getSites({
            estacion_id: filter_.estacion_id,
            id_externo: filter_.id_externo
        })
        const series : Array<Serie> = []
        for(var estacion of estaciones) {
            for(var variable of this.var_map) {
                if(filter_.var_id.length && filter_.var_id.indexOf(variable.var_id) < 0) {
                    continue
                }
                series.push({
                    tipo: "puntual",
                    estacion: estacion,
                    var: variable.variable,
                    procedimiento: this.procedimiento,
                    unidades: this.getUnidades(variable.var_id)
                })
            }
        }
        return series
    }
}

