import {Feature, Geometry, Polygon} from './geometry_types' 
import {Pool, Client} from 'pg'
import { Position, FeatureCollection, Feature } from 'geojson'
import {Variable as variable} from 'a5base/variable'
import {BaseArray, baseModel} from 'a5base/baseModel'
import { SeriesFilter } from './accessors/accessor_utils'

export type ObservacionDict = {
    timestart : Date,
    timeend ? : Date,
    valor : number,
    series_id ? : number,
    id? : number
}

export class observacion extends baseModel {
    constructor(args: ObservacionDict)
    timestart : Date
    timeend : Date
    valor : number
    series_id ? : number
    id? : number
}

export class observaciones extends BaseArray {
    constructor(args: ObservacionDict[]|observacion[])
}

export type ObservacionRaster = {
    timestart : Date,
    timeend ? : Date,
    valor : Buffer,
    series_id ? : number
}

export type Red = {
    id : number
    nombre : string
    tabla_id : string
    public ? : boolean
    public_his_plata ? : boolean
}

export type Location = {
    id ? : number,
    nombre : string,
    geom : Geometry,
    [x : string] : unknown,
    red ? : Red
}

export interface EstacionDict extends Location {
    id_externo : string
    provincia ? : string
    pais ? : string
    rio ? : string
    has_obs ? : boolean
    tipo ? : string
    automatica ? : boolean
    habilitar ? : boolean
    propietario ? : string
    abreviatura ? : string
    URL ? : string
    localidad ? : string
    real ? : boolean
    nivel_alerta ? : number
    nivel_evacuacion ? : number
    nivel_aguas_bajas ? : number
    altitud ? : number
    public ? : boolean
    cero_ign ? : number
    ubicacion ? : string
    drainage_basin ? : Geometry
    tabla: string
}

interface EstacionFilter {
    id? : numnber[]
    unid?: number|numnber[]
    estacion_id?: number|numnber[]
    id_externo?: string|string[]
    tabla?: string|string[]
    tabla_id?: string|string[]
    red_id?: number|number[]
    nombre?: string
    distrito?: string
    pais?: string
    has_obs?: boolean
    real?: boolean
    habilitar?: boolean
    tipo?: string
    has_prono?: boolean
    rio?: string
    geom?: Geometry
    propietario?: string
    automatica?: boolean
    ubicacion?: string
    localidad?: string
    tipo_2?: string
    abrev?: string
    limit?: number
    offset?: number
}

interface ReadOptions {
    pagination?: boolean
    get_drainage_basin?: boolean
}

interface CreateOptions {
    no_update?: boolean
    no_update_id?: boolean
}

export class estacion extends baseModel{
    constructor(args : EstacionDict);
    id_externo : string
    provincia ? : string
    pais ? : string
    rio ? : string
    has_obs ? : boolean
    tipo ? : string
    automatica ? : boolean
    habilitar ? : boolean
    propietario ? : string
    abreviatura ? : string
    URL ? : string
    localidad ? : string
    real ? : boolean
    nivel_alerta ? : number
    nivel_evacuacion ? : number
    nivel_aguas_bajas ? : number
    altitud ? : number
    public ? : boolean
    cero_ign ? : number
    ubicacion ? : string
    drainage_basin ? : Geometry
    tabla: string
    id ? : number
    nombre : string
    geom : Geometry
    async getId(pool? : Pool, client? : Client) : Promise<void>
    async getEstacionId(pool? : Pool, client? : Client) : Promise<void>
    toString() : string
    toCSV() : string
    toCSVLess() : string
    toJSON() : EstacionDict
    toGeoJSON() : FeatureCollection
    static toGeoJSON() : FeatureCollection
    static fromGeoJSON(
		geojson_file : string,
		nombre_property : string = "nombre",
		id_property : string = "id",
		tabla? : string
    ) : this[]
    static parseGeoJsonFeatureEstacion(
        feature : Feature, 
        nombre_property : string="nombre", 
        id_property : string="id", 
        tabla? : string
    ) : this
    isWithinPolygon(polygon : Geometry) : boolean
    static async read(
        filter : number|{id: number}, 
        options : ReadOptions, 
        user_id? : string, 
        client? : Client
    ) : Promise<this>
    static async read(
        filter : EstacionFilter={}, 
        options : ReadOptions, 
        user_id? : string, 
        client? : Client
    ) : Promise<this[]>
    static async read(
        filter : number|{id: number}|EstacionFilter={}, 
        options : ReadOptions, 
        user_id? : string, 
        client? : Client
    ) : Promise<this|this[]>
    async create(
        options? : CreateOptions
    ) : Promise<void>
    static async create(
        data : this[],
        options : CreateOptions
    ) : Promise<this[]>
    async update(fields : Record<string, any>={}) : Promise<this>
    async updateId(
        id : number, 
        client?: Client
    ) : Promise<void>
    async delete() : Promise<this>
    static async delete(filter : EstacionFilter) : Promise<this[]>
    static async getDrainageBasins(
        filter : {estacion_id?: number|number[], area_id?: number|number[]}={},
        client? : Client
    ) : Promise<FeatureCollection>
    async getDrainageBasin(client?: Client) : Promise<void>
}

export type SeriesDateRange = {
    timestart : Date,
    timeend : Date,
    count : number,
    data_availability ? : "N" | "S" | "H" | "C" | "NRT" | "RT"  | "H+S" | "C+S" | "NRT+S" | "RT+S"
}

export type VariableDict = {

    /** unique int id */
    id : number,

    /** varchar id of max length=6 */
    var : string,

    /** Nombre de la variable */
    nombre : string,

    /** Abreviatura de la variable */
    abrev : string,

    /** tipo de la variable */
    type : string,
    
    /** tipo de dato de la variable según ODM */
    datatype : string,
    
    /** tipo de valor de la variable según ODM */
    valuetype : string,
    
    /** categoría general de la variable según ODM */
    GeneralCategory : string,
    
    /** nombre de la variable según ODM */
    VariableName : string,

    /** Medio de muestreo según ODM */
    SampleMedium : string,

    /** id de unidades por defecto */
    def_unit_id : number,
    
    /** soporte temporal de la medición */
    timeSupport : Interval,

    def_hora_corte : string | Interval
}

export type MonthlyStats = {
    tipo : "puntual" | "areal" | "raster",
    series_id : number,
    mon : number,
    count : number,
    min : number,
    max : number,
    mean : number,
    p01 : number,
    p10 : number,
    p50 : number,
    p90 : number,
    p99 : number,
    timestart : Date,
    timeend : Date
}


export interface Area extends Location {
    exutorio ? : Geometry
}

export interface Escena extends Location {
}

export type ProcedimientoDict = {

    /** id del Procedimiento */
    id : number,
    
    /** Nombre del Procedimiento */
    nombre : string,
    
    /** Nombre abreviado del Procedimiento */
    abrev : string,

    /** descripción del Procedimiento */
    descripcion : string
}

interface ProcedimientoFilter {
    id?: number[]
    nombre?: string
    abrev?: string
    descripcion?: string
}

export class procedimiento  extends baseModel {
    id: number
    nombre: string
    abrev: string
    descripcion: string
    constructor(args : ProcedimientoDict)
    async getId(pool : Pool, client : Client) : Promise<void>
    toString() : string
    toCSV() : string
    toCSVLess() : string
    toJSON() : EstacionDict
    static async read(
        filter : {id: number}, 
        options?: ReadOptions, 
        client?: Client
    ) : Promise<this>
    static async read(
        filter : ProcedimientoFilter={}, 
        options?: ReadOptions, 
        client?: Client
    ) : Promise<this[]>
    static async read(
        filter : number|{id: number}|ProcedimientoFilter={}, 
        options?: ReadOptions, 
        client?: Client
    ) : Promise<this[]|this>
}

export type UnidadesDict = {
    
    /** id de la unidades */
    id : number,

    /** Nombre de las unidades */
    nombre : string,

    /** Nombre abreviado de las unidades */
    abrev : string,

    /** ID de unidades según ODM */
    UnitsID : number,

    /** tipo de unidades según ODM */
    UnitsType : string
}

interface UnidadesFilter {
    id? : number[],
    nombre?: string
    abrev?: string
    UnitsID?: number
    UnitsType?: string
}

export class unidades extends baseModel {
    constructor(args : UnidadesDict)
    id : number
    nombre:  string
    abrev: string
    UnitsID : number
    UnitsType : string
    async getId(pool : Pool, client : Client) : Promise<void>
    toString() : string
    toCSV() : string
    toCSVLess() : string
    toJSON() : UnidadesDict
    static async read(
        filter : number|{id: number}, 
        options? : ReadOptions, 
        client? : Client
    ) : Promise<this>
    static async read(
        filter : UnidadesFilter={}, 
        options? : ReadOptions, 
        client? : Client) : Promise<this[]>
    static async read(
        filter : number|{id: number}|UnidadesFilter={}, 
        options? : ReadOptions, 
        client? : Client) : Promise<this|this[]>
}

interface TableConstraintDict {
    table_name: string
    constraint_name: string
    constraint_type: string
    column_names: string[]	
}

export class tableConstraint {
	constructor(args : TableConstraintDict) 
    table_name: string
    constraint_name: string
    constraint_type: string
    column_names: string[]
	check(column_names : string[]) : bool
}


export type FuenteDict = {
    
    /** id de la fuente */
    id : number,

    /** nombre de la fuente */
    nombre : string,


    data_table : string,
    
    data_column : string,
    
    /** tipo de la fuente */
    tipo : string,
    
    /** id de procedimiento por defecto de la fuente */
    def_proc_id : number,
    
    /** intervalo temporal por defecto de la fuente */
    def_dt : string | Interval,
    
    /** hora de corte por defecto de la fuente */
    hora_corte : string | Interval,
    
    /** id de unidades por defecto de la fuente */
    def_unit_id : number,
    
    /** id de variable por defecto de la fuente */
    def_var_id : number,
    
    fd_column : string,
    
    mad_table : string,
    
    /** factor de escala por defecto de la fuente */
    scale_factor : number,
    
    /** offset por defecto de la fuente */
    data_offset : number,

    /** altura de pixel por defecto de la fuente */
    def_pixel_height :  number,
    
    /** ancho de pixel por defecto de la fuente */
    def_pixel_width : number,

    /** tipo de dato del pixel */
    def_pixeltype : string,

    /** código SRID de georeferenciación por defecto de la fuente */
    def_srid : number,

    /** extensión espacial de la fuente */
    def_extent : Geometry,

    date_column : string,
    
    /** descripción de la fuente */
    abstract : string,

    /** ubicación del origen de la fuente */
    source : string,

    public : boolean
}

interface FuenteFilter {
    id? : number|number[]
    /** nombre de la fuente */
    nombre? : string
    data_table? : string    
    data_column? : string    
    /** tipo de la fuente */
    tipo? : string    
    /** id de procedimiento por defecto de la fuente */
    def_proc_id? : number    
    /** intervalo temporal por defecto de la fuente */
    def_dt? : string | Interval    
    /** hora de corte por defecto de la fuente */
    hora_corte? : string | Interval    
    /** id de unidades por defecto de la fuente */
    def_unit_id? : number    
    /** id de variable por defecto de la fuente */
    def_var_id? : number    
    fd_column? : string    
    mad_table? : string
    /** factor de escala por defecto de la fuente */
    scale_factor? : number    
    /** offset por defecto de la fuente */
    data_offset? : number
    /** altura de pixel por defecto de la fuente */
    def_pixel_height? :  number    
    /** ancho de pixel por defecto de la fuente */
    def_pixel_width? : number
    /** tipo de dato del pixel */
    def_pixeltype? : string
    /** código SRID de georeferenciación por defecto de la fuente */
    def_srid? : number
    /** extensión espacial de la fuente */
    def_extent? : Geometry
    date_column? : string    
    /** descripción de la fuente */
    abstract? : string
    /** ubicación del origen de la fuente */
    source? : string
    public? : boolean
}

export class fuente extends baseModel {
    constructor(args : FuenteDict)
    id : number
    /** nombre de la fuente */
    nombre : string
    data_table : string    
    data_column : string    
    /** tipo de la fuente */
    tipo : string    
    /** id de procedimiento por defecto de la fuente */
    def_proc_id : number    
    /** intervalo temporal por defecto de la fuente */
    def_dt : string | Interval    
    /** hora de corte por defecto de la fuente */
    hora_corte : string | Interval    
    /** id de unidades por defecto de la fuente */
    def_unit_id : number    
    /** id de variable por defecto de la fuente */
    def_var_id : number    
    fd_column : string    
    mad_table : string
    /** factor de escala por defecto de la fuente */
    scale_factor : number    
    /** offset por defecto de la fuente */
    data_offset : number
    /** altura de pixel por defecto de la fuente */
    def_pixel_height :  number    
    /** ancho de pixel por defecto de la fuente */
    def_pixel_width : number
    /** tipo de dato del pixel */
    def_pixeltype : string
    /** código SRID de georeferenciación por defecto de la fuente */
    def_srid : number
    /** extensión espacial de la fuente */
    def_extent : Geometry
    date_column : string    
    /** descripción de la fuente */
    abstract : string
    /** ubicación del origen de la fuente */
    source : string
    public : boolean
    async getId(pool : Pool, client : Client) : Promise<void>
    getConstraint(column_names : string[]) : tableConstraint
    hasConstraint(column_names : string[]) : boolean
    hasDateConstraint() : boolean
    hasDateFdConstraint() : boolean
    toString() : string
    toCSV() : string
    toCSVLess() : string
    toJSON() : FuenteDict
    static async read(
        filter : number|{id: number},
        options? : {},
        client?: Client) : Promise<this>
    static async read(
        filter : FuenteFilter={},
        options? : {},
        client?: Client) : Promise<this[]>
    static async read(
        filter : number|{id: number}|FuenteFilter={},
        options? : {},
        client?: Client) : Promise<this|this[]>
    static async create(fuentes : this[]=[]) : Promise<this[]>
    async update(
        params : Record<string, any>={}, 
        client? : Client
    ) : Promise<this>
    async create(
        options : {create_cube_table?: boolean}={}, 
        client? : Client
    ) : Promise<this|void>
    async delete(
        options : {drop_cube_table: boolean}={}, 
        client? : Client
    ) : Promise<this>
    static async delete(
        filter : FuenteFilter={},
        options : {drop_cube_table: boolean}={}, 
        client? : Client
    ) : Promise<this[]>
    async checkTableExists(
        table_schema : string='public', 
        client?: Client
    ) : Promise<boolean>
    async createCubeTable(
        schema_name : string="public", 
        client? : Client
    ) : Promise<void>
    static getUserAccessClause(
        user_id : string, 
        source_id_field : string="fuentes.id"
    ) : string    
}

export type Pronostico = {
    
    /** fecha-hora inicial del pronóstico */
    timestart : Date,
    
    /** fecha-hora final del pronóstico */
    timeend ? : Date,

    /** valor del pronóstico */
    valor : number

    /** calificador opcional para diferenciar subseries */
    qualifier ? : string

}

export type SerieAbstracta = {
    tipo : "puntual" | "areal" | "raster",
    id ? : number, 
    var : VariableDict,
    procedimiento : ProcedimientoDict,
    unidades : UnidadesDict,
    date_range ? : SeriesDateRange,
    monthlyStats ? : MonthlyStats,
    beginTime ? : Date,
    endTime ? : Date,
    count ? : number,
    minValor ? : number,
    maxValor ? : number,
    observaciones ? : Array<ObservacionDict>|Array<ObservacionRaster>,
    pronosticos ? : Array<Pronostico>
}

export interface SerieDict extends SerieAbstracta {
    estacion : EstacionDict | Area | Escena
    fuente ? : FuenteDict,
}

interface MnemosRecord {
    codigo_de_estacion : string
    codigo_de_variable : string
    dia : number
    mes : number
    anio : number
    hora : number
    minuto : number
    valor : number|number[]
}

export interface PercentilDict {
    tipo : "puntual"|"areal"|"raster"
    series_id : number
    percentile : number
    valor : number
    timestart : Date
    timeend : Date
    count : number
}

export class percentil extends baseModel {
    constructor(args : PercentilDict)
    tipo : "puntual"|"areal"|"raster"
    series_id : number
    percentile : number
    valor : number
    timestart : Date
    timeend : Date
    count : number
    toString() : string
    toCSV(tipo? : "puntual"|"areal"|"raster",series_id?: number) : string
    toCSVless(tipo? : "puntual"|"areal"|"raster",series_id?: number) : string
} 

export interface PercentilesDict {
    tipo : "puntual"|"areal"|"raster"
    series_id : number
    percentiles : PercentilDict[]
}

export class percentiles extends baseModel {
    constructor(args : PercentilesDict)
    tipo : "puntual"|"areal"|"raster"
    series_id : number
    percentiles : percentil[]
    toString() : string
    toCSV() : string
    toCSVless() : string
}


export class serie extends baseModel {
    constructor(args: SerieDict)
    id: number
    tipo: "puntual"|"areal"|"raster"
    estacion: estacion
    estacion_id: number
    var: variable
    var_id: number
    procedimiento: procedimiento
    proc_id: number
    unidades: unidades
    unit_id: number
    fuente?: fuente
    fuentes_id?: number
    beginTime? : Date
    endTime?: Date
    count?: number
	minValor?: number
	maxValor?: number
    date_range?: {
        timestart: Date,
        timeend: Date,
        count: number
    }
    observaciones?: observaciones
    toJSON() : SerieDict
    toJSONless() : {
        tipo: "puntual"|"areal"|"raster", 
        id: number, 
        estacion_id: number, 
        var_id: number, 
        proc_id: number, 
        unit_id: number,
        fuentes_id?: number
    }
    toString() : string
    toKVP(options : {
        delimiter?: string,
        no_comment?: boolean,
        single_line?: boolean
    }={}) : string
    toGmd() : string
    getCSVHeader(options={
        delimiter?: string,
        print_observaciones?: boolean
    }) : string[]
    toCSV(options={
        delimiter?: string,
        print_observaciones?: boolean
    }) : string|string[]
    toCSVcat(options={
        delimiter?: string
    }) : string
    toCSVless() : string
    toMnemos() : string
    arr2csv(arr : MnemosRecord[]) : string
    async getId(
        default_to_next : bool=true, 
        pool?: Pool, 
        client?: Client
    ) : Promise<void|number>
    async getStats(client? : Client) : Promise<this>
    getDateRange(
        pool? : Pool, 
        client?: Client
    ) : Promise<{
            timestart: undefined,
            timeend: undefined,
            count: 0
        } | true>
    tipo_guess() : void
    idIntoObs() : void
    getWeibullPercentiles(
        reference_timestart : Date=new Date("1991-01-01"),
        reference_timeend : Date=new Date("2021-01-01"),
        percentage_complete_threshold : number=60,
        as_array : boolean=true
    ) : void
    set(changes : Record<string, any>={}) : void
    getDateRangeTable(options={guardadas?: boolean}) : string
    static getDateRangeTable(
        tipo : "puntual"|"areal"|"raster"="puntual",
        options: { guardadas?: boolean}={}
    ) : string
    static async refreshDateRange(
        tipo : "puntual"|"areal"|"raster"="puntual",
        options: { guardadas?: boolean}={}, 
        client?: Client
    ) : Promise<QueryResult<any>>
    async refreshDateRange(
        options: { guardadas?: boolean}={}, 
        client?: Client
    ) : Promise<QueryResult<any>>
    getSeriesTable() : string
    static getSeriesTable(tipo : "puntual"|"areal"|"raster") : string
    static getFeatureIdColumn(tipo : "puntual"|"areal"|"raster") : string
    async create(options : {
        refresh_date_range?: boolean
        series_metadata?: boolean
    }) : Promise<this>
    static async refreshJsonView(client?: Client) : Promise<void>
    static async create(
        series : this[],
        options : {
            refresh_date_range?: boolean,
            series_metadata?: boolean,
            all?: boolean,
            upsert_estacion?: boolean,
            generate_id?: boolean
        }={},
        client?: Client
    ) : Promise<this[]>
    static async read(
        filter : number|{id: number}|SeriesFilter={},
        options: {
            no_metadata?: boolean,
            pagination?: boolean,
            format?: string,
            print_observaciones?: boolean,
            guardadas?: boolean,
            no_data?: boolean,
            getWeibullPercentiles?: boolean,
            getStats?: boolean,
            getMonthlyStats?: boolean,
            getPercentiles?: boolean
        }={}, 
        client?: Client
    ) : Promise<this|this[]>
    // static async getPercentiles(
    //     tipo : "puntual"|"areal"|"raster"="puntual",
    //     series_id? : number|number[],
    //     percentiles? : number|number[],
    //     isPublic?: boolean, 
    //     client?: Client
    // ) : Promise<percentiles[]>
    async update(changes: Record<string, any>={}, client?: Client) : Promise<void|this>
    updateQuery(changes: Record<string, any>={}={}) : string
    static async delete(
        filter : SeriesFilter={},
        options:{}={},
        client?: Client
    ) : Promise<this[]>
    async delete(client?: Client) : Promise<void|this>
    aggregateMonthly(
        timestart : Date,
        timeend : Date,
        agg_function : string="acum",
        precision : number=2,
        time_support?: Interval|string,
        expression?: string,
        min_obs : number=15,
        inst? : boolean,
        date_offset : number=0,
        utc : boolean=false
    ) : void | {
        timestart: Date,
        timeend: Date
    }[]
    aggregateTimeStep(
        timestart : Date,
        timeend : Date,
        time_step : Interval|string,
        agg_function : string="acum",
        precision : number=2,
        time_support? : Interval|string,
        expression?: string,
        min_obs :number=15,
        inst?: boolean,
        dest_series_id?: number,
        offset?: Interval|string,
        utc?:boolean
    ) : void | observaciones
    async createObservaciones(
        client?: Client, 
        options={no_returning?: boolean}
    ) : Promise<observaciones>
    createObservacionesQuery(
        options : {
            no_update?: boolean,
            update_obs_metadata?: boolean
        }={}
    ) : string
    setObservaciones(observaciones : observaciones|ObservacionDict[]) : void
    async getObservaciones(
        timestart : Date,
        timeend : Date,
        inline : boolean=true
    ) : Promise<void|observaciones>
    filterSerie(filter : SeriesFilter={}) : boolean
    static async getDerivedSerie(
		tipo : "puntual"|"areal"|"raster"="puntual",
		series_id : number,
		timestart : Date,
		timeend : Date,
		method : string="expression",
		expression : string="${valor_0}",
		join_type : string="left",
		output_series_id? : number = undefined,
		create_observaciones : boolean = false,
		unit_id? : number = undefined
	) : Promise<this>

}

export interface SeriePuntual extends SerieAbstracta {
    estacion : EstacionDict,
    fuente : undefined,
    observaciones ? : Array<ObservacionDict>
}

export interface SerieAreal extends SerieAbstracta {
    estacion : Area,
    fuente : FuenteDict,
    observaciones ? : Array<ObservacionDict>
}

export interface SerieRaster extends SerieAbstracta {
    estacion : Escena,
    fuente : FuenteDict,
    observaciones ? : Array<ObservacionRaster>
}

export interface SerieOnlyIds {
    tipo: "puntual" | "areal" | "raster"
    id ? : number
    estacion_id : number
    var_id : number
    unit_id : number
    proc_id : number
    fuentes_id ? : number
}

export interface Interval {
    milliseconds ? : number
    seconds ? : number
    minutes ? : number
    hours ? : number
    days ? : number
    months ? : number
    years ? : number
    toEpoch() : number
    getKey() : string
    getValue() : number
}

export type SerieProno = {
    series_table: "series" | "series_areal" | "series_rast"
    series_id : number
    cor_id?: number
    pronosticos: Pronostico[]
}

export type Corrida = {
    forecast_date : Date
    series: SerieProno[]
    cal_id: number

}
