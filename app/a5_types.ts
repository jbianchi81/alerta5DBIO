import {Geometry, Polygon} from './geometry_types' 


export type Observacion = {
    timestart : Date,
    timeend ? : Date,
    valor : number,
    series_id ? : number
}

export type Location = {
    id ? : number,
    nombre : string,
    geom : Geometry,
    [x : string] : unknown
}

export interface Estacion extends Location {
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

export type SeriesDateRange = {
    timestart : Date | string,
    timeend : Date | string,
    count : number,
    data_availability ? : "N" | "S" | "H" | "C" | "NRT" | "RT"  | "H+S" | "C+S" | "NRT+S" | "RT+S"
}

export type Variable = {

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
    timeSupport : string | Interval,

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

export type Procedimiento = {

    /** id del Procedimiento */
    id : number,
    
    /** Nombre del Procedimiento */
    nombre : string,
    
    /** Nombre abreviado del Procedimiento */
    abrev : string,

    /** descripción del Procedimiento */
    descripcion : string
}

export type Unidades = {
    
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

export type Fuente = {
    
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
    var : Variable,
    procedimiento : Procedimiento,
    unidades : Unidades,
    date_range ? : SeriesDateRange,
    monthlyStats ? : MonthlyStats,
    beginTime ? : Date,
    endTime ? : Date,
    count ? : number,
    minValor ? : number,
    maxValor ? : number,
    observaciones ? : Array<Observacion>,
    pronosticos ? : Array<Pronostico>
}

export interface Serie extends SerieAbstracta {
    estacion : Estacion | Area | Escena
    fuente ? : Fuente,
}

export interface SeriePuntual extends SerieAbstracta {
    estacion : Estacion,
    fuente : undefined
}

export interface SerieAreal extends SerieAbstracta {
    estacion : Area,
    fuente : Fuente
}

export interface SerieRaster extends SerieAbstracta {
    estacion : Escena,
    fuente : Fuente
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
}