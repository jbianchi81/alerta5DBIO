import {Geometry} from './geometry_types' 


export type Observacion = {
    timestart : Date,
    timeend ? : Date,
    valor : number,
    series_id ? : number
}

export type Estacion = {
    id ? : number,
    nombre : string,
    id_externo : string,
    geom : Geometry,
    provincia ? : string,
    pais ? : string,
    rio ? : string,
    has_obs ? : Boolean,
    tipo ? : string,
    automatica ? : Boolean,
    habilitar ? : Boolean,
    propietario ? : string,
    abreviatura ? : string,
    URL ? : string,
    localidad ? : string,
    real ? : Boolean,
    nivel_alerta ? : number,
    nivel_evacuacion ? : number,
    nivel_aguas_bajas ? : number,
    altitud ? : number,
    public ? : Boolean,
    cero_ign ? : number,
    ubicacion ? : string,
    drainage_basin ? : Geometry,
    tabla: string
}