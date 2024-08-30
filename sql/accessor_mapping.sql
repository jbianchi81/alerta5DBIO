\set ON_ERROR_STOP on

BEGIN;

CREATE TABLE public."VariableName" (
    "VariableName" character varying NOT NULL,
    href character varying
);

ALTER TABLE ONLY public."VariableName"
    ADD CONSTRAINT "VariableName_pkey" PRIMARY KEY ("VariableName");

-- grant select,update,delete,insert on "VariableName" to actualiza;

INSERT INTO "VariableName" ("VariableName") select distinct("VariableName") from var where "VariableName" is not null;

alter table var add constraint var_variablename_foreign_key foreign key ("VariableName") references "VariableName" ("VariableName") on delete set null;


INSERT INTO "VariableName" ("VariableName",href) VALUES
('Amount of high clouds','http://codes.wmo.int/bufr4/b/20/053'),
('Amount of low clouds','http://codes.wmo.int/bufr4/b/20/051'),
('Amount of middle clouds','http://codes.wmo.int/bufr4/b/20/052'), 
('antecedent precipitation index', NULL),
('Barometric pressure','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/216'),
('Cloud cover','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/180'),
('Discharge','http://codes.wmo.int/wmdr/ObservedVariableTerrestrial/171'),
('Evapotranspiration','http://codes.wmo.int/bufr4/b/13/031'),
('flood guidance','http://codes.wmo.int/grib2/codeflag/4.2/_1-0-0'),
('Flood magnitude',NULL),
('Gage height','http://codes.wmo.int/wmdr/ObservedVariableTerrestrial/12252'),
('Global Radiation','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/573'),
('Number of days with precipitation equal to or more than 1 mm','http://codes.wmo.int/bufr4/b/04/053'),
('Precipitation','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/210'),
('Relative humidity','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/12249'),
('Reservoir inflow',NULL),
('Reservoir outflow',NULL),
('Reservoir spilled',NULL),
('Reservoir storage','http://codes.wmo.int/wmdr/ObservedVariableTerrestrial/12279'),
('Sea-level pressure','http://codes.wmo.int/bufr4/b/10/051'),
('Snow depth','http://codes.wmo.int/wmdr/ObservedVariableTerrestrial/629'),
('Sunshine duration','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/270'),
('Temperature','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/12166'),
('Temperature, dew point','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/225'),
('Tidal stage','http://codes.wmo.int/bufr4/b/22/038'),
('Transfered discharge',NULL),
('Visibility','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/230'),
('Volumetric water content','http://codes.wmo.int/wmdr/ObservedVariableTerrestrial/12277'),
('Water extent','http://codes.wmo.int/wmdr/ObservedVariableTerrestrial/243'),
('Water level','http://codes.wmo.int/wmdr/ObservedVariableTerrestrial/12252'),
('Wind direction','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/12005'),
('Wind Direction','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/12005'),
('Wind speed','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/12006'),
('Wind Speed','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/12006'),
('Wet-bulb temperature','http://codes.wmo.int/bufr4/b/12/002'),
('Evaporation','http://codes.wmo.int/wmdr/ObservedVariableTerrestrial/510'),
('u-component of wind','http://codes.wmo.int/grib2/codeflag/4.2/0-2-2'),
('v-component of wind','http://codes.wmo.int/grib2/codeflag/4.2/0-2-3')
ON CONFLICT ("VariableName") DO UPDATE SET href=excluded.href; 


drop table if exists accessor_feature_of_interest;
create table accessor_feature_of_interest (
    accessor_id varchar not null references accessors(name) on delete cascade,
    feature_id varchar not null,
    name varchar,
    geometry geometry,
    result jsonb,
    estacion_id integer references estaciones(unid) on delete set null on update cascade,
    area_id integer references areas_pluvio(unid)  on delete set null on update cascade,
    escena_id integer references escenas(id)  on delete set null on update cascade,
    network_id varchar references redes(tabla_id)  on delete set null,
    primary key(accessor_id,feature_id)
);

drop table if exists accessor_observed_property;
create table accessor_observed_property (
    accessor_id varchar not null references accessors(name) on delete cascade,
    observed_property_id varchar not null,
    name varchar,
    result jsonb,
    variable_name varchar references "VariableName"("VariableName") on delete set null,
    primary key(accessor_id, observed_property_id)
);

drop table if exists accessor_unit_of_measurement;
create table accessor_unit_of_measurement (
    accessor_id varchar not null references accessors(name) on delete cascade,
    unit_of_measurement_id varchar not null,
    unit_id integer references unidades(id) on delete set null,
    primary key(accessor_id,unit_of_measurement_id)
);


drop table if exists accessor_timeseries_observation; 
create table accessor_timeseries_observation (
    accessor_id varchar not null references accessors(name) on delete cascade,
    timeseries_id varchar not null,
    result jsonb,
    series_puntual_id integer references series(id) on delete set null,
    series_areal_id integer references series_areal(id) on delete set null,
    series_rast_id integer references series_rast(id) on delete set null,
    feature_of_interest_id varchar,
    observed_property_id varchar,
    unit_of_measurement_id varchar,
    time_support interval,
    data_type varchar,
    primary key(accessor_id,timeseries_id), 
    foreign key(accessor_id, feature_of_interest_id) references accessor_feature_of_interest(accessor_id, feature_id),
    foreign key(accessor_id, observed_property_id) references accessor_observed_property(accessor_id, observed_property_id),
    foreign key(accessor_id, unit_of_measurement_id) references accessor_unit_of_measurement(accessor_id, unit_of_measurement_id),
    begin_position timestamp,
    end_position timestamp
);

drop table if exists accessor_time_value_pair;
create table accessor_time_value_pair (
    accessor_id varchar not null,
    timeseries_id varchar not null,
    timestamp timestamp not null,
    numeric_value real,
    json_value jsonb,
    raster_value raster,
    result jsonb,
    observaciones_puntual_id integer references observaciones(id) on delete set null,
    observaciones_areal_id integer references observaciones_areal(id)  on delete set null,
    observaciones_rast_id integer references observaciones_rast(id)  on delete set null,
    foreign key (accessor_id,timeseries_id) references accessor_timeseries_observation(accessor_id,timeseries_id) on delete cascade,
    primary key(accessor_id,timeseries_id,timestamp)
);

-- grant select,update,delete,insert on accessor_feature_of_interest to actualiza;
-- grant select,update,delete,insert on accessor_observed_property to actualiza;
-- grant select,update,delete,insert on accessor_timeseries_observation to actualiza;
-- grant select,update,delete,insert on accessor_time_value_pair to actualiza;
-- grant select,update,delete,insert on accessor_unit_of_measurement to actualiza;


create or replace view series_union_all as select id,'puntual' tipo,estacion_id, var_id, unit_id, proc_id, null fuentes_id from series union all select id,'areal' tipo, area_id estacion_id, var_id,proc_id,unit_id,fuentes_id from series_areal union all select id,'raster' tipo, escena_id estacion_id, var_id, proc_id, unit_id, fuentes_id from series_rast;

-- grant select on series_union_all to actualiza;

CREATE OR REPLACE VIEW series_union_all_with_names AS
WITH s_all AS (
   SELECT series.id,
      'puntual'::text AS tipo,
      series.estacion_id,
      series.var_id,
      series.proc_id,
      series.unit_id,
      NULL::integer AS fuentes_id,
      estaciones.nombre AS estacion_nombre,
      estaciones.tabla AS fuentes_nombre
      FROM series
      JOIN estaciones
         ON estaciones.unid = series.estacion_id
   UNION ALL
   SELECT series_areal.id,
      'areal'::text AS tipo,
      series_areal.area_id AS estacion_id,
      series_areal.var_id,
      series_areal.proc_id AS proc_id,
      series_areal.unit_id AS unit_id,
      series_areal.fuentes_id,
      areas_pluvio.nombre AS estacion_nombre,
      fuentes.nombre AS fuentes_nombre     
      FROM series_areal
      JOIN areas_pluvio
         ON areas_pluvio.unid = series_areal.area_id
      JOIN fuentes
         ON fuentes.id = series_areal.fuentes_id
   UNION ALL
   SELECT series_rast.id,
      'raster'::text AS tipo,
      series_rast.escena_id AS estacion_id,
      series_rast.var_id,
      series_rast.proc_id AS proc_id,
      series_rast.unit_id AS unid_id,
      series_rast.fuentes_id,
      escenas.nombre AS estacion_nombre,
      fuentes.nombre AS fuentes_nombre
      FROM series_rast
      JOIN escenas
         ON escenas.id = series_rast.escena_id
      JOIN fuentes
         ON fuentes.id = series_rast.fuentes_id
) 
SELECT
   s_all.*,
   var.var AS var_var,
   var.nombre AS var_nombre,
   var."timeSupport" AS "var_timeSupport",
   procedimiento.nombre AS proc_nombre,
   unidades.nombre AS unit_nombre,
   unidades.abrev AS unit_abrev
FROM s_all
JOIN var
   ON s_all.var_id = var.id
JOIN procedimiento
   ON s_all.proc_id = procedimiento.id
JOIN unidades
   ON s_all.unit_id = unidades.id;

--
-- PostgreSQL database dump
--


--
-- PostgreSQL database dump complete
--


COMMIT;