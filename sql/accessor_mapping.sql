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

drop table if exists accessor_feature_of_interest;
create table accessor_feature_of_interest (
    accessor_id varchar not null references accessors(name) on delete cascade,
    feature_id varchar not null,
    name varchar,
    geometry geometry,
    result jsonb,
    estacion_id integer references estaciones(unid) on delete set null,
    area_id integer references areas_pluvio(unid)  on delete set null,
    escena_id integer references escenas(id)  on delete set null,
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


--
-- PostgreSQL database dump
--


--
-- PostgreSQL database dump complete
--


COMMIT;