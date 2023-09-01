begin;
drop type observacion_num; 
create type observacion_num as (id integer, series_id integer, timestart timestamptz, timeend timestamptz, timeupdate timestamptz, unit_id integer, nombre varchar, descripcion varchar, valor double precision);
commit;