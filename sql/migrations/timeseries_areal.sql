select 
    series_areal.id series_id,
    areas_pluvio.unid area_id, 
    to_char(min(timestart)::timestamp at time zone 'ART', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') timestart,
    to_char(max(timeend)::timestamp at time zone 'ART', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') timeend,
    areas_pluvio.nombre,
    round(areas_pluvio.area::numeric/1000000) area_km2,
    var.id var_id,
    var.nombre var_nombre,
    var.abrev var_abrev,
    unidades.id unit_id,
    unidades.nombre unit_nombre,
    procedimiento.id proc_id,
    procedimiento.nombre proc_nombre,
    fuentes.id fuentes_id,
    fuentes.nombre fuentes_nombre,
    fuentes.abstract fuentes_resumen,
    fuentes.source fuentes_origen,
    geom,
    round(sum(valor)::numeric,%roundTo%) suma,
    round(min(valor)::numeric,%roundTo%) minimo,
    round(max(valor)::numeric,%roundTo%) maximo,
    count(valor) n_obs,
    round(avg(valor)::numeric,%roundTo%) media,
    sum(timeend - timestart)::text as t_sum,
    round(%function%(valor)::numeric,%roundTo%)::numeric as valor_agg,
    '%function%'::text as agg_function,
    json_object(array_agg(timestart::text order by timestart), array_agg(round(valor::numeric, %roundTo%)::text order by timestart))::text AS valor
  from 
         areas_pluvio,
         series_areal,
         observaciones_areal,
         valores_num_areal,
         var,
         unidades,
         procedimiento,
         fuentes
  where 
         areas_pluvio.unid=series_areal.area_id
         and
         series_areal.id=observaciones_areal.series_id
         and
         observaciones_areal.id=valores_num_areal.obs_id
         and
         fuentes.id = series_areal.fuentes_id
         and
         var.id = fuentes.def_var_id
         and
         procedimiento.id = fuentes.def_proc_id
         and
         unidades.id = fuentes.def_unit_id
         and
         series_areal.fuentes_id = %sourceId%
         and
         series_areal.unit_id = fuentes.def_unit_id
         and
         timestart>=case when '%timeStart%'='1800-01-01' then current_timestamp-'7 days'::interval else '%timeStart%'::timestamp end
         and 
         timeend<=case when '%timeEnd%'='1800-01-01' then current_timestamp+'7 days'::interval else '%timeEnd%'::timestamp end 
         and 
         areas_pluvio.activar=true 
         and 
         areas_pluvio.mostrar = true 
  group by 
        series_areal.id,
    areas_pluvio.unid, 
    areas_pluvio.nombre,
    areas_pluvio.area,
    var.id,
    var.nombre,
    var.abrev,
    unidades.id ,
    unidades.nombre,
    procedimiento.id,
    procedimiento.nombre,
    fuentes.id ,
    fuentes.nombre,
    fuentes.abstract,
    fuentes.source,
    geom
 order by 
    t_sum desc,
    area desc