begin;
alter table asociaciones drop constraint "asociaciones_dest_tipo_dest_series_id_key";
alter table asociaciones drop constraint "asociaciones_source_tipo_source_series_id_dest_tipo_dest_se_key";
alter table asociaciones add unique (dest_tipo, dest_series_id, cal_id);
commit;