BEGIN;

DROP VIEW pmad_gfs_new;
DROP VIEW ma_conae_api;
DROP VIEW apma_ecmwf;
DROP VIEW pmam_ecmwf;
DROP VIEW pmad_sqpe;
DROP VIEW flooded_ndwi37_maxanual_by_areaid;
DROP VIEW flooded_ndwi37_minanual_by_areaid;
DROP VIEW saturogramas_ndwi37;
DROP VIEW pma_cosmo_3h;
DROP VIEW pmad_cosmo;
DROP VIEW observaciones_all;
DROP VIEW pma_campo_3h;
DROP VIEW pmad_gpm_3h;
DROP VIEW compara_gfs;
DROP VIEW pma_gfs_3h;
DROP MATERIALIZED VIEW saturogramas_nrt_stats;
DROP VIEW saturogramas_nrt;
DROP VIEW sm_smops;
DROP VIEW observaciones_numarr_areal_all;
DROP VIEW observaciones_areal_all;
DROP MATERIALIZED VIEW registros_diarios_areal;
DROP VIEW pmad_gfs;
DROP VIEW pmad_eta;
DROP VIEW pmad_hidro_3h;
DROP VIEW pmad_hidro;
DROP VIEW fmad_site_d;
DROP VIEW fmad_amsr2;
DROP VIEW smad_smap;
DROP VIEW pmad_emas;
DROP VIEW pmad_gpm;
DROP VIEW pmad_eta_3h;
DROP VIEW etpd_wm;
DROP VIEW hidrosat;
DROP VIEW pmad_cpc;

alter table observaciones_areal alter column id TYPE bigint;

CREATE OR REPLACE VIEW pmad_cpc AS
SELECT series_areal.id AS series_id,
    series_areal.area_id,
    observaciones_areal.id AS obs_id,
    observaciones_areal.timestart,
    observaciones_areal.timeend,
    observaciones_areal.nombre,
    valores_num_areal.valor
   FROM series_areal,
    observaciones_areal,
    valores_num_areal
  WHERE series_areal.fuentes_id = 2 AND series_areal.proc_id = 6 AND series_areal.var_id = 1 AND series_areal.unit_id = 22 AND series_areal.id = observaciones_areal.series_id AND valores_num_areal.obs_id = observaciones_areal.id
  ORDER BY series_areal.area_id, observaciones_areal.timestart;

CREATE OR REPLACE VIEW hidrosat AS
 WITH series AS (
         SELECT t.t::date AS t
           FROM generate_series('1990-01-01'::date::timestamp with time zone, '2018-02-05'::date::timestamp with time zone, '1 day'::interval) t(t)
        )
 SELECT series.t,
    avg(pmad_cpc.valor) AS precip
   FROM series
     LEFT JOIN pmad_cpc ON pmad_cpc.timestart::date = series.t AND pmad_cpc.area_id = 248
  GROUP BY series.t
  ORDER BY series.t;

CREATE OR REPLACE VIEW etpd_wm AS
SELECT series_areal.id AS series_id,
    series_areal.area_id,
    observaciones_areal.id AS obs_id,
    observaciones_areal.timestart,
    observaciones_areal.timeend,
    observaciones_areal.nombre,
    valores_num_areal.valor
   FROM series_areal,
    observaciones_areal,
    valores_num_areal
  WHERE series_areal.fuentes_id = 3 AND series_areal.proc_id = 7 AND series_areal.var_id = 15 AND series_areal.unit_id = 22 AND series_areal.id = observaciones_areal.series_id AND valores_num_areal.obs_id = observaciones_areal.id
  ORDER BY series_areal.area_id, observaciones_areal.timestart;

CREATE OR REPLACE VIEW pmad_eta_3h AS
SELECT series_areal.id AS series_id,
    series_areal.area_id,
    observaciones_areal.id AS obs_id,
    observaciones_areal.timestart,
    observaciones_areal.timeend,
    observaciones_areal.nombre,
    valores_num_areal.valor
   FROM series_areal,
    observaciones_areal,
    valores_num_areal
  WHERE series_areal.fuentes_id = 4 AND series_areal.proc_id = 4 AND series_areal.var_id = 1 AND series_areal.unit_id = 22 AND series_areal.id = observaciones_areal.series_id AND valores_num_areal.obs_id = observaciones_areal.id
  ORDER BY series_areal.area_id, observaciones_areal.timestart;

CREATE OR REPLACE VIEW pmad_gpm AS
SELECT series_areal.id AS series_id,
    series_areal.area_id,
    observaciones_areal.id AS obs_id,
    observaciones_areal.timestart,
    observaciones_areal.timeend,
    observaciones_areal.nombre,
    valores_num_areal.valor
   FROM series_areal,
    observaciones_areal,
    valores_num_areal
  WHERE series_areal.fuentes_id = 6 AND series_areal.proc_id = 5 AND series_areal.var_id = 1 AND series_areal.unit_id = 22 AND series_areal.id = observaciones_areal.series_id AND valores_num_areal.obs_id = observaciones_areal.id
  ORDER BY series_areal.area_id, observaciones_areal.timestart;

CREATE OR REPLACE VIEW pmad_emas AS
 SELECT series_areal.id AS series_id,
    series_areal.area_id,
    observaciones_areal.id AS obs_id,
    observaciones_areal.timestart,
    observaciones_areal.timeend,
    observaciones_areal.nombre,
    valores_num_areal.valor
   FROM series_areal,
    observaciones_areal,
    valores_num_areal
  WHERE series_areal.fuentes_id = 7 AND series_areal.proc_id = 3 AND series_areal.var_id = 1 AND series_areal.unit_id = 22 AND series_areal.id = observaciones_areal.series_id AND valores_num_areal.obs_id = observaciones_areal.id
  ORDER BY series_areal.area_id, observaciones_areal.timestart;

CREATE OR REPLACE VIEW smad_smap AS
SELECT series_areal.id AS series_id,
    series_areal.area_id,
    observaciones_areal.id AS obs_id,
    observaciones_areal.timestart,
    observaciones_areal.timeend,
    observaciones_areal.nombre,
    valores_num_areal.valor
   FROM series_areal,
    observaciones_areal,
    valores_num_areal
  WHERE series_areal.fuentes_id = 8 AND series_areal.proc_id = 5 AND series_areal.var_id = 20 AND series_areal.unit_id = 23 AND series_areal.id = observaciones_areal.series_id AND valores_num_areal.obs_id = observaciones_areal.id
  ORDER BY series_areal.area_id, observaciones_areal.timestart;

CREATE OR REPLACE VIEW fmad_amsr2 AS
SELECT series_areal.id AS series_id,
    series_areal.area_id,
    observaciones_areal.id AS obs_id,
    observaciones_areal.timestart,
    observaciones_areal.timeend,
    observaciones_areal.nombre,
    valores_num_areal.valor
   FROM series_areal,
    observaciones_areal,
    valores_num_areal
  WHERE series_areal.fuentes_id = 9 AND series_areal.proc_id = 5 AND series_areal.var_id = 21 AND series_areal.unit_id = 24 AND series_areal.id = observaciones_areal.series_id AND valores_num_areal.obs_id = observaciones_areal.id
  ORDER BY series_areal.area_id, observaciones_areal.timestart;

CREATE OR REPLACE VIEW fmad_site_d AS
SELECT fmad_amsr2.area_id AS unid,
    fmad_amsr2.timestart::date AS "time",
    fmad_amsr2.valor * 3.54::double precision * (10::double precision ^ '-6'::integer::double precision) AS anegado
   FROM fmad_amsr2
  WHERE fmad_amsr2.area_id = 142 AND fmad_amsr2.valor >= 0::double precision
  ORDER BY (fmad_amsr2.timestart::date);

CREATE OR REPLACE VIEW pmad_hidro AS
SELECT series_areal.id AS series_id,
    series_areal.area_id,
    observaciones_areal.id AS obs_id,
    observaciones_areal.timestart,
    observaciones_areal.timeend,
    observaciones_areal.nombre,
    valores_num_areal.valor
   FROM series_areal,
    observaciones_areal,
    valores_num_areal
  WHERE series_areal.fuentes_id = 11 AND series_areal.proc_id = 5 AND series_areal.var_id = 1 AND series_areal.unit_id = 22 AND series_areal.id = observaciones_areal.series_id AND valores_num_areal.obs_id = observaciones_areal.id
  ORDER BY series_areal.area_id, observaciones_areal.timestart;

CREATE OR REPLACE VIEW pmad_hidro_3h AS
SELECT series_areal.id AS series_id,
    series_areal.area_id,
    observaciones_areal.id AS obs_id,
    observaciones_areal.timestart,
    observaciones_areal.timeend,
    'hidroestimador_3h_mm/3h'::text AS nombre,
    round((valores_num_areal.valor / 8::double precision)::numeric, 1) AS valor
   FROM series_areal,
    observaciones_areal,
    valores_num_areal
  WHERE series_areal.fuentes_id = 12 AND series_areal.proc_id = 5 AND series_areal.var_id = 1 AND series_areal.unit_id = 22 AND series_areal.id = observaciones_areal.series_id AND valores_num_areal.obs_id = observaciones_areal.id
  ORDER BY series_areal.area_id, observaciones_areal.timestart;

CREATE OR REPLACE VIEW pmad_eta AS
 WITH pp AS (
         SELECT max(pp_eta.fecha_emision) AS fecha_emision,
            pp_eta.date
           FROM pp_eta
          GROUP BY pp_eta.date
        )
 SELECT series_areal.id AS series_id,
    series_areal.area_id,
    observaciones_areal.id AS obs_id,
    observaciones_areal.timestart,
    observaciones_areal.timeend,
    pp.fecha_emision,
    observaciones_areal.nombre,
    valores_num_areal.valor
   FROM series_areal,
    observaciones_areal,
    valores_num_areal,
    pp
  WHERE series_areal.fuentes_id = 1 AND series_areal.proc_id = 4 AND series_areal.var_id = 1 AND series_areal.unit_id = 22 AND series_areal.id = observaciones_areal.series_id AND valores_num_areal.obs_id = observaciones_areal.id AND (pp.date::timestamp without time zone + '09:00:00'::interval) = observaciones_areal.timestart
  ORDER BY series_areal.area_id, observaciones_areal.timestart;

CREATE OR REPLACE VIEW pmad_gfs AS
WITH pp AS (
         SELECT max(gfs_diario.fecha_emision) AS fecha_emision,
            gfs_diario.date
           FROM gfs_diario
          GROUP BY gfs_diario.date
        )
 SELECT series_areal.id AS series_id,
    series_areal.area_id,
    observaciones_areal.id AS obs_id,
    pp.fecha_emision,
    observaciones_areal.timestart,
    observaciones_areal.timeend,
    observaciones_areal.nombre,
    valores_num_areal.valor
   FROM series_areal,
    observaciones_areal,
    valores_num_areal,
    pp
  WHERE series_areal.fuentes_id = 5 AND series_areal.proc_id = 4 AND series_areal.var_id = 1 AND series_areal.unit_id = 22 AND series_areal.id = observaciones_areal.series_id AND valores_num_areal.obs_id = observaciones_areal.id AND pp.date = observaciones_areal.timestart::date
  ORDER BY series_areal.area_id, observaciones_areal.timestart;

CREATE MATERIALIZED VIEW registros_diarios_areal AS
WITH d AS (
         SELECT areas_pluvio.unid,
            generate_series('2000-01-01 00:00:00'::timestamp without time zone, ('now'::text::date + 15)::timestamp without time zone, '1 day'::interval) AS dd
           FROM areas_pluvio
        )
 SELECT d.unid,
    d.dd AS date,
    pmad_eta.valor AS pmad_eta,
    pmad_cpc.valor AS pmad_cpc,
    etpd_wm.valor AS etpd_wm,
    pmad_gfs.valor AS pmad_gfs,
    pmad_gpm.valor AS pmad_gpm,
    pmad_emas.valor AS pmad_emas,
    smad_smap.valor AS smad_smap,
    fmad_amsr2.valor AS fmad_amsr2,
    alturas.valor AS altura,
    caudales.valor AS caudal
   FROM d
     LEFT JOIN pmad_eta ON d.unid = pmad_eta.area_id AND d.dd = pmad_eta.timestart::date
     LEFT JOIN pmad_cpc ON d.unid = pmad_cpc.area_id AND d.dd = pmad_cpc.timestart::date
     LEFT JOIN etpd_wm ON d.unid = etpd_wm.area_id AND d.dd = etpd_wm.timestart::date
     LEFT JOIN pmad_gfs ON d.unid = pmad_gfs.area_id AND d.dd = pmad_gfs.timestart::date
     LEFT JOIN pmad_gpm ON d.unid = pmad_gpm.area_id AND d.dd = pmad_gpm.timestart::date
     LEFT JOIN pmad_emas ON d.unid = pmad_emas.area_id AND d.dd = pmad_emas.timestart::date
     LEFT JOIN smad_smap ON d.unid = smad_smap.area_id AND d.dd = smad_smap.timestart::date
     LEFT JOIN fmad_amsr2 ON d.unid = fmad_amsr2.area_id AND d.dd = fmad_amsr2.timestart::date
     LEFT JOIN ( SELECT alturas_all.unid,
            alturas_all.timestart::date AS date,
            avg(alturas_all.valor) AS valor
           FROM alturas_all
          WHERE alturas_all.timestart >= '2000-01-01 00:00:00'::timestamp without time zone
          GROUP BY alturas_all.unid, (alturas_all.timestart::date)) alturas ON d.unid = alturas.unid AND d.dd = alturas.date
     LEFT JOIN ( SELECT caudales_all.unid,
            caudales_all.timestart::date AS date,
            avg(caudales_all.valor) AS valor
           FROM caudales_all
          WHERE caudales_all.timestart >= '2000-01-01 00:00:00'::timestamp without time zone
          GROUP BY caudales_all.unid, (caudales_all.timestart::date)) caudales ON d.unid = caudales.unid AND d.dd = caudales.date
  ORDER BY d.unid, d.dd;

CREATE OR REPLACE VIEW observaciones_areal_all AS
SELECT series_areal.id AS series_id,
    series_areal.area_id,
    series_areal.var_id,
    series_areal.proc_id,
    series_areal.unit_id,
    series_areal.fuentes_id,
    observaciones_areal.id AS obs_id,
    observaciones_areal.timestart,
    observaciones_areal.timeend,
    valores_num_areal.valor
   FROM series_areal,
    observaciones_areal,
    valores_num_areal
  WHERE series_areal.id = observaciones_areal.series_id AND observaciones_areal.id = valores_num_areal.obs_id
  ORDER BY series_areal.area_id, series_areal.var_id, series_areal.proc_id, series_areal.unit_id, series_areal.fuentes_id, observaciones_areal.timestart;

CREATE OR REPLACE VIEW observaciones_numarr_areal_all AS
SELECT series_areal.id AS series_id,
    series_areal.area_id,
    series_areal.var_id,
    series_areal.proc_id,
    series_areal.unit_id,
    series_areal.fuentes_id,
    observaciones_areal.id AS obs_id,
    observaciones_areal.timestart,
    observaciones_areal.timeend,
    valores_numarr_areal.valor
   FROM series_areal,
    observaciones_areal,
    valores_numarr_areal
  WHERE series_areal.id = observaciones_areal.series_id AND observaciones_areal.id = valores_numarr_areal.obs_id
  ORDER BY series_areal.area_id, series_areal.var_id, series_areal.proc_id, series_areal.unit_id, series_areal.fuentes_id, observaciones_areal.timestart;

CREATE OR REPLACE VIEW sm_smops AS
SELECT series_areal.id AS series_id,
    series_areal.area_id,
    observaciones_areal.id AS obs_id,
    observaciones_areal.timestart,
    observaciones_areal.timeend,
    observaciones_areal.nombre,
    valores_num_areal.valor
   FROM series_areal,
    observaciones_areal,
    valores_num_areal
  WHERE series_areal.fuentes_id = 15 AND series_areal.proc_id = 5 AND series_areal.var_id = 20 AND series_areal.unit_id = 23 AND series_areal.id = observaciones_areal.series_id AND valores_num_areal.obs_id = observaciones_areal.id
  ORDER BY series_areal.area_id, observaciones_areal.timestart;

CREATE OR REPLACE VIEW saturogramas_nrt AS
WITH series_id AS (
         SELECT series_areal.id,
            series_areal.area_id,
            series_areal.proc_id,
            series_areal.var_id,
            series_areal.unit_id,
            series_areal.fuentes_id
           FROM series_areal
          WHERE series_areal.fuentes_id = 17 AND series_areal.proc_id = 5 AND series_areal.var_id = 30 AND series_areal.unit_id = 14
        )
 SELECT series_id.id AS series_id,
    series_id.area_id,
    observaciones_areal.timestart,
    valores_numarr_areal.valor[1] AS anegado,
    valores_numarr_areal.valor[3] AS nubes
   FROM observaciones_areal,
    valores_numarr_areal,
    series_id
  WHERE series_id.id = observaciones_areal.series_id AND valores_numarr_areal.obs_id = observaciones_areal.id
  ORDER BY series_id.area_id, observaciones_areal.timestart;


CREATE OR REPLACE VIEW saturogramas_nrt_stats AS
WITH series_id AS (
         SELECT series_areal.id,
            series_areal.area_id,
            series_areal.proc_id,
            series_areal.var_id,
            series_areal.unit_id,
            series_areal.fuentes_id
           FROM series_areal
          WHERE series_areal.fuentes_id = 17 AND series_areal.proc_id = 5 AND series_areal.var_id = 30 AND series_areal.unit_id = 14
        ), saturogramas AS (
         SELECT series_id.id AS series_id,
            series_id.area_id,
            observaciones_areal.timestart,
            valores_numarr_areal.valor[1] AS anegado,
            valores_numarr_areal.valor[3] AS nubes
           FROM observaciones_areal,
            valores_numarr_areal,
            series_id
          WHERE series_id.id = observaciones_areal.series_id AND valores_numarr_areal.obs_id = observaciones_areal.id
          ORDER BY series_id.area_id, observaciones_areal.timestart
        )
 SELECT saturogramas.series_id,
    saturogramas.area_id,
    count(saturogramas.anegado) AS count,
    min(saturogramas.timestart) AS timestart,
    max(saturogramas.timestart) AS timeend,
    min(saturogramas.anegado) AS rmin,
    max(saturogramas.anegado) AS rmax,
    percentile_cont(0.1::double precision) WITHIN GROUP (ORDER BY (saturogramas.anegado::double precision)) AS perc_10,
    percentile_cont(0.5::double precision) WITHIN GROUP (ORDER BY (saturogramas.anegado::double precision)) AS perc_50,
    percentile_cont(0.9::double precision) WITHIN GROUP (ORDER BY (saturogramas.anegado::double precision)) AS perc_90,
    avg(saturogramas.anegado) AS media
   FROM saturogramas
  GROUP BY saturogramas.series_id, saturogramas.area_id
  ORDER BY saturogramas.area_id;

CREATE OR REPLACE VIEW pma_gfs_3h AS
WITH pp AS (
         SELECT max(gfs_3h.fecha_emision) AS fecha_emision,
            gfs_3h.date
           FROM gfs_3h
          GROUP BY gfs_3h.date
        )
 SELECT series_areal.id AS series_id,
    series_areal.area_id,
    observaciones_areal.id AS obs_id,
    pp.fecha_emision,
    observaciones_areal.timestart,
    observaciones_areal.timeend,
    observaciones_areal.nombre,
    valores_num_areal.valor
   FROM series_areal,
    observaciones_areal,
    valores_num_areal,
    pp
  WHERE series_areal.fuentes_id = 35 AND series_areal.proc_id = 4 AND series_areal.var_id = 34 AND series_areal.unit_id = 9 AND series_areal.id = observaciones_areal.series_id AND valores_num_areal.obs_id = observaciones_areal.id AND pp.date = observaciones_areal.timestart::date
  ORDER BY series_areal.area_id, observaciones_areal.timestart;

CREATE OR REPLACE VIEW compara_gfs AS
 WITH p AS (
         SELECT pma_gfs_3h.area_id,
            (pma_gfs_3h.timestart - '09:00:00'::interval)::date AS date,
            sum(pma_gfs_3h.valor) AS sum
           FROM pma_gfs_3h
          WHERE pma_gfs_3h.timestart >= 'now'::text::date
          GROUP BY pma_gfs_3h.area_id, ((pma_gfs_3h.timestart - '09:00:00'::interval)::date)
          ORDER BY pma_gfs_3h.area_id, ((pma_gfs_3h.timestart - '09:00:00'::interval)::date)
        ), d AS (
         SELECT pmad_gfs.area_id,
            pmad_gfs.timestart::date AS date,
            pmad_gfs.valor
           FROM pmad_gfs
          WHERE pmad_gfs.timestart >= 'now'::text::date
        )
 SELECT p.area_id,
    p.date,
    p.sum AS p3horaria,
    d.valor AS pdiaria
   FROM p,
    d
  WHERE p.area_id = d.area_id AND p.date = d.date
  ORDER BY p.area_id, p.date;


CREATE OR REPLACE VIEW pmad_gpm_3h AS
SELECT series_areal.id AS series_id,
    series_areal.area_id,
    observaciones_areal.id AS obs_id,
    observaciones_areal.timestart,
    observaciones_areal.timeend,
    'gpm_3h_mm/3h'::text AS nombre,
    round(valores_num_areal.valor::numeric, 1) AS valor
   FROM series_areal,
    observaciones_areal,
    valores_num_areal
  WHERE series_areal.fuentes_id = 13 AND series_areal.proc_id = 5 AND series_areal.var_id = 34 AND series_areal.unit_id = 9 AND series_areal.id = observaciones_areal.series_id AND valores_num_areal.obs_id = observaciones_areal.id
  ORDER BY series_areal.area_id, observaciones_areal.timestart;

CREATE OR REPLACE VIEW pma_campo_3h AS
SELECT series_areal.id AS series_id,
    series_areal.area_id,
    observaciones_areal.id AS obs_id,
    observaciones_areal.timestart,
    observaciones_areal.timeend,
    observaciones_areal.nombre,
    valores_num_areal.valor
   FROM series_areal,
    observaciones_areal,
    valores_num_areal
  WHERE series_areal.fuentes_id = 36 AND series_areal.proc_id = 3 AND series_areal.var_id = 34 AND series_areal.unit_id = 9 AND series_areal.id = observaciones_areal.series_id AND valores_num_areal.obs_id = observaciones_areal.id
  ORDER BY series_areal.area_id, observaciones_areal.timestart;

CREATE OR REPLACE VIEW observaciones_all AS
SELECT 'point'::text AS "featureType",
    series.id AS "seriesID",
    'p:'::text || series.id AS "seriesCode",
    series.estacion_id AS "siteID",
    'p:'::text || series.estacion_id AS "siteCode",
    series.var_id AS "variableCode",
    series.proc_id AS "methodID",
    series.unit_id AS "unitID",
    '-1'::integer AS "sourceID",
    observaciones.id AS obs_id,
    observaciones.timestart AS "startDate",
    observaciones.timeend AS "endDate",
    valores_num.valor AS value
   FROM series,
    observaciones,
    valores_num
  WHERE series.id = observaciones.series_id AND observaciones.id = valores_num.obs_id
UNION ALL
 SELECT 'area'::text AS "featureType",
    series_areal.id AS "seriesID",
    'a:'::text || series_areal.id AS "seriesCode",
    series_areal.area_id AS "siteID",
    'a:'::text || series_areal.area_id AS "siteCode",
    series_areal.var_id AS "variableCode",
    series_areal.proc_id AS "methodID",
    series_areal.unit_id AS "unitID",
    series_areal.fuentes_id AS "sourceID",
    observaciones_areal.id AS obs_id,
    observaciones_areal.timestart AS "startDate",
    observaciones_areal.timeend AS "endDate",
    valores_num_areal.valor AS value
   FROM series_areal,
    observaciones_areal,
    valores_num_areal
  WHERE series_areal.id = observaciones_areal.series_id AND observaciones_areal.id = valores_num_areal.obs_id
  ORDER BY 1, 5, 6, 7, 8, 9, 11;

CREATE OR REPLACE VIEW pmad_gfs_new AS
WITH pp AS (
         SELECT max(gfs_diario_view.fecha_emision) AS fecha_emision,
            gfs_diario_view.date
           FROM gfs_diario_view
          GROUP BY gfs_diario_view.date
        )
 SELECT series_areal.id AS series_id,
    series_areal.area_id,
    observaciones_areal.id AS obs_id,
    pp.fecha_emision,
    observaciones_areal.timestart,
    observaciones_areal.timeend,
    observaciones_areal.nombre,
    valores_num_areal.valor
   FROM series_areal,
    observaciones_areal,
    valores_num_areal,
    pp
  WHERE series_areal.fuentes_id = 5 AND series_areal.proc_id = 4 AND series_areal.var_id = 1 AND series_areal.unit_id = 22 AND series_areal.id = observaciones_areal.series_id AND valores_num_areal.obs_id = observaciones_areal.id AND pp.date = observaciones_areal.timestart::date
  ORDER BY series_areal.area_id, observaciones_areal.timestart;

CREATE OR REPLACE VIEW ma_conae_api AS
SELECT series_areal.id AS series_id,
    series_areal.area_id,
    observaciones_areal.id AS obs_id,
    observaciones_areal.timestart,
    observaciones_areal.timeend,
    observaciones_areal.nombre,
    valores_num_areal.valor
   FROM series_areal,
    observaciones_areal,
    valores_num_areal
  WHERE series_areal.fuentes_id = 47 AND series_areal.proc_id = 4 AND series_areal.var_id = 47 AND series_areal.unit_id = 14 AND series_areal.id = observaciones_areal.series_id AND valores_num_areal.obs_id = observaciones_areal.id
  ORDER BY series_areal.area_id, observaciones_areal.timestart;

CREATE OR REPLACE VIEW apma_ecmwf AS
WITH pp AS (
         SELECT max(ecmwf_mensual.fecha_emision) - '03:00:00'::interval AS fecha_emision,
            ecmwf_mensual.date - '03:00:00'::interval AS date
           FROM ecmwf_mensual
          GROUP BY (ecmwf_mensual.date - '03:00:00'::interval)
        )
 SELECT series_areal.id AS series_id,
    series_areal.area_id,
    observaciones_areal.id AS obs_id,
    pp.fecha_emision,
    observaciones_areal.timestart,
    observaciones_areal.timeend,
    observaciones_areal.nombre,
    valores_num_areal.valor
   FROM series_areal,
    observaciones_areal,
    valores_num_areal,
    pp
  WHERE series_areal.fuentes_id = 46 AND series_areal.proc_id = 4 AND series_areal.var_id = 46 AND series_areal.unit_id = 9 AND series_areal.id = observaciones_areal.series_id AND valores_num_areal.obs_id = observaciones_areal.id AND pp.date = observaciones_areal.timestart
  ORDER BY series_areal.area_id, observaciones_areal.timestart;

CREATE OR REPLACE VIEW pmam_ecmwf AS
WITH pp AS (
         SELECT max(ecmwf_mensual.fecha_emision) - '03:00:00'::interval AS fecha_emision,
            ecmwf_mensual.date - '03:00:00'::interval AS date
           FROM ecmwf_mensual
          GROUP BY (ecmwf_mensual.date - '03:00:00'::interval)
        )
 SELECT series_areal.id AS series_id,
    series_areal.area_id,
    observaciones_areal.id AS obs_id,
    pp.fecha_emision,
    observaciones_areal.timestart,
    observaciones_areal.timeend,
    observaciones_areal.nombre,
    valores_num_areal.valor
   FROM series_areal,
    observaciones_areal,
    valores_num_areal,
    pp
  WHERE series_areal.fuentes_id = 45 AND series_areal.proc_id = 4 AND series_areal.var_id = 41 AND series_areal.unit_id = 9 AND series_areal.id = observaciones_areal.series_id AND valores_num_areal.obs_id = observaciones_areal.id AND pp.date = observaciones_areal.timestart
  ORDER BY series_areal.area_id, observaciones_areal.timestart;

CREATE OR REPLACE VIEW pmad_sqpe AS
SELECT observaciones_areal.series_id,
    series_areal.area_id,
    valores_num_areal.obs_id,
    observaciones_areal.timestart,
    observaciones_areal.timeend,
    observaciones_areal.nombre,
    valores_num_areal.valor
   FROM series_areal
     JOIN observaciones_areal ON series_areal.id = observaciones_areal.series_id
     JOIN valores_num_areal ON observaciones_areal.id = valores_num_areal.obs_id
  WHERE series_areal.fuentes_id = 44 AND series_areal.var_id = 1 AND series_areal.proc_id = 5
  ORDER BY observaciones_areal.series_id, observaciones_areal.timestart;

CREATE OR REPLACE VIEW saturogramas_ndwi37 AS
WITH series_id AS (
         SELECT series_areal.id,
            series_areal.area_id,
            series_areal.proc_id,
            series_areal.var_id,
            series_areal.unit_id,
            series_areal.fuentes_id
           FROM series_areal
          WHERE series_areal.fuentes_id = 40 AND series_areal.proc_id = 5 AND series_areal.var_id = 30 AND series_areal.unit_id = 14
        )
 SELECT series_id.id AS series_id,
    series_id.area_id,
    observaciones_areal.timestart,
    valores_numarr_areal.valor[1] AS anegado,
    valores_numarr_areal.valor[2] AS nulos
   FROM observaciones_areal,
    valores_numarr_areal,
    series_id
  WHERE series_id.id = observaciones_areal.series_id AND valores_numarr_areal.obs_id = observaciones_areal.id
  ORDER BY series_id.area_id, observaciones_areal.timestart;

CREATE OR REPLACE VIEW flooded_ndwi37_maxanual_by_areaid AS
WITH flood AS (
         SELECT saturogramas_ndwi37.area_id,
            date_part('year'::text, saturogramas_ndwi37.timestart) AS y,
            max(saturogramas_ndwi37.anegado) AS area
           FROM saturogramas_ndwi37
          WHERE saturogramas_ndwi37.nulos < 0.01::double precision AND date_part('year'::text, saturogramas_ndwi37.timestart) < date_part('year'::text, now())
          GROUP BY saturogramas_ndwi37.area_id, (date_part('year'::text, saturogramas_ndwi37.timestart))
        )
 SELECT flood.area_id,
    flood.area AS anegado,
    percent_rank() OVER (PARTITION BY flood.area_id ORDER BY flood.area) AS frecuencia
   FROM flood
  GROUP BY flood.area_id, flood.area
  ORDER BY flood.area_id, flood.area;

CREATE OR REPLACE VIEW flooded_ndwi37_minanual_by_areaid AS
WITH flood AS (
         SELECT saturogramas_ndwi37.area_id,
            date_part('year'::text, saturogramas_ndwi37.timestart) AS y,
            min(saturogramas_ndwi37.anegado) AS area
           FROM saturogramas_ndwi37
          WHERE saturogramas_ndwi37.nulos < 0.01::double precision AND date_part('year'::text, saturogramas_ndwi37.timestart) < date_part('year'::text, now())
          GROUP BY saturogramas_ndwi37.area_id, (date_part('year'::text, saturogramas_ndwi37.timestart))
        )
 SELECT flood.area_id,
    flood.area AS anegado,
    percent_rank() OVER (PARTITION BY flood.area_id ORDER BY flood.area) AS frecuencia
   FROM flood
  GROUP BY flood.area_id, flood.area
  ORDER BY flood.area_id, flood.area;

CREATE OR REPLACE VIEW pma_cosmo_3h AS
WITH pp AS (
         SELECT max(cosmo_3h.fecha_emision) AS fecha_emision,
            cosmo_3h.date
           FROM cosmo_3h
          GROUP BY cosmo_3h.date
        )
 SELECT series_areal.id AS series_id,
    series_areal.area_id,
    observaciones_areal.id AS obs_id,
    pp.fecha_emision,
    observaciones_areal.timestart,
    observaciones_areal.timeend,
    observaciones_areal.nombre,
    valores_num_areal.valor
   FROM series_areal,
    observaciones_areal,
    valores_num_areal,
    pp
  WHERE series_areal.fuentes_id = 38 AND series_areal.proc_id = 4 AND series_areal.var_id = 34 AND series_areal.unit_id = 9 AND series_areal.id = observaciones_areal.series_id AND valores_num_areal.obs_id = observaciones_areal.id AND pp.date = observaciones_areal.timestart::date
  ORDER BY series_areal.area_id, observaciones_areal.timestart;

CREATE OR REPLACE VIEW pmad_cosmo AS
WITH pp AS (
         SELECT max(cosmo_diario.fecha_emision) AS fecha_emision,
            cosmo_diario.date
           FROM cosmo_diario
          GROUP BY cosmo_diario.date
        )
 SELECT series_areal.id AS series_id,
    series_areal.area_id,
    observaciones_areal.id AS obs_id,
    pp.fecha_emision,
    observaciones_areal.timestart,
    observaciones_areal.timeend,
    observaciones_areal.nombre,
    valores_num_areal.valor
   FROM series_areal,
    observaciones_areal,
    valores_num_areal,
    pp
  WHERE series_areal.fuentes_id = 39 AND series_areal.proc_id = 4 AND series_areal.var_id = 1 AND series_areal.unit_id = 22 AND series_areal.id = observaciones_areal.series_id AND valores_num_areal.obs_id = observaciones_areal.id AND pp.date = observaciones_areal.timestart::date
  ORDER BY series_areal.area_id, observaciones_areal.timestart;

COMMIT;

