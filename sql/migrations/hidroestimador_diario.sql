BEGIN;
DROP VIEW rasters_all;
DROP MATERIALIZED VIEW hidroestimador_semanal;
DROP MATERIALIZED VIEW hidroestimador_diario;
CREATE OR REPLACE VIEW hidroestimador_diario AS
 SELECT
    o.id AS id, 
    o.timestart::date AS date,
    o.timestart AS timestart,
    o.timeend AS timeend,
    o.valor AS rast,
    o.timeupdate AS timeupdate
     FROM observaciones_rast AS o
     WHERE series_id=20
;

GRANT SELECT ON hidroestimador_diario TO actualiza, sololectura;

CREATE OR REPLACE VIEW hidroestimador_semanal AS
WITH h AS (
   SELECT
    o.id AS id, 
    o.timestart::date AS date,
    o.timestart AS timestart,
    o.timeend AS timeend,
    o.valor AS rast,
    o.timeupdate AS timeupdate
     FROM observaciones_rast AS o
     WHERE series_id=20
),
d AS (
   SELECT h1.timestart - '6 days'::interval AS timestart,
      h1.timeend
      FROM h h1
)
 SELECT d.timestart AS date,
    d.timestart,
    d.timeend,
    st_union(h.rast, 'sum'::text) AS rast,
    count(h.rast) AS count
   FROM h,
    d
  WHERE h.timestart >= d.timestart AND h.timeend <= d.timeend
  GROUP BY d.timestart, d.timeend
  ORDER BY d.timestart, d.timeend;

GRANT SELECT ON hidroestimador_semanal TO actualiza, sololectura;

CREATE VIEW rasters_all AS
SELECT 1 AS source_id,
    pp_eta.date,
    pp_eta.rast,
    pp_eta.fecha_emision AS timeupdate
   FROM pp_eta
UNION ALL
 SELECT 2 AS source_id,
    pp_cpc.date,
    pp_cpc.rast,
    pp_cpc.date + '2 days'::interval AS timeupdate
   FROM pp_cpc
UNION ALL
 SELECT 3 AS source_id,
    etp_wm.date,
    etp_wm.rast,
    '2018-01-01'::date AS timeupdate
   FROM etp_wm
UNION ALL
 SELECT 4 AS source_id,
    pp_eta_3h.date,
    pp_eta_3h.rast,
    pp_eta_3h.fecha_emision AS timeupdate
   FROM pp_eta_3h
UNION ALL
 SELECT 5 AS source_id,
    gfs_diario.date,
    gfs_diario.rast,
    gfs_diario.fecha_emision AS timeupdate
   FROM gfs_diario
UNION ALL
 SELECT 6 AS source_id,
    pp_gpm.date,
    pp_gpm.rast,
    pp_gpm.date + '1 day'::interval AS timeupdate
   FROM pp_gpm
UNION ALL
 SELECT 7 AS source_id,
    pp_emas.date,
    pp_emas.rast,
    pp_emas.date + '1 day'::interval AS timeupdate
   FROM pp_emas
UNION ALL
 SELECT 8 AS source_id,
    smap.date,
    smap.rast,
    smap.date + '1 day'::interval AS timeupdate
   FROM smap
UNION ALL
 SELECT 9 AS source_id,
    amsr2_mag_4days.date,
    amsr2_mag_4days.rast,
    amsr2_mag_4days.date + '1 day'::interval AS timeupdate
   FROM amsr2_mag_4days
UNION ALL
 SELECT 11 AS source_id,
    hidroestimador_diario.date,
    hidroestimador_diario.rast,
    hidroestimador_diario.date + '1 day'::interval AS timeupdate
   FROM hidroestimador_diario
UNION ALL
 SELECT 12 AS source_id,
    hidroestimador_3h.date,
    hidroestimador_3h.rast,
    hidroestimador_3h.date + '03:00:00'::interval AS timeupdate
   FROM hidroestimador_3h
UNION ALL
 SELECT 13 AS source_id,
    pp_gpm_3h.date,
    pp_gpm_3h.rast,
    pp_gpm_3h.date + '03:00:00'::interval AS timeupdate
   FROM pp_gpm_3h
UNION ALL
 SELECT 14 AS source_id,
    hidroestimador_semanal.date,
    hidroestimador_semanal.rast,
    hidroestimador_semanal.date + '7 days'::interval AS timeupdate
   FROM hidroestimador_semanal
UNION ALL
 SELECT 15 AS source_id,
    smops.date,
    smops.rast,
    smops.date + '1 day'::interval AS timeupdate
   FROM smops
UNION ALL
 SELECT 17 AS source_id,
    nrt_global_floodmap."time" AS date,
    nrt_global_floodmap.rast,
    nrt_global_floodmap."time" + '1 day'::interval AS timeupdate
   FROM nrt_global_floodmap
UNION ALL
 SELECT 18 AS source_id,
    resample_nrt.date,
    resample_nrt.rast,
    resample_nrt.date + '1 day'::interval AS timeupdate
   FROM resample_nrt
UNION ALL
 SELECT 31 AS source_id,
    etp_wm_view.date,
    etp_wm_view.rast,
    '2018-01-01'::date AS timeupdate
   FROM etp_wm_view
UNION ALL
 SELECT 32 AS source_id,
    marea_riodelaplata.date,
    marea_riodelaplata.rast,
    marea_riodelaplata.fecha_emision AS timeupdate
   FROM marea_riodelaplata
UNION ALL
 SELECT 33 AS source_id,
    pp_emas_spl.date,
    pp_emas_spl.rast,
    pp_emas_spl.date + '1 day'::interval AS timeupdate
   FROM pp_emas_spl
UNION ALL
 SELECT 34 AS source_id,
    pp_wrf.date,
    pp_wrf.rast,
    pp_wrf.fecha_emision AS timeupdate
   FROM pp_wrf
UNION ALL
 SELECT 35 AS source_id,
    gfs_3h.date,
    gfs_3h.rast,
    gfs_3h.fecha_emision AS timeupdate
   FROM gfs_3h
UNION ALL
 SELECT 36 AS source_id,
    pp_emas_3h.date,
    pp_emas_3h.rast,
    pp_emas_3h.date + '03:00:00'::interval AS timeupdate
   FROM pp_emas_3h
UNION ALL
 SELECT 37 AS source_id,
    pp_emas_3h_spl.date,
    pp_emas_3h_spl.rast,
    pp_emas_3h_spl.date + '03:00:00'::interval AS timeupdate
   FROM pp_emas_3h_spl;

GRANT SELECT ON rasters_all TO actualiza, sololectura;