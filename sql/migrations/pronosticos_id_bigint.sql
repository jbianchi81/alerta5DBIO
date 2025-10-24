begin;

drop view calibrados_view;
drop view ultimas_alturas_prono;
drop view alturas_prono_last;
drop view alturas_prono;
drop view caudales_prono_last;
drop view caudales_prono;
drop view pronosticos_altura_semanal;
drop view pronosticos_last;
drop view corridas_view;
drop materialized view alturas_mensuales_prono_view;
drop view alturas_mensuales_prono;

alter table valores_prono_num alter column prono_id TYPE bigint;
alter table pronosticos alter column id TYPE bigint;

create view alturas_mensuales_prono AS 
 WITH s AS (
         SELECT series_1.id,
            series_1.estacion_id,
            series_1.var_id,
            series_1.proc_id,
            series_1.unit_id
           FROM series series_1
          WHERE series_1.var_id = 33
        ), p AS (
         SELECT DISTINCT ON (pronosticos.series_id, pronosticos.timestart) pronosticos.series_id,
            pronosticos.id,
            pronosticos.cor_id,
            pronosticos.timestart,
            pronosticos.timeend
           FROM pronosticos,
            s
          WHERE pronosticos.series_id = s.id
          ORDER BY pronosticos.series_id, pronosticos.timestart, pronosticos.cor_id DESC
        ), v AS (
         SELECT p.id,
            p.cor_id,
            p.series_id,
            p.timestart,
            p.timeend,
            valores_prono_num.prono_id,
            valores_prono_num.valor
           FROM p,
            valores_prono_num
          WHERE p.id = valores_prono_num.prono_id
        )
 SELECT estaciones.unid,
    estaciones.id,
    estaciones.tabla,
    estaciones.id_externo,
    estaciones.nombre,
    series.id AS series_id,
    corridas.id AS cor_id,
    corridas.cal_id,
    corridas.date AS cor_date,
    calibrados.nombre AS cal_name,
    modelos.nombre AS model_name,
    modelos.id AS model_id,
    v.timestart,
    v.timeend,
    v.valor
   FROM estaciones,
    series,
    v,
    corridas,
    calibrados,
    modelos
  WHERE series.var_id = 33 AND (series.proc_id = ANY (ARRAY[1, 4, 8])) AND series.unit_id = 11 AND estaciones.unid = series.estacion_id AND series.id = v.series_id AND v.cor_id = corridas.id AND corridas.cal_id = calibrados.id AND calibrados.model_id = modelos.id
  ORDER BY estaciones.unid, v.timestart;

ALTER VIEW public.alturas_mensuales_prono OWNER TO actualiza;

--
-- Name: TABLE alturas_mensuales_prono_view; Type: ACL; Schema: public; Owner: actualiza
--

GRANT SELECT ON TABLE public.alturas_mensuales_prono TO sololectura;



CREATE MATERIALIZED VIEW public.alturas_mensuales_prono_view AS
 WITH d AS (
         SELECT generate_series((make_date((date_part('year'::text, ('now'::text)::date))::integer, (date_part('month'::text, ('now'::text)::date))::integer, 1))::timestamp without time zone, ((make_date((date_part('year'::text, ('now'::text)::date))::integer, (date_part('month'::text, ('now'::text)::date))::integer, 1))::timestamp without time zone + '4 mons'::interval), '1 mon'::interval) AS dd
        ), s AS (
         SELECT alturas_mensuales_prono.unid,
            alturas_mensuales_prono.nombre,
            alturas_mensuales_prono.tabla,
            alturas_mensuales_prono.series_id,
            (alturas_mensuales_prono.timestart)::date AS timestart,
            d.dd,
            avg(alturas_mensuales_prono.valor) AS valor,
            ((alturas_mensuales_prono.timeend)::date - ('now'::text)::date) AS horiz
           FROM public.alturas_mensuales_prono,
            d
          WHERE ((alturas_mensuales_prono.timestart)::date = (d.dd)::date)
          GROUP BY alturas_mensuales_prono.unid, alturas_mensuales_prono.nombre, alturas_mensuales_prono.tabla, alturas_mensuales_prono.series_id, ((alturas_mensuales_prono.timestart)::date), ((alturas_mensuales_prono.timeend)::date - ('now'::text)::date), d.dd
          ORDER BY alturas_mensuales_prono.unid, d.dd
        )
 SELECT s.unid AS sitecode,
    estaciones.nombre,
    estaciones.tabla,
    s.series_id,
    estaciones.geom,
    to_char(s0.dd, 'YYYY/MM'::text) AS date,
    round((s0.valor)::numeric, 3) AS este_mes,
    (json_object(array_agg(to_char(s.dd, 'YYYY-MM-DD'::text)), array_agg((round((s.valor)::numeric, 3))::text)))::text AS valor,
        CASE
            WHEN (max(s.valor) > max(s0.valor)) THEN 'crece'::text
            WHEN (min(s.valor) < min(s0.valor)) THEN 'baja'::text
            ELSE 'permanece'::text
        END AS tendencia
   FROM s,
    public.estaciones,
    ( SELECT s_1.unid,
            s_1.nombre,
            s_1.tabla,
            s_1.series_id,
            s_1.timestart,
            s_1.dd,
            s_1.valor,
            s_1.horiz
           FROM s s_1
          WHERE ((s_1.dd)::date = make_date((date_part('year'::text, ('now'::text)::date))::integer, (date_part('month'::text, ('now'::text)::date))::integer, 1))) s0
  WHERE ((estaciones.unid = s.unid) AND (s.unid = s0.unid))
  GROUP BY s.unid, estaciones.nombre, estaciones.tabla, s.series_id, estaciones.geom, s0.valor, s0.dd
  ORDER BY s.unid
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.alturas_mensuales_prono_view OWNER TO actualiza;

--
-- Name: TABLE alturas_mensuales_prono_view; Type: ACL; Schema: public; Owner: actualiza
--

GRANT SELECT ON TABLE public.alturas_mensuales_prono_view TO sololectura;

CREATE VIEW public.corridas_view AS
 SELECT corridas.cal_id,
    corridas.date,
    corridas.id,
    corridas.series_n,
    corridas.plan_cor_id,
    count(pronosticos.timestart) AS count,
    min(pronosticos.timestart) AS timestart,
    max(pronosticos.timeend) AS timeend,
    min(valores_prono_num.valor) AS min_valor,
    max(valores_prono_num.valor) AS max_valor
   FROM public.corridas,
    public.pronosticos,
    public.valores_prono_num
  WHERE ((corridas.id = pronosticos.cor_id) AND (pronosticos.id = valores_prono_num.prono_id))
  GROUP BY corridas.id, corridas.date, corridas.cal_id, corridas.series_n, corridas.plan_cor_id
  ORDER BY corridas.date, corridas.cal_id;


ALTER VIEW public.corridas_view OWNER TO jbianchi;

--
-- Name: TABLE corridas_view; Type: ACL; Schema: public; Owner: jbianchi
--

GRANT SELECT ON TABLE public.corridas_view TO actualiza;

--
-- Name: pronosticos_last; Type: VIEW; Schema: public; Owner: alerta5
--

CREATE VIEW public.pronosticos_last AS
 SELECT pronosticos.id,
    pronosticos.cor_id,
    corridas.cal_id,
    pronosticos.series_id,
    pronosticos.timestart,
    pronosticos.timeend,
    valores_prono_num.valor,
    pronosticos.qualifier
   FROM public.pronosticos,
    public.corridas,
    public.valores_prono_num
  WHERE ((pronosticos.cor_id = corridas.id) AND (pronosticos.id = valores_prono_num.prono_id) AND (valores_prono_num.valor IS NOT NULL) AND (corridas.date >= (('now'::text)::date - 14)));


ALTER VIEW public.pronosticos_last OWNER TO alerta5;

--
-- Name: TABLE pronosticos_last; Type: ACL; Schema: public; Owner: alerta5
--

GRANT SELECT ON TABLE public.pronosticos_last TO sololectura;
GRANT SELECT ON TABLE public.pronosticos_last TO actualiza;

--
-- Name: pronosticos_altura_semanal; Type: VIEW; Schema: public; Owner: jbianchi
--

CREATE VIEW public.pronosticos_altura_semanal AS
 WITH max_fdate AS (
         SELECT max(corridas_1.date) AS date
           FROM public.corridas corridas_1
          WHERE (corridas_1.cal_id = 434)
        )
 SELECT pronosticos.cor_id,
    pronosticos.series_id,
    pronosticos.timestart,
    pronosticos.timeend,
    valores_prono_num.valor
   FROM max_fdate,
    public.corridas,
    public.pronosticos,
    public.valores_prono_num
  WHERE ((max_fdate.date = corridas.date) AND (corridas.cal_id = 434) AND (corridas.id = pronosticos.cor_id) AND (pronosticos.id = valores_prono_num.prono_id))
  ORDER BY pronosticos.series_id, pronosticos.timestart;


ALTER VIEW public.pronosticos_altura_semanal OWNER TO jbianchi;

--
-- Name: caudales_prono; Type: VIEW; Schema: public; Owner: alerta5
--

CREATE VIEW public.caudales_prono AS
 SELECT estaciones.unid,
    estaciones.id,
    estaciones.tabla,
    estaciones.id_externo,
    estaciones.nombre,
    series.id AS series_id,
    corridas.id AS cor_id,
    corridas.cal_id,
    corridas.date AS cor_date,
    calibrados.nombre AS cal_name,
    modelos.nombre AS model_name,
    modelos.id AS model_id,
    pronosticos.timestart,
    pronosticos.timeend,
    valores_prono_num.valor
   FROM public.estaciones,
    public.series,
    public.pronosticos,
    public.corridas,
    public.calibrados,
    public.modelos,
    public.valores_prono_num
  WHERE ((series.var_id = 4) AND (series.proc_id = 4) AND (series.unit_id = 10) AND (estaciones.unid = series.estacion_id) AND (series.id = pronosticos.series_id) AND (pronosticos.id = valores_prono_num.prono_id) AND (pronosticos.cor_id = corridas.id) AND (corridas.cal_id = calibrados.id) AND (calibrados.model_id = modelos.id))
  ORDER BY estaciones.unid, pronosticos.timestart;


ALTER VIEW public.caudales_prono OWNER TO alerta5;

--
-- Name: TABLE caudales_prono; Type: ACL; Schema: public; Owner: alerta5
--

GRANT SELECT ON TABLE public.caudales_prono TO actualiza;
GRANT SELECT ON TABLE public.caudales_prono TO sololectura;

--
-- Name: caudales_prono_last; Type: VIEW; Schema: public; Owner: alerta5
--

CREATE VIEW public.caudales_prono_last AS
 WITH m AS (
         SELECT caudales_prono_1.unid,
            caudales_prono_1.cal_id,
            max(caudales_prono_1.cor_date) AS max
           FROM public.caudales_prono caudales_prono_1
          WHERE (caudales_prono_1.timestart >= (('now'::text)::date - 30))
          GROUP BY caudales_prono_1.unid, caudales_prono_1.cal_id
        )
 SELECT caudales_prono.unid,
    caudales_prono.id,
    caudales_prono.tabla,
    caudales_prono.id_externo,
    caudales_prono.nombre,
    caudales_prono.series_id,
    caudales_prono.cor_id,
    caudales_prono.cal_id,
    caudales_prono.cor_date,
    caudales_prono.cal_name,
    caudales_prono.model_name,
    caudales_prono.model_id,
    caudales_prono.timestart,
    caudales_prono.timeend,
    caudales_prono.valor
   FROM public.caudales_prono,
    m
  WHERE ((caudales_prono.cor_date = m.max) AND (caudales_prono.cal_id = m.cal_id) AND (caudales_prono.timestart >= (('now'::text)::date - 30)))
  ORDER BY caudales_prono.unid, caudales_prono.cal_id, caudales_prono.timestart;


ALTER VIEW public.caudales_prono_last OWNER TO alerta5;

--
-- Name: TABLE caudales_prono_last; Type: ACL; Schema: public; Owner: alerta5
--

GRANT SELECT ON TABLE public.caudales_prono_last TO sololectura;

--
-- Name: alturas_prono; Type: VIEW; Schema: public; Owner: alerta5
--

CREATE VIEW public.alturas_prono AS
 SELECT estaciones.unid,
    estaciones.id,
    estaciones.tabla,
    estaciones.id_externo,
    estaciones.nombre,
    series.id AS series_id,
    corridas.id AS cor_id,
    corridas.cal_id,
    corridas.date AS cor_date,
    calibrados.nombre AS cal_name,
    modelos.nombre AS model_name,
    modelos.id AS model_id,
    pronosticos.timestart,
    pronosticos.timeend,
    valores_prono_num.valor
   FROM public.estaciones,
    public.series,
    public.pronosticos,
    public.corridas,
    public.calibrados,
    public.modelos,
    public.valores_prono_num
  WHERE ((series.var_id = 2) AND (series.proc_id = 4) AND (series.unit_id = 11) AND (estaciones.unid = series.estacion_id) AND (series.id = pronosticos.series_id) AND (pronosticos.id = valores_prono_num.prono_id) AND (pronosticos.cor_id = corridas.id) AND (corridas.cal_id = calibrados.id) AND (calibrados.model_id = modelos.id))
  ORDER BY estaciones.unid, pronosticos.timestart;


ALTER VIEW public.alturas_prono OWNER TO alerta5;

--
-- Name: TABLE alturas_prono; Type: ACL; Schema: public; Owner: alerta5
--

GRANT SELECT ON TABLE public.alturas_prono TO sololectura;
GRANT SELECT ON TABLE public.alturas_prono TO actualiza;

--
-- Name: alturas_prono_last; Type: VIEW; Schema: public; Owner: alerta5
--

CREATE VIEW public.alturas_prono_last AS
 WITH m AS (
         SELECT alturas_prono_1.unid,
            alturas_prono_1.cal_id,
            max(alturas_prono_1.cor_date) AS max
           FROM public.alturas_prono alturas_prono_1
          WHERE ((alturas_prono_1.timestart >= (('now'::text)::date - 30)) AND (alturas_prono_1.valor IS NOT NULL))
          GROUP BY alturas_prono_1.unid, alturas_prono_1.cal_id
        )
 SELECT alturas_prono.unid,
    alturas_prono.id,
    alturas_prono.tabla,
    alturas_prono.id_externo,
    alturas_prono.nombre,
    alturas_prono.series_id,
    alturas_prono.cor_id,
    alturas_prono.cal_id,
    alturas_prono.cor_date,
    alturas_prono.cal_name,
    alturas_prono.model_name,
    alturas_prono.model_id,
    alturas_prono.timestart,
    alturas_prono.timeend,
    alturas_prono.valor
   FROM public.alturas_prono,
    m
  WHERE ((alturas_prono.cor_date = m.max) AND (alturas_prono.cal_id = m.cal_id) AND (alturas_prono.timestart >= (('now'::text)::date - 30)))
  ORDER BY alturas_prono.unid, alturas_prono.cal_id, alturas_prono.timestart;


ALTER VIEW public.alturas_prono_last OWNER TO alerta5;

--
-- Name: TABLE alturas_prono_last; Type: ACL; Schema: public; Owner: alerta5
--

GRANT SELECT ON TABLE public.alturas_prono_last TO sololectura;
GRANT SELECT ON TABLE public.alturas_prono_last TO actualiza;

--
-- Name: ultimas_alturas_prono; Type: VIEW; Schema: public; Owner: alerta5
--

CREATE VIEW public.ultimas_alturas_prono AS
 WITH m AS (
         SELECT alturas_prono_last_1.cal_id,
            alturas_prono_last_1.unid,
            max(alturas_prono_last_1.valor) AS max
           FROM public.alturas_prono_last alturas_prono_last_1
          WHERE (alturas_prono_last_1.timestart >= alturas_prono_last_1.cor_date)
          GROUP BY alturas_prono_last_1.cal_id, alturas_prono_last_1.unid
        )
 SELECT DISTINCT ON (alturas_prono_last.unid, alturas_prono_last.cal_id) alturas_prono_last.unid,
    alturas_prono_last.id,
    alturas_prono_last.tabla,
    alturas_prono_last.id_externo,
    alturas_prono_last.nombre,
    alturas_prono_last.series_id,
    alturas_prono_last.cor_id,
    alturas_prono_last.cal_id,
    alturas_prono_last.cor_date,
    alturas_prono_last.cal_name,
    alturas_prono_last.model_name,
    alturas_prono_last.model_id,
    alturas_prono_last.timestart,
    alturas_prono_last.timeend,
    alturas_prono_last.valor,
    estaciones.geom
   FROM public.alturas_prono_last,
    m,
    public.estaciones
  WHERE ((alturas_prono_last.unid = m.unid) AND (alturas_prono_last.cal_id = m.cal_id) AND (m.max = alturas_prono_last.valor) AND (alturas_prono_last.timestart >= alturas_prono_last.cor_date) AND (estaciones.unid = alturas_prono_last.unid))
  ORDER BY alturas_prono_last.unid, alturas_prono_last.cal_id, alturas_prono_last.timestart;


ALTER VIEW public.ultimas_alturas_prono OWNER TO alerta5;

--
-- Name: TABLE ultimas_alturas_prono; Type: ACL; Schema: public; Owner: alerta5
--

GRANT SELECT ON TABLE public.ultimas_alturas_prono TO sololectura;

--
-- Name: calibrados_view; Type: VIEW; Schema: public; Owner: alerta5
--

CREATE VIEW public.calibrados_view AS
 WITH series_prono_last AS (
         WITH pronosticos_last AS (
                 SELECT pronosticos.id,
                    pronosticos.cor_id,
                    pronosticos.series_id,
                    pronosticos.timestart,
                    pronosticos.timeend,
                    valores_prono_num.valor
                   FROM public.pronosticos,
                    public.corridas,
                    public.valores_prono_num
                  WHERE ((pronosticos.cor_id = corridas.id) AND (pronosticos.id = valores_prono_num.prono_id) AND (valores_prono_num.valor IS NOT NULL) AND (corridas.date >= (('now'::text)::date - 14)))
                ), series_prono AS (
                 SELECT pronosticos_last.series_id,
                    corridas.cal_id,
                    max(corridas.date) AS fecha_emision,
                    min(pronosticos_last.timestart) AS timestart,
                    max(pronosticos_last.timeend) AS timeend
                   FROM pronosticos_last,
                    public.corridas,
                    public.calibrados calibrados_1
                  WHERE ((pronosticos_last.cor_id = corridas.id) AND (corridas.cal_id = calibrados_1.id))
                  GROUP BY pronosticos_last.series_id, corridas.cal_id
                ), s AS (
                 SELECT series_prono.series_id,
                    series_prono.cal_id,
                    calibrados_1.nombre,
                    calibrados_1.modelo,
                    series_prono.fecha_emision,
                    corridas.id AS cor_id
                   FROM series_prono,
                    public.corridas,
                    public.calibrados calibrados_1
                  WHERE ((series_prono.cal_id = corridas.cal_id) AND (series_prono.fecha_emision = corridas.date) AND (series_prono.cal_id = calibrados_1.id))
                )
         SELECT s.series_id,
            s.cal_id,
            s.nombre,
            s.modelo,
            s.fecha_emision,
            s.cor_id
           FROM s
          ORDER BY s.series_id, s.cal_id
        )
 SELECT estaciones.unid,
    estaciones.geom,
    estaciones.nombre AS nombre_estacion,
    calibrados.nombre AS nombre_calibrado,
    calibrados.id AS cal_id,
    calibrados.modelo AS nombre_modelo,
    calibrados.model_id,
    series_prono_last.fecha_emision AS ultima_fecha_de_emision,
    series_prono_last.series_id,
    series_prono_last.cor_id,
    series.var_id,
    var.nombre AS nombre_var
   FROM public.estaciones,
    public.calibrados,
    series_prono_last,
    public.series,
    public.var
  WHERE ((estaciones.unid = calibrados.out_id) AND (calibrados.id = series_prono_last.cal_id) AND (series_prono_last.series_id = series.id) AND (series.var_id = var.id) AND (series.estacion_id = estaciones.unid) AND (series.proc_id = 4))
  ORDER BY calibrados.id;


ALTER VIEW public.calibrados_view OWNER TO alerta5;

--
-- Name: TABLE calibrados_view; Type: ACL; Schema: public; Owner: alerta5
--

GRANT SELECT ON TABLE public.calibrados_view TO sololectura;


commit;