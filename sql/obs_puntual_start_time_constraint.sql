BEGIN;
CREATE OR REPLACE FUNCTION public.obs_puntual_start_time_constraint_trigger()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$

declare
_sql text;
_dt interval;
_hora_corte interval;
_new_dt interval;
_sql2 text;
_match boolean;

begin
_sql := 'select var."timeSupport",def_hora_corte  from var,series where var.id=series.var_id and series.id='||new.series_id;
execute _sql into _dt, _hora_corte;
IF (_dt is null)
THEN 
	return NEW;
ELSE
    IF _dt = interval '01:00:00'
    THEN 
    	IF (extract(minute FROM new.timestart) <> 0 OR  extract(second FROM new.timestart) <> 0 OR extract(millisecond FROM new.timestart) <> 0)
        THEN
            raise notice 'El timestart no corresponde al inicio de un paso horario';
            return NULL; 
        ELSE
            return NEW;
        END IF;
	ELSIF _dt = interval '1 day'
    THEN
        IF (_hora_corte is NULL)
        THEN
            IF (extract(hour FROM new.timestart) <> 0 OR extract(minute FROM new.timestart) <> 0 OR  extract(second FROM new.timestart) <> 0 OR extract(millisecond FROM new.timestart) <> 0)
            THEN
                raise notice 'El timestart no corresponde al inicio de un paso diario';
                return NULL; 
            ELSE
                return NEW;
            END IF;
        ELSE
            IF (extract(hour FROM new.timestart) <> extract(hour from _hora_corte) OR extract(minute FROM new.timestart) <> extract(minute FROM _hora_corte) OR  extract(second FROM new.timestart) <> extract(second FROM _hora_corte) OR extract(millisecond FROM new.timestart) <> extract(millisecond FROM _hora_corte))
            THEN
                raise notice 'El timestart no corresponde al inicio de un paso diario con el horario de corte requerido';
                return NULL; 
            ELSE
                return NEW;
            END IF;
        END IF;
    ELSIF _dt = interval '1 month'
    THEN
        IF (_hora_corte is NULL)
        THEN
            IF (extract(day FROM new.timestart) <> 1 OR extract(hour FROM new.timestart) <> 0 OR extract(minute FROM new.timestart) <> 0 OR  extract(second FROM new.timestart) <> 0 OR extract(millisecond FROM new.timestart) <> 0)
            THEN
                raise notice 'El timestart no corresponde al inicio de un paso mensual';
                return NULL; 
            ELSE
                return NEW;
            END IF;
        ELSE
            IF (extract(day FROM new.timestart) <> 1 OR  extract(hour FROM new.timestart) <> extract(hour from _hora_corte) OR extract(minute FROM new.timestart) <> extract(minute FROM _hora_corte) OR  extract(second FROM new.timestart) <> extract(second FROM _hora_corte) OR extract(millisecond FROM new.timestart) <> extract(millisecond FROM _hora_corte))
            THEN
                raise notice 'El timestart no corresponde al inicio de un paso mensual con el horario de corte requerido';
                return NULL; 
            ELSE
                return NEW;
            END IF;
        END IF;
    ELSE
        return NEW;
	END IF;
END IF;
end
$function$

;

CREATE TRIGGER obs_puntual_start_time_constraint BEFORE INSERT OR UPDATE ON public.observaciones FOR EACH ROW EXECUTE PROCEDURE public.obs_puntual_start_time_constraint_trigger();

COMMIT;