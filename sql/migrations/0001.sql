BEGIN;

ALTER TABLE
    estados DROP CONSTRAINT estados_model_id_fkey,
ADD
    CONSTRAINT estados_model_id_fkey foreign key (model_id) references modelos(id) on delete cascade;

ALTER TABLE
    modelos_forzantes DROP CONSTRAINT modelos_forzantes_model_id_fkey,
ADD
    CONSTRAINT modelos_forzantes_model_id_fkey foreign key (model_id) references modelos(id) on delete cascade;

ALTER TABLE
    parametros DROP CONSTRAINT parametros_model_id_fkey,
ADD
    CONSTRAINT parametros_model_id_fkey foreign key (model_id) references modelos(id) on delete cascade;

ALTER TABLE
    modelos_out DROP CONSTRAINT modelos_out_model_id_fkey,
ADD
    CONSTRAINT modelos_out_model_id_fkey foreign key (model_id) references modelos(id) on delete cascade;

ALTER TABLE cal_estados 
    DROP CONSTRAINT cal_estados_cal_id_fkey,
ADD
    CONSTRAINT cal_estados_cal_id_fkey foreign key (cal_id) references calibrados(id) on delete cascade;

ALTER TABLE cal_pars 
    DROP CONSTRAINT cal_pars_cal_id_fkey,
    ADD CONSTRAINT cal_pars_cal_id_fkey FOREIGN KEY (cal_id) REFERENCES public.calibrados(id) on delete cascade;

ALTER TABLE ONLY public.forzantes
    DROP CONSTRAINT forzantes_cal_id_fkey,
    ADD CONSTRAINT forzantes_cal_id_fkey FOREIGN KEY (cal_id) REFERENCES public.calibrados(id) on delete cascade;

ALTER TABLE ONLY public.calibrados_out
    DROP CONSTRAINT calibrados_out_cal_id_fkey, 
    ADD CONSTRAINT calibrados_out_cal_id_fkey FOREIGN KEY (cal_id) REFERENCES public.calibrados(id) on delete cascade;

ALTER TABLE ONLY public.extra_pars
    DROP CONSTRAINT extra_pars_cal_id_fkey, 
    ADD CONSTRAINT extra_pars_cal_id_fkey FOREIGN KEY (cal_id) REFERENCES public.calibrados(id) on delete cascade;

ALTER TABLE ONLY public.cal_stats
    DROP CONSTRAINT cal_stats_cal_id_fkey, 
    ADD CONSTRAINT cal_stats_cal_id_fkey FOREIGN KEY (cal_id) REFERENCES public.calibrados(id) on delete cascade;

ALTER TRIGGER pars_get_mid ON cal_pars RENAME TO apars_get_mid;

COMMIT;