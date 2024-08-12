BEGIN;

ALTER TABLE ONLY public.valores_prono_num
    DROP CONSTRAINT valores_prono_num_prono_id_fkey;


ALTER TABLE ONLY public.valores_prono_num
    ADD CONSTRAINT valores_prono_num_prono_id_fkey FOREIGN KEY (prono_id) REFERENCES public.pronosticos(id) ON DELETE CASCADE;

COMMIT;
