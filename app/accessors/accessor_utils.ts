import axios, { AxiosRequestConfig, AxiosError } from "axios";
import https from "https";

export type SeriesFilter = {
    id? : number|number[]
    tipo? : "puntual" | "areal" | "raster"
    series_id? : number|number[]
    id_externo? : string|string[]
    estacion_id?: number|number[] 
    var_id? : number|number[]
    proc_id?: number|number[]
    unit_id?: number|number[]
}

export interface ObservacionesFilter extends SeriesFilter {
    timestart: Date,
    timeend: Date
}

export interface FetchDataOptions extends AxiosRequestConfig {
    disable_validation?: boolean;
}

export async function fetchData<T = unknown>(
    url: string,
    options?: FetchDataOptions
): Promise<T> {
    const agent = new https.Agent({
        rejectUnauthorized: !options?.disable_validation,
    });

    try {
        const response = await axios.get<T>(url, {
            ...options,
            httpsAgent: agent,
        });

        return response.data;
    } catch (err) {
        const error = err as AxiosError<{ message?: string }>;

        if (error.response) {
            const status = error.response.status;
            const message =
                error.response.data?.message ?? error.message;

            throw new Error(
                `Request failed with status ${status}: ${message}`
            );
        }

        throw new Error(error.message || "Unknown error");
    }
}

export function parseUtcDateTime(s: string): Date {
    const [date, time] = s.split(" ");
    const [year, month, day] = date.split("-").map(Number);
    const [hour, minute, second] = time.split(":").map(Number);

    return new Date(Date.UTC(year, month - 1, day, hour, minute, second));
}

export function filterByParam(
    filter_value : any, 
    item_value : any, 
    func? : CallableFunction,
    is_numeric?: boolean
) : boolean {
    if(filter_value == undefined || ( Array.isArray(filter_value) && filter_value.length == 0 )) {
        return true
    }
    if(func) {
        return func(filter_value, item_value)
    } else if (item_value == undefined) {
        return false
    }
    if(Array.isArray(filter_value)) {
        if(is_numeric) {
            const filter_values_num = filter_value.map(v => parseFloat(v))
            if(filter_values_num.map(v => v.toString()).indexOf("NaN")  >= 0) {
                throw new Error("Invalid filter, must be numeric")
            }
            if(filter_values_num.indexOf(parseFloat(item_value)) >= 0) {
                return true
            }
        } else {
            if(filter_value.indexOf(item_value) >= 0) {
                return true
            }
        }
    } else if(is_numeric) {
        const filter_value_num = parseFloat(filter_value)
        if(filter_value_num.toString() == "NaN") {
            throw new Error("Invalid filter, must be numeric")
        }
        if(parseFloat(item_value) == filter_value_num) {
            return true
        }
    } else if(item_value == filter_value) {
        return true
    }
    return false
}

export function filterSeries(series : any[]=[],params : SeriesFilter={}) : any[] {
	return series.filter(serie => {
        return (
            [
                filterByParam(params.estacion_id, serie.estacion.id, undefined, true),
                filterByParam(params.var_id, serie.var.id, undefined, true),
                filterByParam(params.unit_id, serie.unidades.id, undefined, true),
                filterByParam(params.id_externo, serie.estacion.id_externo),
                filterByParam(params.series_id, serie.id, undefined, true),
                filterByParam(params.id, serie.id, undefined, true),
                filterByParam(params.tipo, serie.tipo)
            ].indexOf(false) < 0
        )		
	})
}

export function filterSeriesByIds(series : any[]=[],params : SeriesFilter={}) : any[] {
	return series.filter(serie => {
        return (
            [
                filterByParam(params.estacion_id, serie.estacion_id),
                filterByParam(params.var_id, serie.var_id),
                filterByParam(params.unit_id, serie.unit_id),
                filterByParam(params.series_id, serie.id),
                filterByParam(params.id, serie.id),
                filterByParam(params.tipo, serie.tipo)
            ].indexOf(false) < 0
        )
	})
}

