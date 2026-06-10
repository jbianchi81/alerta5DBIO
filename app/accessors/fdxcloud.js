"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Client = void 0;
const abstract_accessor_engine_1 = require("./abstract_accessor_engine");
const accessor_utils_1 = require("../accessor_utils");
const CRUD_1 = require("../CRUD");
class Client extends abstract_accessor_engine_1.AbstractAccessorEngine {
    constructor(config) {
        super(config);
        this.defaults = {
            pagination_length: 100
        };
        if (config.series_map) {
            this.series_map = config.series_map;
        }
        this.url = config.url;
        this.token = config.token;
        this.tabla_id = config.tabla_id;
        this.pagination_length = config.pagination_length || this.defaults.pagination_length;
    }
    headers() {
        return {
            "x-api-token": this.token,
            "Content-Type": "application/json"
        };
    }
    getMeasuresLatest(measurement_point, device) {
        return __awaiter(this, void 0, void 0, function* () {
            return (0, accessor_utils_1.fetchData)(`${this.url}/measures/latest`, {
                params: {
                    measurement_point: measurement_point,
                    device: device
                },
                headers: this.headers()
            });
        });
    }
    getMeasures(measurement_point, device, since, to, length, page) {
        return __awaiter(this, void 0, void 0, function* () {
            return (0, accessor_utils_1.fetchData)(`${this.url}/measures`, {
                params: {
                    measurement_point: measurement_point,
                    device: device,
                    since: since,
                    to: to,
                    length: length,
                    page: page
                },
                headers: this.headers()
            });
        });
    }
    getTimeseries(measurement_point, since, to, device) {
        return __awaiter(this, void 0, void 0, function* () {
            return (0, accessor_utils_1.fetchData)(`${this.url}/measures/timeseries`, {
                params: {
                    measurement_point: measurement_point,
                    device: device,
                    since: since,
                    to: to
                },
                headers: this.headers()
            });
        });
    }
    setSeriesMap(series) {
        this.series_map = {};
        for (const serie of series) {
            this.series_map[serie.estacion.id] = {
                series_id: serie.id,
                point_id: parseInt(serie.estacion.id_externo),
                metadata: serie
            };
        }
    }
    getSeries() {
        return __awaiter(this, arguments, void 0, function* (filter = {}) {
            filter.tabla_id = this.tabla_id;
            filter.var_id = 2;
            filter.proc_id = 1;
            filter.unit_id = 11;
            const series = yield CRUD_1.serie.read(filter);
            this.setSeriesMap(series);
            return series;
        });
    }
    getMeasuresWithPagination(measurement_point_1, device_1, since_1, to_1) {
        return __awaiter(this, arguments, void 0, function* (measurement_point, device, since, to, length = this.pagination_length, start_page = 1) {
            let last = false;
            let page = start_page;
            let measurements = [];
            while (last == false) {
                const measures_response = yield this.getMeasures(measurement_point, device, since, to, length, page);
                measurements = measurements.concat(measures_response.rows);
                const last_page = Math.floor(measures_response.total / length + 1);
                page = page + 1;
                if (page > last_page) {
                    last = true;
                }
            }
            return measurements;
        });
    }
    getSerieFromId(s_id) {
        if (!this.series_map) {
            throw new Error("series_map not set");
        }
        for (const [k, v] of Object.entries(this.series_map)) {
            if (v.series_id == s_id) {
                return v;
            }
        }
        throw new Error("Serie not found with id=" + s_id);
    }
    get(filter_1) {
        return __awaiter(this, arguments, void 0, function* (filter, options = {}) {
            if (!filter.timestart || !filter.timeend) {
                throw new Error("Missing filter.timestart, filter.timeend");
            }
            if (!this.series_map) {
                yield this.getSeries(filter);
            }
            if (filter.series_id) {
                if (Array.isArray(filter.series_id)) {
                    throw new Error("Multiple series_id are not allowed in filter");
                }
                var serie_map = this.getSerieFromId(filter.series_id);
            }
            else {
                if (!filter.estacion_id) {
                    throw new Error("Missing filter.series_id or filter.estacion_id");
                }
                if (Array.isArray(filter.estacion_id)) {
                    throw new Error("Multiple estacion_id are not allowed in filter");
                }
                if (this.series_map === undefined) {
                    throw new Error("series_map not set");
                }
                var serie_map = this.series_map[filter.estacion_id];
                if (!serie_map) {
                    throw new Error("estacion_id not found in series map");
                }
            }
            if (options.use_measures_endpoint) {
                const measurements = yield this.getMeasuresWithPagination(serie_map.point_id, undefined, filter.timestart.toISOString().substring(0, 10), new Date(filter.timeend.getTime() + 24 * 3600 * 1000).toISOString().substring(0, 10) // avanza 1 día para que tome el día final completo
                );
                var obs = this.parseMeasurements(measurements, serie_map);
            }
            else {
                const timeseries = yield this.getTimeseries(serie_map.point_id, filter.timestart.toISOString().substring(0, 19).replace("T", " "), filter.timeend.toISOString().substring(0, 19).replace("T", " "));
                var obs = this.parseTimeseries(timeseries, serie_map);
            }
            if (options.return_series) {
                const serie = serie_map.metadata;
                serie.observaciones = obs;
                return [serie];
            }
            else {
                return obs;
            }
        });
    }
    parseMeasurements(measurements, serie_map) {
        const obs = [];
        for (const m of measurements) {
            obs.push(this.parseMeasurement(m, serie_map));
        }
        return obs;
    }
    parseMeasurement(m, serie_map) {
        return {
            timestart: new Date(m.measureDate),
            timeend: new Date(m.measureDate),
            series_id: serie_map.series_id,
            valor: m.interpretedValue
        };
    }
    parseTimeseries(timeseries, serie_map) {
        const obs = [];
        for (const tvp of timeseries) {
            obs.push(this.parseTimeValuePair(tvp, serie_map));
        }
        return obs;
    }
    parseTimeValuePair(tvp, serie_map) {
        return {
            timestart: new Date(tvp.time),
            timeend: new Date(tvp.time),
            series_id: serie_map.series_id,
            valor: tvp.value
        };
    }
}
exports.Client = Client;
Client._get_is_multiseries = false;
