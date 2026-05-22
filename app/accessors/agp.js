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
const csv_string_1 = require("csv-string");
const CRUD_1 = require("../CRUD");
const fs_1 = require("fs");
class Client extends abstract_accessor_engine_1.AbstractAccessorEngine {
    constructor(config) {
        super(config);
        this._get_is_multiseries = true;
        this.series_map = config.series_map;
        this.date_col = config.date_col;
        this.value_col = config.value_col;
        this.skip_rows = config.skip_rows;
    }
    downloadDataAndParseCSV(url, timestart, timeend, series_id) {
        return __awaiter(this, void 0, void 0, function* () {
            const data = yield (0, accessor_utils_1.fetchData)(url);
            const parsed_data = (0, csv_string_1.parse)(data);
            return this.arrayToObs(parsed_data, this.date_col, this.value_col, this.skip_rows, timestart, timeend, series_id);
        });
    }
    arrayToObs(csv_data, date_col = 0, value_col = 2, skip_rows = 4, timestart, timeend, series_id) {
        const result = [];
        for (const [index, row] of csv_data.entries()) {
            if (index < skip_rows) {
                continue;
            }
            const date = new Date(row[date_col]);
            if (date.toString() == 'Invalid Date') {
                console.warn("Invalid date: " + row[date_col] + ", skipping.");
                continue;
            }
            if (timestart && date < timestart) {
                continue;
            }
            if (timeend && date > timeend) {
                continue;
            }
            const value = parseFloat(row[value_col]);
            if (value.toString() == "NaN") {
                console.warn("Invalid value: " + row[value_col] + ", skipping.");
                continue;
            }
            result.push({
                "timestart": date,
                "valor": value,
                "series_id": series_id
            });
        }
        return result;
    }
    getSerieFromId(s_id) {
        for (const [k, v] of Object.entries(this.series_map)) {
            if (v.series_id == s_id) {
                return Object.assign(Object.assign({}, v), { estacion_id: parseInt(k) });
            }
        }
        throw new Error("Serie not found with id=" + s_id);
    }
    getOne(estacion_id, timestart, timeend) {
        return __awaiter(this, void 0, void 0, function* () {
            const serie = this.series_map[estacion_id];
            if (!serie) {
                throw new Error("Serie not found in series_map configuration for estacion_id=" + estacion_id);
            }
            const obs = yield this.downloadDataAndParseCSV(serie.url, timestart, timeend, serie.series_id);
            return {
                id: serie.series_id,
                observaciones: obs
            };
        });
    }
    get(filter, options = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            const result = [];
            if (!filter.series_id) {
                if (!filter.estacion_id) {
                    throw new Error("missing estacion_id or series_id");
                }
                if (filter.estacion_id instanceof Array) {
                    for (const e_id of filter.estacion_id) {
                        result.push(yield this.getOne(e_id, filter.timestart, filter.timeend));
                    }
                }
                else {
                    result.push(yield this.getOne(filter.estacion_id, filter.timestart, filter.timeend));
                }
            }
            else {
                if (filter.series_id instanceof Array) {
                    for (const s_id of filter.series_id) {
                        const serie = this.getSerieFromId(s_id);
                        result.push(yield this.getOne(serie.estacion_id));
                    }
                }
                else {
                    const serie = this.getSerieFromId(filter.series_id);
                    result.push(yield this.getOne(serie.estacion_id));
                }
            }
            if (options.return_series) {
                return result;
            }
            else {
                const all_obs = [];
                for (const serie of result) {
                    all_obs.push(...serie.observaciones);
                }
                return all_obs;
            }
        });
    }
    readStationsCsv(filepath, has_header = true, lon_col = 1, lat_col = 2, columns = ["nombre", "longitud", "latitud", "url"], output) {
        const datastr = (0, fs_1.readFileSync)(filepath, { encoding: "utf-8" });
        const data = (0, csv_string_1.parse)(datastr);
        const features = [];
        for (const [i, row] of data.entries()) {
            if (has_header && i == 0) {
                continue;
            }
            const properties = Object.fromEntries(columns.map((key, i) => [key, row[i]]));
            properties.tabla = "agp";
            properties.id_externo = properties.url;
            features.push({
                type: "Feature",
                properties: properties,
                geometry: {
                    type: "Point",
                    coordinates: [
                        parseFloat(row[lon_col]),
                        parseFloat(row[lat_col])
                    ]
                }
            });
        }
        const fc = {
            type: "FeatureCollection",
            features: features
        };
        if (output) {
            (0, fs_1.writeFileSync)(output, JSON.stringify(fc));
        }
        return fc;
    }
    createSites(json_filepath) {
        return __awaiter(this, void 0, void 0, function* () {
            const estaciones = CRUD_1.estacion.fromGeojson(json_filepath);
            for (const e of estaciones) {
                yield e.create();
            }
            return estaciones;
        });
    }
    createSeries(create = false) {
        return __awaiter(this, void 0, void 0, function* () {
            const estaciones = yield CRUD_1.estacion.read({ tabla: "agp" });
            const series = [];
            for (const e of estaciones) {
                const serie = new CRUD_1.serie({
                    estacion_id: e.id,
                    var_id: 2,
                    proc_id: 1,
                    unit_id: 11
                });
                if (create) {
                    yield serie.create();
                }
                series.push(serie);
            }
            return series;
        });
    }
}
exports.Client = Client;
