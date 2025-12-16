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
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
var __await = (this && this.__await) || function (v) { return this instanceof __await ? (this.v = v, this) : new __await(v); }
var __asyncGenerator = (this && this.__asyncGenerator) || function (thisArg, _arguments, generator) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var g = generator.apply(thisArg, _arguments || []), i, q = [];
    return i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i;
    function verb(n) { if (g[n]) i[n] = function (v) { return new Promise(function (a, b) { q.push([n, v, a, b]) > 1 || resume(n, v); }); }; }
    function resume(n, v) { try { step(g[n](v)); } catch (e) { settle(q[0][3], e); } }
    function step(r) { r.value instanceof __await ? Promise.resolve(r.value.v).then(fulfill, reject) : settle(q[0][2], r); }
    function fulfill(value) { resume("next", value); }
    function reject(value) { resume("throw", value); }
    function settle(f, v) { if (f(v), q.shift(), q.length) resume(q[0][0], q[0][1]); }
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Client = exports.parseEmas = void 0;
const abstract_accessor_engine_1 = require("./abstract_accessor_engine");
const accessor_utils_1 = require("../accessor_utils");
const CRUD_1 = require("../CRUD");
const node_fetch_1 = __importDefault(require("node-fetch"));
const fs_1 = __importDefault(require("fs"));
const readline_1 = __importDefault(require("readline"));
const VARIABLES = [
    "tempmedia", "tempmax", "tempmin", "humrel", "puntorocio", "velvientomedia", "dirviento",
    "recviento", "velvientomax", "dirvientomax", "sensterm", "indcalor", "indthw", "indthws",
    "presion", "precip", "intprecip", "radsolar", "enersolar", "maxradsolar", "graddcalor",
    "graddfrio", "tempint", "humint", "rocioint", "incalint", "et", "humsuelo1", "humsuelo2",
    "humsuelo3", "tempsuelo1", "tempsuelo2", "tempsuelo3", "humhoja", "muestviento",
    "txviento", "recepiss", "intarc"
];
const COMPASS_16 = {
    N: 0,
    NNE: 22.5,
    NE: 45,
    ENE: 67.5,
    E: 90,
    ESE: 112.5,
    SE: 135,
    SSE: 157.5,
    S: 180,
    SSW: 202.5,
    SW: 225,
    WSW: 247.5,
    W: 270,
    WNW: 292.5,
    NW: 315,
    NNW: 337.5
};
function dirToDegrees(direction) {
    if (!(direction in COMPASS_16)) {
        throw new Error("Bad compass direction");
    }
    return COMPASS_16[direction];
}
function parseDate(dateStr, timeStr, USADateFormat = false) {
    // dateStr: "m/d/yy"
    // timeStr: "hh:mm"
    if (USADateFormat) {
        var [m, d, yy] = dateStr.split("/").map(Number);
    }
    else {
        var [d, m, yy] = dateStr.split("/").map(Number);
    }
    const [hh, mm] = timeStr.split(":").map(Number);
    const yyyy = 2000 + yy;
    const dt = new Date(Date.UTC(yyyy, m - 1, d, hh, mm));
    if (isNaN(dt.getTime()))
        return null;
    return dt;
    //   const pad = (n: number) => String(n).padStart(2, "0");
    //   return `${dt.getUTCFullYear()}-${pad(dt.getUTCMonth() + 1)}-${pad(dt.getUTCDate())} `
    //        + `${pad(dt.getUTCHours())}:${pad(dt.getUTCMinutes())}`;
}
function readLinesLocalFile(path) {
    return __asyncGenerator(this, arguments, function* readLinesLocalFile_1() {
        var e_1, _a;
        const rl = readline_1.default.createInterface({
            input: fs_1.default.createReadStream(path),
            crlfDelay: Infinity,
        });
        try {
            for (var rl_1 = __asyncValues(rl), rl_1_1; rl_1_1 = yield __await(rl_1.next()), !rl_1_1.done;) {
                const line = rl_1_1.value;
                yield yield __await(line);
            }
        }
        catch (e_1_1) { e_1 = { error: e_1_1 }; }
        finally {
            try {
                if (rl_1_1 && !rl_1_1.done && (_a = rl_1.return)) yield __await(_a.call(rl_1));
            }
            finally { if (e_1) throw e_1.error; }
        }
    });
}
function readLinesUrl(url) {
    return __asyncGenerator(this, arguments, function* readLinesUrl_1() {
        const res = yield __await((0, node_fetch_1.default)(url));
        if (!res.ok)
            throw new Error(`Failed to fetch ${url}: ${res.statusText}`);
        const text = yield __await(res.text());
        for (const line of text.split(/\r?\n/)) {
            yield yield __await(line);
        }
    });
}
/**
 * Parse EMAS file (local or URL) into a table-like object.
 */
function parseEmas(source, // local file path OR http(s) URL
stationId, variables = VARIABLES, timestart, timeend) {
    var e_2, _a;
    return __awaiter(this, void 0, void 0, function* () {
        const rows = [];
        const isUrl = /^https?:\/\//i.test(source);
        const lineGenerator = isUrl
            ? readLinesUrl(source)
            : readLinesLocalFile(source);
        try {
            for (var lineGenerator_1 = __asyncValues(lineGenerator), lineGenerator_1_1; lineGenerator_1_1 = yield lineGenerator_1.next(), !lineGenerator_1_1.done;) {
                let line = lineGenerator_1_1.value;
                line = line.trim();
                if (!line)
                    continue;
                const parts = line.split(/\s+/);
                if (!/^\d{1,2}\/\d{1,2}\/\d\d$/.test(parts[0]))
                    continue; // skip non-date lines
                const dateStr = parts[0];
                const timeStr = parts[1];
                const timestamp = parseDate(dateStr, timeStr);
                if (!timestamp)
                    continue;
                if (timestart && timestamp.getTime() < timestart.getTime())
                    continue;
                if (timeend && timestamp.getTime() > timeend.getTime())
                    continue;
                const values = parts.slice(2);
                const record = {};
                for (let i = 0; i < values.length; i++) {
                    const val = values[i];
                    if (val === "---")
                        continue;
                    const name = VARIABLES[i];
                    if (!name)
                        continue;
                    if (/^[+-]?\d+(?:\.\d+)?$/.test(val)) {
                        record[name] = parseFloat(val);
                    }
                    else {
                        record[name] = val;
                    }
                }
                rows.push({
                    date_time: timestamp,
                    values: record
                });
            }
        }
        catch (e_2_1) { e_2 = { error: e_2_1 }; }
        finally {
            try {
                if (lineGenerator_1_1 && !lineGenerator_1_1.done && (_a = lineGenerator_1.return)) yield _a.call(lineGenerator_1);
            }
            finally { if (e_2) throw e_2.error; }
        }
        return { station_id: stationId, rows };
    });
}
exports.parseEmas = parseEmas;
class Client extends abstract_accessor_engine_1.AbstractAccessorEngine {
    constructor(config) {
        super(config);
        this.default_config = {
            "stations_file": "data/emas/stations.json",
            "url": "https://www.hidraulica.gob.ar/ema",
            "variable_lists": {
                "basabil": [
                    "tempmedia", "tempmax", "tempmin", "humrel", "puntorocio", "velvientomedia", "dirviento",
                    "recviento", "velvientomax", "dirvientomax", "sensterm", "indcalor", "indthw", "indthws",
                    "presion", "precip", "intprecip", "radsolar", "enersolar", "maxradsolar", "graddcalor",
                    "graddfrio", "tempint", "humint", "rocioint", "incalint", "et", "humsuelo1", "humsuelo2",
                    "humsuelo3", "tempsuelo1", "tempsuelo2", "tempsuelo3", "humhoja", "muestviento",
                    "txviento", "recepiss", "intarc"
                ],
                "bovril": [
                    "tempmedia", "tempmax", "tempmin", "humrel", "puntorocio", "velvientomedia", "dirviento",
                    "recviento", "velvientomax", "dirvientomax", "sensterm", "indcalor", "indthw", "indthws",
                    "presion", "precip", "intprecip", "radsolar", "enersolar", "maxradsolar", "indiceuv",
                    "dosisuv", "uvmax", "graddcalor", "graddfrio", "tempint", "humint", "rocioint", "incalint",
                    "et", "humsuelo1", "humsuelo2", "humsuelo3", "tempsuelo1", "tempsuelo2", "tempsuelo3",
                    "humhoja", "muestviento", "txviento", "recepiss", "intarc"
                ],
                "colon": ["tempmedia", "tempmax", "tempmin", "humrel", "puntorocio", "velvientomedia", "dirviento", "recviento", "velvientomax", "dirvientomax", "sensterm", "indcalor", "indthw", "indthws", "presion", "precip", "intprecip", "radsolar", "enersolar", "maxradsolar", "graddcalor", "graddfrio", "tempint", "humint", "rocioint", "incalint", "et", "muestviento", "txviento", "recepiss", "intarc"],
                "galarza": ["tempmedia", "tempmax", "tempmin", "humrel", "puntorocio", "velvientomedia", "dirviento", "recviento", "velvientomax", "dirvientomax", "sensterm", "indcalor", "indthw", "indthws", "presion", "precip", "intprecip", "radsolar", "enersolar", "maxradsolar", "indiceuv", "dosisuv", "uvmax", "graddcalor", "graddfrio", "tempint", "humint", "rocioint", "incalint", "emcint", "densintaire", "et", "humsuelo1", "humsuelo2", "humsuelo3", "tempsuelo1", "tempsuelo2", "tempsuelo3", "humhoja", "muestviento", "txviento", "recepiss", "intarc"],
                "gualeguay": ["tempmedia", "tempmax", "tempmin", "humrel", "puntorocio", "velvientomedia", "dirviento", "recviento", "velvientomax", "dirvientomax", "sensterm", "indcalor", "indthw", "indthws", "presion", "precip", "intprecip", "radsolar", "enersolar", "maxradsolar", "indiceuv", "dosisuv", "uvmax", "graddcalor", "graddfrio", "tempint", "humint", "rocioint", "incalint", "emcint", "densintaire", "et", "humsuelo1", "humsuelo2", "humsuelo3", "tempsuelo1", "tempsuelo2", "tempsuelo3", "humhoja", "muestviento", "txviento", "recepiss", "intarc"],
                "gualeguaychu": ["tempmedia", "tempmax", "tempmin", "humrel", "puntorocio", "velvientomedia", "dirviento", "recviento", "velvientomax", "dirvientomax", "sensterm", "indcalor", "indthw", "presion", "precip", "intprecip", "graddcalor", "graddfrio", "tempint", "humint", "rocioint", "incalint", "muestviento", "txviento", "recepiss", "intarc"]
            },
            "variable_map": {
                53: { name: "tempmedia", unit_id: 12 },
                58: { name: "humrel", unit_id: 15 },
                43: { name: "puntorocio", unit_id: 12 },
                55: { name: "velvientomedia", unit_id: 13 },
                57: { name: "dirviento", unit_id: 16 },
                68: { name: "presion", unit_id: 17 },
                27: { name: "precip", unit_id: 9 },
                14: { name: "radsolar", unit_id: 144 }
            },
            "station_map": {
                928: {
                    "url": "https://www.hidraulica.gob.ar/ema/ema-curuguay/downld08.txt",
                    "variable_list_key": "bovril"
                },
                930: {
                    "url": "https://www.hidraulica.gob.ar/ema/ema-villaparanacito/downld08.txt",
                    "variable_list_key": "bovril"
                },
                931: {
                    "url": "https://www.hidraulica.gob.ar/ema/ema-basavilbaso/downld08.txt",
                    "variable_list_key": "basavil"
                },
                932: {
                    "url": "https://www.hidraulica.gob.ar/ema/ema-urdinarrain/downld08.txt",
                    "variable_list_key": "basavil"
                },
                914: {
                    "url": "https://www.hidraulica.gob.ar/ema/ema-macia/downld08.txt",
                    "variable_list_key": "bovril"
                }
            }
        };
        this.setConfig(config);
    }
    getData(station_id, timestart, timeend) {
        return __awaiter(this, void 0, void 0, function* () {
            const station_config = this.config.station_map[station_id];
            if (!station_config) {
                throw new Error("station_id = " + station_id + " not found in configuration");
            }
            const variables = this.config.variable_lists[station_config.variable_list_key];
            if (!variables) {
                throw new Error("variable_list_key = " + station_config.variable_list_key + " not found in configuration");
            }
            const emas_table = yield parseEmas(station_config.url, // local file path OR http(s) URL
            station_id, variables, timestart, timeend);
            return emas_table;
        });
    }
    extractSerieFromTable(emas_table, var_id, series_id) {
        const variable = this.config.variable_map[var_id];
        if (!variable) {
            throw new Error("var_id" + var_id + " not found in variable_map");
        }
        const obs = []
        for(const row of emas_table.rows) {
            if(row.values[variable.name] == null) {
                continue
            }
            const valor = (["dirviento", "dirvientomax"].indexOf(variable.name) >= 0) ? dirToDegrees(row.values[variable.name].toString()) : Number(row.values[variable.name]);
            obs.push({
                timestart: row.date_time,
                timeend: row.date_time,
                valor: valor,
                series_id: series_id
            })
        }
        return obs
    }
    getSites(filter = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.config.stations_file) {
                throw new Error("Missing stations_file in config");
            }
            if (!fs_1.default.existsSync(this.config.stations_file)) {
                throw new Error("Stations file not found");
            }
            const stations_ = fs_1.default.readFileSync(this.config.stations_file, { encoding: "utf-8" });
            const stations = JSON.parse(stations_);
            if (!Array.isArray(stations)) {
                throw new Error("stations file must be a json array");
            }
            const crud_stations = (0, accessor_utils_1.filterSites)(stations, filter).map((s) => new CRUD_1.estacion(s));
            for (const s of crud_stations) {
                yield s.getId();
            }
            return crud_stations;
        });
    }
    getSeries(filter = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            const stations = yield this.getSites({ estacion_id: filter.estacion_id });
            var series = [];
            for (const station of stations) {
                const station_series = Object.keys(this.config.variable_map).map(var_id => {
                    return {
                        estacion_id: station.id,
                        var_id: var_id,
                        unit_id: this.config.variable_map[var_id].unit_id,
                        proc_id: 1
                    };
                });
                series.push(...station_series);
            }
            var crud_series = series.map(s => new CRUD_1.serie(s));
            for (const s of crud_series) {
                yield s.getId(false);
            }
            crud_series = (0, accessor_utils_1.filterSeries)(crud_series, filter);
            return crud_series;
        });
    }
    getSavedSeries(filter = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            const station_ids = Object.keys(this.config.station_map);
            var series = yield CRUD_1.serie.read({ estacion_id: station_ids });
            series = (0, accessor_utils_1.filterSeries)(series, filter);
            const series_map = {};
            for (const serie of series) {
                series_map[serie.id] = {
                    tipo: serie.tipo,
                    estacion_id: serie.estacion.id,
                    var_id: serie.var.id,
                    unit_id: serie.unidades.id,
                    proc_id: serie.procedimiento.id
                };
            }
            this.series_map = series_map;
            return series;
        });
    }
    get(filter, options) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.series_map) {
                yield this.getSavedSeries({ series_id: filter.series_id });
            }
            const observaciones = [];
            const series = [];
            if (filter.series_id) {
                if (Array.isArray(filter.series_id)) {
                    for (const s_id of filter.series_id) {
                        const serie = this.series_map[s_id];
                        if (!serie) {
                            throw new Error("series_id " + filter.series_id + " not found");
                        }
                        const emas_table = yield this.getData(serie.estacion_id, filter.timestart, filter.timeend);
                        const obs = this.extractSerieFromTable(emas_table, serie.var_id, s_id);
                        if (options && options.return_series) {
                            series.push(Object.assign(Object.assign({}, serie), { observaciones: obs }));
                        }
                        else {
                            observaciones.push(...obs);
                        }
                    }
                }
                else {
                    const serie = this.series_map[filter.series_id];
                    if (!serie) {
                        throw new Error("series_id " + filter.series_id + " not found");
                    }
                    const emas_table = yield this.getData(serie.estacion_id, filter.timestart, filter.timeend);
                    const obs = this.extractSerieFromTable(emas_table, serie.var_id, filter.series_id);
                    if (options && options.return_series) {
                        series.push(Object.assign(Object.assign({}, serie), { observaciones: obs }));
                    }
                    else {
                        observaciones.push(...obs);
                    }
                }
            }
            else {
                // filter series and group by estacion_id
                const grouped = (0, accessor_utils_1.filterSeriesByIds)(Object.entries(this.series_map).map(([key, obj]) => (Object.assign({ id: key }, obj))), filter).reduce((acc, obj) => {
                    var _a;
                    const key = obj.estacion_id;
                    ((_a = acc[key]) !== null && _a !== void 0 ? _a : (acc[key] = [])).push(obj);
                    return acc;
                }, {});
                const observaciones = [];
                // iter estaciones
                for (const estacion_id of Object.keys(grouped)) {
                    const e_id = Number(estacion_id);
                    const emas_table = yield this.getData(e_id, filter.timestart, filter.timeend);
                    for (const serie of grouped[estacion_id]) {
                        const obs = this.extractSerieFromTable(emas_table, serie.var_id);
                        if (options && options.return_series) {
                            series.push(Object.assign(Object.assign({}, serie), { observaciones: obs }));
                        }
                        else {
                            observaciones.push(...obs);
                        }
                    }
                }
            }
            if (options && options.return_series) {
                return series;
            }
            else {
                return observaciones;
            }
        });
    }
    createAccessor() {
        return __awaiter(this, void 0, void 0, function* () {
            const emas_accessor = new CRUD_1.accessor({
                name: "emas",
                class: "emas",
                url: this.config.url,
                config: this.config,
                series_tipo: "puntual",
                title: "Estaciones Meteorológicas Automáticas DPH Entre Ríos"
            });
            const created = yield emas_accessor.create();
            return created;
        });
    }
}
exports.Client = Client;
Client._get_is_multiseries = true;
