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
function parseDate(dateStr, timeStr) {
    // dateStr: "m/d/yy"
    // timeStr: "hh:mm"
    const [m, d, yy] = dateStr.split("/").map(Number);
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
        const res = yield __await(fetch(url));
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
    constructor() {
        super(...arguments);
        this.default_config = {
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
                53: "tempmedia",
                58: "humrel",
                43: "puntorocio",
                55: "velvientomedia",
                57: "dirviento",
                68: "presion",
                27: "precip",
                14: "radsolar"
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
        return emas_table.rows.map(row => {
            return {
                timestart: row.date_time,
                timeend: row.date_time,
                valor: row[variable],
                series_id: series_id
            };
        });
    }
    getSeries(filter = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            const station_ids = Object.keys(this.config.station_map);
            var series = yield CRUD_1.serie.read({ estacion_id: station_ids }, { no_metadata: true });
            series = (0, accessor_utils_1.filterSeries)(series, filter);
            const series_map = {};
            for (const serie of series) {
                series_map[serie.id] = {
                    estacion_id: serie.estacion.id,
                    var_id: serie.var.id
                };
            }
            this.series_map = series_map;
            return series;
        });
    }
    get(filter) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!filter || !filter.series_id) {
                throw new Error("Missing series_id");
            }
            if (!this.series_map) {
                yield this.getSeries({ series_id: filter.series_id });
            }
            if (Array.isArray(filter.series_id)) {
                const observaciones = [];
                for (const s_id of filter.series_id) {
                    const serie = this.series_map[s_id];
                    if (!serie) {
                        throw new Error("series_id " + filter.series_id + " not found");
                    }
                    const emas_table = yield this.getData(serie.estacion_id, filter.timestart, filter.timeend);
                    observaciones.push(...(this.extractSerieFromTable(emas_table, serie.var_id, s_id)));
                }
                return observaciones;
            }
            else {
                const serie = this.series_map[filter.series_id];
                if (!serie) {
                    throw new Error("series_id " + filter.series_id + " not found");
                }
                const emas_table = yield this.getData(serie.estacion_id, filter.timestart, filter.timeend);
                return this.extractSerieFromTable(emas_table, serie.var_id, filter.series_id);
            }
        });
    }
}
exports.Client = Client;
