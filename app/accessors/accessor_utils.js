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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.filterSeriesByIds = exports.filterSites = exports.filterSeries = exports.filterByParam = exports.parseUtcDateTime = exports.fetchData = void 0;
const axios_1 = __importDefault(require("axios"));
const https_1 = __importDefault(require("https"));
const boolean_point_in_polygon_1 = require("@turf/boolean-point-in-polygon");
function fetchData(url, options) {
    var _a, _b;
    return __awaiter(this, void 0, void 0, function* () {
        const agent = new https_1.default.Agent({
            rejectUnauthorized: !(options === null || options === void 0 ? void 0 : options.disable_validation),
        });
        try {
            const response = yield axios_1.default.get(url, Object.assign(Object.assign({}, options), { httpsAgent: agent }));
            return response.data;
        }
        catch (err) {
            const error = err;
            if (error.response) {
                const status = error.response.status;
                const message = (_b = (_a = error.response.data) === null || _a === void 0 ? void 0 : _a.message) !== null && _b !== void 0 ? _b : error.message;
                throw new Error(`Request failed with status ${status}: ${message}`);
            }
            throw new Error(error.message || "Unknown error");
        }
    });
}
exports.fetchData = fetchData;
function parseUtcDateTime(s) {
    const [date, time] = s.split(" ");
    const [year, month, day] = date.split("-").map(Number);
    const [hour, minute, second] = time.split(":").map(Number);
    return new Date(Date.UTC(year, month - 1, day, hour, minute, second));
}
exports.parseUtcDateTime = parseUtcDateTime;
function filterByParam(filter_value, item_value, func, is_numeric) {
    if (filter_value == undefined || (Array.isArray(filter_value) && filter_value.length == 0)) {
        return true;
    }
    if (func) {
        return func(filter_value, item_value);
    }
    else if (item_value == undefined) {
        return false;
    }
    if (Array.isArray(filter_value)) {
        if (is_numeric) {
            const filter_values_num = filter_value.map(v => parseFloat(v));
            if (filter_values_num.map(v => v.toString()).indexOf("NaN") >= 0) {
                throw new Error("Invalid filter, must be numeric");
            }
            if (filter_values_num.indexOf(parseFloat(item_value)) >= 0) {
                return true;
            }
        }
        else {
            if (filter_value.indexOf(item_value) >= 0) {
                return true;
            }
        }
    }
    else if (is_numeric) {
        const filter_value_num = parseFloat(filter_value);
        if (filter_value_num.toString() == "NaN") {
            throw new Error("Invalid filter, must be numeric");
        }
        if (parseFloat(item_value) == filter_value_num) {
            return true;
        }
    }
    else if (item_value == filter_value) {
        return true;
    }
    return false;
}
exports.filterByParam = filterByParam;
function filterSeries(series = [], params = {}) {
    return series.filter(serie => {
        return ([
            filterByParam(params.estacion_id, serie.estacion.id, undefined, true),
            filterByParam(params.var_id, serie.var.id, undefined, true),
            filterByParam(params.unit_id, serie.unidades.id, undefined, true),
            filterByParam(params.id_externo, serie.estacion.id_externo),
            filterByParam(params.series_id, serie.id, undefined, true),
            filterByParam(params.id, serie.id, undefined, true),
            filterByParam(params.tipo, serie.tipo)
        ].indexOf(false) < 0);
    });
}
exports.filterSeries = filterSeries;
function pointInPolygon(filter_geom, item_geom) {
    return (0, boolean_point_in_polygon_1.booleanPointInPolygon)(item_geom, filter_geom);
}
function filterSites(sites = [], params = {}) {
    return sites.filter(s => {
        return ([
            filterByParam(params.name, s.name),
            filterByParam(params.nombre, s.nombre),
            filterByParam(params.id_externo, s.id_externo),
            filterByParam(params.estacion_id, s.id),
            filterByParam(params.id, s.id),
            filterByParam(params.geom, s.geom, pointInPolygon)
        ].indexOf(false) < 0);
    });
}
exports.filterSites = filterSites;
function filterSeriesByIds(series = [], params = {}) {
    return series.filter(serie => {
        return ([
            filterByParam(params.estacion_id, serie.estacion_id),
            filterByParam(params.var_id, serie.var_id),
            filterByParam(params.unit_id, serie.unit_id),
            filterByParam(params.series_id, serie.id),
            filterByParam(params.id, serie.id),
            filterByParam(params.tipo, serie.tipo)
        ].indexOf(false) < 0);
    });
}
exports.filterSeriesByIds = filterSeriesByIds;
