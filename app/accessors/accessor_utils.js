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
exports.groupBySeriesIdAndQualifier = exports.flatten = exports.grib2obs = exports.rast2obs = exports.downloadAndWriteStream = exports.filterSeriesByIds = exports.filterSites = exports.filterSeries = exports.filterByParam = exports.parseUtcDateTime = exports.fetchData = void 0;
const axios_1 = __importDefault(require("axios"));
const https_1 = __importDefault(require("https"));
const boolean_point_in_polygon_1 = require("@turf/boolean-point-in-polygon");
const node_fs_1 = require("node:fs");
const promises_1 = require("node:stream/promises");
const child_process_promise_1 = require("child-process-promise");
const CRUD_1 = require("../CRUD");
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
function downloadAndWriteStream(url, params, localfilepath, connection) {
    return __awaiter(this, void 0, void 0, function* () {
        if (!connection) {
            connection = axios_1.default.create();
        }
        const writer = (0, node_fs_1.createWriteStream)(localfilepath);
        let response;
        try {
            response = yield connection.get(url, {
                params,
                responseType: "stream",
            });
        }
        catch (e) {
            const error = e instanceof Error ? e : new Error(String(e));
            console.error(`Download error: ${error}`);
            throw error;
        }
        try {
            yield (0, promises_1.pipeline)(response.data, writer);
        }
        catch (e) {
            const error = e instanceof Error ? e : new Error(String(e));
            throw new Error(`file:${localfilepath} write failed, error:${error.message}`);
        }
    });
}
exports.downloadAndWriteStream = downloadAndWriteStream;
function rast2obs(filename, series_id, to_prono, qualifier) {
    return __awaiter(this, void 0, void 0, function* () {
        // LEE GTIFF , GENERA observación  
        const gdalinfo_result = yield (0, child_process_promise_1.exec)(`gdalinfo -json ${filename}`);
        var stdout = gdalinfo_result.stdout;
        var stderr = gdalinfo_result.stderr;
        if (stderr) {
            console.error(stderr);
        }
        var gdalinfo = JSON.parse(stdout);
        var band = gdalinfo.bands[0];
        var ref_time = new Date(parseInt(band.metadata[""].GRIB_REF_TIME.split(/\s/)[0]) * 1000);
        var valid_time = new Date(parseInt(band.metadata[""].GRIB_VALID_TIME.split(/\s/)[0]) * 1000);
        const data = (0, node_fs_1.readFileSync)(filename, 'hex');
        if (to_prono) {
            return new CRUD_1.pronostico({
                tipo: "raster",
                // timeupdate: ref_time,
                timestart: new Date(valid_time),
                timeend: new Date(valid_time),
                series_id: series_id,
                valor: `\\x${data}`,
                qualifier: qualifier
            });
        }
        return new CRUD_1.observacion({
            tipo: "raster",
            timeupdate: ref_time,
            timestart: new Date(valid_time),
            timeend: new Date(valid_time),
            series_id: series_id,
            valor: `\\x${data}`
        });
    });
}
exports.rast2obs = rast2obs;
function grib2obs(filepath, variable_map, bbox, // [leftlon, toplat, rightlon, bottomlat]
units, to_prono, qualifier) {
    return __awaiter(this, void 0, void 0, function* () {
        if (!filepath) {
            return Promise.reject("Falta filepath");
        }
        if (!variable_map) {
            return Promise.reject("Falta variable_map");
        }
        units = units !== null && units !== void 0 ? units : "meters_per_second";
        const gdalinfo_result = yield (0, child_process_promise_1.exec)(`gdalinfo -json ${filepath}`);
        var stdout = gdalinfo_result.stdout;
        var stderr = gdalinfo_result.stderr;
        var gdalinfo = JSON.parse(stdout);
        //~ var time_update = new Date(parseInt(gdalinfo.bands[0].metadata[""].GRIB_REF_TIME.split(/\s/)[0])*1000)
        const observaciones = [];
        for (var band of gdalinfo.bands) {
            // var ref_time = new Date(parseInt(band.metadata[""].GRIB_REF_TIME.split(/\s/)[0])*1000)
            // var valid_time = new Date(parseInt(band.metadata[""].GRIB_VALID_TIME.split(/\s/)[0])*1000)
            var var_index = Object.keys(variable_map).indexOf(band.metadata[""].GRIB_ELEMENT);
            if (var_index < 0) {
                console.warn("band not mapped:" + band.metadata[""].GRIB_ELEMENT);
                continue;
            }
            var variable = variable_map[band.metadata[""].GRIB_ELEMENT];
            var gtiff_filename = filepath.replace(/\.grib2$/, "." + variable.name.replace(new RegExp(/\s/g), "") + ".tif");
            var bbox_options = "";
            if (bbox) {
                bbox_options = `-a_ullr ${bbox[0]} ${bbox[1]} ${bbox[2]} ${bbox[3]}`;
            }
            yield (0, child_process_promise_1.exec)(`gdal_translate -b ${band.band} -a_srs EPSG:4326 ${bbox_options} -of GTiff ${filepath} "${gtiff_filename}"`);
            yield (0, child_process_promise_1.exec)(`gdal_edit.py -mo "UNITS=${units}" ${gtiff_filename}`);
            observaciones.push(yield rast2obs(gtiff_filename, variable.series_id, to_prono, qualifier));
        }
        console.log("got " + observaciones.length + " observaciones");
        return observaciones;
    });
}
exports.grib2obs = grib2obs;
function flatten(arr) {
    const result = [];
    for (const item of arr) {
        if (Array.isArray(item)) {
            result.push(...flatten(item));
        }
        else {
            result.push(item);
        }
    }
    return result;
}
exports.flatten = flatten;
//   return arr.reduce(function (flat, toFlatten) {
//     return flat.concat(Array.isArray(toFlatten) ? flatten(toFlatten) : toFlatten);
//   }, []);
// }
function groupBySeriesIdAndQualifier(pronosticos, series_id, series_table = "series") {
    const series = [];
    for (const pronostico of pronosticos) {
        const existing_serie = series.find(s => s.series_id == pronostico.series_id && s.qualifier == pronostico.qualifier);
        if (existing_serie) {
            existing_serie.pronosticos.push(pronostico);
        }
        else {
            series.push(new CRUD_1.SerieTemporalSim({
                series_id: pronostico.series_id,
                series_table: series_table,
                qualifier: pronostico.qualifier,
                pronosticos: [pronostico]
            }));
        }
    }
    return series;
}
exports.groupBySeriesIdAndQualifier = groupBySeriesIdAndQualifier;
