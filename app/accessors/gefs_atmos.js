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
var _a;
Object.defineProperty(exports, "__esModule", { value: true });
exports.Client = void 0;
const abstract_accessor_engine_1 = require("./abstract_accessor_engine");
const child_process_promise_1 = require("child-process-promise");
const CRUD_1 = require("../CRUD");
const axios_1 = __importDefault(require("axios"));
const sprintf_js_1 = require("sprintf-js");
const fs_1 = require("fs");
const accessor_utils_1 = require("./accessor_utils");
class Client extends abstract_accessor_engine_1.AbstractAccessorEngine {
    constructor(config) {
        super(config);
        this.default_variable_map = {
            "APCP": {
                name: "apcp",
                var_id: 91,
                proc_id: 4,
                unit_id: 9,
                series_id: 55
            }
        };
        this.default_config = {
            url: "https://nomads.ncep.noaa.gov/cgi-bin/filter_gefs_atmos_0p25s.pl",
            // files_url: "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gens/prod/",
            data_dir: "/../data/gefs_atmos/",
            bbox: { leftlon: -70, rightlon: -40, toplat: -10, bottomlat: -40 },
            start_hour: 6,
            end_hour: 241,
            dt: 6,
            levels: ["surface"],
            variables: ["APCP"],
            variable_map: this.default_variable_map,
            ens: 31
        };
        this.max_hour = 240;
        this.connection = axios_1.default.create();
        this.config = Object.assign(Object.assign({}, this.default_config), config);
        this.url = this.config.url;
        this.start_hour = this.config.start_hour || 6;
        this.end_hour = this.config.end_hour || 241;
        this.dt = this.config.dt || 6;
        this.ens = this.config.ens || 31;
        this.variable_map = this.config.variable_map || this.default_variable_map;
    }
    createSerie() {
        return __awaiter(this, void 0, void 0, function* () {
            const fuente = yield Client.fuente.create();
            const escena = yield Client.escena.create();
            const series = yield CRUD_1.serie.create([Client.serie]);
            if (!series.length) {
                throw new Error("Nothing created");
            }
            return series[0];
        });
    }
    test() {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                yield this.connection.get(this.config.url);
            }
            catch (e) {
                console.error("accessor test failed");
                return false;
            }
            console.info("accessor test ok");
            return true;
        });
    }
    get(filter = {}, options = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            var dates = this.getDates(filter);
            var forecast_date = dates.forecast_date;
            var timestart = dates.timestart;
            var timeend = dates.timeend;
            var dates_dir = dates.dates_dir;
            var times_dir = dates.times_dir;
            var forecast_date_path = dates.forecast_date_path;
            var forecast_time_path = dates.forecast_time_path;
            if (!(0, fs_1.existsSync)(forecast_date_path)) {
                (0, fs_1.mkdirSync)(forecast_date_path);
            }
            if (!(0, fs_1.existsSync)(forecast_time_path)) {
                (0, fs_1.mkdirSync)(forecast_time_path);
            }
            console.info("accessors.gefs_wave.get: path: " + this.path);
            var hours = [];
            for (var i = this.start_hour; i <= this.end_hour; i = i + this.dt) {
                if (i > this.max_hour) {
                    continue;
                }
                const forecast_time = new Date(forecast_date);
                forecast_time.setHours(forecast_time.getHours() + i);
                // SKIPS DATES OUT OF RANGE
                if (timestart && forecast_time < timestart) {
                    continue;
                }
                if (timeend && forecast_time > timeend) {
                    continue;
                }
                hours.push(i);
            }
            const results = [];
            var member = 1;
            while (member <= this.ens) {
                for (const i of hours) {
                    var file = (0, sprintf_js_1.sprintf)("gep%02d.t%02dz.pgrb2s.0p25.f%03d.grib2", member, forecast_date.getUTCHours(), i);
                    console.debug(`file: ${file}`);
                    var params = {
                        file: file,
                        subregion: "",
                        dir: "/" + dates_dir + times_dir + "wave/gridded"
                    };
                    if (this.config.bbox) {
                        params = Object.assign(Object.assign({}, params), this.config.bbox);
                    }
                    if (this.config.levels) {
                        this.config.levels.forEach(level => {
                            params["lev_" + level] = "on";
                        });
                    }
                    if (this.config.variables) {
                        this.config.variables.forEach(variable => {
                            params["var_" + variable] = "on";
                        });
                    }
                    var localfilepath = __dirname + this.config.data_dir + dates_dir + times_dir + file;
                    //~ console.log({localfilepath:localfilepath})
                    yield (0, accessor_utils_1.downloadAndWriteStream)(this.url, params, localfilepath, this.connection);
                    results.push(yield (0, accessor_utils_1.grib2obs)(localfilepath, this.variable_map, (this.config.bbox) ? [this.config.bbox.leftlon, this.config.bbox.toplat, this.config.bbox.rightlon, this.config.bbox.bottomlat] : undefined, "milímetros"));
                }
                member = member + 1;
            }
            var observaciones = (0, accessor_utils_1.flatten)(results);
            return observaciones;
        });
    }
    update(filter, options) {
        return __awaiter(this, void 0, void 0, function* () {
            const observaciones = yield this.get(filter, options);
            console.info(`length: ${observaciones.length}`);
            const result = yield CRUD_1.observacion.create(observaciones);
            if (options === null || options === void 0 ? void 0 : options.no_send_data) {
                if (result && result.length > 0) {
                    var timestart = new Date(result.map(o => o.timestart).reduce((a, b) => new Date(Math.min(a.getTime(), b.getTime()))));
                    var timeend = new Date(result.map(o => o.timeend).reduce((a, b) => new Date(Math.max(a.getTime(), b.getTime()))));
                    var count = result.length;
                    return {
                        path: this.path,
                        count: count,
                        timestart: timestart,
                        timeend: timeend
                    };
                }
                else {
                    return {
                        path: this.path
                    };
                }
            }
            else if (options === null || options === void 0 ? void 0 : options.return_series) {
                const serie = Client.serie;
                serie.observaciones = result;
                return [serie];
            }
            else {
                return result;
            }
        });
    }
    getDates(filter) {
        var forecast_date = (filter.forecast_date) ? new Date(filter.forecast_date) : new Date();
        if (forecast_date.toString() == "Invalid Date") {
            throw new Error("Invalid forecast date");
        }
        var timestart, timeend;
        if (filter.timestart) {
            timestart = new Date(filter.timestart);
            if (timestart.toString() == "Invalid Date") {
                throw new Error("Invalid timestart");
            }
        }
        if (filter.timeend) {
            timeend = new Date(filter.timeend);
            if (timeend.toString() == "Invalid Date") {
                throw new Error("Invalid timeend");
            }
        }
        // 	SET TIME TO MULTIPLE OF 6 //
        forecast_date.setUTCHours(forecast_date.getUTCHours() - forecast_date.getUTCHours() % 6);
        var dates_dir = (0, sprintf_js_1.sprintf)("gefs.%04d%02d%02d/", forecast_date.getUTCFullYear(), forecast_date.getUTCMonth() + 1, forecast_date.getUTCDate());
        var times_dir = (0, sprintf_js_1.sprintf)("%02d/", forecast_date.getUTCHours());
        // console.log({forecast_date: forecast_date.toISOString(),times_dir:times_dir, dates_dir:dates_dir})
        var forecast_date_path = __dirname + this.config.data_dir + dates_dir.replace(/\/$/, "");
        var forecast_time_path = __dirname + this.config.data_dir + dates_dir + times_dir.replace(/\/$/, "");
        this.path = forecast_time_path;
        return {
            forecast_date: forecast_date,
            timestart: timestart,
            timeend: timeend,
            dates_dir: dates_dir,
            times_dir: times_dir,
            forecast_date_path: forecast_date_path,
            forecast_time_path: forecast_time_path
        };
    }
    printMaps(forecast_date) {
        return __awaiter(this, void 0, void 0, function* () {
            const dates = this.getDates({ forecast_date: forecast_date });
            return this.callPrintMaps(dates.forecast_date_path);
        });
    }
    callPrintMaps(path, skip_print, location) {
        var _b, _c;
        return __awaiter(this, void 0, void 0, function* () {
            var mapset = (0, sprintf_js_1.sprintf)("%04d", Math.floor(Math.random() * 10000));
            if (!location) {
                if (!((_c = (_b = global.config) === null || _b === void 0 ? void 0 : _b.grass) === null || _c === void 0 ? void 0 : _c.location)) {
                    throw new Error("global.config.grass.location no definido");
                }
                var location_mapset = (0, sprintf_js_1.sprintf)("%s/%s", global.config.grass.location, mapset); // sprintf("%s/GISDATABASE/WGS84/%s",process.env.HOME,mapset)
            }
            else {
                var location_mapset = (0, sprintf_js_1.sprintf)("%s/%s", location, mapset);
            }
            var batchjob = (0, sprintf_js_1.sprintf)("%s/../py/print_wind_map.py", __dirname);
            if (path) {
                console.debug("callPrintWindMap: path: " + path);
                process.env.gefs_run_path = path;
            }
            if (skip_print) {
                process.env.skip_print = "True";
            }
            var command = (0, sprintf_js_1.sprintf)("grass %s -c --exec %s", location_mapset, batchjob);
            const result = yield (0, child_process_promise_1.exec)(command);
            console.debug("batch job called");
            var stdout = result.stdout;
            var stderr = result.stderr;
            if (stdout) {
                console.log(stdout);
            }
            if (stderr) {
                console.error(stderr);
            }
            process.env.gefs_run_path = undefined;
        });
    }
}
_a = Client;
Client.fuente = new CRUD_1.fuente({
    nombre: "gefs_atmos",
    id: 55,
    data_table: "",
    data_column: "",
    tipo: "QPF",
    def_proc_id: 4,
    def_dt: "06:00:00",
    hora_corte: "00:00:00",
    def_unit_id: 9,
    def_var_id: 91,
    fd_column: "",
    mad_table: "",
    scale_factor: 0,
    data_offset: 0,
    def_pixel_height: 0.25,
    def_pixel_width: 0.25,
    def_pixeltype: "64BF",
    def_srid: 4326,
    def_extent: { "type": "Polygon", "coordinates": [[[-70, -10], [-40, -10], [-40, -40], [-70, -40], [-70, -10]]] },
    date_column: "",
    abstract: "GEFS ATMOS",
    source: "https://nomads.ncep.noaa.gov/cgi-bin/filter_gefs_atmos_0p25s.pl",
    public: false
});
Client.escena = new CRUD_1.escena({
    id: 25,
    nombre: "cdp",
    geom: { "type": "Polygon", "coordinates": [[[-70, -10], [-40, -10], [-40, -40], [-70, -40], [-70, -10]]] }
});
Client.serie = new CRUD_1.serie({
    tipo: "raster",
    id: 55,
    estacion: _a.escena,
    var: {
        id: 91
    },
    procedimiento: {
        id: 4
    },
    unidades: {
        id: 9
    },
    fuente: _a.fuente
});
exports.Client = Client;
