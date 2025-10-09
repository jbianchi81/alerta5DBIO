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
exports.Client = void 0;
const abstract_accessor_engine_1 = require("./abstract_accessor_engine");
const axios_1 = __importDefault(require("axios"));
const path_1 = __importDefault(require("path"));
const fs_1 = require("fs");
const accessor_utils_1 = require("../accessor_utils");
const timeSteps_1 = require("../timeSteps");
const child_process_promise_1 = require("child-process-promise");
const CRUD_1 = require("../CRUD");
const print_rast_1 = require("../print_rast");
const sprintf_js_1 = require("sprintf-js");
const print_rast_2 = require("../print_rast");
class Escena_ extends CRUD_1.escena {
    constructor(fields) {
        super(fields);
    }
}
/**
 * Docs: https://pmmpublisher.pps.eosdis.nasa.gov/docs
 *
 * Available date range for daily product: last 2 months
 *
 * Time offset of daily product: 00Z
 *
 * Latency approx. 12 hours
 */
class Client extends abstract_accessor_engine_1.AbstractAccessorEngine {
    constructor(config) {
        super(config);
        this.default_config = {
            url: "https://pmmpublisher.pps.eosdis.nasa.gov/opensearch",
            local_path: "data/gpm/3h/",
            dia_local_path: "data/gpm/dia",
            search_params: {
                q: "precip_1d",
                lat: -25,
                lon: -45,
                limit: 64,
                area: 0.25
            },
            bbox: [-70, -10, -40, -40],
            tmpfile: "/tmp/gpm_transformed.tif",
            tmpfile_json: "/tmp/gpm_dia.json",
            series_id: 4,
            scale: 0.1,
            dia_series_id: 13,
            escena_id: 11
        };
        this.setConfig(config);
    }
    getSites(filter) {
        return __awaiter(this, void 0, void 0, function* () {
            const escenas = [
                new Escena_({
                    "id": this.config.escena_id,
                    "nombre": "pp_gpm_3h",
                    "geom": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [
                                    -90,
                                    10
                                ],
                                [
                                    -30,
                                    10
                                ],
                                [
                                    -30,
                                    -60
                                ],
                                [
                                    -90,
                                    -60
                                ],
                                [
                                    -90,
                                    10
                                ]
                            ]
                        ]
                    }
                })
            ];
            return escenas;
        });
    }
    updateSites(filter) {
        return __awaiter(this, void 0, void 0, function* () {
            const sites = (yield this.getSites(filter));
            const upserted = yield CRUD_1.escena.create(sites);
            return upserted.map((site) => new Escena_(site));
        });
    }
    getSeries(filter) {
        return __awaiter(this, void 0, void 0, function* () {
            const series = [
                {
                    "id": this.config.series_id,
                    "tipo": "raster",
                    "estacion": {
                        "id": 11,
                        "nombre": "pp_gpm_3h",
                        "geom": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [
                                        -90,
                                        10
                                    ],
                                    [
                                        -30,
                                        10
                                    ],
                                    [
                                        -30,
                                        -60
                                    ],
                                    [
                                        -90,
                                        -60
                                    ],
                                    [
                                        -90,
                                        10
                                    ]
                                ]
                            ]
                        }
                    },
                    "var": {
                        "id": 34,
                        "var": "P3h",
                        "nombre": "precipitación 3 horaria",
                        "abrev": "precip_3h",
                        "type": "num",
                        "datatype": "Cumulative",
                        "valuetype": "Field Observation",
                        "GeneralCategory": "Climate",
                        "VariableName": "Precipitation",
                        "SampleMedium": "Precipitation",
                        "def_unit_id": 9,
                        "timeSupport": new timeSteps_1.Interval({
                            "hours": 3
                        }),
                        "def_hora_corte": new timeSteps_1.Interval({})
                    },
                    "procedimiento": {
                        "id": 5,
                        "nombre": "Estimado",
                        "abrev": "est",
                        "descripcion": "Estimado a partir de observaciones indirectas"
                    },
                    "unidades": {
                        "id": 9,
                        "nombre": "milímetros",
                        "abrev": "mm",
                        "UnitsID": 54,
                        "UnitsType": "Length"
                    },
                    "fuente": {
                        "id": 13,
                        "nombre": "pp_gpm_3h",
                        "data_table": "pp_gpm_3h",
                        "data_column": "rast",
                        "tipo": "QPE",
                        "def_proc_id": 5,
                        "def_dt": new timeSteps_1.Interval({
                            "hours": 3
                        }),
                        "hora_corte": new timeSteps_1.Interval({}),
                        "def_unit_id": 9,
                        "def_var_id": 34,
                        "fd_column": null,
                        "mad_table": "pmad_gpm_3h",
                        "scale_factor": 1,
                        "data_offset": 0,
                        "def_pixel_height": 0.1,
                        "def_pixel_width": 0.1,
                        "def_srid": 4326,
                        "def_extent": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [
                                        -90,
                                        10
                                    ],
                                    [
                                        -30,
                                        10
                                    ],
                                    [
                                        -30,
                                        -60
                                    ],
                                    [
                                        -90,
                                        -60
                                    ],
                                    [
                                        -90,
                                        10
                                    ]
                                ]
                            ]
                        },
                        "date_column": "date",
                        "def_pixeltype": "32BF",
                        "abstract": "Estimación satelital de la precipitación a paso 3 horario de la misión GPM. Fuente NASA",
                        "source": "ftp://jsimpson.pps.eosdis.nasa.gov/data/imerg/early/",
                        "public": true
                    },
                    "date_range": {
                        "timestart": new Date("2021-07-31T00:00:00.000Z"),
                        "timeend": new Date("2024-09-09T21:00:00.000Z"),
                        "count": "3836"
                    }
                },
                {
                    "id": this.config.dia_series_id,
                    "tipo": "raster",
                    "estacion": {
                        "id": 11,
                        "nombre": "pp_gpm_3h",
                        "geom": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [
                                        -90,
                                        10
                                    ],
                                    [
                                        -30,
                                        10
                                    ],
                                    [
                                        -30,
                                        -60
                                    ],
                                    [
                                        -90,
                                        -60
                                    ],
                                    [
                                        -90,
                                        10
                                    ]
                                ]
                            ]
                        }
                    },
                    "var": {
                        "id": 1,
                        "var": "P",
                        "nombre": "precipitación diaria 12Z",
                        "abrev": "precip_diaria_met",
                        "type": "num",
                        "datatype": "Succeeding Total",
                        "valuetype": "Field Observation",
                        "GeneralCategory": "Climate",
                        "VariableName": "Precipitation",
                        "SampleMedium": "Precipitation",
                        "def_unit_id": 22,
                        "timeSupport": new timeSteps_1.Interval({
                            "days": 1
                        }),
                        "def_hora_corte": new timeSteps_1.Interval({
                            "hours": 9
                        })
                    },
                    "procedimiento": {
                        "id": 5,
                        "nombre": "Estimado",
                        "abrev": "est",
                        "descripcion": "Estimado a partir de observaciones indirectas"
                    },
                    "unidades": {
                        "id": 22,
                        "nombre": "milímetros por día",
                        "abrev": "mm/d",
                        "UnitsID": 305,
                        "UnitsType": "velocity"
                    },
                    "fuente": {
                        "id": 6,
                        "nombre": "gpm",
                        "data_table": "pp_gpm",
                        "data_column": "rast",
                        "tipo": "QPE",
                        "def_proc_id": 5,
                        "def_dt": new timeSteps_1.Interval({
                            "days": 1
                        }),
                        "hora_corte": new timeSteps_1.Interval({
                            "hours": 9
                        }),
                        "def_unit_id": 22,
                        "def_var_id": 1,
                        "fd_column": null,
                        "mad_table": "pmad_gpm",
                        "scale_factor": null,
                        "data_offset": null,
                        "def_pixel_height": 0.1,
                        "def_pixel_width": 0.1,
                        "def_srid": 4326,
                        "def_extent": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [
                                        -90,
                                        10
                                    ],
                                    [
                                        -30,
                                        10
                                    ],
                                    [
                                        -30,
                                        -60
                                    ],
                                    [
                                        -90,
                                        -60
                                    ],
                                    [
                                        -90,
                                        10
                                    ]
                                ]
                            ]
                        },
                        "date_column": "date",
                        "def_pixeltype": "32BF",
                        "abstract": "Estimación satelital de la precipitación a paso diario de la misión GPM. Fuente NASA",
                        "source": "ftp://jsimpson.pps.eosdis.nasa.gov/data/imerg/early/",
                        "public": true
                    },
                    "date_range": {
                        "timestart": new Date("2021-07-31T12:00:00.000Z"),
                        "timeend": new Date("2024-02-29T12:00:00.000Z"),
                        "count": 380
                    }
                }
            ];
            return (0, accessor_utils_1.filterSeries)(series.map(s => new CRUD_1.serie(s)), filter);
        });
    }
    getFilesList(filter) {
        return __awaiter(this, void 0, void 0, function* () {
            const params = Object.assign({}, this.config.search_params);
            // q: "precip_3hr",
            // lat: "-25",
            // lon: "-45",
            // limit: "56",
            params.startTime = filter.timestart.toISOString().substring(0, 10);
            params.endTime = filter.timeend.toISOString().substring(0, 10);
            console.debug({ url: this.config.url, params: params });
            return (0, axios_1.default)(this.config.url, { params: params });
        });
    }
    get(filter, options) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!filter.timestart) {
                filter.timestart = new Date(new Date().getTime() - 8 * 24 * 3600 * 1000);
            }
            else {
                filter.timestart = new Date(filter.timestart);
                if (filter.timestart.toString() == "Invalid Date") {
                    throw new Error("Invalid timestart");
                }
            }
            if (!filter.timeend) {
                filter.timeend = new Date();
            }
            else {
                filter.timeend = new Date(filter.timeend);
                if (filter.timeend.toString() == "Invalid Date") {
                    throw new Error("Invalid timeend");
                }
            }
            const response = yield this.getFilesList(filter);
            // console.log(response.data)
            if (!response.data || !response.data.items || !response.data.items.length) {
                console.error("accessors/gpm: No products found");
                return [];
            }
            var product_urls = [];
            var product_ids = [];
            for (var item of response.data.items) {
                if (item["@type"] != "geoss:precipitation") {
                    continue;
                }
                for (var j in item.action) {
                    var action = item.action[j];
                    if (action["@type"] != "ojo:download") {
                        continue;
                    }
                    for (var k in action.using) {
                        var using = action.using[k];
                        if (using.mediaType == "image/tiff") {
                            product_urls.push(using.url);
                            product_ids.push(using["@id"]);
                        }
                    }
                }
            }
            const local_path = (this.config.search_params.q == "precip_1d") ? this.config.dia_local_path : this.config.local_path;
            var local_filenames = product_ids.map(id => {
                return path_1.default.resolve(local_path, id);
            });
            const downloaded_files = yield this.downloadFiles(product_urls, local_filenames);
            const dt = (this.config.search_params.q == "precip_1d") ? new timeSteps_1.Interval({ days: 1 }) : new timeSteps_1.Interval({ hours: 3 });
            const observaciones = yield this.rast2obsList(downloaded_files, dt);
            for (const file of downloaded_files) {
                (0, fs_1.unlinkSync)(file);
            }
            return observaciones;
        });
    }
    update(filter, options) {
        return __awaiter(this, void 0, void 0, function* () {
            const observaciones = yield this.get(filter, options);
            if (!observaciones || observaciones.length == 0) {
                console.error("accessors/gpm/update: Nothing retrieved");
                return [];
            }
            const result = yield CRUD_1.observaciones.create(observaciones); //, "raster", this.config.series_id)
            // const result_diario = await this.getDiario(filter, options)
            // writeFileSync(this.config.tmpfile_json, JSON.stringify(result_diario, null, 4))
            return result;
        });
    }
    test() {
        return __awaiter(this, void 0, void 0, function* () {
            const params = Object.assign({}, this.config.search_params);
            params.startTime = new Date().toISOString().substring(0, 10);
            params.endTime = new Date().toISOString().substring(0, 10);
            try {
                var response = yield (0, axios_1.default)(this.config.url, { params: params });
            }
            catch (e) {
                console.error(e);
                return false;
            }
            if (response.status <= 299) {
                return true;
            }
            else {
                return false;
            }
        });
    }
    downloadFiles(product_urls, local_filenames) {
        return __awaiter(this, void 0, void 0, function* () {
            const downloaded_files = [];
            this.downloaded_files = downloaded_files;
            for (var u in product_urls) {
                var filename = local_filenames[u];
                console.log("accessors/gpm: downloading: " + product_urls[u] + " into: " + filename);
                try {
                    yield (0, accessor_utils_1.fetch)(product_urls[u], undefined, filename);
                }
                catch (e) {
                    console.error(e);
                    continue;
                }
                downloaded_files.push(filename);
            }
            return downloaded_files;
        });
    }
    rast2obsList(filenames_1) {
        return __awaiter(this, arguments, void 0, function* (filenames, dt = new timeSteps_1.Interval({ hours: 3 })) {
            // console.log(JSON.stringify(filenames))
            var observaciones = [];
            this.subset_files = [];
            for (var filename of filenames) {
                var filename2 = filename.replace(/\.tif$/, "_subset.tif");
                var base = path_1.default.basename(filename);
                var b = base.split(".");
                if (b.length == 4) {
                    // has time
                    var timestart = new Date(Date.UTC(parseInt(b[1].substring(0, 4)), parseInt(b[1].substring(4, 6)) - 1, parseInt(b[1].substring(6, 8)), parseInt(b[2].substring(0, 2))) - 2 * 3600 * 1000);
                }
                else {
                    // only date
                    var timestart = new Date(Date.UTC(parseInt(b[1].substring(0, 4)), parseInt(b[1].substring(4, 6)) - 1, parseInt(b[1].substring(6, 8))));
                }
                if (timestart.toString() == "Invalid Date") {
                    throw ("Invalid Date: " + b[1]);
                }
                var timeend = (0, timeSteps_1.advanceInterval)(timestart, dt);
                var scale = this.config.scale;
                // return new Promise( (resolve, reject) => {
                try {
                    yield (0, child_process_promise_1.exec)('gdal_translate -projwin ' + this.config.bbox.join(" ") + ' -a_nodata 9999 ' + filename + ' ' + filename2); // ('gdal_translate -a_scale 0.1 -unscale -projwin ' + this.config.bbox.join(" ") + ' ' + filename + ' ' + filename2)  this.config.tmpfile)  // ulx uly lrx lrt
                }
                catch (e) {
                    console.error(e);
                    continue;
                }
                try {
                    var data = (0, fs_1.readFileSync)(filename2, 'hex');
                }
                catch (e) {
                    console.error(e);
                    continue;
                }
                this.subset_files.push(filename2);
                observaciones.push({
                    tipo: "raster",
                    series_id: (this.config.search_params.q == "precip_1d") ? this.config.dia_series_id : this.config.series_id,
                    timestart: timestart,
                    timeend: timeend,
                    scale: scale,
                    valor: '\\x' + data
                });
            }
            return observaciones;
        });
    }
    getDiario(filter_1) {
        return __awaiter(this, arguments, void 0, function* (filter, options = {}) {
            // console.log({config:this.config})
            options["insertSeriesId"] = this.config.dia_series_id;
            options["t_offset"] = "12:00:00";
            const result = yield (0, CRUD_1.getRegularSeries)("raster", this.config.series_id, { days: 1 }, filter.timestart, filter.timeend, options);
            const dia_location = path_1.default.resolve(this.config.dia_local_path);
            return (0, print_rast_1.print_rast_series)({
                id: 13,
                observaciones: result
            }, {
                location: dia_location,
                format: "GTiff",
                patron_nombre: "gpm_dia.YYYYMMDD.HHMMSS.tif"
            });
        });
    }
    printMaps(timestart, timeend) {
        return this.callPrintMaps(timestart, timeend, false);
    }
    callPrintMaps(timestart, timeend, skip_print) {
        return __awaiter(this, void 0, void 0, function* () {
            var mapset = (0, sprintf_js_1.sprintf)("%04d", Math.floor(Math.random() * 10000));
            var location = (0, sprintf_js_1.sprintf)("%s/%s", global.config.grass.location, mapset); // sprintf("%s/GISDATABASE/WGS84/%s",process.env.HOME,mapset)
            var batchjob = path_1.default.resolve(__dirname, "../py/print_precip_map.py");
            if (timestart) {
                // console.log("callPrintMaps: timestart: " + timestart.toISOString().replace("Z",""))
                process.env.timestart = timestart.toISOString().replace("Z", "");
            }
            if (timeend) {
                // console.log("callPrintMaps: timeend: " + timeend.toISOString().replace("Z",""))
                process.env.timeend = timeend.toISOString().replace("Z", "");
            }
            if (skip_print) {
                process.env.skip_print = "True";
            }
            var command = (0, sprintf_js_1.sprintf)("grass %s -c --exec %s", location, batchjob);
            const result = yield (0, child_process_promise_1.exec)(command);
            // console.log("batch job called")
            var stdout = result.stdout;
            var stderr = result.stderr;
            if (stdout) {
                console.log(stdout);
            }
            if (stderr) {
                console.error(stderr);
            }
            process.env.timestart = undefined;
            process.env.timeend = undefined;
            process.env.skip_print = undefined;
        });
    }
    printMapSemanal(timestart, timeend) {
        return __awaiter(this, void 0, void 0, function* () {
            const location = path_1.default.resolve("data/gpm/semanal"); // this.config.mes_local_path
            var ts = new Date(timestart.getTime());
            var te = new Date(timestart.getTime());
            te.setUTCDate(te.getUTCDate() + 7);
            var results = [];
            while (te <= timeend) {
                console.log({ ts: ts.toISOString(), te: te.toISOString() });
                try {
                    var serie = yield (0, CRUD_1.rastExtract)(13, ts, te, { funcion: "SUM", min_count: 7 });
                    if (!serie.observaciones || !serie.observaciones.length) {
                        throw ("No se encontraron suficientes observaciones");
                    }
                    var result = yield (0, print_rast_2.print_rast)({
                        prefix: "",
                        location: location,
                        patron_nombre: "gpm_semanal.YYYYMMDD.HHMMSS.tif"
                    }, undefined, serie.observaciones[0]);
                    results.push(result);
                }
                catch (e) {
                    console.error(e);
                }
                ts.setUTCDate(ts.getUTCDate() + 1);
                te.setUTCDate(te.getUTCDate() + 1);
            }
            var new_timeend = new Date(timeend);
            new_timeend.setDate(new_timeend.getDate() - 7);
            yield this.printSemanalPNG(timestart, new_timeend, false);
            return results;
        });
    }
    printSemanalPNG(timestart, timeend, skip_print) {
        return __awaiter(this, void 0, void 0, function* () {
            var mapset = (0, sprintf_js_1.sprintf)("%04d", Math.floor(Math.random() * 10000));
            var location = (0, sprintf_js_1.sprintf)("%s/%s", global.config.grass.location, mapset); // sprintf("%s/GISDATABASE/WGS84/%s",process.env.HOME,mapset)
            var batchjob = path_1.default.resolve(__dirname, "../py/print_precip_map.py");
            if (timestart) {
                // console.log("callPrintMaps: timestart: " + timestart.toISOString().replace("Z",""))
                process.env.timestart = timestart.toISOString().replace("Z", "");
            }
            if (timeend) {
                // console.log("callPrintMaps: timeend: " + timeend.toISOString().replace("Z",""))
                process.env.timeend = timeend.toISOString().replace("Z", "");
            }
            if (skip_print) {
                process.env.skip_print = "True";
            }
            process.env.base_path = "data/gpm/semanal"; // this.config.local_path
            process.env.type = "semanal";
            var command = (0, sprintf_js_1.sprintf)("grass %s -c --exec %s", location, batchjob);
            const result = yield (0, child_process_promise_1.exec)(command);
            // console.log("batch job called")
            var stdout = result.stdout;
            var stderr = result.stderr;
            if (stdout) {
                console.log(stdout);
            }
            if (stderr) {
                console.error(stderr);
            }
            process.env.timestart = undefined;
            process.env.timeend = undefined;
            process.env.skip_print = undefined;
        });
    }
}
exports.Client = Client;
