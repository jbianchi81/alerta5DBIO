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
const path_1 = __importDefault(require("path"));
const fs_1 = require("fs");
const promises_1 = require("fs/promises");
const accessor_utils_1 = require("../accessor_utils");
const timeSteps_1 = require("../timeSteps");
const child_process_promise_1 = require("child-process-promise");
const CRUD_1 = require("../CRUD");
const sprintf_js_1 = require("sprintf-js");
const axios_1 = __importDefault(require("axios"));
const util_1 = require("util");
const zlib_1 = __importDefault(require("zlib"));
const child_process_1 = require("child_process");
const dateutils_1 = require("./dateutils");
// import * as gdal from "@gdal/warp"; // Node-GDAL bindings
class FileItem {
}
class Escena_ extends CRUD_1.escena {
    constructor(fields) {
        super(fields);
    }
}
/**
 * The current operational PERSIANN (Precipitation Estimation from Remotely Sensed Information using Artificial Neural Networks) system developed by the Center for Hydrometeorology and Remote Sensing (CHRS) at the University of California, Irvine (UCI) uses neural network function classification/approximation procedures to compute an estimate of rainfall rate at each 0.25° x 0.25° pixel of the infrared brightness temperature image provided by geostationary satellites. An adaptive training feature facilitates updating of the network parameters whenever independent estimates of rainfall are available. The PERSIANN system was based on geostationary infrared imagery and later extended to include the use of both infrared and daytime visible imagery. The PERSIANN algorithm used here is based on the geostationary longwave infrared imagery to generate global rainfall. Rainfall product covers 60°S to 60°N globally.
 *
 * source: https://chrsdata.eng.uci.edu/
 *
 */
class Client extends abstract_accessor_engine_1.AbstractAccessorEngine {
    constructor(config) {
        super(config);
        this.default_config = {
            url: "https://persiann.eng.uci.edu/CHRSdata/PERSIANN/daily",
            input_dir: "data/persiann/downloads",
            output_dir: "data/persiann/processed",
            bbox: { "type": "Polygon", "coordinates": [[[-67, -14], [-43.5, -14], [-43.5, -38.25], [-67, -38.25], [-67, -14]]] },
            pixelsize: 0.25,
            xs: 1440,
            ys: 400,
            originx: -180,
            originy: 50,
            nodata_value: -9999,
            escena_id: 776,
            series_id: 21
        };
        this.setConfig(config);
    }
    getSites(filter) {
        return __awaiter(this, void 0, void 0, function* () {
            const escenas = [
                new Escena_({
                    "id": this.config.escena_id,
                    "nombre": "persiann",
                    "geom": this.config.bbox
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
            const escena = yield this.getSites({})[0];
            const series = [
                {
                    "id": this.config.series_id,
                    "tipo": "raster",
                    "estacion": escena,
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
                        "id": 51,
                        "nombre": "persiann",
                        "data_table": "persiann",
                        "data_column": "rast",
                        "tipo": "PA",
                        "def_proc_id": 6,
                        "def_dt": "1 days",
                        "hora_corte": "9 hours",
                        "def_unit_id": 22,
                        "def_var_id": 1,
                        "fd_column": null,
                        "mad_table": null,
                        "scale_factor": null,
                        "data_offset": null,
                        "def_pixel_height": 0.25,
                        "def_pixel_width": 0.25,
                        "def_srid": 4326,
                        "def_extent": { "type": "Polygon", "coordinates": [[[-67, -14], [-43.5, -14], [-43.5, -38.25], [-67, -38.25], [-67, -14]]] },
                        "date_column": "timestart",
                        "def_pixeltype": "32BF",
                        "abstract": "The current operational PERSIANN (Precipitation Estimation from Remotely Sensed Information using Artificial Neural Networks) system developed by the Center for Hydrometeorology and Remote Sensing (CHRS) at the University of California, Irvine (UCI) uses neural network function classification/approximation procedures to compute an estimate of rainfall rate at each 0.25° x 0.25° pixel of the infrared brightness temperature image provided by geostationary satellites. An adaptive training feature facilitates updating of the network parameters whenever independent estimates of rainfall are available. The PERSIANN system was based on geostationary infrared imagery and later extended to include the use of both infrared and daytime visible imagery. The PERSIANN algorithm used here is based on the geostationary longwave infrared imagery to generate global rainfall. Rainfall product covers 60°S to 60°N globally.", "source": "https://chrsdata.eng.uci.edu/",
                        "public": true
                    }
                }
            ];
            return (0, accessor_utils_1.filterSeries)(series.map(s => new CRUD_1.serie(s)), filter);
        });
    }
    getFilesList(timestart, timeend) {
        const dates = (0, dateutils_1.generateDailyDates)(timestart, timeend);
        return dates.map(d => {
            const year = d.getUTCFullYear();
            const doy = (0, dateutils_1.getDayOfYear)(d);
            return (0, sprintf_js_1.sprintf)("ms6s4_d%02d%03d.bin.gz", year - 2000, doy);
        });
    }
    getFile(filename, local_copy) {
        return __awaiter(this, void 0, void 0, function* () {
            yield (0, dateutils_1.downloadFile)(filename, local_copy);
            // const stream = await this.ftp.get(filename)
            // return new Promise(function (resolve, reject) {
            //     stream.once('close', resolve);
            //     stream.once('error', reject);
            //     stream.pipe(createWriteStream(local_copy));
            // })        
        });
    }
    parseDateFromFilename(filename) {
        const matches = filename.match(/ms6s4_d(\d{2})(\d{3})\.bin\.gz/);
        if (!matches) {
            return null;
        }
        const year = 2000 + parseInt(matches[1]);
        const doy = parseInt(matches[2]);
        return new Date(year, 0, doy, 9);
    }
    filterByDate(filename, timestart, timeend) {
        const date = this.parseDateFromFilename(filename);
        if (!date) {
            return false;
        }
        if (date < timestart) {
            return false;
        }
        if (date > timeend) {
            return false;
        }
        return true;
    }
    get(filter, options) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!filter.timestart) {
                filter.timestart = new Date(new Date().getTime() - 9 * 24 * 3600 * 1000);
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
            const filenames = this.getFilesList(filter.timestart, filter.timeend);
            // filter by date
            const filtered_filenames = filenames.filter(f => this.filterByDate(f, filter.timestart, filter.timeend));
            console.debug({ filtered_filenames });
            if (!filtered_filenames.length) {
                console.error("accessors/persiann: No products found for the specified dates");
                return [];
            }
            const downloaded_files = yield this.downloadFiles(filtered_filenames);
            const observaciones = yield this.rast2obsList(downloaded_files, options.update);
            // for (const file of downloaded_files) {
            //     unlinkSync(file)
            // }
            if (options.print_maps) {
                yield this.printMaps(filter.timestart, filter.timeend);
            }
            return observaciones;
        });
    }
    update(filter, options) {
        return __awaiter(this, void 0, void 0, function* () {
            const observaciones = yield this.get(filter, Object.assign(Object.assign({}, options), { update: true }));
            if (!observaciones || observaciones.length == 0) {
                console.error("accessors/persiann/update: Nothing retrieved");
                return [];
            }
            return observaciones;
        });
    }
    test() {
        return __awaiter(this, void 0, void 0, function* () {
            const response = yield axios_1.default.get(this.config.url);
            if (response.status == 200) {
                return true;
            }
            else {
                return false;
            }
        });
    }
    downloadFiles(filenames) {
        return __awaiter(this, void 0, void 0, function* () {
            const downloaded_files = [];
            this.downloaded_files = downloaded_files;
            for (var filename of filenames) {
                const local_filename = path_1.default.resolve(this.config.input_dir, filename);
                const remote_filename = `${this.config.url}/${filename}`;
                console.debug("accessors/persiann: downloading: " + remote_filename + " into: " + local_filename);
                try {
                    yield this.getFile(remote_filename, local_filename);
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
    decompressGz(gzPath, binPath) {
        return __awaiter(this, void 0, void 0, function* () {
            const gunzip = (0, util_1.promisify)(zlib_1.default.gunzip);
            const compressed = yield (0, promises_1.readFile)(gzPath);
            const decompressed = yield gunzip(compressed);
            yield (0, promises_1.writeFile)(binPath, decompressed);
        });
    }
    /** Run a shell command and wait for it to finish */
    runCommand(cmd, args) {
        return __awaiter(this, void 0, void 0, function* () {
            return new Promise((resolve, reject) => {
                const child = (0, child_process_1.spawn)(cmd, args, { stdio: "inherit" });
                child.on("close", code => {
                    if (code === 0)
                        resolve();
                    else
                        reject(new Error(`${cmd} failed with code ${code}`));
                });
            });
        });
    }
    binToTif(binPath, tifPath) {
        return __awaiter(this, void 0, void 0, function* () {
            // Read binary file into Float32Array
            const buffer = yield (0, promises_1.readFile)(binPath);
            const numElements = buffer.byteLength / 4;
            const data = new Float32Array(numElements);
            for (let i = 0; i < numElements; i++) {
                data[i] = buffer.readFloatBE(i * 4);
                if (data[i] < 0)
                    data[i] = this.config.nodata_value;
            }
            const flipped = new Float32Array(numElements);
            for (let row = 0; row < this.config.ys; row++) {
                const srcStart = row * this.config.xs;
                const dstStart = (this.config.ys - row - 1) * this.config.xs;
                flipped.set(data.subarray(srcStart, srcStart + this.config.xs), dstStart);
            }
            // Write raw float32 binary file for gdal_translate
            const rawPath = binPath + ".float32.bin";
            yield (0, promises_1.writeFile)(rawPath, Buffer.from(flipped.buffer));
            const translateArgs = [
                "-of", "GTiff",
                "-ot", "Float32",
                "-a_srs", "EPSG:4326",
                "-a_ullr", this.config.originx.toString(),
                this.config.originy.toString(),
                (this.config.originx + this.config.xs * this.config.pixelsize).toString(),
                (this.config.originy - this.config.ys * this.config.pixelsize).toString(),
                rawPath,
                tifPath
            ];
            yield this.runCommand("gdal_translate", translateArgs);
            yield (0, promises_1.unlink)(rawPath); // cleanup
        });
    }
    recortarTif(tifPath, outputPath, geom) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!geom) {
                if (!this.escena) {
                    this.escena = yield this.getSites({})[0];
                }
                geom = this.escena.geom.toString();
            }
            const warpArgs = [
                "-cutline", geom,
                "-crop_to_cutline",
                "-dstnodata", this.config.nodata_value.toString(),
                tifPath,
                outputPath
            ];
            yield this.runCommand("gdalwarp", warpArgs);
        });
    }
    procesarArchivo(filename, output_filename) {
        var _a;
        return __awaiter(this, void 0, void 0, function* () {
            const datecode = filename.substring(7, 12);
            const gzPath = path_1.default.join(this.config.input_dir, filename);
            const binPath = path_1.default.join(this.config.output_dir, `${datecode}.bin`);
            const tifPath = path_1.default.join(this.config.output_dir, `${datecode}.tif`);
            const recortadoPath = path_1.default.join(this.config.output_dir, output_filename);
            console.log(`\nProcesando ${filename}...`);
            try {
                yield this.decompressGz(gzPath, binPath);
                yield this.binToTif(binPath, tifPath);
                yield this.recortarTif(tifPath, recortadoPath);
                yield (0, promises_1.unlink)(binPath);
                yield (0, promises_1.unlink)(tifPath);
                console.log(`Guardado: ${recortadoPath}`);
            }
            catch (err) {
                console.error(`Error procesando ${filename}:`, err);
            }
            finally {
                (_a = global.gc) === null || _a === void 0 ? void 0 : _a.call(global); // optional GC trigger
            }
        });
    }
    rast2obsList(filenames, update_ = false) {
        return __awaiter(this, void 0, void 0, function* () {
            // console.log(JSON.stringify(filenames))
            this.subset_files = [];
            for (var filename of filenames) {
                var timestart = this.parseDateFromFilename(filename);
                var timeend = new Date(timestart);
                timeend.setDate(timestart.getDate() + 1);
                const output_filename = `persiann.${timestart.toISOString().substring(0, 10).replace(/-/g, "")}.${timestart.toISOString().substring(11, 19).replace(/:/g, "")}.cdp.tif`; // persiann.YYYYMMDD.HHMMSS.cdp.tif
                const input_file_path = path_1.default.join(this.config.input_dir, filename);
                const output_file_path = path_1.default.join(this.config.output_dir, output_filename);
                try {
                    yield this.runCommand(`${global.config.python_bin}/persiann-process`, ["-f", input_file_path, "-o", output_file_path, "-b", "public/json/persiann_bbox.geojson"]);
                    // await this.procesarArchivo(filename, output_filename) 
                }
                catch (e) {
                    console.error(e);
                    continue;
                }
                this.subset_files.push({
                    filename: output_filename,
                    timestart: timestart,
                    timeend: timeend
                });
            }
            var observaciones = [];
            for (const file of this.subset_files) {
                const data = (0, fs_1.readFileSync)(path_1.default.join(this.config.output_dir, file.filename), 'hex');
                const obs = new CRUD_1.observacion({
                    tipo: "raster",
                    series_id: this.config.series_id,
                    timestart: file.timestart,
                    timeend: file.timeend,
                    valor: '\\x' + data
                });
                if (update_) {
                    const created_obs = yield obs.create();
                    observaciones.push(created_obs);
                }
                else {
                    observaciones.push(obs);
                }
            }
            return observaciones;
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
            process.env.maptitle = "PERSIANN (HCRS)";
            process.env.base_path = path_1.default.join(__dirname, "..", "..", this.config.output_dir);
            process.env.file_pattern = "^persiann\.\d{8}\.\d{6}\.cdp\.tif$";
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
