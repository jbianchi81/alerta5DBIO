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
const accessor_utils_1 = require("../accessor_utils");
const timeSteps_1 = require("../timeSteps");
const CRUD_1 = require("../CRUD");
const basic_ftp_1 = require("basic-ftp");
class Escena_ extends CRUD_1.escena {
    constructor(fields) {
        super(fields);
    }
}
/**
 * Docs: http://repositorio.smn.gob.ar/handle/20.500.12160/2972
 *
 * Available date range for daily product: last year
 *
 * Time offset of daily product: 12Z
 *
 * Latency approx. 4:30 hours
 */
class Client extends abstract_accessor_engine_1.AbstractAccessorEngine {
    constructor(config) {
        super(config);
        this.default_config = {
            url: "ftp.smn.gob.ar",
            user: "user",
            password: "password",
            remote_path: "SQPE",
            local_path: "data/hidroestimador",
            series_id: 16,
            escena_id: 24,
            file_pattern: "^Ajuste_(\d{8}).tif$",
            t_offset: 9
        };
        this.file_pattern = new RegExp(/^Ajuste_(\d{8})\.tif$/);
        this.setConfig(config);
    }
    getSites(filter) {
        return __awaiter(this, void 0, void 0, function* () {
            const escenas = [
                new Escena_({
                    "id": this.config.escena_id,
                    "nombre": "hidroestimador",
                    "geom": { "type": "Polygon", "coordinates": [[[-76.099995422, -19.899998856], [-48.900000763, -19.899998856], [-48.900000763, -56.099999237], [-76.099995422, -56.099999237], [-76.099995422, -19.899998856]]] }
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
                        "id": 24,
                        "nombre": "hidroestimador",
                        "geom": { "type": "Polygon", "coordinates": [[[-76.099995422, -19.899998856], [-48.900000763, -19.899998856], [-48.900000763, -56.099999237], [-76.099995422, -56.099999237], [-76.099995422, -19.899998856]]] }
                    },
                    "var": { "id": 1, "var": "P", "nombre": "precipitación diaria 12Z", "abrev": "precip_diaria_met", "type": "num", "datatype": "Succeeding Total", "valuetype": "Field Observation", "GeneralCategory": "Climate", "VariableName": "Precipitation", "SampleMedium": "Precipitation", "def_unit_id": "22", "timeSupport": { "years": 0, "months": 0, "days": 1, "hours": 0, "minutes": 0, "seconds": 0, "milliseconds": 0 }, "def_hora_corte": { "years": 0, "months": 0, "days": 0, "hours": 9, "minutes": 0, "seconds": 0, "milliseconds": 0 } },
                    "procedimiento": {
                        "id": 5,
                        "nombre": "Estimado",
                        "abrev": "est",
                        "descripcion": "Estimado a partir de observaciones indirectas"
                    },
                    "unidades": { "id": 22, "nombre": "milímetros por día", "abrev": "mm/d", "UnitsID": 305, "UnitsType": "velocity" },
                    "fuente": { "id": 11, "nombre": "hidroestimador_diario", "data_table": "hidroestimador_diario_table", "data_column": "rast", "tipo": "QPE", "def_proc_id": 5, "def_dt": { "years": 0, "months": 0, "days": 1, "hours": 0, "minutes": 0, "seconds": 0, "milliseconds": 0 }, "hora_corte": { "years": 0, "months": 0, "days": 0, "hours": 9, "minutes": 0, "seconds": 0, "milliseconds": 0 }, "def_unit_id": 22, "def_var_id": 1, "fd_column": null, "mad_table": null, "scale_factor": null, "data_offset": null, "def_pixel_height": 0.1, "def_pixel_width": 0.1, "def_srid": 4326, "def_extent": { "type": "Polygon", "coordinates": [[[-76.0999954223633, -19.8999988555908], [-48.9000007629395, -19.8999988555908], [-48.9000007629395, -56.0999992370605], [-76.0999954223633, -56.0999992370605], [-76.0999954223633, -19.8999988555908]]] }, "date_column": "date", "def_pixeltype": "16BUI", "abstract": "Estimación satelital de la precipitación a paso diario de la misión GOES, producto Hidrestimador. Fuente: SMN", "source": "http://www.smn.gob.ar", "public": false }
                }
            ];
            return (0, accessor_utils_1.filterSeries)(series.map(s => new CRUD_1.serie(s)), filter);
        });
    }
    accessServer() {
        return __awaiter(this, void 0, void 0, function* () {
            this.ftp_client = new basic_ftp_1.Client();
            this.ftp_client.ftp.verbose = false;
            const response = yield this.ftp_client.access({
                host: this.config.url,
                user: this.config.user,
                password: this.config.password
            });
            console.debug(response.message);
        });
    }
    getFilesList(filter) {
        return __awaiter(this, void 0, void 0, function* () {
            const files_info_all = yield this.ftp_client.list(this.config.remote_path);
            const files_list = [];
            for (const f of files_info_all) {
                if (!f.isFile) {
                    // not a file
                    continue;
                }
                if (!this.file_pattern.test(f.name)) {
                    // doesn't match pattern
                    continue;
                }
                const timestart = this.parseDateFromFilename(f.name);
                if (filter.timestart && timestart.getTime() < filter.timestart.getTime()) {
                    // before date range
                    continue;
                }
                if (filter.timeend && timestart.getTime() > filter.timeend.getTime()) {
                    // after date range
                    continue;
                }
                files_list.push(`${this.config.remote_path}/${f.name}`);
            }
            return files_list;
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
            if (!this.ftp_client) {
                try {
                    yield this.accessServer();
                }
                catch (e) {
                    throw (e);
                }
            }
            var files_list = yield this.getFilesList(filter);
            if (!files_list.length) {
                console.error("accessors/hidroestimador: No files found");
                return [];
            }
            const downloaded_files = yield this.downloadFiles(files_list);
            const observaciones = yield this.rast2obsList(downloaded_files);
            for (const file of downloaded_files) {
                (0, fs_1.unlinkSync)(file);
            }
            return observaciones;
        });
    }
    parseDateFromFilename(filename) {
        const d = filename.match(this.file_pattern)[1];
        if (!d.length) {
            throw new Error("File pattern not matched in filename: " + filename);
        }
        return new Date(parseInt(`${d[0]}${d[1]}${d[2]}${d[3]}`), parseInt(`${d[4]}${d[5]}`) - 1, parseInt(`${d[6]}${d[7]}`), this.config.t_offset);
    }
    update(filter, options) {
        return __awaiter(this, void 0, void 0, function* () {
            const observaciones = yield this.get(filter, options);
            if (!observaciones || observaciones.length == 0) {
                console.error("accessors/hidroestimador/update: Nothing retrieved");
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
            try {
                this.accessServer();
            }
            catch (e) {
                console.error(e);
                return false;
            }
            return true;
        });
    }
    // Downloads files to this.config.local_path
    downloadFiles(product_urls) {
        return __awaiter(this, void 0, void 0, function* () {
            const downloaded_files = [];
            this.downloaded_files = downloaded_files;
            if (!this.ftp_client) {
                yield this.accessServer();
            }
            for (var u of product_urls) {
                var local_filename = path_1.default.join(this.config.local_path, path_1.default.basename(u));
                console.debug("accessors/hidroestimador: downloading: " + u + " into: " + local_filename);
                yield this.ftp_client.downloadTo(local_filename, u);
                downloaded_files.push(local_filename);
            }
            return downloaded_files;
        });
    }
    rast2obsList(filenames) {
        return __awaiter(this, void 0, void 0, function* () {
            // console.log(JSON.stringify(filenames))
            var observaciones = [];
            for (const filename of filenames) {
                const base = path_1.default.basename(filename);
                const timestart = this.parseDateFromFilename(base);
                if (timestart.toString() == "Invalid Date") {
                    throw ("Invalid Date at filename: " + base);
                }
                var timeend = (0, timeSteps_1.advanceInterval)(timestart, { days: 1 });
                try {
                    var data = (0, fs_1.readFileSync)(filename, 'hex');
                }
                catch (e) {
                    console.error(e);
                    continue;
                }
                observaciones.push({
                    tipo: "raster",
                    series_id: this.config.series_id,
                    timestart: timestart,
                    timeend: timeend,
                    valor: '\\x' + data
                });
            }
            return observaciones;
        });
    }
    deleteRemote(filter) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!filter.timestart) {
                throw new Error("Falta filter.timestart");
                //     filter.timestart = new Date(new Date().getTime() - 30 * 24 * 3600 * 1000)
            }
            if (!filter.timeend) {
                throw new Error("Falta filter.timeend");
                //     filter.timeend = new Date(new Date().getTime() - 15 * 24 * 3600 * 1000)
            }
            if (!this.ftp_client) {
                yield this.accessServer();
            }
            var files_list = yield this.getFilesList(filter);
            if (!files_list || !files_list.length) {
                console.error("accessors/hidroestimador: No files found");
                return [];
            }
            const deleted = [];
            for (const file of files_list) {
                try {
                    yield this.ftp_client.remove(file);
                    console.log(`File ${file} removed successfully`);
                    deleted.push(file);
                }
                catch (_a) {
                    console.error(`Failed to remove file ${file}`);
                }
            }
            return deleted;
        });
    }
}
exports.Client = Client;
