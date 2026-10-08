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
const CRUD_1 = require("../CRUD");
const child_process_promise_1 = require("child-process-promise");
const promises_1 = require("fs/promises");
const fs_1 = require("fs");
const os_1 = require("os");
const path_1 = require("path");
const promise_ftp_1 = __importDefault(require("promise-ftp"));
class Client extends abstract_accessor_engine_1.AbstractAccessorEngine {
    constructor(config) {
        super(config);
        this.config = config;
        this.ftp = new promise_ftp_1.default();
    }
    connect() {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                var serverMessage = yield this.ftp.connect({ host: this.config.host, user: this.config.user, password: this.config.password });
                console.log('serverMessage:' + serverMessage);
                return;
            }
            catch (e) {
                throw e;
            }
        });
    }
    findMetadataValue(info, key, band) {
        const metadata = [band === null || band === void 0 ? void 0 : band.metadata, info.metadata];
        for (const domains of metadata) {
            for (const values of Object.values(domains !== null && domains !== void 0 ? domains : {})) {
                const entry = Object.entries(values).find(([name]) => name.toUpperCase() === key);
                if (entry)
                    return entry[1];
            }
        }
        throw new Error(`GRIB metadata ${key} not found`);
    }
    parseGribDate(value) {
        const epoch = parseInt(value);
        const date = new Date(epoch * 1000);
        if (Number.isNaN(date.getTime())) {
            throw new Error(`Invalid GRIB date: ${value}`);
        }
        return date;
    }
    writeStreamToFile(stream, output) {
        return __awaiter(this, void 0, void 0, function* () {
            return new Promise(function (resolve, reject) {
                stream.once('close', resolve);
                stream.once('error', reject);
                stream.pipe((0, fs_1.createWriteStream)(output));
            });
        });
    }
    getPronostico(filter = {}, options = {}) {
        var _a;
        return __awaiter(this, void 0, void 0, function* () {
            const workDir = yield (0, promises_1.mkdtemp)((0, path_1.join)((0, os_1.tmpdir)(), "wrf-"));
            const gribPath = (0, path_1.join)(workDir, "input.grib");
            var filepath = this.config.filepath;
            if (filter.forecast_date && this.config.available_files) {
                const utc_time = filter.forecast_date.getUTCHours();
                for (const f of this.config.available_files) {
                    if (f.utc_time == utc_time) {
                        filepath = f.path;
                    }
                }
            }
            try {
                yield this.connect();
                // const response = await axios.get<ArrayBuffer>(this.config.url, {
                //     responseType: "arraybuffer"
                // })
                console.debug("conectado a ftp");
                const stream = this.ftp.get(filepath);
                console.debug("Descargando " + filepath);
                yield this.writeStreamToFile(yield stream, gribPath);
                console.debug("se escribió " + gribPath);
                yield this.ftp.end();
                // await writeFile(gribPath, Buffer.from(response.read()))
                const { stdout } = yield (0, child_process_promise_1.exec)(`gdalinfo -json "${gribPath}"`);
                const info = JSON.parse(stdout);
                const bands = (_a = info.bands) !== null && _a !== void 0 ? _a : [];
                if (!bands.length) {
                    throw new Error("GRIB file contains no raster bands");
                }
                console.debug("se leyeron " + bands.length + " bandas");
                const forecastDate = this.parseGribDate(this.findMetadataValue(info, "GRIB_REF_TIME", bands[0]));
                const corrida = new CRUD_1.corrida({
                    cal_id: this.config.cal_id,
                    forecast_date: forecastDate,
                    series: []
                });
                if (options.update) {
                    yield corrida.create();
                    console.debug("se creó la corrida " + corrida.id);
                }
                const serie = {
                    series_table: "series_rast",
                    series_id: this.config.series_id,
                    pronosticos: []
                };
                for (let index = 0; index < bands.length; index++) {
                    const band = bands[index];
                    const validTime = this.parseGribDate(this.findMetadataValue(info, "GRIB_VALID_TIME", band));
                    const bandPath = (0, path_1.join)(workDir, `band-${index + 1}.tif`);
                    yield (0, child_process_promise_1.exec)(`gdal_translate -q -of GTiff -b ${index + 1} "${gribPath}" "${bandPath}"`);
                    const pronostico = {
                        timestart: validTime,
                        valor: yield (0, promises_1.readFile)(bandPath),
                        series_id: this.config.series_id,
                        series_table: "series_rast",
                        srid: 990001
                    };
                    if (options.update) {
                        yield CRUD_1.pronostico.create([pronostico], { tipo: "raster", cor_id: corrida.id, no_send_data: true });
                        serie.pronosticos.push({ timestart: validTime });
                        console.debug("se creó el pronóstico " + validTime.toISOString());
                    }
                    else {
                        serie.pronosticos.push(pronostico);
                    }
                }
                console.debug("fin de la corrida");
                corrida.series.push(serie);
                return corrida;
            }
            finally {
                yield (0, promises_1.rm)(workDir, { recursive: true, force: true });
            }
        });
    }
    updatePronostico(filter = {}, options = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.getPronostico(filter, { update: true });
        });
    }
}
exports.Client = Client;
