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
const path_1 = __importDefault(require("path"));
const fs_1 = __importDefault(require("fs"));
const accessor_utils_1 = require("./accessor_utils");
class Client extends abstract_accessor_engine_1.AbstractAccessorEngine {
    constructor(config) {
        super(config);
        this.config = config;
    }
    getPronostico(filter = {}, options = {}) {
        var _a;
        return __awaiter(this, void 0, void 0, function* () {
            // agrega cosmo
            if (!((_a = global.config) === null || _a === void 0 ? void 0 : _a.agrega_cosmo_exec)) {
                throw new Error("agrega_cosmo_exec not set in config");
            }
            const params = {};
            if (filter.forecast_date) {
                var forecast_date = new Date(filter.forecast_date);
                params.emision = forecast_date.toISOString().slice(0, 10).replace(/-/g, "");
                params.inicializacion = forecast_date.toISOString().slice(11, 13);
            }
            else {
                var forecast_date = new Date();
                forecast_date.setUTCHours(0);
                forecast_date.setUTCMinutes(0);
                forecast_date.setUTCSeconds(0);
                forecast_date.setUTCMilliseconds(0);
                params.emision = forecast_date.toISOString().slice(0, 10).replace(/-/g, "");
                params.inicializacion = "00";
            }
            params.host = this.config.url;
            params.host_1d = this.config.dir_1d;
            params.host_3h = this.config.dir_3h;
            yield (0, accessor_utils_1.spawnAsync)(global.config.agrega_cosmo_exec, params);
            // lee .tif
            // diario
            const dir_1d = path_1.default.resolve(this.config.dir_1d, forecast_date.toISOString().substring(0, 4), forecast_date.toISOString().substring(5, 7));
            if (!fs_1.default.existsSync(path_1.default.resolve(dir_1d, "index.csv"))) {
                throw new Error("Falta index.csv en " + dir_1d);
            }
            const files_1d = fs_1.default.readFileSync(path_1.default.resolve(dir_1d, "index.csv"), { encoding: "utf-8" }).split("\n");
            // const pronosticos_1d = fs.readdirSync(dir_1d).filter(f => f.endsWith(".tif")).map(f => {
            const pronosticos_1d = files_1d.map(f => {
                if (!fs_1.default.existsSync(path_1.default.resolve(dir_1d, f))) {
                    throw new Error("Missing file: " + dir_1d + "/" + f);
                }
                const match = f.match(/PLATA_(\d{10})_(\d{10})\.tif/);
                if (!match)
                    return null;
                const timeend = new Date(parseYYYYMMDDHH(match[2]));
                timeend.setUTCHours(timeend.getUTCHours() + 1);
                const timestart = new Date(timeend);
                timestart.setUTCDate(timestart.getUTCDate() - 1);
                return {
                    filename: match[0],
                    forecast_date: forecast_date,
                    timestart: timestart,
                    timeend: timeend,
                    valor: fs_1.default.readFileSync(path_1.default.resolve(dir_1d, f)),
                    qualifier: "main"
                };
            }).filter((v) => !!v);
            // 3 horario
            const dir_3h = path_1.default.resolve(this.config.dir_3h, forecast_date.toISOString().substring(0, 4), forecast_date.toISOString().substring(5, 7));
            if (!fs_1.default.existsSync(path_1.default.resolve(dir_3h, "index.csv"))) {
                throw new Error("Falta index.csv en " + dir_3h);
            }
            const files_3h = fs_1.default.readFileSync(path_1.default.resolve(dir_3h, "index.csv"), { encoding: "utf-8" }).split("\n");
            // const pronosticos_3h = fs.readdirSync(dir_3h).filter(f => f.endsWith(".tif")).map(f => {
            const pronosticos_3h = files_3h.map(f => {
                if (!fs_1.default.existsSync(path_1.default.resolve(dir_3h, f))) {
                    throw new Error("Missing file: " + dir_3h + "/" + f);
                }
                const match = f.match(/PLATA_(\d{10})_(\d{10})\.tif/);
                if (!match)
                    return null;
                const timeend = new Date(parseYYYYMMDDHH(match[2]));
                timeend.setUTCHours(timeend.getUTCHours() + 1);
                const timestart = new Date(timeend);
                timestart.setUTCHours(timestart.getUTCHours() - 3);
                return {
                    filename: match[0],
                    forecast_date: forecast_date,
                    timestart: timestart,
                    timeend: timeend,
                    valor: fs_1.default.readFileSync(path_1.default.resolve(dir_3h, f))
                };
            }).filter((v) => !!v);
            // devuelve corrida
            return new CRUD_1.corrida({
                forecast_date: forecast_date,
                cal_id: this.config.cal_id,
                series: [
                    {
                        series_table: "series_rast",
                        series_id: this.config.series_id_1d,
                        pronosticos: pronosticos_1d
                    },
                    {
                        series_table: "series_rast",
                        series_id: this.config.series_id_3h,
                        pronosticos: pronosticos_3h
                    }
                ]
            });
        });
    }
    updatePronostico(filter = {}, options = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            const corrida = yield this.getPronostico(filter, options);
            yield corrida.create();
            return corrida;
        });
    }
}
exports.Client = Client;
function parseYYYYMMDDHH(dateString) {
    const year = parseInt(dateString.substring(0, 4), 10);
    const month = parseInt(dateString.substring(4, 6), 10) - 1;
    const day = parseInt(dateString.substring(6, 8), 10);
    const hour = parseInt(dateString.substring(8, 10), 10);
    return new Date(Date.UTC(year, month, day, hour));
}
