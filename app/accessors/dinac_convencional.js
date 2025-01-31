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
const axios_1 = __importDefault(require("axios"));
class Client extends abstract_accessor_engine_1.AbstractAccessorEngine {
    constructor(config) {
        super(config);
        this.sites_map = [];
        this.series_map = [];
        this.default_config = {
            url: "https://www.meteorologia.gov.py/nivel-rio/vermas_convencional.php",
            page_size: 15,
            tabla: "alturas_dinac",
            pais: "Paraguay",
            propietario: "DMH-DINAC",
            proc_id: 1
        };
        this.setConfig(config);
    }
    loadSitesMap() {
        return __awaiter(this, void 0, void 0, function* () {
            const estaciones = yield CRUD_1.estacion.read({
                tabla: this.config.tabla
            });
            this.sites_map = estaciones.map(estacion => {
                return {
                    estacion_id: estacion.id,
                    code: parseInt(estacion.id_externo),
                    estacion: estacion
                };
            });
        });
    }
    loadSeriesMap() {
        return __awaiter(this, void 0, void 0, function* () {
            const series = yield CRUD_1.serie.read({
                tabla_id: this.config.tabla,
                var_id: 2,
                proc_id: 1,
                unit_id: 11
            });
            this.series_map = series.map(serie => {
                return {
                    series_id: serie.id,
                    estacion_id: serie.estacion.id,
                    var_id: serie.var.id,
                    serie: serie
                };
            });
        });
    }
    get(filter, options = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!filter || !filter.timestart || !filter.timeend) {
                throw ("Missing timestart and/or timeend");
            }
            if (!this.sites_map.length) {
                yield this.loadSitesMap();
            }
            if (!this.series_map.length) {
                yield this.loadSeriesMap();
            }
            const filter_ = Client.setFilterValuesToArray(filter, false);
            const observaciones = [];
            for (var site_index = 0; site_index < this.sites_map.length; site_index++) {
                const site = this.sites_map[site_index];
                if (filter_.estacion_id && filter_.estacion_id.indexOf(site.estacion_id) < 0) {
                    // filter out by estacion_id
                    return;
                }
                if (filter_.id_externo && filter_.id_externo.indexOf(site.estacion.id_externo) < 0) {
                    // filter out by id_externo
                    return;
                }
                const serie_index = this.series_map.map(s => s.estacion_id).indexOf(site.estacion_id);
                if (serie_index < 0) {
                    console.warn(`Serie not found in map for estacion_id: ${site.estacion_id}, code: ${site.code}`);
                    return;
                }
                const code = site.code;
                const series_id = this.series_map[serie_index].series_id;
                if (filter_.series_id && filter_.series_id.indexOf(series_id) < 0) {
                    // filter out by series_id
                    return;
                }
                const page_range = this.predict_page_range(filter.timestart, filter.timeend);
                for (var page = page_range.begin; page <= page_range.end; page++) {
                    const page_obs = yield this.getPage(code, page, series_id);
                    var filtered_obs = page_obs.filter(o => o.timestart.getTime() >= filter_.timestart.getTime()
                        &&
                            o.timestart.getTime() <= filter_.timeend.getTime());
                    observaciones.push(...filtered_obs);
                }
            }
            return observaciones;
        });
    }
    predict_date_range(page, data_length = this.config.page_size) {
        var pred_end_date = new Date();
        pred_end_date.setHours(0, 0, 0, 0);
        pred_end_date.setDate(pred_end_date.getDate() - client.config.page_size * (page - 1));
        var pred_begin_date = new Date(pred_end_date);
        pred_begin_date.setDate(pred_begin_date.getDate() - data_length + 1);
        return {
            begin: pred_begin_date,
            end: pred_end_date
        };
    }
    predict_page_range(timestart, timeend) {
        const today = new Date();
        today.setHours(0, 0, 0, 0);
        const begin_days_ago = Math.round((today.getTime() - timeend.getTime()) / 1000 / 3600 / 24);
        const end_days_ago = Math.round((today.getTime() - timestart.getTime()) / 1000 / 3600 / 24);
        return {
            begin: Math.trunc(begin_days_ago / this.config.page_size) + 1,
            end: Math.trunc(end_days_ago / this.config.page_size) + 1
        };
    }
    getPage(code, page, series_id) {
        return __awaiter(this, void 0, void 0, function* () {
            // const code = 2000086134
            // const page = 381
            // const size = 15
            const page_url = `${this.config.url}?code=${code}&page=${page}`;
            const response = yield (0, axios_1.default)(page_url);
            const matches = response.data.match(/var\sphp_vars\s?=\s?(\{.*\})/);
            const data = JSON.parse(matches[1]);
            const observaciones = [];
            data.categories.forEach((category, i) => {
                if (data.data.length < i + 1) {
                    console.warn(`Data array is shorter than categories array: skipping category ${category}`);
                    return;
                }
                const split_date = category.split("-").map(d => parseInt(d));
                const date = new Date(split_date[2], split_date[1] - 1, split_date[0]);
                observaciones.push(new CRUD_1.observacion({
                    tipo: "puntual",
                    series_id: series_id,
                    timestart: date,
                    timeend: date,
                    valor: parseFloat(data.data[i])
                }));
            });
            observaciones.sort((a, b) => a.timestart.getTime() - b.timestart.getTime());
            return observaciones;
        });
    }
}
Client._get_is_multiseries = true;
exports.Client = Client;
