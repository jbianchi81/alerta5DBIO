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
Object.defineProperty(exports, "__esModule", { value: true });
exports.Client = void 0;
const abstract_accessor_engine_1 = require("./abstract_accessor_engine");
const accessor_utils_1 = require("../accessor_utils");
const CRUD_1 = require("../CRUD");
class Client extends abstract_accessor_engine_1.AbstractAccessorEngine {
    constructor(config) {
        super(config);
        this.defaults = {
            url: "https://api.hidraulica.gob.ar/v1",
            tabla_id: "dph_er_pluvio"
        };
        // if(config.series_map) {
        //     this.series_map = config.series_map
        // }
        this.url = config.url || this.defaults.url;
        this.token = config.token;
        this.tabla_id = config.tabla_id || this.defaults.tabla_id;
    }
    headers() {
        return {
            "Authorization": `Bearer ${this.token}`,
            "Content-Type": "application/json"
        };
    }
    createRed() {
        return __awaiter(this, void 0, void 0, function* () {
            return CRUD_1.red.create({
                tabla_id: this.tabla_id,
                nombre: "Dirección de Hidráulica de Entre Ríos - Red pluviométrica",
                public: true
            });
        });
    }
    get_estaciones() {
        return __awaiter(this, void 0, void 0, function* () {
            return (0, accessor_utils_1.fetchData)(`${this.url}/estaciones`, { headers: this.headers() });
        });
    }
    parseEstacion(e) {
        return new CRUD_1.estacion({
            tabla: this.tabla_id,
            nombre: e.nombre,
            geom: {
                type: "Point",
                coordinates: [e.longitud, e.latitud]
            },
            propietario: e.propietario,
            id_externo: e.id.toString(),
            distrito: "Entre Ríos",
            pais: "Argentina",
            localidad: e.departamento,
            tipo: "P",
            real: true,
            habilitar: true
        });
    }
    getSites(filter = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            const response = yield this.get_estaciones();
            const estaciones = response.data.estaciones.map(e => this.parseEstacion(e));
            for (const e of estaciones) {
                yield e.getEstacionId();
            }
            return estaciones;
        });
    }
    getSeries(filter = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            const estaciones = yield this.getSites();
            return estaciones.map(e => new CRUD_1.serie({
                tipo: "puntual",
                estacion: e,
                var: { id: 1 },
                procedimiento: { id: 1 },
                unidades: { id: 22 }
            }));
        });
    }
    get_precipitaciones_global(desde, hasta) {
        return __awaiter(this, void 0, void 0, function* () {
            return (0, accessor_utils_1.fetchData)(`${this.url}/precipitaciones`, {
                headers: this.headers(),
                params: {
                    desde: desde,
                    hasta: hasta
                }
            });
        });
    }
    /**
     * El rango entre desde y hasta no puede superar 365 días.
     * @param estacion_id
     * @param desde
     * @param hasta
     * @returns
     */
    get_precipitaciones(estacion_id, desde, hasta) {
        return __awaiter(this, void 0, void 0, function* () {
            return (0, accessor_utils_1.fetchData)(`${this.url}/estaciones/${estacion_id}/precipitaciones`, {
                headers: this.headers(),
                params: {
                    desde: desde,
                    hasta: hasta
                }
            });
        });
    }
    parsePrecipitacion(p, series_id) {
        const timestart = new Date(parseInt(p.fecha.substring(0, 4)), parseInt(p.fecha.substring(5, 7)) - 1, parseInt(p.fecha.substring(8, 10)), 9);
        if (timestart.toString() == "Invalid Date") {
            throw new Error(`Invalid date: ${timestart.toString()}`);
        }
        if (!p.medicion_realizada || !p.precipitacion_mm) {
            console.warn(`medicion nula`);
            return null;
        }
        const timeend = new Date(timestart);
        timeend.setDate(timeend.getDate() + 1);
        if (!series_id && p.hasOwnProperty("estacion_id")) {
            series_id = this.findSerie(undefined, undefined, p.estacion_id).series_id;
        }
        return {
            timestart: timestart,
            timeend: timeend,
            valor: p.precipitacion_mm,
            series_id: series_id
        };
    }
    getSeriesMap() {
        return __awaiter(this, void 0, void 0, function* () {
            this.series_map = {};
            const series = yield CRUD_1.serie.read({
                tabla_id: this.tabla_id,
                var_id: 1,
                proc_id: 1,
                unit_id: 22
            });
            for (const s of series) {
                this.series_map[s.estacion.id_externo] = {
                    series_id: s.id,
                    estacion_id: s.estacion.id,
                    id_externo: s.estacion.id_externo,
                    metadata: s
                };
            }
        });
    }
    findSerie(estacion_id, series_id) {
        if (!estacion_id && !series_id) {
            throw new Error("At least one of estacion_id, series_id must be set");
        }
        if (!this.series_map) {
            throw new Error("series_map not set");
        }
        for (const [key, mapping] of Object.entries(this.series_map)) {
            if (estacion_id) {
                if (mapping.estacion_id == estacion_id) {
                    return mapping;
                }
            }
            else {
                if (mapping.series_id == series_id) {
                    return mapping;
                }
            }
        }
        throw new Error(`estacion_id: ${estacion_id}, series_id: ${series_id} not found in series mapping`);
    }
    get_one(estacion_id, desde, hasta, series_id) {
        return __awaiter(this, void 0, void 0, function* () {
            const mapping = this.findSerie(estacion_id, series_id);
            const response = yield this.get_precipitaciones(mapping.id_externo, desde, hasta);
            return response.data.precipitaciones.map(p => this.parsePrecipitacion(p, mapping.series_id)).filter(o => o != null);
        });
    }
    get(filter, options = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.series_map) {
                yield this.getSeriesMap();
            }
            const desde = (filter.timestart) ? formatLocalDate(filter.timestart) : undefined;
            const hasta = (filter.timeend) ? formatLocalDate(filter.timeend) : undefined;
            if (filter.estacion_id) {
                const observaciones = [];
                if (Array.isArray(filter.estacion_id)) {
                    for (const estacion_id of filter.estacion_id) {
                        const obs = yield this.get_one(estacion_id, desde, hasta);
                        observaciones.push(...obs);
                    }
                }
                else {
                    const obs = yield this.get_one(filter.estacion_id, desde, hasta);
                    observaciones.push(...obs);
                }
                return observaciones;
            }
            else if (filter.series_id) {
                const observaciones = [];
                if (Array.isArray(filter.series_id)) {
                    for (const series_id of filter.series_id) {
                        const obs = yield this.get_one(undefined, desde, hasta, series_id);
                        observaciones.push(...obs);
                    }
                }
                else {
                    const obs = yield this.get_one(undefined, desde, hasta, filter.series_id);
                    observaciones.push(...obs);
                }
                return observaciones;
            }
            // all stations
            const response = yield this.get_precipitaciones_global(desde, hasta);
            const observaciones = response.data.precipitaciones.map(p => this.parsePrecipitacion(p)).filter(o => o != null);
            return observaciones;
        });
    }
}
Client._get_is_multiseries = true;
exports.Client = Client;
function formatLocalDate(date) {
    const pad = (n) => String(n).padStart(2, "0");
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
}
