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
const axios_1 = __importDefault(require("axios"));
const abstract_accessor_engine_1 = require("./abstract_accessor_engine");
const CRUD_1 = require("../CRUD");
class Client extends abstract_accessor_engine_1.AbstractAccessorEngine {
    get headers() {
        return {
            "User-Agent": "DEPilcomayoXM2",
            "X-authorization-token": this.tokenAuth
        };
    }
    get loginHeaders() {
        return {
            "User-Agent": "DEPilcomayoXM2"
        };
    }
    constructor(config) {
        super(config);
        this.series_map = {};
        this.default_config = {
            url: "https://api.pilcomayo.net",
            username: "my_username",
            password: "my_password"
        };
        this.setConfig(config);
        this.connection = axios_1.default.create();
    }
    login() {
        return __awaiter(this, void 0, void 0, function* () {
            const response = yield this.connection.post(`${this.config.url}/login/`, {
                username: this.config.username,
                password: this.config.password
            }, {
                headers: this.loginHeaders
            });
            if (response.status != 200) {
                throw new Error(`Login failed: ${response.statusText}`);
            }
            if (!response.data || !response.data.tokenAuth) {
                throw new Error(`Login failed: tokenAuth missing in response`);
            }
            this.tokenAuth = response.data.tokenAuth;
            return response.data;
        });
    }
    listEstaciones(id_estacion) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.tokenAuth) {
                throw new Error("Must login first.");
            }
            const response = yield this.connection.post(`${this.config.url}/list_estaciones/`, {
                id_estacion
            }, {
                headers: this.headers
            });
            if (response.status != 200) {
                throw new Error(`Request failed: ${response.statusText}`);
            }
            if (!response.data) {
                throw new Error(`listEstaciones failed: no data`);
            }
            return response.data;
        });
    }
    getAlturas(id_estacion, ano, mes, dia, hora) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.tokenAuth) {
                throw new Error("Must login first.");
            }
            const response = yield this.connection.post(`${this.config.url}/get_alturas/`, {
                id_estacion,
                ano,
                mes,
                dia,
                hora
            }, {
                headers: this.headers
            });
            if (response.status != 200) {
                throw new Error(`Request failed: ${response.statusText}`);
            }
            if (!response.data) {
                throw new Error(`getEstaciones failed: no data`);
            }
            return response.data;
        });
    }
    getPrecipitaciones(id_estacion, ano, mes, dia, hora) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.tokenAuth) {
                throw new Error("Must login first.");
            }
            const response = yield this.connection.post(`${this.config.url}/get_precipitaciones/`, {
                id_estacion,
                ano,
                mes,
                dia,
                hora
            }, {
                headers: this.headers
            });
            if (response.status != 200) {
                throw new Error(`Request failed: ${response.statusText}`);
            }
            if (!response.data) {
                throw new Error(`getPrecipitaciones failed: no data`);
            }
            return response.data;
        });
    }
    parseEstacion(estacion) {
        return {
            tabla: "ctp",
            id_externo: estacion.id_estacion,
            nombre: estacion.nombre,
            geom: {
                type: "Point",
                coordinates: [parseFloat(estacion.longitud), parseFloat(estacion.latitud)]
            },
            has_obs: (estacion.en_actividad == "1") ? true : false,
            ubicacion: estacion.descripcion,
            tipo: "A",
            automatica: true,
            URL: this.config.url,
            propietario: "Comisión Trinacional para el Desarrollo de la Cuenca del Río Pilcomayo",
            real: true
        };
    }
    parseRegistroAltura(registro, series_id) {
        const ymd = registro.fecha.split("-").map(v => parseInt(v));
        const hms = registro.hora.split(":").map(v => parseInt(v));
        const timestart = new Date(ymd[0], ymd[1] - 1, ymd[2], hms[0], hms[1], hms[2]);
        return {
            timestart: timestart,
            timeend: timestart,
            valor: parseFloat(registro.altura),
            series_id: series_id
        };
    }
    parseRegistroPrecipitacion(registro, series_id) {
        const ymd = registro.fecha.split("-").map(v => parseInt(v));
        const hms = registro.hora.split(":").map(v => parseInt(v));
        const timestart = new Date(ymd[0], ymd[1] - 1, ymd[2], hms[0], hms[1], hms[2]);
        return {
            timestart: timestart,
            timeend: timestart,
            valor: parseFloat(registro.precipitacion),
            series_id: series_id
        };
    }
    getSites(filter) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.tokenAuth) {
                yield this.login();
            }
            let estaciones = [];
            if (filter.id_externo) {
                if (Array.isArray(filter.id_externo)) {
                    for (const id_externo of filter.id_externo) {
                        const data = yield this.listEstaciones(id_externo);
                        estaciones.push(...data.estaciones.map(e => this.parseEstacion(e)));
                    }
                }
                else {
                    const data = yield this.listEstaciones(filter.id_externo);
                    estaciones.push(...data.estaciones.map(e => this.parseEstacion(e)));
                }
            }
            else {
                const data = yield this.listEstaciones();
                estaciones.push(...data.estaciones.map(e => this.parseEstacion(e)));
            }
            // busca ids
            for (const estacion of estaciones) {
                const matches = yield CRUD_1.estacion.read({ tabla: estacion.tabla, id_externo: estacion.id_externo });
                if (matches.length) {
                    estacion.id = matches[0].id;
                }
            }
            return estaciones;
        });
    }
    getSeries() {
        return __awaiter(this, arguments, void 0, function* (filter = {}) {
            if (!this.tokenAuth) {
                yield this.login();
            }
            const variable_altura = yield CRUD_1.var.read({ id: 2 });
            const variable_precipitacion = yield CRUD_1.var.read({ id: 27 });
            const proc = yield CRUD_1.procedimiento.read({ id: 1 });
            const unidades_metro = yield CRUD_1.unidades.read({ id: 11 });
            const unidades_milimetro = yield CRUD_1.unidades.read({ id: 9 });
            const estaciones = yield this.getSites(filter);
            let series = [];
            for (const estacion of estaciones) {
                const var_h_match = (filter.var_id) ? (Array.isArray(filter.var_id)) ? (filter.var_id.indexOf(2) >= 0) ? true : false : (filter.var_id == 2) ? true : false : true;
                if (var_h_match) {
                    let serie_altura;
                    if (estacion.id) {
                        const matches = yield CRUD_1.serie.read({
                            tipo: "puntual",
                            estacion_id: estacion.id,
                            var_id: 2,
                            proc_id: 1,
                            unit_id: 11
                        });
                        if (matches.length) {
                            serie_altura = matches[0];
                            this.series_map[serie_altura.id] = {
                                id_estacion: parseInt(estacion.id_externo),
                                variable: "altura",
                                var_id: 2,
                                estacion_id: estacion.id,
                                serie: matches[0]
                            };
                        }
                    }
                    if (!serie_altura) {
                        serie_altura = {
                            tipo: "puntual",
                            estacion: estacion,
                            var: variable_altura,
                            procedimiento: proc,
                            unidades: unidades_metro
                        };
                    }
                    series.push(serie_altura);
                }
                const var_p_match = (filter.var_id) ? (Array.isArray(filter.var_id)) ? (filter.var_id.indexOf(27) >= 0) ? true : false : (filter.var_id == 27) ? true : false : true;
                if (var_p_match) {
                    let serie_precipitacion;
                    if (estacion.id) {
                        const matches = yield CRUD_1.serie.read({
                            tipo: "puntual",
                            estacion_id: estacion.id,
                            var_id: 27,
                            proc_id: 1,
                            unit_id: 9
                        });
                        if (matches.length) {
                            serie_precipitacion = matches[0];
                            this.series_map[serie_precipitacion.id] = {
                                id_estacion: parseInt(estacion.id_externo),
                                variable: "precipitacion",
                                var_id: 27,
                                estacion_id: estacion.id,
                                serie: matches[0]
                            };
                        }
                    }
                    if (!serie_precipitacion) {
                        serie_precipitacion = {
                            tipo: "puntual",
                            estacion: estacion,
                            var: variable_precipitacion,
                            procedimiento: proc,
                            unidades: unidades_milimetro
                        };
                    }
                    series.push(serie_precipitacion);
                }
            }
            return series;
        });
    }
    getOneSerie(series_id, year, month, day) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.tokenAuth) {
                yield this.login();
            }
            if (!this.series_map[series_id]) {
                console.error(`series_id=${series_id} missing in series map. Run getSeries`);
                return;
            }
            let observaciones;
            if (this.series_map[series_id].variable == "altura") {
                const data = yield this.getAlturas(this.series_map[series_id].id_estacion, year, month, day);
                observaciones = data.alturas.map(o => this.parseRegistroAltura(o, series_id));
            }
            else if (this.series_map[series_id].variable == "precipitacion") {
                const data = yield this.getPrecipitaciones(this.series_map[series_id].id_estacion, year, month, day);
                observaciones = data.precipitaciones.map(o => this.parseRegistroPrecipitacion(o, series_id));
            }
            return observaciones;
        });
    }
    getDateParams(timestart, timeend) {
        if (!timestart) {
            throw new Error("Missing timestart");
        }
        if (!timeend) {
            throw new Error("Missing timeend");
        }
        const year = timestart.getFullYear();
        const month = timestart.getMonth() + 1;
        const day_ = timestart.getDate();
        if (timeend.getFullYear() != year || timeend.getMonth() + 1 != month) {
            throw new Error("Requested period must be within a calendar month");
        }
        const day = (timeend.getDate() == day_) ? day_ : undefined;
        return {
            year,
            month,
            day
        };
    }
    getSeriesIdList(series_id, var_id, estacion_id, id_externo) {
        let series_id_list;
        if (series_id) {
            if (Array.isArray(series_id)) {
                series_id_list = series_id;
            }
            else {
                series_id_list = [series_id];
            }
        }
        else {
            for (const [key, mapped_serie] of Object.entries(this.series_map)) {
                if (var_id) {
                    if (Array.isArray(var_id)) {
                        if (var_id.indexOf(mapped_serie.var_id) < 0) {
                            continue;
                        }
                    }
                    else {
                        if (mapped_serie.var_id != var_id) {
                            continue;
                        }
                    }
                }
                if (estacion_id) {
                    if (Array.isArray(estacion_id)) {
                        if (estacion_id.indexOf(mapped_serie.estacion_id) < 0) {
                            continue;
                        }
                    }
                    else {
                        if (mapped_serie.estacion_id != estacion_id) {
                            continue;
                        }
                    }
                }
                if (id_externo) {
                    if (Array.isArray(id_externo)) {
                        if (id_externo.indexOf(mapped_serie.id_estacion.toString()) < 0) {
                            continue;
                        }
                    }
                    else {
                        if (mapped_serie.id_estacion.toString() != id_externo) {
                            continue;
                        }
                    }
                }
                series_id_list.push(parseInt(key));
            }
        }
        return series_id_list;
    }
    get(filter) {
        return __awaiter(this, void 0, void 0, function* () {
            const dateParams = this.getDateParams(filter.timestart, filter.timeend);
            const series_id_list = this.getSeriesIdList(filter.series_id, filter.var_id, filter.estacion_id, filter.id_externo);
            if (!series_id_list.length) {
                console.error("Series not found for the requested filter");
            }
            const series_id = series_id_list[0];
            const observaciones = yield this.getOneSerie(series_id, dateParams.year, dateParams.month, dateParams.day);
            //filter by date
            return observaciones.filter(o => {
                if (o.timestart.getTime() < filter.timestart.getTime() || o.timeend.getTime() > filter.timeend.getTime()) {
                    return false;
                }
                return true;
            });
        });
    }
    update(filter) {
        return __awaiter(this, void 0, void 0, function* () {
            const observaciones = yield this.get(filter);
            return new CRUD_1.observaciones(observaciones).create();
        });
    }
    getMulti(filter) {
        return __awaiter(this, void 0, void 0, function* () {
            const dateParams = this.getDateParams(filter.timestart, filter.timeend);
            const series_id_list = this.getSeriesIdList(filter.series_id, filter.var_id, filter.estacion_id, filter.id_externo);
            let series = [];
            for (const series_id of series_id_list) {
                const observaciones = yield this.getOneSerie(series_id, dateParams.year, dateParams.month, dateParams.day);
                if (observaciones && observaciones.length) {
                    series.push(Object.assign(Object.assign({}, this.series_map[series_id].serie), { observaciones: observaciones }));
                }
                else {
                    console.error("Data not found for series_id " + series_id);
                }
            }
            return series;
        });
    }
}
exports.Client = Client;
Client._get_is_multiseries = false;
