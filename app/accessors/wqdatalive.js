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
const accessor_utils_js_1 = require("./accessor_utils.js");
const abstract_accessor_engine_js_1 = require("./abstract_accessor_engine.js");
const CRUD_js_1 = require("../CRUD.js");
const timeSteps_js_1 = require("../timeSteps.js");
class Client extends abstract_accessor_engine_js_1.AbstractAccessorEngine {
    getParameterId(var_id, proc_id, unit_id) {
        if (!this.var_map) {
            return;
        }
        for (const [par_id, vmap] of Object.entries(this.var_map)) {
            if (vmap.var_id == var_id && vmap.proc_id == proc_id && vmap.unit_id == unit_id) {
                return parseInt(par_id);
            }
        }
        return;
    }
    /**
     * https://www.nexsens.com/knowledge-base-v2/software/wqdatalive/user-guide/wqdata-live-data-api-doc#operation/devicesInfo
     * no tiene las coordenadas
     * @param baseUrl
     * @param apiKey
     * @returns
     */
    static getDevices() {
        return __awaiter(this, arguments, void 0, function* (baseUrl = "https://www.wqdatalive.com/api/v1", apiKey = "") {
            return (0, accessor_utils_js_1.fetchData)(`${baseUrl}/devices`, { "params": { "apiKey": apiKey } });
        });
    }
    /**
     * https://www.nexsens.com/knowledge-base-v2/software/wqdatalive/user-guide/wqdata-live-data-api-doc#operation/deviceInfo
     * @param baseUrl
     * @param apiKey
     * @param deviceId
     * @returns
     */
    static getDevice() {
        return __awaiter(this, arguments, void 0, function* (baseUrl = "https://www.wqdatalive.com/api/v1", apiKey = "", deviceId) {
            return (0, accessor_utils_js_1.fetchData)(`${baseUrl}/devices/${deviceId}`, { "params": { "apiKey": apiKey } });
        });
    }
    /**
     * https://www.nexsens.com/knowledge-base-v2/software/wqdatalive/user-guide/wqdata-live-data-api-doc#tag/Parameters
     * @param baseUrl
     * @param apiKey
     * @param deviceId
     * @returns
     */
    static getParameters() {
        return __awaiter(this, arguments, void 0, function* (baseUrl = "https://www.wqdatalive.com/api/v1", apiKey = "", deviceId) {
            return (0, accessor_utils_js_1.fetchData)(`${baseUrl}/devices/${deviceId}/parameters`, { "params": { "apiKey": apiKey } });
        });
    }
    /**
     * https://www.nexsens.com/knowledge-base-v2/software/wqdatalive/user-guide/wqdata-live-data-api-doc#operation/ParameterInfo
     * @param baseUrl
     * @param apiKey
     * @param deviceId
     * @param parameterId
     * @returns
     */
    static getParameter() {
        return __awaiter(this, arguments, void 0, function* (baseUrl = "https://www.wqdatalive.com/api/v1", apiKey = "", deviceId, parameterId) {
            return (0, accessor_utils_js_1.fetchData)(`${baseUrl}/devices/${deviceId}/parameters/${parameterId}`, { "params": { "apiKey": apiKey } });
        });
    }
    /**
     * https://www.nexsens.com/knowledge-base-v2/software/wqdatalive/user-guide/wqdata-live-data-api-doc#operation/MultipleParametersData
     * @param baseUrl
     * @param apiKey
     * @param deviceId
     * @param from - start time in UTC YYYY-MM-DD HH:mm:ss
     * @param to - end time in UTC YYYY-MM-DD HH:mm:ss
     * @returns
     */
    static getData() {
        return __awaiter(this, arguments, void 0, function* (baseUrl = "https://www.wqdatalive.com/api/v1", apiKey = "", deviceId, from, to, parameterIds) {
            from = (from instanceof Date) ? from.toISOString().replace("T", " ").substring(0, 19) : from;
            to = (to instanceof Date) ? to.toISOString().replace("T", " ").substring(0, 19) : to;
            const parameterIds_str = (parameterIds) ? parameterIds.join(",") : undefined;
            return (0, accessor_utils_js_1.fetchData)(`${baseUrl}/devices/${deviceId}/parameters/data`, { "params": { "apiKey": apiKey, "from": from, "to": to, "parameterIds": parameterIds_str } });
        });
    }
    /**
     * https://www.nexsens.com/knowledge-base-v2/software/wqdatalive/user-guide/wqdata-live-data-api-doc#operation/SingleParameterData
     * @param baseUrl
     * @param apiKey
     * @param deviceId
     * @param parameterId
     * @param from
     * @param to
     * @returns
     */
    static getParameterData() {
        return __awaiter(this, arguments, void 0, function* (baseUrl = "https://www.wqdatalive.com/api/v1", apiKey = "", deviceId, parameterId, from, to) {
            from = (from instanceof Date) ? from.toISOString().replace("T", " ").substring(0, 19) : from;
            to = (to instanceof Date) ? to.toISOString().replace("T", " ").substring(0, 19) : to;
            return (0, accessor_utils_js_1.fetchData)(`${baseUrl}/devices/${deviceId}/parameters/${parameterId}/data`, { "params": { "apiKey": apiKey, "from": from, "to": to } });
        });
    }
    static getDataLatest() {
        return __awaiter(this, arguments, void 0, function* (baseUrl = "https://www.wqdatalive.com/api/v1", apiKey = "", deviceId, parameterIds) {
            const parameterIds_str = (parameterIds) ? parameterIds.join(",") : undefined;
            return (0, accessor_utils_js_1.fetchData)(`${baseUrl}/devices/${deviceId}/parameters/data/latest`, { "params": { "apiKey": apiKey, "parameterIds": parameterIds_str } });
        });
    }
    static getParameterDataLatest() {
        return __awaiter(this, arguments, void 0, function* (baseUrl = "https://www.wqdatalive.com/api/v1", apiKey = "", deviceId, parameterId) {
            return (0, accessor_utils_js_1.fetchData)(`${baseUrl}/devices/${deviceId}/parameters/${parameterId}/data/latest`, { "params": { "apiKey": apiKey } });
        });
    }
    constructor(config) {
        super(config);
        this.url = config.url;
        this.key = config.key;
        this.devices = config.devices;
        this.parameters = config.parameters;
        this.tabla_id = config.tabla_id;
        this.coordinates = config.coordinates;
        this.var_map = config.var_map;
        this.dt = (config.dt) ? new timeSteps_js_1.Interval(config.dt) : undefined;
    }
    getDevices() {
        return __awaiter(this, void 0, void 0, function* () {
            return Client.getDevices(this.url, this.key);
        });
    }
    getDevice(deviceId) {
        return __awaiter(this, void 0, void 0, function* () {
            return Client.getDevice(this.url, this.key, deviceId);
        });
    }
    getParameters(deviceId) {
        return __awaiter(this, void 0, void 0, function* () {
            return Client.getParameters(this.url, this.key, deviceId);
        });
    }
    getParameter(deviceId, parameterId) {
        return __awaiter(this, void 0, void 0, function* () {
            return Client.getParameter(this.url, this.key, deviceId, parameterId);
        });
    }
    getData(deviceId, from, to, parameterIds) {
        return __awaiter(this, void 0, void 0, function* () {
            return Client.getData(this.url, this.key, deviceId, from, to, parameterIds);
        });
    }
    getParameterData(deviceId, parameterId, from, to) {
        return __awaiter(this, void 0, void 0, function* () {
            return Client.getParameterData(this.url, this.key, deviceId, parameterId, from, to);
        });
    }
    getDataLatest(deviceId, parameterIds) {
        return __awaiter(this, void 0, void 0, function* () {
            return Client.getDataLatest(this.url, this.key, deviceId, parameterIds);
        });
    }
    getParameterDataLatest(deviceId, parameterId) {
        return __awaiter(this, void 0, void 0, function* () {
            return Client.getParameterDataLatest(this.url, this.key, deviceId, parameterId);
        });
    }
    // a5 interface //////////////////////////////
    parseDevice(device) {
        const geom = (this.coordinates && device.id in this.coordinates) ? { type: "Point", coordinates: this.coordinates[device.id] } : undefined;
        return new CRUD_js_1.estacion({
            id_externo: device.id,
            tabla: this.tabla_id,
            geom: geom,
            nombre: `${device.site} - ${device.name}`
        });
    }
    getSites() {
        return __awaiter(this, arguments, void 0, function* (filter = {}) {
            if (!this.tabla_id) {
                throw new Error("missing tabla_id from config");
            }
            if (filter.id_externo && typeof filter.id_externo == "string") {
                const device = yield this.getDevice(parseInt(filter.id_externo));
                var devices = [device];
            }
            else {
                var devices = (yield this.getDevices()).devices;
            }
            const estaciones = [];
            for (const device of devices) {
                const estacion = this.parseDevice(device);
                yield estacion.getEstacionId();
                estaciones.push(estacion);
            }
            return estaciones;
        });
    }
    parseParameters(parameters_1, estacion_1) {
        return __awaiter(this, arguments, void 0, function* (parameters, estacion, skip_unmatched = true) {
            if (!this.var_map) {
                throw new Error("Var map missing");
            }
            const series = [];
            for (const parameter of parameters) {
                if (parameter.id in this.var_map) {
                    const serie = new CRUD_js_1.serie({
                        tipo: "puntual",
                        estacion: estacion,
                        var: yield CRUD_js_1.var.read(this.var_map[parameter.id].var_id),
                        procedimiento: yield CRUD_js_1.procedimiento.read({ id: this.var_map[parameter.id].proc_id || 1 }),
                        unidades: yield CRUD_js_1.unidades.read({ id: this.var_map[parameter.id].unit_id })
                    });
                    yield serie.getId(false);
                    series.push(serie);
                }
                else {
                    if (!skip_unmatched) {
                        throw new Error(`Parameter id ${parameter.id} not found in var_map`);
                    }
                    console.warn(`Parameter id ${parameter.id} not found in var_map`);
                }
            }
            return series;
        });
    }
    getSeries(filter) {
        return __awaiter(this, void 0, void 0, function* () {
            const estaciones = yield this.getSites(filter);
            const series = [];
            for (const estacion of estaciones) {
                const parameters = (yield this.getParameters(parseInt(estacion.id_externo))).parameters;
                const series_ = yield this.parseParameters(parameters, estacion);
                series.push(...series_);
            }
            return (0, accessor_utils_js_1.filterSeries)(series, filter);
        });
    }
    parseData(data, series_id) {
        const obs = [];
        for (const d of data) {
            if (d.value == null) {
                // skips null
                continue;
            }
            const valor = parseFloat(d.value);
            if (valor.toString() == "NaN") {
                // skip invalid
                continue;
            }
            if (valor <= -100000) {
                // skip error code
                continue;
            }
            const ts = (0, accessor_utils_js_1.parseUtcDateTime)(d.timestamp);
            var te = new Date(ts);
            if (this.dt) {
                te = (0, timeSteps_js_1.advanceTimeStep)(te, this.dt);
            }
            obs.push({
                tipo: "puntual",
                series_id: series_id,
                timestart: ts,
                timeend: te,
                valor: valor
            });
        }
        return obs;
    }
    /**
     * No multiseries, retrieves only first series match
     * @param filter
     * @param options
     * @returns
     */
    get(filter_1) {
        return __awaiter(this, arguments, void 0, function* (filter, options = {}) {
            if (!filter) {
                throw new Error("Missing filter");
            }
            if (!filter.timestart || !filter.timeend) {
                throw new Error("Missing timestart+timeend");
            }
            // find matching serie
            if (filter.series_id) {
                const series = yield CRUD_js_1.serie.read({ id: filter.series_id });
                if (!series) {
                    throw new Error("serie not found with id=" + filter.series_id);
                }
                if (Array.isArray(series)) {
                    var serie = series[0];
                }
                else {
                    var serie = series;
                }
            }
            else if (filter.estacion_id && filter.var_id) {
                const series = yield CRUD_js_1.serie.read(filter);
                if (!series.length) {
                    throw new Error("series not found with filter: estacion_id" + filter.estacion_id + " var_id=" + filter.var_id);
                }
                var serie = series[0];
            }
            else {
                throw new Error("missing filter.series_id or filter.var_id+filter.estacion_id");
            }
            // find matching parameter id
            const par_id = this.getParameterId(serie.var.id, serie.procedimiento.id, serie.unidades.id);
            if (!par_id) {
                throw new Error("Parameter id not found for series_id " + serie.id);
            }
            // retrieve data
            const data = yield this.getParameterData(parseInt(serie.estacion.id_externo), par_id, filter.timestart, filter.timeend);
            const observaciones = this.parseData(data.data, serie.id);
            // return
            if (options.return_series) {
                serie.observaciones = observaciones;
                return [serie];
            }
            else {
                return observaciones;
            }
        });
    }
}
exports.Client = Client;
