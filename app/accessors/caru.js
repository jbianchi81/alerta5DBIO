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
const CRUD_1 = require("../CRUD");
class Client extends abstract_accessor_engine_1.AbstractAccessorEngine {
    constructor(config) {
        super(config);
        this.default_config = {
            "url": "http://190.0.152.194:8080",
            "template_path": "alturas/web/user/estacion/${0}/0",
            "series_map": {
                3280: 22,
                3306: 21,
                7059: 39,
                7060: 19,
                7061: 20,
                7062: 37
            },
            "estaciones_map": {
                3280: {
                    id: 1699,
                    nombre: "Nueva Palmira",
                    geom: { "type": "Point", "coordinates": [-58.422029, -33.878478] },
                    id_externo: "22",
                    tabla: "estaciones_varios"
                },
                3306: {
                    id: 1700,
                    nombre: "Boca Gualeguaychú CARU",
                    geom: { "type": "Point", "coordinates": [-58.4166666666667, -33.0666666666667] },
                    id_externo: "21",
                    tabla: "estaciones_varios"
                },
                7059: {
                    id: 2231,
                    nombre: "Nuevo Berlín",
                    geom: { "type": "Point", "coordinates": [-58.061997, -32.979836] },
                    id_externo: "39",
                    tabla: "estaciones_varios"
                },
                7060: {
                    id: 2232,
                    nombre: "Paysandú",
                    geom: { "type": "Point", "coordinates": [-58.102575, -32.313646] },
                    id_externo: "19",
                    tabla: "estaciones_varios"
                },
                7061: {
                    id: 2233,
                    nombre: "Concepción del Uruguay - CARU",
                    geom: { "type": "Point", "coordinates": [-58.221219, -32.477173] },
                    id_externo: "20",
                    tabla: "estaciones_varios"
                },
                7062: {
                    id: 2235,
                    nombre: "Fray Bentos - CARU",
                    geom: { "type": "Point", "coordinates": [-58.314443, -33.112132] },
                    id_externo: "37",
                    tabla: "estaciones_varios"
                }
            }
        };
        this.setConfig(config);
        this.connection = axios_1.default.create({ proxy: this.config.proxy });
    }
    getValues(id, series_id) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!id) {
                if (!series_id) {
                    throw new Error("Missing id or series_id");
                }
                if (!(series_id in this.config.series_map)) {
                    throw new Error(`series_id ${series_id} not found in series_map`);
                }
                id = this.config.series_map[series_id];
            }
            const url = `${this.config.url}/${this.config.template_path.replace("${0}", id.toString())}`;
            console.debug(`descargando ${url}`);
            const response = yield this.connection.get(url);
            const matches = response.data.match(/alturasJson\s*=\s*([\s\S]*?);/); //  response.data.match(/alturasJson\s*=(.+?);/)
            if (!matches.length) {
                throw new Error("Data not found in downloaded file");
            }
            const data = JSON.parse(matches[1]);
            const observaciones = [];
            for (const item of data) {
                const d = item.fecha.split(" ");
                const f = d[0].split("\/").map((i) => parseInt(i));
                const t = d[1].split(":").map((i) => parseInt(i));
                const timestart = new Date(f[2], f[1] - 1, f[0], t[0], t[1]);
                if (timestart.toString() == "Invalid Date") {
                    console.error(`Invalid date string: ${item.fecha}. Skipping`);
                    continue;
                }
                const valor = parseFloat(item.altura);
                if (valor.toString() == "NaN") {
                    console.error(`Invalid value: ${item.value}. Skipping`);
                    continue;
                }
                observaciones.push({
                    series_id: series_id,
                    timestart: timestart,
                    timeend: timestart,
                    valor: valor
                });
            }
            return observaciones;
        });
    }
    getSeries(filter) {
        return __awaiter(this, void 0, void 0, function* () {
            const variable = yield CRUD_1.var.read({ id: 2 });
            const proc = yield CRUD_1.procedimiento.read({ id: 1 });
            const unidades = yield CRUD_1.unidades.read({ id: 11 });
            return Object.keys(this.config.estaciones_map).map(series_id => {
                return new CRUD_1.serie({
                    tipo: "puntual",
                    id: parseInt(series_id),
                    var: variable,
                    unidades: unidades,
                    estacion: this.config.estaciones_map[series_id],
                    procedimiento: proc // {id: 1}
                });
            });
        });
    }
    get(filter, options = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!filter.series_id) {
                throw new Error("Missing series_id");
            }
            var observaciones = [];
            if (Array.isArray(filter.series_id)) {
                for (const series_id of filter.series_id) {
                    try {
                        const observaciones_ = yield this.get(Object.assign(Object.assign({}, filter), { series_id: series_id }));
                        const c_obs = new CRUD_1.observaciones(observaciones_);
                        observaciones.push(...c_obs.removeDuplicates());
                    }
                    catch (e) {
                        console.error(e.toString());
                    }
                }
            }
            else {
                try {
                    const obs = yield this.getValues(undefined, filter.series_id);
                    var c_obs = new CRUD_1.observaciones(obs);
                    observaciones = c_obs.removeDuplicates();
                }
                catch (e) {
                    console.error(e.toString());
                    return;
                }
            }
            if (filter.timestart) {
                observaciones = observaciones.filter(obs => obs.timestart >= filter.timestart);
            }
            if (filter.timeend) {
                observaciones = observaciones.filter(obs => obs.timeend <= filter.timeend);
            }
            return observaciones;
        });
    }
    update(filter, options = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            var observaciones = yield this.get(filter, options);
            return CRUD_1.observaciones.create(observaciones);
        });
    }
}
Client._get_is_multiseries = false;
exports.Client = Client;
