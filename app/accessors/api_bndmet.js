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
// @ts-ignore
const accessor_utils_1 = require("../accessor_utils");
// @ts-ignore
const CRUD_1 = require("../CRUD");
const variable_1 = require("a5base/variable");
class Client extends abstract_accessor_engine_1.AbstractAccessorEngine {
    constructor(config) {
        super(config);
        // map attribute names to VariableNames
        this.defaults = {
            url: "https://api-bndmet.decea.mil.br/v1",
            tabla_id: "inmet",
            var_map: {
                "Precipitação": "Precipitation"
            },
            dt_map: {
                "Horário": "01:00:00",
                "Diario": "24:00:00",
                "Mensal": "1 mon"
            },
            unit_map: {
                "mm": 9
            },
            fenomeno_map: {
                "I175": {
                    var_id: 31,
                    unit_id: 9
                },
                "I006": {
                    var_id: 1,
                    unit_id: 22
                },
            }
        };
        this.red = {
            nombre: "inmet",
            tabla_id: "inmet",
            public: false,
            public_his_plata: false
        };
        this.url = config.url || this.defaults.url;
        this.token = config.token;
        this.tabla_id = config.tabla_id || this.defaults.tabla_id;
        this.var_map = config.var_map || this.defaults.var_map;
        this.dt_map = config.dt_map || this.defaults.dt_map;
        this.unit_map = config.unit_map || this.defaults.unit_map;
        this.fenomeno_map = config.fenomeno_map || this.defaults.fenomeno_map || {};
        this.estaciones_map = config.estaciones_map || {};
        this.TipoEstacao = config.TipoEstacao || "automatica";
    }
    headers() {
        return {
            "accept": "application/json",
            "x-api-key": this.token
        };
    }
    createRed() {
        return __awaiter(this, void 0, void 0, function* () {
            return CRUD_1.red.create(Object.assign(Object.assign({}, this.red), { tabla_id: this.tabla_id }));
        });
    }
    // api calls
    get_estacoes(tipo, estado, regiao) {
        return __awaiter(this, void 0, void 0, function* () {
            const params = {
                tipo: tipo
            };
            if (estado) {
                params.estado = estado;
            }
            if (regiao) {
                params.regiao = regiao;
            }
            return (0, accessor_utils_1.fetchData)(`${this.url}/estacoes`, {
                params: params,
                headers: this.headers()
            });
        });
    }
    getAtributos(codEstacao, periodo, agruparPor = "classe") {
        return __awaiter(this, void 0, void 0, function* () {
            return (0, accessor_utils_1.fetchData)(`${this.url}/estaciones/${codEstacao}/atributos`, {
                params: {
                    periodo: periodo,
                    agruparPor: agruparPor
                },
                headers: this.headers()
            });
        });
    }
    getFenomeno(codEstacao, codFenomeno, dataInicio, dataFinal) {
        return __awaiter(this, void 0, void 0, function* () {
            return (0, accessor_utils_1.fetchData)(`${this.url}/estaciones/${codEstacao}/fenomenos/${codFenomeno}`, {
                params: {
                    dataInicio: dataInicio,
                    dataFinal: dataFinal
                },
                headers: this.headers()
            });
        });
    }
    // parsers
    parseEstacion(e) {
        return new CRUD_1.estacion({
            tabla: this.tabla_id,
            nombre: e.nome,
            geom: {
                type: "Point",
                coordinates: [parseFloat(e.longitude), parseFloat(e.latitude)]
            },
            propietario: e.entidadeResponsavel,
            id_externo: e.codEstacao,
            distrito: e.estado,
            pais: "Brasil",
            tipo: (e.tipo == "Automatica") ? "A" : "M",
            automatica: (e.tipo == "Automatica") ? true : false,
            altitud: parseFloat(e.altitudeEmMetros),
            real: true,
            habilitar: true,
            observaciones: `WSI=${e.codWsi}`
        });
    }
    // mappers
    mapFenomeno(fenomeno) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!(fenomeno.classe in this.var_map)) {
                console.warn(`Clase de variable ${fenomeno.classe} no mapeado`);
                return;
            }
            if (fenomeno.unidade in this.unit_map) {
                var unit_id = this.unit_map[fenomeno.unidade];
            }
            else {
                const unidades = yield CRUD_1.unidades.read({ abrev: fenomeno.unidade });
                if (!unidades.length) {
                    console.warn(`Unidades '${fenomeno.unidade}' no encontradas`);
                    return;
                }
                var unit_id = unidades[0].id;
            }
            if (!(fenomeno.periodicidade in this.dt_map)) {
                console.warn(`Periodicidad '${fenomeno.periodicidade}' no mapeada`);
                return;
            }
            const variables = yield variable_1.Variable.read({
                VariableName: this.var_map[fenomeno.classe],
                timeSupport: this.dt_map[fenomeno.periodicidade]
            });
            if (!variables.length) {
                console.warn(`No se encontró variable con VariableName=${this.var_map[fenomeno.classe]} y timeSupport=${this.dt_map[fenomeno.periodicidade]}`);
                return;
            }
            if (variables.length > 1) {
                console.warn(`Se encontró más de una variable con VariableName=${this.var_map[fenomeno.classe]} y timeSupport=${this.dt_map[fenomeno.periodicidade]}. Se utilizará la primera`);
            }
            return {
                var_id: variables[0].id,
                unit_id: unit_id
            };
        });
    }
    parseSeries(get_attr_response, estacion_id) {
        return __awaiter(this, void 0, void 0, function* () {
            const series = [];
            for (const [attr_name, atributo] of Object.entries(get_attr_response.data.atributos)) {
                for (const [periodo, fenomeno_list] of Object.entries(atributo.fenomenosPorPeriodo)) {
                    for (const fenomeno of fenomeno_list) {
                        const fenomeno_map = yield this.mapFenomeno(fenomeno);
                        if (fenomeno_map) {
                            this.fenomeno_map[fenomeno.codFenomeno] = fenomeno_map;
                            console.debug(`Mapped codFenomeno=${fenomeno.codFenomeno}`);
                            const serie = new CRUD_1.serie({
                                estacion: { id: estacion_id },
                                var: { id: fenomeno_map.var_id },
                                unit_id: { id: fenomeno_map.unit_id },
                                proc_id: { id: 1 }
                            });
                            yield serie.getId();
                            this.estaciones_map[get_attr_response.params.codEstacao].series[fenomeno.codFenomeno] = serie.id;
                            series.push(serie);
                        }
                        else {
                            console.warn(`Unable to map codFenomeno=${fenomeno.codFenomeno}`);
                        }
                    }
                }
            }
            return series;
        });
    }
    // accessor interface
    getSites(filter = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            const response = yield this.get_estacoes(this.TipoEstacao || "automatica");
            const estaciones = response.data.map(e => this.parseEstacion(e));
            for (const e of estaciones) {
                yield e.getEstacionId();
                this.estaciones_map[e.id_externo] = { estacion_id: e.id, series: {} };
            }
            return estaciones;
        });
    }
    getSeriesOfSite(estacion_id, filter = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!Object.keys(this.estaciones_map).length) {
                yield this.getSites();
            }
            for (const [codEstacao, estacion_map] of Object.entries(this.estaciones_map)) {
                if (estacion_id != estacion_map.estacion_id) {
                    continue;
                }
                const get_attr_response = yield this.getAtributos(codEstacao);
                const series = yield this.parseSeries(get_attr_response, estacion_id);
                return (0, accessor_utils_1.filterSeries)(series, filter);
            }
            console.warn(`El id de estación ${estacion_id} no está mapeado`);
            return [];
        });
    }
    getSeries(filter = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            var estaciones = yield this.getSites();
            estaciones = (0, accessor_utils_1.filterSites)(estaciones, filter);
            const series = [];
            for (const e of estaciones) {
                const series_of_site = yield this.getSeriesOfSite(e.id, filter);
                series.push(...series_of_site);
            }
            return series;
        });
    }
    findSerie(estacion_id, series_id) {
        if (!this.estaciones_map) {
            throw new Error("estaciones_map not set");
        }
        for (const [codEstacao, mapping] of Object.entries(this.estaciones_map)) {
            if (mapping.estacion_id == estacion_id) {
                for (const [codFenomeno, mapped_series_id] of Object.entries(mapping.series)) {
                    if (mapped_series_id == series_id) {
                        return [codEstacao, codFenomeno];
                    }
                }
            }
        }
        throw new Error(`estacion_id: ${estacion_id}, series_id: ${series_id} not found in series mapping`);
    }
}
Client._get_is_multiseries = false;
exports.Client = Client;
function formatLocalDate(date) {
    const pad = (n) => String(n).padStart(2, "0");
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
}
function checkMaxDays(desde, hasta, maxDays = 365 * 5) {
    if (!desde || !hasta) {
        return;
    }
    const startDate = new Date(desde);
    const endDate = new Date(hasta);
    const diffTime = Math.abs(endDate.getTime() - startDate.getTime());
    const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
    if (diffDays > maxDays) {
        throw new Error(`The date range between ${desde} and ${hasta} exceeds the maximum allowed days (${maxDays})`);
    }
}
