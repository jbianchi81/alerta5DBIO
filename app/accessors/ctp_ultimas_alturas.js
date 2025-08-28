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
const jsdom_1 = require("jsdom");
const node_fetch_1 = __importDefault(require("node-fetch"));
const luxon_1 = require("luxon");
const CRUD_1 = require("../CRUD");
const util_1 = require("util");
const abstract_accessor_engine_1 = require("./abstract_accessor_engine");
function fetchLatin1(url) {
    return __awaiter(this, void 0, void 0, function* () {
        const res = yield (0, node_fetch_1.default)(url);
        const buffer = yield res.arrayBuffer();
        const decoder = new util_1.TextDecoder("iso-8859-1");
        return decoder.decode(buffer);
    });
}
function parseDate(date_part, time_part) {
    const dmy = date_part.split("-").map(p => parseInt(p));
    const hm = time_part.split(" ")[0].split(":").map(p => parseInt(p));
    const z = time_part.split(" ")[2];
    const timezone = (z === "(RA)") ? "America/Argentina/Buenos_Aires" : "America/La_Paz";
    return new Date(luxon_1.DateTime.fromObject({ year: dmy[2], month: dmy[1], day: dmy[0], hour: hm[0], minute: hm[1] }, { zone: timezone }).toJSDate());
}
function parseValue(value_string) {
    return parseFloat(value_string.replace(/^\s+/, "").replace(/\s+/, ""));
}
const series_id_map = {
    "Misión La Paz DE CTN": 42292,
    "Villa Montes": 42291,
    "Puente Aruma": 42294,
    "Viña Quemada": 42295,
    "Talula": 42296,
    "Tarapaya": 42297,
    "Palca Grande": 42298,
    "San Josecito": 42299,
};
function getSeriesId(nombre_estacion) {
    const n = nombre_estacion.replace(/\-.*$/, "").replace(/\s+$/, "");
    return series_id_map[n];
}
function parseObs(o) {
    return new CRUD_1.observacion({
        tipo: "puntual",
        valor: parseValue(o.valor),
        timestart: parseDate(o.date, o.time),
        timeend: parseDate(o.date, o.time),
        series_id: getSeriesId(o.nombre_estacion),
    });
}
function getUltimasAlturas(url) {
    var _a, _b, _c, _d, _e, _f, _g, _h;
    return __awaiter(this, void 0, void 0, function* () {
        const html = yield fetchLatin1(url);
        const dom = new jsdom_1.JSDOM(html);
        const container = dom.window.document.querySelector("div.pt-md-3:nth-child(3)");
        if (!container) {
            return [];
        }
        const items = container.querySelectorAll("div.col-3");
        const ultimas_alturas = [];
        for (const item of items) {
            const date = (_b = (_a = item.querySelector("div:nth-child(1) > small:nth-child(2)")) === null || _a === void 0 ? void 0 : _a.textContent) !== null && _b !== void 0 ? _b : "";
            if (date == "") {
                console.warn("Date string not found, skipping");
                continue;
            }
            const nombre_estacion = (_d = (_c = item.querySelector("div:nth-child(1) > small:nth-child(4) > a:nth-child(1)")) === null || _c === void 0 ? void 0 : _c.textContent) !== null && _d !== void 0 ? _d : "";
            if (nombre_estacion == "") {
                console.warn("Nombre estacion string not found, skipping");
                continue;
            }
            const valor = (_f = (_e = item.querySelector("div:nth-child(1) > h3:nth-child(5) > span:nth-child(1)")) === null || _e === void 0 ? void 0 : _e.textContent) !== null && _f !== void 0 ? _f : "";
            if (valor == "") {
                console.warn("Valor string not found, skipping");
                continue;
            }
            const time = (_h = (_g = item.querySelector("div:nth-child(1) > small:nth-child(6)")) === null || _g === void 0 ? void 0 : _g.textContent) !== null && _h !== void 0 ? _h : "";
            if (time == "") {
                console.warn("Time string not found, skipping");
                continue;
            }
            ultimas_alturas.push({
                valor,
                date,
                time,
                nombre_estacion,
            });
        }
        return ultimas_alturas;
    });
}
// interface Config {
//     url: string
// }
class Client extends abstract_accessor_engine_1.AbstractAccessorEngine {
    constructor(config) {
        super(config);
        this.default_config = {
            url: "https://www.pilcomayo.net"
        };
        this.setConfig(config);
    }
    get(filter = {}, options = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            const ultimas_alturas = yield getUltimasAlturas(this.config.url);
            return ultimas_alturas.map(o => parseObs(o));
        });
    }
    update(filter = {}, options = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            const observaciones = yield this.get(filter, options);
            return Promise.all(observaciones.map((o) => o.create()));
        });
    }
}
Client._get_is_multiseries = true;
exports.Client = Client;
