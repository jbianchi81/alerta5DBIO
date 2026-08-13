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
const fast_xml_parser_1 = require("fast-xml-parser");
const promises_1 = require("node:fs/promises");
const accessor_utils_1 = require("./accessor_utils");
const CRUD_1 = require("../CRUD");
const variable_1 = require("a5base/variable");
const abstract_accessor_engine_1 = require("./abstract_accessor_engine");
const axios_1 = __importDefault(require("axios"));
function escapeRegExp(str) {
    return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); // $& means the whole matched string
}
function replaceAll(str, find, replace) {
    return str.replace(new RegExp(escapeRegExp(find), 'g'), replace);
}
class Client extends abstract_accessor_engine_1.AbstractAccessorEngine {
    getVariableFromVarId(var_id) {
        if (!this.var_map) {
            throw new Error("var map not set");
        }
        for (const [key, varmap] of Object.entries(this.var_map)) {
            if (varmap.var_id == var_id) {
                return key;
            }
        }
        throw new Error(`Var map for var_id=${var_id} not found`);
    }
    constructor(config) {
        super(config);
        this.config = config;
        this.var_map = {
            "P": {
                var_id: 27,
                proc_id: 1,
                unit_id: 9
            },
            "H": {
                var_id: 2,
                proc_id: 1,
                unit_id: 11
            },
            "Q": {
                var_id: 4,
                proc_id: 1,
                unit_id: 10
            }
        };
        this.config = config;
        this.url = config.url;
        this.tabla_id = config.tabla_id;
    }
    getData(filter, options = {}) {
        var _a, _b, _c, _d;
        return __awaiter(this, void 0, void 0, function* () {
            const { idEstacion, variable, fechaDesde, fechaHasta } = filter;
            if (!idEstacion) {
                throw new Error("Missing idEstacion");
            }
            if (!variable) {
                throw new Error("Missing variable");
            }
            const xml = `<?xml version="1.0"?>
<soap:Envelope
    xmlns:soap="http://www.w3.org/2003/05/soap-envelope"
    xmlns:m="https://www.saltogrande.org/ws.php">
    <soap:Header/>
    <soap:Body>
        <m:HidroSerieHistorica>
            <m:idEstacion>${this.escapeXml(idEstacion)}</m:idEstacion>
            <m:variable>${this.escapeXml(variable.toUpperCase())}</m:variable>
            <m:fechaDesde>${this.formatDate(fechaDesde)}</m:fechaDesde>
            <m:fechaHasta>${this.formatDate(fechaHasta)}</m:fechaHasta>
        </m:HidroSerieHistorica>
    </soap:Body>
</soap:Envelope>`;
            const response = yield axios_1.default.post(this.url, xml, {
                headers: {
                    "Content-Type": "text/xml;charset=UTF-8",
                    "SOAPAction": "HidroSerieHistorica",
                }
            });
            const responseText = yield response.data;
            if (options.save_raw_result) {
                yield (0, promises_1.writeFile)(options.save_raw_result, responseText);
            }
            if (response.statusText != "OK") {
                throw new Error(`Salto Grande SOAP request failed: ${response.status} ${response.statusText}`);
            }
            const parser = new fast_xml_parser_1.XMLParser({
                ignoreAttributes: false,
                removeNSPrefix: true,
            });
            const parsed = parser.parse(responseText);
            const itemValue = (_d = (_c = (_b = (_a = parsed === null || parsed === void 0 ? void 0 : parsed.Envelope) === null || _a === void 0 ? void 0 : _a.Body) === null || _b === void 0 ? void 0 : _b.HidroSerieHistoricaResponse) === null || _c === void 0 ? void 0 : _c.return) === null || _d === void 0 ? void 0 : _d.item;
            if (itemValue == null) {
                throw new Error("No se encontró tag <return> en la respuesta SOAP");
            }
            const items = Array.isArray(itemValue)
                ? itemValue
                : [itemValue];
            return items
                .map((item) => {
                const fecha = item === null || item === void 0 ? void 0 : item.Fecha["#text"];
                const valor = Number(item === null || item === void 0 ? void 0 : item.Valor["#text"]);
                if (!fecha || !Number.isFinite(valor)) {
                    return undefined;
                }
                // Same filtering as the Perl script.
                if (valor === -999 || valor === -999.9) {
                    return undefined;
                }
                const parsedDate = new Date(fecha);
                if (Number.isNaN(parsedDate.getTime())) {
                    return undefined;
                }
                return {
                    idEstacion,
                    variable: variable.toUpperCase(),
                    fecha: parsedDate,
                    valor,
                };
            })
                .filter((item) => item !== undefined);
        });
    }
    getListaEstacionesTelemetricas(options = {}) {
        var _a, _b, _c;
        return __awaiter(this, void 0, void 0, function* () {
            const xml = `<?xml version="1.0"?>
	<soap:Envelope
		xmlns:soap="http://www.w3.org/2003/05/soap-envelope"
		xmlns:m="https://www.saltogrande.org/ws.php">
		<soap:Header/>
		<soap:Body>
			<m:ListaEstacionesTelemetricas>
				<m:Activas>true</m:Activas>
			</m:ListaEstacionesTelemetricas>
		</soap:Body>
	</soap:Envelope>`;
            const response = yield axios_1.default.post(this.url, xml, {
                headers: {
                    "Content-Type": "text/xml;charset=UTF-8",
                    "SOAPAction": "ListaEstacionesTelemetricas",
                }
            });
            const responseText = yield response.data;
            if (response.statusText != "OK") {
                throw new Error(`Salto Grande SOAP request failed: ${response.status} ${response.statusText}`);
            }
            const parser = new fast_xml_parser_1.XMLParser({
                ignoreAttributes: false,
                removeNSPrefix: true,
            });
            if (options.save_raw_result) {
                yield (0, promises_1.writeFile)(options.save_raw_result, responseText);
            }
            const parsed = parser.parse(responseText);
            const returnValue = (_c = (_b = (_a = parsed === null || parsed === void 0 ? void 0 : parsed.Envelope) === null || _a === void 0 ? void 0 : _a.Body) === null || _b === void 0 ? void 0 : _b.ListaEstacionesTelemetricasResponse) === null || _c === void 0 ? void 0 : _c.return;
            if (returnValue == null) {
                throw new Error("No se encontró <return> en la respuesta de ListaEstacionesTelemetricas");
            }
            const items = Array.isArray(returnValue.item)
                ? returnValue.item
                : returnValue.item
                    ? [returnValue.item]
                    : [];
            return items.map((item) => {
                var _a;
                const variables = (_a = item.Variables) === null || _a === void 0 ? void 0 : _a.item;
                return {
                    id: String(item.Id["#text"]),
                    name: String(item.Nombre["#text"]),
                    lat: Number(item.Latitud["#text"]),
                    lon: Number(item.Longitud["#text"]),
                    date: new Date(item.Fecha["#text"]),
                    variables: variables == null
                        ? []
                        : Array.isArray(variables)
                            ? variables.map(v => String(v["#text"]))
                            : [String(variables["#text"])],
                };
            });
        });
    }
    parseSite(site) {
        if (!site.variables.length) {
            return [];
        }
        const estacion = new CRUD_1.estacion({
            id_externo: site.id,
            nombre: site.name,
            geom: {
                type: "Point",
                coordinates: [site.lon, site.lat]
            },
            tabla: this.tabla_id,
            automatica: true,
            habilitar: true,
            propietario: "Salto Grande",
            real: true,
            tipo: "A"
        });
        return site.variables.map(v => {
            if (!(v in this.var_map)) {
                throw new Error(`Variable not found in mapping: ${v}`);
            }
            const varmap = this.var_map[v];
            return new CRUD_1.serie({
                tipo: "puntual",
                estacion: estacion,
                "var": (varmap.var) ? varmap.var : { id: varmap.var_id },
                procedimiento: (varmap.procedimiento) ? varmap.procedimiento : { id: varmap.proc_id },
                unidades: (varmap.unidades) ? varmap.unidades : { id: varmap.unit_id }
            });
        });
    }
    formatDate(date) {
        const pad = (n) => String(n).padStart(2, "0");
        return [
            date.getFullYear(),
            pad(date.getMonth() + 1),
            pad(date.getDate()),
        ].join("-") + ` ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
    }
    escapeXml(value) {
        value = replaceAll(value, "&", "&amp;");
        value = replaceAll(value, "<", "&lt;");
        value = replaceAll(value, ">", "&gt;");
        value = replaceAll(value, '"', "&quot;");
        value = replaceAll(value, "'", "&apos;");
        return value;
    }
    getMetadataOfVarMap() {
        return __awaiter(this, void 0, void 0, function* () {
            for (const key of Object.keys(this.var_map)) {
                const varmap = this.var_map[key];
                if (!varmap.var) {
                    varmap.var = yield variable_1.Variable.read(varmap.var_id);
                }
                if (!varmap.procedimiento) {
                    varmap.procedimiento = yield CRUD_1.procedimiento.read({ id: varmap.proc_id });
                }
                if (!varmap.unidades) {
                    varmap.unidades = yield CRUD_1.unidades.read(varmap.unit_id);
                }
            }
        });
    }
    mapSeries(series) {
        this.serie_map = {};
        for (const s of series) {
            if (!s.var.id) {
                throw new Error(`Missing var id for series id=${s.id}`);
            }
            this.serie_map[s.id] = {
                idEstacion: s.estacion.id_externo,
                variable: this.getVariableFromVarId(s.var.id),
                serie: s
            };
        }
    }
    getSeries(filter = {}, options = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.getMetadataOfVarMap();
            const sites = yield this.getListaEstacionesTelemetricas(options);
            const series = [];
            for (const s of sites) {
                const se = this.parseSite(s);
                for (const ser of se) {
                    yield ser.estacion.getEstacionId();
                    yield ser.getId(false);
                }
                series.push(...se);
            }
            this.mapSeries(series);
            return (0, accessor_utils_1.filterSeries)(series, filter);
        });
    }
    updateSeries(filter = {}, options = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            const series = yield this.getSeries(filter, options);
            return CRUD_1.serie.create(series, {
                refresh_date_range: false,
                series_metadata: true,
                upsert_estacion: true,
                no_update: options.no_update
            });
        });
    }
    parseObservacion(o, series_id) {
        return {
            series_id: series_id,
            timestart: o.fecha,
            timeend: o.fecha,
            valor: o.valor
        };
    }
    get(filter, options) {
        return __awaiter(this, void 0, void 0, function* () {
            const series_id = filter.series_id;
            if (!series_id) {
                throw new Error("Missing series_id");
            }
            if (Array.isArray(series_id)) {
                throw new Error("Bad filter series_id. must be a number, not an array");
            }
            if (!(typeof series_id == "number")) {
                throw new Error("Bad filter series_id. must be a number");
            }
            if (!filter.timestart) {
                throw new Error("Missing timestart");
            }
            if (!filter.timeend) {
                throw new Error("Missing timeend");
            }
            if (!this.serie_map) {
                console.debug("serie map not set. Calling .getSeries()");
                yield this.getSeries();
            }
            if (!this.serie_map) {
                throw new Error("Failed to get series");
            }
            if (!(series_id in this.serie_map)) {
                throw new Error(`series_id=${series_id} not found in series mapping`);
            }
            const seriemap = this.serie_map[series_id];
            const data = yield this.getData({
                idEstacion: seriemap.idEstacion,
                variable: seriemap.variable,
                fechaDesde: filter.timestart,
                fechaHasta: filter.timeend
            });
            const observaciones = data.map(d => this.parseObservacion(d, series_id));
            if (options === null || options === void 0 ? void 0 : options.return_series) {
                const serie = seriemap.serie;
                serie.setObservaciones(observaciones);
                return [serie];
            }
            else {
                return new CRUD_1.observaciones(observaciones);
            }
        });
    }
}
exports.Client = Client;
