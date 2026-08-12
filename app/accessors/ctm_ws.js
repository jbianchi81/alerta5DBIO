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
const fast_xml_parser_1 = require("fast-xml-parser");
const promises_1 = require("node:fs/promises");
const accessor_utils_1 = require("./accessor_utils");
const CRUD_1 = require("../CRUD");
class Client {
    constructor(config) {
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
            const response = yield fetch(this.url, {
                method: "POST",
                headers: {
                    "Content-Type": "text/xml;charset=UTF-8",
                    "SOAPAction": "HidroSerieHistorica",
                },
                body: xml,
            });
            const responseText = yield response.text();
            if (options.save_raw_result) {
                yield (0, promises_1.writeFile)(options.save_raw_result, responseText);
            }
            if (!response.ok) {
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
    getSites(options = {}) {
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
            const response = yield fetch(this.url, {
                method: "POST",
                headers: {
                    "Content-Type": "text/xml;charset=UTF-8",
                    "SOAPAction": "ListaEstacionesTelemetricas",
                },
                body: xml,
            });
            const responseText = yield response.text();
            if (!response.ok) {
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
                    id: String(item.Id),
                    name: String(item.Nombre),
                    lat: Number(item.Latitud),
                    lon: Number(item.Longitud),
                    date: new Date(item.Fecha),
                    variables: variables == null
                        ? []
                        : Array.isArray(variables)
                            ? variables.map(String)
                            : [String(variables)],
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
            tabla: this.tabla_id
        });
        return site.variables.map(v => {
            const varmap = this.var_map[v];
            return new CRUD_1.serie({
                estacion: estacion,
                variable: { id: varmap.var_id },
                procedimiento: { id: varmap.proc_id },
                unidades: { id: varmap.unit_id }
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
        return value
            .replaceAll("&", "&amp;")
            .replaceAll("<", "&lt;")
            .replaceAll(">", "&gt;")
            .replaceAll('"', "&quot;")
            .replaceAll("'", "&apos;");
    }
    getSeries(filter = {}, options = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            const sites = yield this.getSites(options);
            const series = [];
            for (const s of sites) {
                const se = this.parseSite(s);
                for (const ser of se) {
                    yield ser.estacion.getEstacionId();
                    yield ser.getId();
                }
                series.push(...se);
            }
            return (0, accessor_utils_1.filterSeries)(series, filter);
        });
    }
}
exports.Client = Client;
