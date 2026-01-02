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
const utils2_1 = require("../utils2");
const a5base_1 = require("a5base");
class Area extends a5base_1.baseModel {
    constructor(args) {
        super();
        this.id = arguments[0].id;
        this.nombre = arguments[0].nombre;
        this.geom = (arguments[0].geom) ? new GeometryObject(arguments[0].geom) : undefined;
        this.exutorio = (arguments[0].exutorio) ? (arguments[0].exutorio.geom) ? new GeometryObject(arguments[0].exutorio.geom) : (arguments[0].exutorio.type && arguments[0].exutorio.coordinates) ? new GeometryObject(arguments[0].exutorio) : null : null;
        this.exutorio_id = arguments[0].exutorio_id;
        this.ae = arguments[0].ae;
        this.rho = arguments[0].rho;
        this.wp = arguments[0].wp;
        this.activar = arguments[0].activar;
        this.mostrar = arguments[0].mostrar;
        this.area = arguments[0].area;
    }
    static list(filter = {}, options = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            if (filter.id) {
                filter.unid = filter.id;
                delete filter.id;
            }
            const valid_filters = {
                nombre: {
                    type: "regex_string"
                },
                unid: {
                    type: "integer"
                },
                geom: {
                    type: "geometry",
                },
                exutorio: {
                    type: "geometry"
                },
                exutorio_id: {
                    type: "integer"
                },
                activar: {
                    type: "boolean"
                },
                mostrar: {
                    type: "boolean"
                }
            };
            var filter_string = (0, utils2_1.control_filter2)(valid_filters, filter, "areas_pluvio");
            if (!filter_string) {
                throw ("Invalid filters");
            }
            var join_type = "LEFT";
            var tabla_id_filter = "";
            if (filter.tabla_id) {
                if (/[';]/.test(filter.tabla_id)) {
                    throw ("Invalid filter value");
                }
                join_type = "RIGHT";
                tabla_id_filter += ` AND estaciones.tabla='${filter.tabla_id}'`;
            }
            var pagination_clause = (filter.limit) ? `LIMIT ${filter.limit}` : "";
            pagination_clause += (filter.offset) ? ` OFFSET ${filter.offset}` : "";
            //~ console.log("filter_string:" + filter_string)
            if (options && options.no_geom) {
                const stmt = "SELECT \
				areas_pluvio.unid id, \
				areas_pluvio.nombre, \
				st_astext(areas_pluvio.exutorio) exutorio, \
				areas_pluvio.exutorio_id, \
				areas_pluvio.area, \
				areas_pluvio.ae, \
				areas_pluvio.rho, \
				areas_pluvio.wp, \
				areas_pluvio.activar, \
				areas_pluvio.mostrar \
			FROM areas_pluvio \
			" + join_type + " JOIN estaciones ON (estaciones.unid=areas_pluvio.exutorio_id" + tabla_id_filter + ") \
			WHERE areas_pluvio.geom IS NOT NULL " + filter_string + " ORDER BY areas_pluvio.id\
			" + pagination_clause;
                // console.debug(stmt)
                return global.pool.query(stmt)
                    .then(res => {
                    return res.rows.map(r => {
                        if (r.exutorio) {
                            r.exutorio = new GeometryObject(r.exutorio);
                        }
                        return r;
                    });
                });
            }
            else {
                const stmt = "SELECT \
				areas_pluvio.unid id, \
				areas_pluvio.nombre, \
				st_astext(areas_pluvio.geom) geom, \
				st_astext(areas_pluvio.exutorio) exutorio, \
				areas_pluvio.exutorio_id, \
				areas_pluvio.area, \
				areas_pluvio.ae, \
				areas_pluvio.rho, \
				areas_pluvio.wp, \
				areas_pluvio.activar, \
				areas_pluvio.mostrar \
			FROM areas_pluvio \
			" + join_type + " JOIN estaciones ON (estaciones.unid=areas_pluvio.exutorio_id" + tabla_id_filter + ") \
			WHERE areas_pluvio.geom IS NOT NULL " + filter_string + " ORDER BY id\
			" + pagination_clause;
                // console.debug(stmt)
                const res = yield global.pool.query(stmt);
                var areas = res.rows.map((row) => {
                    return new this(row);
                });
                return areas;
            }
        });
    }
}
exports.default = Area;
