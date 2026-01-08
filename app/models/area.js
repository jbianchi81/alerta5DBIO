"use strict";
// models/AreaGroup.ts
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
exports.isGeometryDict = isGeometryDict;
const setGlobal_1 = __importDefault(require("a5base/setGlobal"));
// import {Area as AreaType} from '../a5_types'
const custom_errors_1 = require("../custom_errors");
const utils2_1 = require("../utils2");
// import { Geometry as GeomType } from '../geometry_types'
const geometry_1 = require("a5base/geometry");
const area_group_1 = __importDefault(require("./area_group"));
const g = (0, setGlobal_1.default)();
const node_querystring_1 = require("node:querystring");
class Area {
    constructor(params) {
        this.id = params.id;
        this.nombre = params.nombre;
        this.geom = new geometry_1.Geometry(params.geom);
        this.exutorio = (params.exutorio) ? ("geom" in params.exutorio) ? new geometry_1.Geometry(params.exutorio.geom) : (params.exutorio.type && params.exutorio.coordinates) ? new geometry_1.Geometry(params.exutorio) : undefined : undefined;
        this.exutorio_id = params.exutorio_id;
        this.ae = params.ae;
        this.rho = params.rho;
        this.wp = params.wp;
        this.activar = params.activar;
        this.mostrar = params.mostrar;
        this.area = params.area;
        this.group_id = params.group_id;
    }
    static create(areas, user_id) {
        return __awaiter(this, void 0, void 0, function* () {
            const created_areas = [];
            for (const area of areas) {
                if (!area.geom) {
                    throw new custom_errors_1.BadRequestError("Invalid area: missing geom");
                }
                const created_area = yield this.createOne(area, user_id);
                if (created_area) {
                    created_areas.push(created_area);
                }
            }
            return created_areas;
        });
    }
    static createOne(area_params, user_id) {
        return __awaiter(this, void 0, void 0, function* () {
            const area = new this(area_params);
            if (!area.id) {
                yield area.getId();
            }
            if (area.geom && area.geom.type && area.geom.type == "MultiPolygon") {
                assertPosition3D(area.geom.coordinates);
                area.geom = new geometry_1.Geometry("Polygon", area.geom.coordinates[0]);
            }
            if (area.group_id && user_id) {
                const has_access = yield area_group_1.default.hasAccess(user_id, area.group_id, true);
                if (!has_access) {
                    throw new custom_errors_1.AuthError("El usuario no tiene acceso de escritura para el grupo de áreas indicado");
                }
            }
            const q = this.upsertAreaQuery(area);
            const result = yield g.pool.query(q);
            if (!result.rows.length) {
                throw new Error("Area upsert failed: no rows returned");
            }
            console.info("Upserted areas_pluvio.unid=" + result.rows[0].id);
            return new this(result.rows[0]);
        });
    }
    getId() {
        return __awaiter(this, void 0, void 0, function* () {
            var res = yield g.pool.query(`
            SELECT unid 
            FROM areas_pluvio 
            WHERE nombre = $1
            AND geom = st_geomfromtext($2,4326)
            `, [this.nombre, this.geom.toString()]);
            if (res.rows.length > 0) {
                this.id = res.rows[0].unid;
                return;
            }
            else {
                res = yield g.pool.query(`
                SELECT max(unid)+1 AS id
                FROM areas_pluvio
                `);
                this.id = res.rows[0].id;
            }
        });
    }
    static upsertAreaQuery(area) {
        var query = "";
        var params = [];
        if (area.exutorio) {
            if (area.id) {
                query = `
				INSERT INTO areas_pluvio (unid, nombre, geom, exutorio, exutorio_id, ae, rho, wp, activar, mostrar, group_id) 
				VALUES ($1, $2, ST_GeomFromText($3,4326), ST_GeomFromText($4,4326), $5, $6, $7, $8, $9, $10, $11)
				ON CONFLICT (unid) DO UPDATE SET 
					nombre=excluded.nombre, 
					geom=excluded.geom, 
					exutorio=excluded.exutorio, 
					exutorio_id=excluded.exutorio_id, 
					area = excluded.area, 
					ae = excluded.ae, 
					rho = excluded.rho, 
					wp = excluded.wp, 
					activar = excluded.activar, 
					mostrar = excluded.mostrar,
					group_id = excluded.group_id
				RETURNING 
					unid AS id, 
					nombre, 
					st_astext(geom) AS geom, 
					st_astext(exutorio) AS exutorio, 
					exutorio_id, 
					area, 
					ae, 
					rho, 
					wp, 
					activar, 
					mostrar,
					group_id`;
                params = [area.id, area.nombre, area.geom.toString(), area.exutorio.toString(), area.exutorio_id, area.ae, area.rho, area.wp, area.activar, area.mostrar, area.group_id];
            }
            else {
                query = `
				INSERT INTO areas_pluvio (nombre, geom, exutorio, exutorio_id, ae, rho, wp, activar, mostrar, group_id) 
				VALUES ($1, ST_GeomFromText($2,4326), ST_GeomFromText($3,4326), $4, $5, $6, $7, $8, $9, $10)
				RETURNING 
					unid AS id, 
					nombre, 
					st_astext(geom) AS geom, 
					st_astext(exutorio) AS exutorio, 
					exutorio_id, 
					area, 
					ae, 
					rho, 
					wp, 
					activar, 
					mostrar,
					group_id`;
                params = [area.nombre, area.geom.toString(), area.exutorio.toString(), area.exutorio_id, area.ae, area.rho, area.wp, area.activar, area.mostrar, area.group_id];
            }
        }
        else {
            if (area.id) {
                query = `
				INSERT INTO areas_pluvio (unid, nombre, geom, exutorio_id, ae, rho, wp, activar, mostrar, group_id) 
				VALUES ($1, $2, ST_GeomFromText($3,4326), $4, $5, $6, $7, $8, $9, $10)
				ON CONFLICT (unid) DO UPDATE SET 
					nombre=excluded.nombre,
					geom=excluded.geom,
					exutorio_id=excluded.exutorio_id, 
					area = excluded.area, 
					ae = excluded.ae, 
					rho = excluded.rho, 
					wp = excluded.wp, 
					activar = excluded.activar, 
					mostrar = excluded.mostrar,
					group_id = excluded.group_id 
				RETURNING 
					unid AS id, 
					nombre, 
					st_astext(geom) AS geom, 
					st_astext(exutorio) AS exutorio, 
					exutorio_id, 
					area, 
					ae, 
					rho, 
					wp, 
					activar, 
					mostrar,
					group_id`;
                params = [area.id, area.nombre, area.geom.toString(), area.exutorio_id, area.ae, area.rho, area.wp, area.activar, area.mostrar, area.group_id];
            }
            else {
                query = `
				INSERT INTO areas_pluvio (nombre, geom, exutorio_id, ae, rho, wp, activar, mostrar, group_id) 
				VALUES ($1, ST_GeomFromText($2,4326), $3, $4, $5, $6, $7, $8, $9)
				RETURNING 
					unid AS id, 
					nombre, 
					st_astext(geom) AS geom, 
					st_astext(exutorio) AS exutorio, 
					exutorio_id, 
					area, 
					ae, 
					rho, 
					wp, 
					activar, 
					mostrar,
					areas_pluvio.group_id`;
                params = [area.nombre, area.geom.toString(), area.exutorio_id, area.ae, area.rho, area.wp, area.activar, area.mostrar, area.group_id];
            }
        }
        return (0, utils2_1.pasteIntoSQLQuery)(query, params);
    }
    static list() {
        return __awaiter(this, arguments, void 0, function* (filter = {}, options = {}, user_id) {
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
                },
                group_id: {
                    type: "integer"
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
            const access_join = (user_id) ? `JOIN user_area_access ON (areas_pluvio.group_id=user_area_access.ag_id AND user_id=${user_id})` : "";
            if (options && options.no_geom) {
                const stmt = `SELECT 
					areas_pluvio.unid id, 
					areas_pluvio.nombre, 
					st_astext(areas_pluvio.exutorio) exutorio, 
					areas_pluvio.exutorio_id, 
					areas_pluvio.area, 
					areas_pluvio.ae, 
					areas_pluvio.rho, 
					areas_pluvio.wp, 
					areas_pluvio.activar, 
					areas_pluvio.mostrar,
					areas_pluvio.group_id
				FROM areas_pluvio 
				${join_type} JOIN estaciones ON (estaciones.unid=areas_pluvio.exutorio_id ${tabla_id_filter})
				${access_join}
				WHERE areas_pluvio.geom IS NOT NULL ${filter_string} ORDER BY areas_pluvio.id
				${pagination_clause}`;
                const res = yield g.pool.query(stmt);
                const areas = res.rows.map((r) => {
                    if (r.exutorio) {
                        r.exutorio = new geometry_1.Geometry(r.exutorio);
                    }
                    return r;
                });
                return areas;
            }
            else {
                const stmt = `SELECT 
					areas_pluvio.unid id, 
					areas_pluvio.nombre, 
					st_astext(areas_pluvio.geom) geom, 
					st_astext(areas_pluvio.exutorio) exutorio, 
					areas_pluvio.exutorio_id, 
					areas_pluvio.area, 
					areas_pluvio.ae, 
					areas_pluvio.rho, 
					areas_pluvio.wp, 
					areas_pluvio.activar, 
					areas_pluvio.mostrar,
					areas_pluvio.group_id 
				FROM areas_pluvio 
				${join_type} JOIN estaciones ON (estaciones.unid=areas_pluvio.exutorio_id ${tabla_id_filter})
				${access_join}
				WHERE areas_pluvio.geom IS NOT NULL ${filter_string} ORDER BY id
				${pagination_clause}`;
                const res = yield g.pool.query(stmt);
                const areas = res.rows.map((r) => {
                    return new this(r);
                });
                return areas;
            }
        });
    }
    static delete(filter, user_id) {
        return __awaiter(this, void 0, void 0, function* () {
            if (filter.area_id && !filter.id) {
                filter.id = filter.area_id;
            }
            const matches = yield this.list(filter, { no_geom: true }, user_id);
            if (!matches) {
                console.warn("No matches to delete");
                return [];
            }
            const results = [];
            for (var area of matches) {
                try {
                    if (!area.id) {
                        throw new Error("Falta area.id");
                    }
                    console.debug("Try delete area.id=" + area.id);
                    var result = yield this.deleteOne(area.id);
                }
                catch (e) {
                    throw (e);
                }
                results.push(result);
            }
            return results;
        });
    }
    static deleteOne(id, user_id) {
        return __awaiter(this, void 0, void 0, function* () {
            const group_join = (user_id) ? `USING user_area_access WHERE areas_pluvio.group_id=user_area_access.ag_id and user_id=${user_id} and effective_access='write'` : "WHERE 1=1";
            const result = yield g.pool.query(`
			DELETE FROM areas_pluvio
			${group_join}
			AND unid=$1
			RETURNING areas_pluvio.unid id, 
			areas_pluvio.nombre, 
			st_astext(ST_ForcePolygonCCW(areas_pluvio.geom)) AS geom, 
			st_astext(areas_pluvio.exutorio) AS exutorio,
			areas_pluvio.exutorio_id, 
			areas_pluvio.area, 
			areas_pluvio.ae, 
			areas_pluvio.rho, 
			areas_pluvio.wp, 
			areas_pluvio.activar, 
			areas_pluvio.mostrar,
			areas_pluvio.group_id`, [id]);
            if (!result.rows.length) {
                throw new custom_errors_1.NotFoundError("unid not found");
            }
            console.log("Deleted areas_pluvio.unid=" + result.rows[0].id);
            return new this(result.rows[0]);
        });
    }
    delete() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.id) {
                throw Error("Falta id");
            }
            return Area.deleteOne(this.id);
        });
    }
    static read(id_1) {
        return __awaiter(this, arguments, void 0, function* (id, options = {}, user_id) {
            const results = yield Area.list({ id: id }, options, user_id);
            if (!results.length) {
                throw new custom_errors_1.NotFoundError("No se encontró área con el id especificado");
            }
            return results[0];
        });
    }
    static listWithPagination() {
        return __awaiter(this, arguments, void 0, function* (filter = {}, options = {}, req, user_id) {
            var _a, _b, _c;
            const config_pagination = (_a = g.config.pagination) !== null && _a !== void 0 ? _a : { default_limit: 1000, max_limit: 10000 };
            filter.limit = (_b = filter.limit) !== null && _b !== void 0 ? _b : config_pagination.default_limit;
            filter.limit = parseInt(filter.limit.toString());
            if (filter.limit > config_pagination.max_limit) {
                throw (new Error("limit exceeds maximum records per page (" + config_pagination.max_limit) + ")");
            }
            filter.offset = (_c = filter.offset) !== null && _c !== void 0 ? _c : 0;
            filter.offset = parseInt(filter.offset.toString());
            const result = yield this.list(filter, options, user_id);
            var is_last_page = (result.length < filter.limit);
            if (is_last_page) {
                return {
                    areas: result,
                    is_last_page: true
                };
            }
            else {
                // var query_arguments = {...filter,...options}
                // if(query_arguments.geom && isGeometryDict(query_arguments.geom)) {
                // 	query_arguments.geom = "a" // new Geometry(query_arguments.geom).toString()
                // }
                // query_arguments.offset = filter.offset + filter.limit 
                const offset = filter.offset + filter.limit;
                const query_arguments = serializeFilter(Object.assign(Object.assign({}, filter), { offset: offset }), options);
                var next_page_url = (req) ? `${req.protocol}://${req.get('host')}${req.path}?${(0, node_querystring_1.stringify)(query_arguments)}` : `obs/areal/areas?${(0, node_querystring_1.stringify)(query_arguments)}`;
                return {
                    areas: result,
                    is_last_page: false,
                    next_page: next_page_url
                };
            }
        });
    }
    static hasAccess(user_id_1, area_id_1) {
        return __awaiter(this, arguments, void 0, function* (user_id, area_id, write = false) {
            const max_priority = (write) ? 2 : 1;
            var query = (0, utils2_1.pasteIntoSQLQuery)(`SELECT EXISTS (
			SELECT 1
				FROM areas_pluvio
			WHERE unid=$3
			AND group_id IS NULL 
			UNION ALL
			SELECT 1
				FROM areas_pluvio
				JOIN user_area_access 
				ON 
					areas_pluvio.group_id=user_area_access.ag_id 
					AND user_id=$1
					AND max_priority>=$2				
				WHERE areas_pluvio.unid=$3
		)`, [user_id, max_priority, area_id]);
            const result = yield g.pool.query(query);
            if (result.rows.length && result.rows[0].exists) {
                return true;
            }
            else {
                return false;
            }
        });
    }
    static hasAccessSerie(user_id_1, series_id_1) {
        return __awaiter(this, arguments, void 0, function* (user_id, series_id, write = false) {
            const max_priority = (write) ? 2 : 1;
            var query = (0, utils2_1.pasteIntoSQLQuery)(`WITH s AS (
				SELECT area_id 
				FROM series_areal 
				WHERE id=$1
			)
			SELECT EXISTS (
			SELECT 1
				FROM areas_pluvio
				JOIN s ON s.area_id=areas_pluvio.unid
			WHERE group_id IS NULL 
			UNION ALL
			SELECT 1
				FROM areas_pluvio
				JOIN s ON s.area_id=areas_pluvio.unid
				JOIN user_area_access 
				ON 
					areas_pluvio.group_id=user_area_access.ag_id 
					AND user_id=$2
					AND max_priority>=$3
		)`, [series_id, user_id, max_priority]);
            const result = yield g.pool.query(query);
            if (result.rows.length && result.rows[0].exists) {
                return true;
            }
            else {
                return false;
            }
        });
    }
}
exports.default = Area;
function serializeFilter(filter, options) {
    const out = {};
    for (const [key, value] of Object.entries(filter)) {
        if (value == null)
            continue;
        if (key === "geom" && isGeometryDict(value)) {
            out.geom = new geometry_1.Geometry(value).toString(); // ✅ WKT (if you have it)
        }
        else {
            out[key] = String(value);
        }
    }
    for (const [key, value] of Object.entries(options)) {
        if (value == null)
            continue;
        out[key] = String(value);
    }
    return out;
}
function assertPosition3D(value) {
    if (!Array.isArray(value)) {
        throw new custom_errors_1.BadRequestError("Expected Position[][][]");
    }
    for (const polygon of value) {
        if (!Array.isArray(polygon)) {
            throw new custom_errors_1.BadRequestError("Expected Position[][][]");
        }
        for (const ring of polygon) {
            if (!Array.isArray(ring)) {
                throw new custom_errors_1.BadRequestError("Expected Position[][][]");
            }
            for (const pos of ring) {
                if (!Array.isArray(pos) ||
                    (pos.length !== 2 && pos.length !== 3) ||
                    pos.some((n) => typeof n !== "number")) {
                    throw new custom_errors_1.BadRequestError("Invalid Position");
                }
            }
        }
    }
}
function isGeometryDict(value) {
    if (typeof value !== "object" || value === null) {
        return false;
    }
    if (typeof value.type !== "string") {
        return false;
    }
    switch (value.type) {
        case "Point":
        case "MultiPoint":
        case "LineString":
        case "MultiLineString":
        case "Polygon":
        case "MultiPolygon":
            return "coordinates" in value;
        default:
            return false;
    }
}
