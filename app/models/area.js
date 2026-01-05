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
const setGlobal_1 = __importDefault(require("a5base/setGlobal"));
// import {Area as AreaType} from '../a5_types'
const custom_errors_1 = require("../custom_errors");
// import { Geometry as GeomType } from '../geometry_types'
const geometry_1 = require("a5base/geometry");
const area_group_1 = require("./area_group");
const g = (0, setGlobal_1.default)();
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
                area.geom = new geometry_1.Geometry({
                    type: "Polygon",
                    coordinates: area.geom.coordinates[0]
                });
            }
            if (area.group_id && user_id) {
                const has_access = yield area_group_1.AreaGroup.hasAccess(user_id, area.group_id, true);
                if (!has_access) {
                    throw new custom_errors_1.AuthError("El usuario no tiene acceso de escritura para el grupo de áreas indicado");
                }
            }
            const q = this.upsertAreaQuery(area);
            const result = yield g.pool.query(q);
            if (result.rows.length <= 0) {
                throw new Error("Area upsert failed: no rows returned");
            }
            console.info("Upserted areas_pluvio.unid=" + result.rows[0].id);
            //~ console.log(result.rows[0])
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
				INSERT INTO areas_pluvio (unid, nombre, geom, exutorio, exutorio_id, ae, rho, wp, activar, mostrar) 
				VALUES ($1, $2, ST_GeomFromText($3,4326), ST_GeomFromText($4,4326), $5, $6, $7, $8, $9, $10)
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
					mostrar = excluded.mostrar 
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
					mostrar`;
                params = [area.id, area.nombre, area.geom.toString(), area.exutorio.toString(), area.exutorio_id, area.ae, area.rho, area.wp, area.activar, area.mostrar];
            }
            else {
                query = "\
				INSERT INTO areas_pluvio (nombre, geom, exutorio, exutorio_id, ae, rho, wp, activar, mostrar) \
				VALUES ($1, ST_GeomFromText($2,4326), ST_GeomFromText($3,4326), $4, $5, $6, $7, $8, $9)\
				RETURNING \
					unid AS id, \
					nombre, \
					st_astext(geom) AS geom, \
					st_astext(exutorio) AS exutorio, \
					exutorio_id, \
					area, \
					ae, \
					rho, \
					wp, \
					activar, \
					mostrar";
                params = [area.nombre, area.geom.toString(), area.exutorio.toString(), area.exutorio_id, area.ae, area.rho, area.wp, area.activar, area.mostrar];
            }
        }
        else {
            if (area.id) {
                query = "\
				INSERT INTO areas_pluvio (unid, nombre, geom, exutorio_id, ae, rho, wp, activar, mostrar) \
				VALUES ($1, $2, ST_GeomFromText($3,4326), $4, $5, $6, $7, $8, $9)\
				ON CONFLICT (unid) DO UPDATE SET \
					nombre=excluded.nombre,\
					geom=excluded.geom,\
					exutorio_id=excluded.exutorio_id, \
					area = excluded.area, \
					ae = excluded.ae, \
					rho = excluded.rho, \
					wp = excluded.wp, \
					activar = excluded.activar, \
					mostrar = excluded.mostrar \
				RETURNING \
					unid AS id, \
					nombre, \
					st_astext(geom) AS geom, \
					st_astext(exutorio) AS exutorio, \
					exutorio_id, \
					area, \
					ae, \
					rho, \
					wp, \
					activar, \
					mostrar";
                params = [area.id, area.nombre, area.geom.toString(), area.exutorio_id, area.ae, area.rho, area.wp, area.activar, area.mostrar];
            }
            else {
                query = "\
				INSERT INTO areas_pluvio (nombre, geom, exutorio_id, ae, rho, wp, activar, mostrar) \
				VALUES ($1, ST_GeomFromText($2,4326), $3, $4, $5, $6, $7, $8)\
				RETURNING \
					unid AS id, \
					nombre, \
					st_astext(geom) AS geom, \
					st_astext(exutorio) AS exutorio, \
					exutorio_id, \
					area, \
					ae, \
					rho, \
					wp, \
					activar, \
					mostrar";
                params = [area.nombre, area.geom.toString(), area.exutorio_id, area.ae, area.rho, area.wp, area.activar, area.mostrar];
            }
        }
        return pasteIntoSQLQuery(query, params);
    }
}
exports.default = Area;
