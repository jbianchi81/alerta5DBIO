// models/AreaGroup.ts

import setGlobal from 'a5base/setGlobal'

// import {Area as AreaType} from '../a5_types'
import { BadRequestError, AuthError, NotFoundError } from '../custom_errors'
import { control_filter2, pasteIntoSQLQuery, QueryFilter } from '../utils2'
// import { Geometry as GeomType } from '../geometry_types'
import { Geometry, GeometryDict } from 'a5base/geometry'
import { Position } from 'geojson'
import { AreaGroup } from './area_group'
import { Request } from 'express'
const g = setGlobal()
import { stringify } from 'node:querystring';

interface AreaParams {
	id?: number
	nombre: string
	geom: GeometryDict
	exutorio?: { geom: GeometryDict } | GeometryDict
	exutorio_id?: number
	ae?: number
	rho?: number
	wp?: number
	activar?: boolean
	mostrar?: boolean
	area?: number
	group_id?: number
}

interface AreasFilter {
	id?: number | number[]
	area_id?: number | number[]
	nombre?: string
	unid?: number | number[]
	geom?: GeometryDict | string
	exutorio?: GeometryDict | string
	exutorio_id?: number
	activar?: boolean
	mostrar?: boolean
	tabla_id?: string
	limit?: number
	offset?: number
	group_id?: number
}

export default class Area {

	id?: number
	nombre: string
	geom: Geometry
	exutorio?: Geometry
	exutorio_id?: number
	ae?: number
	rho?: number
	wp?: number
	activar?: boolean
	mostrar?: boolean
	area?: number
	group_id?: number

	constructor(params: AreaParams) {
		this.id = params.id
		this.nombre = params.nombre
		this.geom = new Geometry(params.geom)
		this.exutorio = (params.exutorio) ? ("geom" in params.exutorio) ? new Geometry(params.exutorio.geom) : (params.exutorio.type && params.exutorio.coordinates) ? new Geometry(params.exutorio) : undefined : undefined
		this.exutorio_id = params.exutorio_id
		this.ae = params.ae
		this.rho = params.rho
		this.wp = params.wp
		this.activar = params.activar
		this.mostrar = params.mostrar
		this.area = params.area
		this.group_id = params.group_id
	}

	static async create(areas: AreaParams[], user_id?: number): Promise<Area[]> {
		const created_areas: Area[] = []
		for (const area of areas) {
			if (!area.geom) {
				throw new BadRequestError("Invalid area: missing geom")
			}
			const created_area = await this.createOne(area, user_id)
			if (created_area) {
				created_areas.push(created_area)
			}
		}
		return created_areas
	}

	static async createOne(area_params: AreaParams, user_id?: number): Promise<Area | null> {
		const area = new this(area_params)
		if (!area.id) {
			await area.getId()
		}
		if (area.geom && area.geom.type && area.geom.type == "MultiPolygon") {
			assertPosition3D(area.geom.coordinates)
			area.geom = new Geometry(
				"Polygon",
				area.geom.coordinates[0]
			)
		}
		if (area.group_id && user_id) {
			const has_access = await AreaGroup.hasAccess(user_id, area.group_id, true)
			if (!has_access) {
				throw new AuthError("El usuario no tiene acceso de escritura para el grupo de áreas indicado")
			}
		}
		const q = this.upsertAreaQuery(area)
		const result = await (g.pool as any).query(q)
		if (!result.rows.length) {
			throw new Error("Area upsert failed: no rows returned")
		}
		console.info("Upserted areas_pluvio.unid=" + result.rows[0].id)
		return new this(result.rows[0])
	}

	async getId() {
		var res = await (g.pool as any).query(`
            SELECT unid 
            FROM areas_pluvio 
            WHERE nombre = $1
            AND geom = st_geomfromtext($2,4326)
            `, [this.nombre, this.geom.toString()])
		if (res.rows.length > 0) {
			this.id = res.rows[0].unid
			return
		} else {
			res = await (g.pool as any).query(`
                SELECT max(unid)+1 AS id
                FROM areas_pluvio
                `)
			this.id = res.rows[0].id
		}
	}

	static upsertAreaQuery(area: Area) {
		var query = ""
		var params = []
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
					group_id`
				params = [area.id, area.nombre, area.geom.toString(), area.exutorio.toString(), area.exutorio_id, area.ae, area.rho, area.wp, area.activar, area.mostrar, area.group_id]
			} else {
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
					group_id`
				params = [area.nombre, area.geom.toString(), area.exutorio.toString(), area.exutorio_id, area.ae, area.rho, area.wp, area.activar, area.mostrar, area.group_id]
			}
		} else {
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
					group_id`
				params = [area.id, area.nombre, area.geom.toString(), area.exutorio_id, area.ae, area.rho, area.wp, area.activar, area.mostrar, area.group_id]
			} else {
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
					areas_pluvio.group_id`
				params = [area.nombre, area.geom.toString(), area.exutorio_id, area.ae, area.rho, area.wp, area.activar, area.mostrar, area.group_id]
			}
		}
		return pasteIntoSQLQuery(query, params)
	}

	static async list(filter: AreasFilter = {}, options: { no_geom?: boolean } = {}, user_id? : number): Promise<Area[]> {
		if (filter.id) {
			filter.unid = filter.id
			delete filter.id
		}
		const valid_filters: Record<string, QueryFilter> = {
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
		}
		var filter_string = control_filter2(valid_filters, filter, "areas_pluvio")
		if (!filter_string) {
			throw ("Invalid filters")
		}
		var join_type = "LEFT"
		var tabla_id_filter = ""
		if (filter.tabla_id) {
			if (/[';]/.test(filter.tabla_id)) {
				throw ("Invalid filter value")
			}
			join_type = "RIGHT"
			tabla_id_filter += ` AND estaciones.tabla='${filter.tabla_id}'`
		}
		var pagination_clause = (filter.limit) ? `LIMIT ${filter.limit}` : ""
		pagination_clause += (filter.offset) ? ` OFFSET ${filter.offset}` : ""
		const access_join = (user_id) ? `JOIN user_area_access ON (areas_pluvio.group_id=user_area_access.ag_id AND user_id=${user_id})` : "" 
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
				${pagination_clause}`
			const res = await (g.pool as any).query(stmt)
			const areas = res.rows.map((r: any) => {
				if (r.exutorio) {
					r.exutorio = new Geometry(r.exutorio)
				}
				return r
			})
			return areas
		} else {
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
				${pagination_clause}`
			const res = await (g.pool as any).query(stmt)
			const areas = res.rows.map((r: any) => {
				return new this(r)
			})
			return areas
		}
	}

	static async delete(filter : AreasFilter, user_id? : number) : Promise<Area[]> {
		if(filter.area_id && !filter.id) {
			filter.id = filter.area_id
		}
		const matches = await this.list(filter,{no_geom:true}, user_id)
		if(!matches) {
			console.warn("No matches to delete")
			return []
		}
		const results = []
		for(var area of matches) {
			try {
				if(!area.id) {
					throw new Error("Falta area.id")
				}
				console.debug("Try delete area.id=" + area.id)
				var result = await this.deleteOne(area.id)
			} catch(e) {
				throw(e)
			}
			results.push(result)
		}
		return results
	}

	static async deleteOne(id : number, user_id? : number) : Promise<Area> {
		const group_join = (user_id) ? `USING user_area_access WHERE areas_pluvio.group_id=user_area_access.ag_id and user_id=${user_id} and effective_access='write'` : "WHERE 1=1"
		const result = await (g.pool as any).query(`
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
			areas_pluvio.group_id`, [id])
		if(!result.rows.length) {
			throw new NotFoundError("unid not found")
		}
		console.log("Deleted areas_pluvio.unid=" + result.rows[0].id)
		return new this(result.rows[0])
	}

	async delete() : Promise<Area> {
		if(!this.id) {
			throw Error("Falta id")
		}
		return Area.deleteOne(this.id)
	}

	static async read(id : number, options : {no_geom?: boolean}={}, user_id? : number) : Promise<Area> {
		const results = await Area.list({id: id}, options, user_id)
		if(!results.length) {
			throw new NotFoundError("No se encontró área con el id especificado")
		}
		return results[0]
	}

	static async listWithPagination(filter : AreasFilter={},options: {no_geom? : boolean}={},req : Request, user_id? : number) : Promise<{is_last_page: boolean, areas: Area[], next_page?: string}> {
		const config_pagination = (g.config as any).pagination ?? {default_limit: 1000, max_limit: 10000}
		filter.limit = filter.limit ?? config_pagination.default_limit as number
		filter.limit = parseInt(filter.limit.toString())
		if (filter.limit > config_pagination.max_limit) {
			throw(new Error("limit exceeds maximum records per page (" + config_pagination.max_limit) + ")")
		}
		filter.offset = filter.offset ?? 0
		filter.offset = parseInt(filter.offset.toString())
		const result = await this.list(filter,options,user_id)
		var is_last_page = (result.length < filter.limit)
		if(is_last_page) {
			return {
				areas: result,
				is_last_page: true
			}
		} else {
			// var query_arguments = {...filter,...options}
			// if(query_arguments.geom && isGeometryDict(query_arguments.geom)) {
			// 	query_arguments.geom = "a" // new Geometry(query_arguments.geom).toString()
			// }
			// query_arguments.offset = filter.offset + filter.limit 
			const offset = filter.offset + filter.limit
			const query_arguments = serializeFilter({...filter, offset: offset}, options)
			var next_page_url = (req) ? `${req.protocol}://${req.get('host')}${req.path}?${stringify(query_arguments)}` : `obs/areal/areas?${stringify(query_arguments)}`
				return {
					areas: result,
					is_last_page: false,
					next_page: next_page_url
				}
			}
	
		}
}

function serializeFilter(filter: AreasFilter, options : {no_geom?: boolean}): Record<string, string> {
  const out: Record<string, string> = {}

  for (const [key, value] of Object.entries(filter)) {
    if (value == null) continue

    if (key === "geom" && isGeometryDict(value)) {
      out.geom = new Geometry(value).toString()      // ✅ WKT (if you have it)
    } else {
      out[key] = String(value)
    }
  }
  
  for (const [key, value] of Object.entries(options)) {
    if (value == null) continue
    out[key] = String(value)
  }

  return out
}


function assertPosition3D(
	value: unknown
): asserts value is Position[][][] {
	if (!Array.isArray(value)) {
		throw new BadRequestError("Expected Position[][][]")
	}

	for (const polygon of value) {
		if (!Array.isArray(polygon)) {
			throw new BadRequestError("Expected Position[][][]")
		}

		for (const ring of polygon) {
			if (!Array.isArray(ring)) {
				throw new BadRequestError("Expected Position[][][]")
			}

			for (const pos of ring) {
				if (
					!Array.isArray(pos) ||
					(pos.length !== 2 && pos.length !== 3) ||
					pos.some((n) => typeof n !== "number")
				) {
					throw new BadRequestError("Invalid Position")
				}
			}
		}
	}
}

export function isGeometryDict(
  value: GeometryDict | string
): value is GeometryDict {
  if (typeof value !== "object" || value === null) {
    return false
  }

  if (typeof (value as any).type !== "string") {
    return false
  }

  switch ((value as any).type) {
    case "Point":
    case "MultiPoint":
    case "LineString":
    case "MultiLineString":
    case "Polygon":
    case "MultiPolygon":
      return "coordinates" in value
    default:
      return false
  }
}
