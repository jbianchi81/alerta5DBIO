// models/AreaGroup.ts

import setGlobal from 'a5base/setGlobal'

// import {Area as AreaType} from '../a5_types'
import { BadRequestError, AuthError } from '../custom_errors'
import { control_filter2 } from '../utils2'
// import { Geometry as GeomType } from '../geometry_types'
import { Geometry, GeometryDict } from 'a5base/geometry'
import { AreaGroup } from './area_group'

const g = setGlobal()

interface AreaParams {
    id? : number
    nombre : string 
    geom : GeometryDict
    exutorio? : {geom: GeometryDict} | GeometryDict
    exutorio_id? : number
    ae? : number
    rho? : number
    wp? : number
    activar?: boolean
    mostrar?: boolean
    area?: number
    group_id?: number
}

export default class Area {

    id? : number
    nombre : string
    geom : Geometry
    exutorio? : Geometry
    exutorio_id? : number
    ae? : number
    rho? : number
    wp? : number
    activar?: boolean
    mostrar?: boolean
    area?: number
    group_id?: number

    constructor(params : AreaParams) {
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

    static async create(areas : AreaParams[], user_id? : number) : Promise<Area[]> {
		const created_areas = []
		for(const area of areas) {
			if(!area.geom) {
				throw new BadRequestError("Invalid area: missing geom")
			}
			const created_area = await this.createOne(area, user_id)
			if(created_area) {
				created_areas.push(created_area)
			}
		}
		return created_areas
	}

    static async createOne(area_params :AreaParams, user_id?: number) : Promise<Area|null> {
        const area = new this(area_params)
        if(!area.id) {
            await area.getId()
        }
        if(area.geom && area.geom.type && area.geom.type == "MultiPolygon") {
            area.geom = new Geometry({
                type: "Polygon",
                coordinates: area.geom.coordinates[0]
            })
        }
        if(area.group_id && user_id) {
            const has_access = await AreaGroup.hasAccess(user_id, area.group_id, true)
            if(!has_access) {
                throw new AuthError("El usuario no tiene acceso de escritura para el grupo de áreas indicado")
            }
        }
        const q  = this.upsertAreaQuery(area)
        const result = await g.pool.query(q)
        if(result.rows.length<=0) {
            throw new Error ("Area upsert failed: no rows returned")
        }
        console.info("Upserted areas_pluvio.unid=" + result.rows[0].id)
        //~ console.log(result.rows[0])
        return new this(result.rows[0])
    }

    async getId() {
		var res = await (g.pool as any).query(`
            SELECT unid 
            FROM areas_pluvio 
            WHERE nombre = $1
            AND geom = st_geomfromtext($2,4326)
            `,[this.nombre, this.geom.toString()])
        if (res.rows.length>0) {
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

    static upsertAreaQuery(area : Area)  {
		var query = ""
		var params = []
		if(area.exutorio) {
			if(area.id) {
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
					mostrar`
				params = [area.id,area.nombre,area.geom.toString(),area.exutorio.toString(),area.exutorio_id, area.ae, area.rho, area.wp, area.activar, area.mostrar]
			} else {
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
					mostrar"
				params = [area.nombre, area.geom.toString(), area.exutorio.toString(), area.exutorio_id, area.ae, area.rho, area.wp, area.activar, area.mostrar]
			}
		} else {
			if(area.id) {
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
					mostrar"
				params = [area.id,area.nombre,area.geom.toString(),area.exutorio_id, area.ae, area.rho, area.wp, area.activar, area.mostrar]
			} else {
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
					mostrar"
				params = [area.nombre, area.geom.toString(), area.exutorio_id, area.ae, area.rho, area.wp, area.activar, area.mostrar]
			}
		}
		return pasteIntoSQLQuery(query,params)
	}
}