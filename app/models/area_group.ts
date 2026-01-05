// models/AreaGroup.ts

import setGlobal from 'a5base/setGlobal'

import {Area} from '../a5_types'
import { BadRequestError, NotFoundError } from '../custom_errors'
import { control_filter2 } from '../utils2'

const g = setGlobal()

export interface AreaGroupRecord {
    id : number
    name : string
    owner_id : number
    areas?: Area[]
}

export interface AreaGroupCreateParams {
    id? : number
    name : string
    owner_id : number
}

export interface AreaGroupUpdateParams {
    name? : string
    owner_id : string
}

export interface AccessParams {
  name : string
  access : "write" | "read"
}

export class AreaGroup {
  
  id : number
  name : string
  owner_id : number
  areas : Area[] | undefined

  constructor(params : AreaGroupRecord) {
    this.id = params.id
    this.name = params.name
    this.owner_id = params.owner_id
    this.areas = params.areas
  }

  static async list(filter: {name?: string, id?: number, owner_id?: number}={}): Promise<AreaGroupRecord[]> {
    let result : any
    if(filter.id) {
      const q = `SELECT id,name,owner_id FROM area_groups WHERE id=$1`;
      result = await g.pool.query(q,[filter.id]);
    } else if(filter.name) {
      const q = `SELECT id,name,owner_id FROM area_groups WHERE name=$1`;
      result = await g.pool.query(q,[filter.name]);
    } else if(filter.owner_id) {
      const q = `SELECT id,name,owner_id FROM area_groups WHERE owner_id=$1 ORDER BY id`;
      result = await g.pool.query(q,[filter.name]);
    } else {
      const q = `SELECT id,name,owner_id FROM area_groups ORDER BY id`;
      result = await g.pool.query(q);
    }
    return result.rows as AreaGroupRecord[];
  }

  static async read(id: number): Promise<AreaGroupRecord | null> {
    const q = `SELECT 
      area_groups.id, 
      area_groups.name, 
      area_groups.owner_id, 
      json_array_agg(
      json_build_object(
      'id', areas_pluvio.unid, 
      'nombre', areas_pluvio.nombre
      )) AS areas
      FROM area_groups 
      LEFT OUTER JOIN areas_pluvio ON area_groups.id=areas_pluvio.group_id
      WHERE area_groups.id = $1
      GROUP BY area_groups.id, area_groups.name, area_groups.owner_id`;
    const result = await g.pool.query(q, [id]);
    
    return (result.rows[0] as AreaGroupRecord) || null;
  }

  static async create(params: AreaGroupCreateParams|AreaGroupCreateParams[]) : Promise<AreaGroupRecord[]> {
    if(Array.isArray(params)) {
        const results : AreaGroupRecord[] = []
        for(const p of params) {
            results.push(await this.createOne(p))
        }
        return results
    } else {
        return [await this.createOne(params)]
    }
  }

  static async createOne(params: AreaGroupCreateParams): Promise<AreaGroupRecord> {
    const q = `
      INSERT INTO area_groups (id, name, owner_id)
      VALUES (COALESCE($1, nextval('area_groups_id_seq'::regclass)), $2, $3)
      ON CONFLICT (id) DO UPDATE SET name=excluded.name, owner_id=excluded.owner_id
      RETURNING id, name, owner_id
    `;
    const result = await g.pool.query(q, [params.id, params.name, params.owner_id]);
    return result.rows[0] as AreaGroupRecord;
  }

  static async update(
    id: number,
    params: AreaGroupUpdateParams
  ): Promise<AreaGroupRecord | null> {
    const q = `
      UPDATE area_groups
         SET name = COALESCE($1, name),
             owner_id = COALESCE($2, owner_id)
       WHERE id = $3
   RETURNING id, name, owner_id
    `;
    const result = await g.pool.query(q, [params.name, params.owner_id, id]);
    return (result.rows[0] as AreaGroupRecord) || null;
  }

  static async delete(id : number): Promise<AreaGroupRecord | null> {
    const q = `
      DELETE FROM area_groups
       WHERE id = $1
   RETURNING id, name, owner_id
    `;
    const result = await g.pool.query(q, [id]);
    return (result.rows[0] as AreaGroupRecord) || null;
  }

  static async grantAccess(id : number, user_groups : AccessParams[]) {
    // Check all user_group names exist
    const group_names = user_groups.map(ug => ug.name).filter(ug => ug)
    if (group_names.length === 0) throw new BadRequestError("Falta 'name' en items");

    const checkUserGroups = await g.pool.query(
      `SELECT name FROM groups WHERE name = ANY($1)`,
      [group_names]
    );

    if (checkUserGroups.rows.length !== user_groups.length) {
      throw new NotFoundError("GROUP_NOT_FOUND");
    }

    const results = []
    var i = 0
    for(const user_group of user_groups) {
      if(!user_group.name) {
        throw new BadRequestError("Falta 'name' en item " + i)
      }
      const granted = await this.grantAccessOne(id, user_group.name, user_group.access ?? "read")
      results.push(granted)
      i = i + 1 
    }
    return results   
  }

  static async grantAccessOne(id : number, name : string, access : "read" | "write") {
    const q  = `INSERT INTO user_area_groups_access (ag_id, group_name, access) VALUES ($1, $2, $3) ON CONFLICT (group_name, ag_id) DO UPDATE SET access=excluded.access RETURNING group_name, ag_id`
    const result = await g.pool.query(q, [id, name, access]);
    if(!result.rows.length) {
      throw new Error("No se insertó fila en user_area_groups_access")
    }
    return result.rows[0] as AccessParams
  }

  static async listAccess(id : number, filter : {name? : string, access?: "read" | "write"}) : Promise<AccessParams[]> {
    if(!id) {
      throw new BadRequestError("Falta id")
    }
    const filter_string = control_filter2({name: {type: "string"}, access: {type: "string"}}, filter)
    const q = `SELECT group_name AS name, access FROM user_area_groups_access WHERE ag_id=$1 ${filter_string}`
    const results = await g.pool.query(q, [id])
    return results.rows as AccessParams[]
  }

  static async readAccess(id : number, group_name : string) : Promise<AccessParams|null> {
    if(!id) {
      throw new BadRequestError("Falta id")
    }
    if(!group_name) {
      throw new BadRequestError("Falta group_name")
    }
    const q = `SELECT group_name AS name, access FROM user_area_groups_access WHERE ag_id=$1 AND group_name=$2`
    const results = await g.pool.query(q, [id, group_name])
    return (results.rows[0] as AccessParams) || null
  }

  static async hasAccess(user_id : number, ag_id : number, write : boolean=false) : Promise<boolean> {
    var q = `SELECT EXISTS (
      SELECT 1 
      FROM user_area_access 
      WHERE user_id=$1 
      AND ag_id=$2 
      ${(write) ? "AND effective_access='write'" : ""})`
    const result = await g.pool.query(q, [user_id, ag_id])
    if(result.rows.length && result.rows[0].exists) {
			return true
		} else {
			return false
		}
  }



  // static async addMember(id : number, area : Area) {

  // }
}
