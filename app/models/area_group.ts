// models/AreaGroup.ts

import setGlobal from 'a5base/setGlobal'

import {Area} from '../a5_types'

const g = setGlobal()

export interface AreaGroupRecord {
    id : number
    name : string
    owner_id : number
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

export class AreaGroup {
  
  id : number
  name : string
  owner_id : number
  areas : Area[]

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
    const q = `SELECT id, name, owner_id FROM area_groups WHERE id = $1`;
    const result = await g.pool.query(q, [id]);
    // TODO add areas
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

  // static async addMember(id : number, area : Area) {

  // }
}
