// models/Group.ts

import setGlobal from 'a5base/setGlobal'

const g = setGlobal()

export interface GroupRecord {
  name: string;
}

export interface GroupCreateParams {
  name: string;
}

export interface GroupUpdateParams {
  name: string;
}

export class Group {
  static async list(): Promise<GroupRecord[]> {
    const q = `SELECT name FROM groups ORDER BY name`;
    const result = await g.pool.query(q);
    return result.rows as GroupRecord[];
  }

  static async read(name: string): Promise<GroupRecord | null> {
    const q = `SELECT name FROM groups WHERE name = $1`;
    const result = await g.pool.query(q, [name]);
    return (result.rows[0] as GroupRecord) || null;
  }

  static async create(params: GroupCreateParams|GroupCreateParams[]) : Promise<GroupRecord[]> {
    if(Array.isArray(params)) {
        const results : GroupRecord[] = []
        for(const p of params) {
            results.push(await this.createOne(p))
        }
        return results
    } else {
        return [await this.createOne(params)]
    }
  }

  static async createOne(params: GroupCreateParams): Promise<GroupRecord> {
    const q = `
      INSERT INTO groups (name)
      VALUES ($1)
      RETURNING name
    `;
    const result = await g.pool.query(q, [params.name]);
    return result.rows[0] as GroupRecord;
  }

  // static async update(
  //   name: string,
  //   params: GroupUpdateParams
  // ): Promise<GroupRecord | null> {
  //   const q = `
  //     UPDATE groups
  //        SET name = $1
  //      WHERE name = $2
  //  RETURNING name
  //   `;
  //   const result = await g.pool.query(q, [params.name, name]);
  //   return (result.rows[0] as GroupRecord) || null;
  // }

  static async delete(name: string): Promise<GroupRecord | null> {
    const q = `
      DELETE FROM groups
       WHERE name = $1
   RETURNING name
    `;
    const result = await g.pool.query(q, [name]);
    return (result.rows[0] as GroupRecord) || null;
  }
}
