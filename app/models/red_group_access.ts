// models/UserGroup.ts

import setGlobal from 'a5base/setGlobal'

import {NotFoundError} from '../custom_errors'

const g = setGlobal()

export interface RedGroupRecord {
  red_id: number;
  group_name: string;
  access: 'read' | 'write'
}

export class RedGroup {
  /** Assign access */
  static async assign(
    group_name: string,
    redes: { red_id: number, access: 'read' | 'write' }[]
  ): Promise<RedGroupRecord[]> {
    // Check all user_ids exist
    const ids = redes.map(m => m.red_id);
    if (ids.length === 0) return [];

    const checkRedes = await g.pool.query(
      `SELECT id FROM redes WHERE id = ANY($1)`,
      [ids]
    );

    if (checkRedes.rows.length !== ids.length) {
      throw new NotFoundError("RED_NOT_FOUND");
    }

    // Delete existing redes for group
    await g.pool.query(
      `DELETE FROM red_group_access WHERE group_name = $1`,
      [group_name]
    );

    // Insert new memberships
    const inserted: RedGroupRecord[] = [];

    for (const r of redes) {
      const result = await g.pool.query(
        `
        INSERT INTO red_group_access (red_id, group_name, access)
        VALUES ($1, $2, $3)
        RETURNING red_id, group_name, access
        `,
        [r.red_id, group_name, r.access]
      );
      inserted.push(result.rows[0] as RedGroupRecord);
    }

    return inserted;
  }

  /** Add users to group */
  static async add(group_name : number, redes: { red_id: number, access: 'read' | 'write' }[]
  ): Promise<RedGroupRecord[]> {
    // Check all user_ids exist
    const ids = redes.map(m => m.red_id);
    if (ids.length === 0) return [];

    const checkUsers = await g.pool.query(
      `SELECT id FROM redes WHERE id = ANY($1)`,
      [ids]
    );

    if (checkUsers.rows.length !== ids.length) {
      throw new NotFoundError("RED_NOT_FOUND");
    }

    // Insert new memberships
    const inserted: RedGroupRecord[] = [];

    for (const r of redes) {
      const result = await g.pool.query(
        `
        INSERT INTO red_group_access (red_id, group_name, access)
        VALUES ($1, $2, $3)
        ON CONFLICT (red_id, group_name) DO UPDATE SET access=excluded.access
        RETURNING red_id, group_name, access
        `,
        [r.red_id, group_name, r.access]
      );
      inserted.push(result.rows[0] as RedGroupRecord);
    }

    return inserted;
  }


  /** List redes of a group */
  static async list(group_name: number): Promise<RedGroupRecord[]> {
    const result = await g.pool.query(
      `SELECT red_id, group_name, access FROM red_group_access WHERE group_name = $1 ORDER BY red_id`,
      [group_name]
    );
    return result.rows as RedGroupRecord[];
  }

  /** Read membership */
  static async read(
    group_id: number,
    red_id: number
  ): Promise<RedGroupRecord | null> {
    const result = await g.pool.query(
      `SELECT red_id, group_name, access FROM red_group_access WHERE group_name = $1 AND red_id = $2`,
      [group_id, red_id]
    );
    return (result.rows[0] as RedGroupRecord) || null;
  }

  /** Delete membership */
  static async delete(
    group_name: number,
    red_id: number
  ): Promise<RedGroupRecord | null> {
    const result = await g.pool.query(
      `
      DELETE FROM red_group_access
        WHERE group_name = $1 AND red_id = $2
    RETURNING red_id, group_name, access
      `,
      [group_name, red_id]
    );
    return (result.rows[0] as RedGroupRecord) || null;
  }
}
