// models/UserGroup.ts

import setGlobal from 'a5base/setGlobal'

import {NotFoundError} from '../custom_errors'

const g = setGlobal()

export interface UserGroupRecord {
  user_id: number;
  group_name: string;
}

export class UserGroup {
  /** Assign memberships */
  static async assign(
    group_name: string,
    members: { user_id: number }[]
  ): Promise<UserGroupRecord[]> {
    // Check all user_ids exist
    const ids = members.map(m => m.user_id);
    if (ids.length === 0) return [];

    const checkUsers = await g.pool.query(
      `SELECT id FROM users WHERE id = ANY($1)`,
      [ids]
    );

    if (checkUsers.rows.length !== ids.length) {
      throw new NotFoundError("USER_NOT_FOUND");
    }

    // Delete existing members for group
    await g.pool.query(
      `DELETE FROM user_groups WHERE group_name = $1`,
      [group_name]
    );

    // Insert new memberships
    const inserted: UserGroupRecord[] = [];

    for (const m of members) {
      const result = await g.pool.query(
        `
        INSERT INTO user_groups (user_id, group_name)
        VALUES ($1, $2)
        RETURNING user_id, group_name
        `,
        [m.user_id, group_name]
      );
      inserted.push(result.rows[0] as UserGroupRecord);
    }

    return inserted;
  }

  /** Add users to group */
  static async add(group_name : number, members: { user_id: number }[]
  ): Promise<UserGroupRecord[]> {
    // Check all user_ids exist
    const ids = members.map(m => m.user_id);
    if (ids.length === 0) return [];

    const checkUsers = await g.pool.query(
      `SELECT id FROM users WHERE id = ANY($1)`,
      [ids]
    );

    if (checkUsers.rows.length !== ids.length) {
      throw new NotFoundError("USER_NOT_FOUND");
    }

    // Insert new memberships
    const inserted: UserGroupRecord[] = [];

    for (const m of members) {
      const result = await g.pool.query(
        `
        INSERT INTO user_groups (user_id, group_name)
        VALUES ($1, $2)
        ON CONFLICT (user_id, group_name) DO UPDATE SET user_id=excluded.user_id
        RETURNING user_id, group_name
        `,
        [m.user_id, group_name]
      );
      inserted.push(result.rows[0] as UserGroupRecord);
    }

    return inserted;
  }


  /** List members of a group */
  static async list(group_name: number): Promise<UserGroupRecord[]> {
    const result = await g.pool.query(
      `SELECT user_id, group_name FROM user_groups WHERE group_name = $1 ORDER BY user_id`,
      [group_name]
    );
    return result.rows as UserGroupRecord[];
  }

  /** Read membership */
  static async read(
    group_id: number,
    user_name: number
  ): Promise<UserGroupRecord | null> {
    const result = await g.pool.query(
      `SELECT user_id, group_name FROM user_groups WHERE group_name = $1 AND user_id = $2`,
      [group_id, user_name]
    );
    return (result.rows[0] as UserGroupRecord) || null;
  }

  /** Delete membership */
  static async delete(
    group_name: number,
    user_id: number
  ): Promise<UserGroupRecord | null> {
    const result = await g.pool.query(
      `
      DELETE FROM user_groups
        WHERE group_name = $1 AND user_id = $2
    RETURNING user_id, group_name
      `,
      [group_name, user_id]
    );
    return (result.rows[0] as UserGroupRecord) || null;
  }
}
