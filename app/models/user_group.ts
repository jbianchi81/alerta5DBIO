// models/UserGroup.ts

import setGlobal from 'a5base/setGlobal'

import {NotFoundError} from '../custom_errors'

const g = setGlobal()

export interface UserGroupRecord {
  user_id: number;
  use_name: string;
  group_name: string;
}

export class UserGroup {
  /** Assign memberships */
  static async assign(
    group_name: string,
    members: { user_name: string }[]
  ): Promise<UserGroupRecord[]> {
    // Check all user_ids exist
    const user_names = members.map(m => m.user_name);
    if (user_names.length === 0) return [];

    const checkUsers = await (g.pool as any).query(
      `SELECT name FROM users WHERE name = ANY($1)`,
      [user_names]
    );

    if (checkUsers.rows.length !== user_names.length) {
      throw new NotFoundError("USER_NOT_FOUND");
    }

    // Delete existing members for group
    await (g.pool as any).query(
      `DELETE FROM user_groups WHERE group_name = $1`,
      [group_name]
    );

    // Insert new memberships
    const inserted: UserGroupRecord[] = [];

    for (const m of members) {
      const result = await (g.pool as any).query(
        `
        INSERT INTO user_groups (user_id, group_name)
        SELECT users.id, $2
        FROM users
        WHERE name=$1
        RETURNING user_id, group_name
        `,
        [m.user_name, group_name]
      );
      inserted.push({...result.rows[0], user_name: m.user_name} as UserGroupRecord);
    }

    return inserted;
  }

  /** Add users to group */
  static async add(group_name : string, members: { user_name: string }[]
  ): Promise<UserGroupRecord[]> {
    // Check all user_ids exist
    const user_names = members.map(m => m.user_name);
    if (user_names.length === 0) return [];

    const checkUsers = await (g.pool as any).query(
      `SELECT name FROM users WHERE name = ANY($1)`,
      [user_names]
    );

    if (checkUsers.rows.length !== user_names.length) {
      throw new NotFoundError("USER_NOT_FOUND");
    }

    // Insert new memberships
    const inserted: UserGroupRecord[] = [];

    for (const m of members) {
      const result = await (g.pool as any).query(
        `
        INSERT INTO user_groups (user_id, group_name)
        SELECT users.id, $2
        FROM users
        WHERE users.name=$1
        ON CONFLICT (user_id, group_name) DO UPDATE SET user_id=excluded.user_id
        RETURNING user_id, group_name
        `,
        [m.user_name, group_name]
      );
      inserted.push({...result.rows[0], user_name: m.user_name} as UserGroupRecord);
    }

    return inserted;
  }


  /** List members of a group */
  static async list(group_name: string): Promise<UserGroupRecord[]> {
    const result = await (g.pool as any).query(
      `SELECT user_groups.user_id, users.name AS user_name, user_groups.group_name FROM user_groups JOIN users on users.id=user_groups.user_id WHERE user_groups.group_name = $1 ORDER BY user_groups.user_id`,
      [group_name]
    );
    return result.rows as UserGroupRecord[];
  }

  /** Read membership */
  static async read(
    group_name: string,
    user_id?: number,
    user_name?: string
  ): Promise<UserGroupRecord | null> {
    let filter : string
    const params : any[] = [group_name]
    if(user_id) {
      filter = `AND user_groups.user_id = $2`
      params.push(user_id)
    } else if(user_name) {
      filter = `AND users.name = $2`
      params.push(user_name)
    } else {
      filter = ""
    }
    const result = await (g.pool as any).query(
      `SELECT user_groups.user_id, users.name as user_name, user_groups.group_name 
        FROM user_groups 
        JOIN users ON users.id=user_groups.user_id
        WHERE user_groups.group_name = $1
        ${filter}`,
      params
    );
    return (result.rows[0] as UserGroupRecord) || null;
  }

  /** Delete membership */
  static async delete(
    group_name: string,
    user_id?: number,
    user_name?: string
  ): Promise<UserGroupRecord | null> {
    let result : any
    if(user_id) {
      result = await (g.pool as any).query(
        `
        DELETE FROM user_groups
          USING users
          WHERE users.id=user_groups.user_id
          AND group_name = $1 
          AND user_id = $2
      RETURNING user_id, group_name, users.name AS user_name
        `,
        [group_name, user_id]
      );
    } else if(user_name) {
      result = await (g.pool as any).query(
        `
        DELETE FROM user_groups
        USING users
          WHERE users.id=user_groups.user_id
          AND group_name = $1 
          AND users.name = $2
      RETURNING user_id, group_name, users.name AS user_name
        `,
        [group_name, user_name]
      );
    } else {
      throw new Error("Falta user_id o user_name")
    }
    return (result.rows[0] as UserGroupRecord) || null;
  }
}
