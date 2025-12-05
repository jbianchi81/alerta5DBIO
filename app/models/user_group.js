"use strict";
// models/UserGroup.ts
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
exports.UserGroup = void 0;
const setGlobal_1 = __importDefault(require("a5base/setGlobal"));
const custom_errors_1 = require("../custom_errors");
const g = (0, setGlobal_1.default)();
class UserGroup {
    /** Assign memberships */
    static assign(group_name, members) {
        return __awaiter(this, void 0, void 0, function* () {
            // Check all user_ids exist
            const user_names = members.map(m => m.user_name);
            if (user_names.length === 0)
                return [];
            const checkUsers = yield g.pool.query(`SELECT name FROM users WHERE name = ANY($1)`, [user_names]);
            if (checkUsers.rows.length !== user_names.length) {
                throw new custom_errors_1.NotFoundError("USER_NOT_FOUND");
            }
            // Delete existing members for group
            yield g.pool.query(`DELETE FROM user_groups WHERE group_name = $1`, [group_name]);
            // Insert new memberships
            const inserted = [];
            for (const m of members) {
                const result = yield g.pool.query(`
        INSERT INTO user_groups (user_id, group_name)
        SELECT users.id, $2
        FROM users
        WHERE name=$1
        RETURNING user_id, group_name
        `, [m.user_name, group_name]);
                inserted.push(Object.assign(Object.assign({}, result.rows[0]), { user_name: m.user_name }));
            }
            return inserted;
        });
    }
    /** Add users to group */
    static add(group_name, members) {
        return __awaiter(this, void 0, void 0, function* () {
            // Check all user_ids exist
            const user_names = members.map(m => m.user_name);
            if (user_names.length === 0)
                return [];
            const checkUsers = yield g.pool.query(`SELECT name FROM users WHERE name = ANY($1)`, [user_names]);
            if (checkUsers.rows.length !== user_names.length) {
                throw new custom_errors_1.NotFoundError("USER_NOT_FOUND");
            }
            // Insert new memberships
            const inserted = [];
            for (const m of members) {
                const result = yield g.pool.query(`
        INSERT INTO user_groups (user_id, group_name)
        SELECT users.id, $2
        FROM users
        WHERE users.name=$1
        ON CONFLICT (user_id, group_name) DO UPDATE SET user_id=excluded.user_id
        RETURNING user_id, group_name
        `, [m.user_name, group_name]);
                inserted.push(Object.assign(Object.assign({}, result.rows[0]), { user_name: m.user_name }));
            }
            return inserted;
        });
    }
    /** List members of a group */
    static list(group_name) {
        return __awaiter(this, void 0, void 0, function* () {
            const result = yield g.pool.query(`SELECT user_groups.user_id, users.name AS user_name, user_groups.group_name FROM user_groups JOIN users on users.id=user_groups.user_id WHERE user_groups.group_name = $1 ORDER BY user_groups.user_id`, [group_name]);
            return result.rows;
        });
    }
    /** Read membership */
    static read(group_id, user_id, user_name) {
        return __awaiter(this, void 0, void 0, function* () {
            let filter;
            const params = [group_id];
            if (user_id) {
                filter = `AND user_groups.user_id = $2`;
                params.push(user_id);
            }
            else if (user_name) {
                filter = `AND users.name = $2`;
                params.push(user_name);
            }
            else {
                filter = "";
            }
            const result = yield g.pool.query(`SELECT user_groups.user_id, users.name as user_name, user_groups.group_name 
        FROM user_groups 
        JOIN users ON users.id=user_groups.user_id
        WHERE user_groups.group_name = $1
        ${filter}`, params);
            return result.rows[0] || null;
        });
    }
    /** Delete membership */
    static delete(group_name, user_id, user_name) {
        return __awaiter(this, void 0, void 0, function* () {
            let result;
            if (user_id) {
                result = yield g.pool.query(`
        DELETE FROM user_groups
          USING users
          WHERE users.id=user_groups.user_id
          AND group_name = $1 
          AND user_id = $2
      RETURNING user_id, group_name, users.name AS user_name
        `, [group_name, user_id]);
            }
            else if (user_name) {
                result = yield g.pool.query(`
        DELETE FROM user_groups
        USING users
          WHERE users.id=user_groups.user_id
          AND group_name = $1 
          AND users.name = $2
      RETURNING user_id, group_name, users.name AS user_name
        `, [group_name, user_name]);
            }
            else {
                throw new Error("Falta user_id o user_name");
            }
            return result.rows[0] || null;
        });
    }
}
exports.UserGroup = UserGroup;
