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
            const ids = members.map(m => m.user_id);
            if (ids.length === 0)
                return [];
            const checkUsers = yield g.pool.query(`SELECT id FROM users WHERE id = ANY($1)`, [ids]);
            if (checkUsers.rows.length !== ids.length) {
                throw new custom_errors_1.NotFoundError("USER_NOT_FOUND");
            }
            // Delete existing members for group
            yield g.pool.query(`DELETE FROM user_groups WHERE group_name = $1`, [group_name]);
            // Insert new memberships
            const inserted = [];
            for (const m of members) {
                const result = yield g.pool.query(`
        INSERT INTO user_groups (user_id, group_name)
        VALUES ($1, $2)
        RETURNING user_id, group_name
        `, [m.user_id, group_name]);
                inserted.push(result.rows[0]);
            }
            return inserted;
        });
    }
    /** Add users to group */
    static add(group_name, members) {
        return __awaiter(this, void 0, void 0, function* () {
            // Check all user_ids exist
            const ids = members.map(m => m.user_id);
            if (ids.length === 0)
                return [];
            const checkUsers = yield g.pool.query(`SELECT id FROM users WHERE id = ANY($1)`, [ids]);
            if (checkUsers.rows.length !== ids.length) {
                throw new custom_errors_1.NotFoundError("USER_NOT_FOUND");
            }
            // Insert new memberships
            const inserted = [];
            for (const m of members) {
                const result = yield g.pool.query(`
        INSERT INTO user_groups (user_id, group_name)
        VALUES ($1, $2)
        ON CONFLICT (user_id, group_name) DO UPDATE SET user_id=excluded.user_id
        RETURNING user_id, group_name
        `, [m.user_id, group_name]);
                inserted.push(result.rows[0]);
            }
            return inserted;
        });
    }
    /** List members of a group */
    static list(group_name) {
        return __awaiter(this, void 0, void 0, function* () {
            const result = yield g.pool.query(`SELECT user_id, group_name FROM user_groups WHERE group_name = $1 ORDER BY user_id`, [group_name]);
            return result.rows;
        });
    }
    /** Read membership */
    static read(group_id, user_name) {
        return __awaiter(this, void 0, void 0, function* () {
            const result = yield g.pool.query(`SELECT user_id, group_name FROM user_groups WHERE group_name = $1 AND user_id = $2`, [group_id, user_name]);
            return result.rows[0] || null;
        });
    }
    /** Delete membership */
    static delete(group_name, user_id) {
        return __awaiter(this, void 0, void 0, function* () {
            const result = yield g.pool.query(`
      DELETE FROM user_groups
        WHERE group_name = $1 AND user_id = $2
    RETURNING user_id, group_name
      `, [group_name, user_id]);
            return result.rows[0] || null;
        });
    }
}
exports.UserGroup = UserGroup;
