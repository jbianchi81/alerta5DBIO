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
exports.RedGroup = void 0;
const setGlobal_1 = __importDefault(require("a5base/setGlobal"));
const custom_errors_1 = require("../custom_errors");
const g = (0, setGlobal_1.default)();
class RedGroup {
    /** Assign access */
    static assign(group_name, redes) {
        return __awaiter(this, void 0, void 0, function* () {
            // Check all user_ids exist
            const ids = redes.map(m => m.red_id);
            if (ids.length === 0)
                return [];
            const checkRedes = yield g.pool.query(`SELECT id FROM redes WHERE id = ANY($1)`, [ids]);
            if (checkRedes.rows.length !== ids.length) {
                throw new custom_errors_1.NotFoundError("RED_NOT_FOUND");
            }
            // Delete existing redes for group
            yield g.pool.query(`DELETE FROM red_group_access WHERE group_name = $1`, [group_name]);
            // Insert new memberships
            const inserted = [];
            for (const r of redes) {
                const result = yield g.pool.query(`
        INSERT INTO red_group_access (red_id, group_name, access)
        VALUES ($1, $2, $3)
        RETURNING red_id, group_name, access
        `, [r.red_id, group_name, r.access]);
                inserted.push(result.rows[0]);
            }
            return inserted;
        });
    }
    /** Add users to group */
    static add(group_name, redes) {
        return __awaiter(this, void 0, void 0, function* () {
            // Check all user_ids exist
            const ids = redes.map(m => m.red_id);
            if (ids.length === 0)
                return [];
            const checkUsers = yield g.pool.query(`SELECT id FROM redes WHERE id = ANY($1)`, [ids]);
            if (checkUsers.rows.length !== ids.length) {
                throw new custom_errors_1.NotFoundError("RED_NOT_FOUND");
            }
            // Insert new memberships
            const inserted = [];
            for (const r of redes) {
                const result = yield g.pool.query(`
        INSERT INTO red_group_access (red_id, group_name, access)
        VALUES ($1, $2, $3)
        ON CONFLICT (red_id, group_name) DO UPDATE SET access=excluded.access
        RETURNING red_id, group_name, access
        `, [r.red_id, group_name, r.access]);
                inserted.push(result.rows[0]);
            }
            return inserted;
        });
    }
    /** List redes of a group */
    static list(group_name) {
        return __awaiter(this, void 0, void 0, function* () {
            const result = yield g.pool.query(`SELECT red_id, group_name, access FROM red_group_access WHERE group_name = $1 ORDER BY red_id`, [group_name]);
            return result.rows;
        });
    }
    /** Read membership */
    static read(group_id, red_id) {
        return __awaiter(this, void 0, void 0, function* () {
            const result = yield g.pool.query(`SELECT red_id, group_name, access FROM red_group_access WHERE group_name = $1 AND red_id = $2`, [group_id, red_id]);
            return result.rows[0] || null;
        });
    }
    /** Delete membership */
    static delete(group_name, red_id) {
        return __awaiter(this, void 0, void 0, function* () {
            const result = yield g.pool.query(`
      DELETE FROM red_group_access
        WHERE group_name = $1 AND red_id = $2
    RETURNING red_id, group_name, access
      `, [group_name, red_id]);
            return result.rows[0] || null;
        });
    }
}
exports.RedGroup = RedGroup;
