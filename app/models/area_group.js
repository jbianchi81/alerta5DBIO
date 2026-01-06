"use strict";
// models/AreaGroup.ts
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
exports.AreaGroup = void 0;
const setGlobal_1 = __importDefault(require("a5base/setGlobal"));
const custom_errors_1 = require("../custom_errors");
const utils2_1 = require("../utils2");
const g = (0, setGlobal_1.default)();
class AreaGroup {
    constructor(params) {
        this.id = params.id;
        this.name = params.name;
        this.owner_id = params.owner_id;
        this.areas = params.areas;
    }
    static list() {
        return __awaiter(this, arguments, void 0, function* (filter = {}) {
            let result;
            if (filter.id) {
                const q = `SELECT id,name,owner_id FROM area_groups WHERE id=$1`;
                result = yield g.pool.query(q, [filter.id]);
            }
            else if (filter.name) {
                const q = `SELECT id,name,owner_id FROM area_groups WHERE name=$1`;
                result = yield g.pool.query(q, [filter.name]);
            }
            else if (filter.owner_id) {
                const q = `SELECT id,name,owner_id FROM area_groups WHERE owner_id=$1 ORDER BY id`;
                result = yield g.pool.query(q, [filter.name]);
            }
            else {
                const q = `SELECT id,name,owner_id FROM area_groups ORDER BY id`;
                result = yield g.pool.query(q);
            }
            return result.rows;
        });
    }
    static read(id, user_id) {
        return __awaiter(this, void 0, void 0, function* () {
            const access_join = (user_id) ? `JOIN user_area_access ON (ag_id=area_groups.id AND user_id=${user_id})` : "";
            const access_level = (user_id) ? "user_area_access.effective_access" : "'write'";
            const q = `SELECT 
      area_groups.id, 
      area_groups.name, 
      area_groups.owner_id, 
      json_agg(
      json_build_object(
      'id', areas_pluvio.unid, 
      'nombre', areas_pluvio.nombre
      )) AS areas,
      ${access_level} AS access_level
      FROM area_groups
      ${access_join} 
      LEFT OUTER JOIN areas_pluvio ON area_groups.id=areas_pluvio.group_id
      WHERE area_groups.id = $1
      GROUP BY area_groups.id, area_groups.name, area_groups.owner_id, access_level`;
            const result = yield g.pool.query(q, [id]);
            return result.rows[0] || null;
        });
    }
    static create(params, user_id) {
        return __awaiter(this, void 0, void 0, function* () {
            if (Array.isArray(params)) {
                const results = [];
                for (const p of params) {
                    results.push(yield this.createOne(Object.assign(Object.assign({}, p), { owner_id: user_id !== null && user_id !== void 0 ? user_id : p.owner_id })));
                }
                return results;
            }
            else {
                return [yield this.createOne(Object.assign(Object.assign({}, params), { owner_id: user_id !== null && user_id !== void 0 ? user_id : params.owner_id }))];
            }
        });
    }
    static createOne(params) {
        return __awaiter(this, void 0, void 0, function* () {
            const q = `
      INSERT INTO area_groups (id, name, owner_id)
      VALUES (COALESCE($1, nextval('area_groups_id_seq'::regclass)), $2, $3)
      ON CONFLICT (id) DO UPDATE SET name=excluded.name, owner_id=excluded.owner_id
      RETURNING id, name, owner_id
    `;
            const result = yield g.pool.query(q, [params.id, params.name, params.owner_id]);
            return result.rows[0];
        });
    }
    static update(id, params) {
        return __awaiter(this, void 0, void 0, function* () {
            const q = `
      UPDATE area_groups
         SET name = COALESCE($1, name),
             owner_id = COALESCE($2, owner_id)
       WHERE id = $3
   RETURNING id, name, owner_id
    `;
            const result = yield g.pool.query(q, [params.name, params.owner_id, id]);
            return result.rows[0] || null;
        });
    }
    static delete(id) {
        return __awaiter(this, void 0, void 0, function* () {
            const q = `
      DELETE FROM area_groups
       WHERE id = $1
   RETURNING id, name, owner_id
    `;
            const result = yield g.pool.query(q, [id]);
            return result.rows[0] || null;
        });
    }
    static grantAccess(id, user_groups) {
        return __awaiter(this, void 0, void 0, function* () {
            var _a;
            // Check all user_group names exist
            const group_names = user_groups.map(ug => ug.name).filter(ug => ug);
            if (group_names.length === 0)
                throw new custom_errors_1.BadRequestError("Falta 'name' en items");
            const checkUserGroups = yield g.pool.query(`SELECT name FROM groups WHERE name = ANY($1)`, [group_names]);
            if (checkUserGroups.rows.length !== user_groups.length) {
                throw new custom_errors_1.NotFoundError("GROUP_NOT_FOUND");
            }
            const results = [];
            var i = 0;
            for (const user_group of user_groups) {
                if (!user_group.name) {
                    throw new custom_errors_1.BadRequestError("Falta 'name' en item " + i);
                }
                const granted = yield this.grantAccessOne(id, user_group.name, (_a = user_group.access) !== null && _a !== void 0 ? _a : "read");
                results.push(granted);
                i = i + 1;
            }
            return results;
        });
    }
    static grantAccessOne(id, name, access) {
        return __awaiter(this, void 0, void 0, function* () {
            const q = `INSERT INTO user_area_groups_access (ag_id, group_name, access) VALUES ($1, $2, $3) ON CONFLICT (group_name, ag_id) DO UPDATE SET access=excluded.access RETURNING group_name AS name, access`;
            const result = yield g.pool.query(q, [id, name, access]);
            if (!result.rows.length) {
                throw new Error("No se insertó fila en user_area_groups_access");
            }
            return result.rows[0];
        });
    }
    static removeAccessOne(id, name) {
        return __awaiter(this, void 0, void 0, function* () {
            const q = `DELETE FROM user_area_groups_access WHERE ag_id=$1 and group_name=$2 RETURNING group_name as name, access`;
            const result = yield g.pool.query(q, [id, name]);
            if (!result.rows.length) {
                throw new Error("No se encontró el registro en user_area_groups_access");
            }
            return result.rows[0];
        });
    }
    static listAccess(id, filter) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!id) {
                throw new custom_errors_1.BadRequestError("Falta id");
            }
            const filter_string = (0, utils2_1.control_filter2)({ name: { type: "string" }, access: { type: "string" } }, filter);
            const q = `SELECT group_name AS name, access FROM user_area_groups_access WHERE ag_id=$1 ${filter_string}`;
            const results = yield g.pool.query(q, [id]);
            return results.rows;
        });
    }
    static readAccess(id, group_name) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!id) {
                throw new custom_errors_1.BadRequestError("Falta id");
            }
            if (!group_name) {
                throw new custom_errors_1.BadRequestError("Falta group_name");
            }
            const q = `SELECT group_name AS name, access FROM user_area_groups_access WHERE ag_id=$1 AND group_name=$2`;
            const results = yield g.pool.query(q, [id, group_name]);
            return results.rows[0] || null;
        });
    }
    static hasAccess(user_id_1, ag_id_1) {
        return __awaiter(this, arguments, void 0, function* (user_id, ag_id, write = false) {
            var q = `SELECT EXISTS (
      SELECT 1 
      FROM user_area_access 
      WHERE user_id=$1 
      AND ag_id=$2 
      ${(write) ? "AND effective_access='write'" : ""})`;
            const result = yield g.pool.query(q, [user_id, ag_id]);
            if (result.rows.length && result.rows[0].exists) {
                return true;
            }
            else {
                return false;
            }
        });
    }
}
exports.AreaGroup = AreaGroup;
