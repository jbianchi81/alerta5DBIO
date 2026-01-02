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
const g = (0, setGlobal_1.default)();
class AreaGroup {
    static list(filter = {}) {
        return __awaiter(this, void 0, void 0, function* () {
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
    static read(id) {
        return __awaiter(this, void 0, void 0, function* () {
            const q = `SELECT id, name, owner_id FROM area_groups WHERE id = $1`;
            const result = yield g.pool.query(q, [id]);
            // TODO add areas
            return result.rows[0] || null;
        });
    }
    static create(params) {
        return __awaiter(this, void 0, void 0, function* () {
            if (Array.isArray(params)) {
                const results = [];
                for (const p of params) {
                    results.push(yield this.createOne(p));
                }
                return results;
            }
            else {
                return [yield this.createOne(params)];
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
}
exports.AreaGroup = AreaGroup;
