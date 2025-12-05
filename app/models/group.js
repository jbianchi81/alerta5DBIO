"use strict";
// models/Group.ts
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
exports.Group = void 0;
const setGlobal_1 = __importDefault(require("a5base/setGlobal"));
const g = (0, setGlobal_1.default)();
class Group {
    static list(filter = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            let result;
            if (filter.name) {
                const q = `SELECT name FROM groups WHERE name=$1 ORDER BY name`;
                result = yield g.pool.query(q, [filter.name]);
            }
            else {
                const q = `SELECT name FROM groups ORDER BY name`;
                result = yield g.pool.query(q);
            }
            return result.rows;
        });
    }
    static read(name) {
        return __awaiter(this, void 0, void 0, function* () {
            const q = `SELECT name FROM groups WHERE name = $1`;
            const result = yield g.pool.query(q, [name]);
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
      INSERT INTO groups (name)
      VALUES ($1)
      RETURNING name
    `;
            const result = yield g.pool.query(q, [params.name]);
            return result.rows[0];
        });
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
    static delete(name) {
        return __awaiter(this, void 0, void 0, function* () {
            const q = `
      DELETE FROM groups
       WHERE name = $1
   RETURNING name
    `;
            const result = yield g.pool.query(q, [name]);
            return result.rows[0] || null;
        });
    }
}
exports.Group = Group;
