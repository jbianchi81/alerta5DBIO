"use strict";
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
const setGlobal_1 = __importDefault(require("a5base/setGlobal"));
const g = (0, setGlobal_1.default)();
class Fuente {
    static hasAccess(user_id, fuentes_id, write = false) {
        return __awaiter(this, void 0, void 0, function* () {
            var q = `SELECT EXISTS (
      SELECT 1 
      FROM user_fuentes_access 
      WHERE user_id=$1 
      AND fuentes_id=$2 
      ${(write) ? "AND effective_access='write'" : ""})`;
            const result = yield g.pool.query(q, [user_id, fuentes_id]);
            if (result.rows.length && result.rows[0].exists) {
                return true;
            }
            else {
                return false;
            }
        });
    }
}
exports.default = Fuente;
