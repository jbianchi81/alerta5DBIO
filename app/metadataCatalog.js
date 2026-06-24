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
exports.MetadataCatalog = void 0;
const setGlobal_1 = __importDefault(require("a5base/setGlobal"));
const g = (0, setGlobal_1.default)();
class MetadataCatalog {
    static list() {
        return __awaiter(this, void 0, void 0, function* () {
            const pool = g.pool;
            const result = yield pool.query(`select gid,name from metadata_catalog`);
            return result.rows;
        });
    }
    static readOne(name) {
        return __awaiter(this, void 0, void 0, function* () {
            const pool = g.pool;
            const result = yield pool.query(`
            SELECT
                gid,
                name,
                location,
                sourcetype,
                title,
                abstract,
                description,
                keywords,
                defaultcrs,
                metadata_creation_date,
                ST_asGeoJSON(boundingbox)::json AS boundingbox,
                units,
                frequency,
                resolution,
                data_sources,
                services_provided    
            FROM metadata_catalog
            WHERE name=$1`, [name]);
            if (!result.rowCount) {
                throw new Error("Metadata item not found");
            }
            return result.rows[0];
        });
    }
}
exports.MetadataCatalog = MetadataCatalog;
