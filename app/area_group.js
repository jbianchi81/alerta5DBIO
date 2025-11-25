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
Object.defineProperty(exports, "__esModule", { value: true });
const baseModel_1 = require("a5base/baseModel");
const utils_1 = require("./utils");
const area_group = class extends baseModel_1.baseModel {
    constructor(args) {
        super();
        this.id = args.id;
        this.name = args.name;
    }
    static read(filter = {}, options = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            const filter_string = (0, utils_1.control_filter2)({
                id: { type: "integer" },
                name: { type: "string" }
            }, filter, "area_groups");
            if (options.get_areas) {
                const result = yield global.pool.query(``);
            }
            else {
                const result = yield global.pool.query(`SELECT id, name FROM area_groups WHERE 1=1 ${filter_string}`);
                return result.rows.map((r) => new this(r));
            }
        });
    }
};
exports.default = area_group;
