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
exports.AbstractAccessorEngine = void 0;
const axios_1 = __importDefault(require("axios"));
class AbstractAccessorEngine {
    setConfig(config) {
        this.config = {
            url: config.url
        };
        Object.assign(this.config, this.default_config);
        Object.assign(this.config, config);
    }
    // async get(filter : ObservacionesFilter) {
    //     console.warn("get method not implemented in this class")
    //     return [] as Array<Observacion>
    // }
    constructor(config) {
        this.default_config = {};
        this.setConfig(config);
    }
    test() {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                var response = yield (0, axios_1.default)(this.config.url);
            }
            catch (e) {
                console.error(e);
                return false;
            }
            if (response.status <= 299) {
                return true;
            }
            else {
                return false;
            }
        });
    }
}
exports.AbstractAccessorEngine = AbstractAccessorEngine;
