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
exports.Client = void 0;
const axios_1 = __importDefault(require("axios"));
const abstract_accessor_engine_1 = require("./abstract_accessor_engine");
class Client extends abstract_accessor_engine_1.AbstractAccessorEngine {
    constructor(config) {
        super(config);
        this.default_config = {
            url: "https://api.pilcomayo.net",
            username: "my_username",
            password: "my_password"
        };
        this.setConfig(config);
        this.connection = axios_1.default.create();
    }
    login() {
        return __awaiter(this, void 0, void 0, function* () {
            const response = yield this.connection.post(`${this.config.url}/login`, {
                username: this.config.username,
                password: this.config.password
            });
            if (response.status != 200) {
                throw new Error(`Login failed: ${response.statusText}`);
            }
            if (!response.data || !response.data.tokenAuth) {
                throw new Error(`Login failed: tokenAuth missing in response`);
            }
            return response.data;
        });
    }
    listEstaciones(id_estacion) {
        return __awaiter(this, void 0, void 0, function* () {
            const response = yield this.connection.post(`${this.config.url}/list_estaciones`, {
                id_estacion
            });
            if (response.status != 200) {
                throw new Error(`Request failed: ${response.statusText}`);
            }
            if (!response.data) {
                throw new Error(`listEstaciones failed: no data`);
            }
            if (Array.isArray(response.data)) {
                return response.data;
            }
            else {
                return [response.data];
            }
        });
    }
}
exports.Client = Client;
Client._get_is_multiseries = false;
