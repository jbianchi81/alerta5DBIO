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
const CRUD_1 = require("../CRUD");
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
        if (new.target === AbstractAccessorEngine) {
            throw new Error("Cannot instantiate an abstract class.");
        }
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
    static setFilterValuesToFirst(filter) {
        const filter_ = Object.assign({}, filter);
        for (const key of Object.keys(filter_)) {
            if (Array.isArray(filter_[key])) {
                if (filter_[key].length) {
                    filter_[key] = filter_[key][0];
                }
                else {
                    filter_[key] = undefined;
                }
            }
        }
        return filter_;
    }
    static setFilterValuesToArray(filter, empty_arrays = true) {
        const filter_ = Object.assign({}, filter);
        const valid_keys = [
            {
                "key": "series_id",
                "type": "int"
            }, {
                "key": "estacion_id",
                "type": "int"
            }, {
                "key": "var_id",
                "type": "int"
            }, {
                "key": "id_externo",
                "type": "string"
            }
        ];
        for (const key of valid_keys) {
            if (filter_[key.key] != undefined) {
                if (!Array.isArray(filter_[key.key])) {
                    if (key.type == "int") {
                        filter_[key.key] = [parseInt(filter_[key.key])];
                    }
                    else {
                        filter_[key.key] = [filter_[key.key].toString()];
                    }
                }
            }
            else if (empty_arrays) {
                filter_[key.key] = [];
            }
        }
        return filter_;
    }
    get(filter, options) {
        return __awaiter(this, void 0, void 0, function* () {
            throw new Error("Method 'get()' must be implemented in child class");
        });
    }
    update(filter_1) {
        return __awaiter(this, arguments, void 0, function* (filter, options = {}) {
            const series = yield this.get(filter, Object.assign(Object.assign({}, options), { return_series: true }));
            const updated = [];
            for (var serie of series) {
                const c_serie = new CRUD_1.serie(serie);
                yield c_serie.createObservaciones();
                updated.push(c_serie);
            }
            if (options.return_series) {
                return updated;
            }
            else {
                const observaciones = [];
                for (var i = 0; i < updated.length; i++) {
                    if (updated[i].observaciones) {
                        observaciones.push(...updated[i].observaciones);
                    }
                }
                return new CRUD_1.observaciones(observaciones);
            }
        });
    }
    getSeries(filter) {
        return __awaiter(this, void 0, void 0, function* () {
            throw new Error("Method 'getSeries()' must be implemented in child class");
        });
    }
    updateSeries(filter) {
        return __awaiter(this, void 0, void 0, function* () {
            const series = yield this.getSeries(filter);
            return CRUD_1.serie.create(series);
        });
    }
    getSites(filter) {
        return __awaiter(this, void 0, void 0, function* () {
            throw new Error("Method 'getSites()' must be implemented in child class");
        });
    }
    updateSites(filter) {
        return __awaiter(this, void 0, void 0, function* () {
            const sites = yield this.getSites(filter);
            return CRUD_1.estacion.create(sites);
        });
    }
}
exports.AbstractAccessorEngine = AbstractAccessorEngine;
