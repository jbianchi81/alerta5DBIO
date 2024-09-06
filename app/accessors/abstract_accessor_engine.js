"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.AbstractAccessorEngine = void 0;
class AbstractAccessorEngine {
    setConfig(config) {
        this.config = {};
        Object.assign(this.config, this.default_config);
        Object.assign(this.config, config);
    }
    // async get(filter : ObservacionesFilter) {
    //     console.warn("get method not implemented in this class")
    //     return [] as Array<Observacion>
    // }
    constructor(config = {}) {
        this.default_config = {};
        this.config = {};
        this.setConfig(config);
    }
}
exports.AbstractAccessorEngine = AbstractAccessorEngine;
