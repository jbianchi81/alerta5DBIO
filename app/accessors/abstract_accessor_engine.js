"use strict";
exports.__esModule = true;
exports.AbstractAccessorEngine = void 0;
var AbstractAccessorEngine = /** @class */ (function () {
    // async get(filter : ObservacionesFilter) {
    //     console.warn("get method not implemented in this class")
    //     return [] as Array<Observacion>
    // }
    function AbstractAccessorEngine(config) {
        if (config === void 0) { config = {}; }
        this.default_config = {};
        this.config = {};
        this.setConfig(config);
    }
    AbstractAccessorEngine.prototype.setConfig = function (config) {
        this.config = {};
        Object.assign(this.config, this.default_config);
        Object.assign(this.config, config);
    };
    return AbstractAccessorEngine;
}());
exports.AbstractAccessorEngine = AbstractAccessorEngine;
