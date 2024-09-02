"use strict";
var __extends = (this && this.__extends) || (function () {
    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (Object.prototype.hasOwnProperty.call(b, p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };
    return function (d, b) {
        if (typeof b !== "function" && b !== null)
            throw new TypeError("Class extends value " + String(b) + " is not a constructor or null");
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
exports.__esModule = true;
exports.Client = void 0;
var abstract_accessor_engine_1 = require("./abstract_accessor_engine");
var duckdb_async_1 = require("duckdb-async");
var accessor_utils_1 = require("../accessor_utils");
var Client = /** @class */ (function (_super) {
    __extends(Client, _super);
    function Client() {
        var _this = _super !== null && _super.apply(this, arguments) || this;
        _this.default_config = {
            url: "https://ons-aws-prod-opendata.s3.amazonaws.com/",
            file_pattern: "dataset/dados_hidrologicos_di/DADOS_HIDROLOGICOS_RES_%YYYY%.parquet",
            output_file: "/tmp/dados_ons.parquet",
            sites_map: [],
            var_map: [
                { field_name: "val_volumeutilcon", var_id: 26 },
                { field_name: "val_vazaoafluente", var_id: 22 },
                { field_name: "val_vazaovertida", var_id: 24 },
                { field_name: "val_vazaodefluente", var_id: 23 }
            ],
            series_map: []
        };
        return _this;
    }
    Client.readParquetFile = function (filename, limit, offset) {
        if (limit === void 0) { limit = 1000000; }
        if (offset === void 0) { offset = 0; }
        return __awaiter(this, void 0, void 0, function () {
            var db, rows;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, duckdb_async_1.Database.create(":memory:")];
                    case 1:
                        db = _a.sent();
                        return [4 /*yield*/, db.all("SELECT * FROM READ_PARQUET('".concat(filename, "') LIMIT ").concat(limit, " OFFSET ").concat(offset))
                            // console.log(rows);
                        ];
                    case 2:
                        rows = _a.sent();
                        // console.log(rows);
                        return [2 /*return*/, rows.map(function (r) { return r; })];
                }
            });
        });
    };
    Client.parseDadosHidrologicosRecord = function (record, field, series_id) {
        var end_date = new Date(record.din_instante);
        end_date.setUTCDate(end_date.getUTCDate() + 1);
        return {
            timestart: record.din_instante,
            timeend: end_date,
            valor: record[field],
            series_id: series_id
        };
    };
    Client.prototype.getEstacionId = function (id_reservatorio) {
        for (var _i = 0, _a = this.config.sites_map; _i < _a.length; _i++) {
            var estacion = _a[_i];
            if (estacion.id_reservatorio == id_reservatorio) {
                return estacion.estacion_id;
            }
        }
        return;
    };
    Client.prototype.getSeriesId = function (estacion_id, var_id) {
        for (var _i = 0, _a = this.config.series_map; _i < _a.length; _i++) {
            var serie = _a[_i];
            if (serie.estacion_id == estacion_id && serie.var_id == var_id) {
                return serie.series_id;
            }
        }
        return;
    };
    Client.setFilterValuesToArray = function (filter) {
        var filter_ = Object.assign({}, filter);
        for (var _i = 0, _a = ["series_id", "estacion_id", "var_id"]; _i < _a.length; _i++) {
            var key = _a[_i];
            if (filter_[key] != undefined) {
                if (!Array.isArray(filter_[key])) {
                    filter_[key] = [filter_[key]];
                }
            }
        }
        return filter_;
    };
    Client.prototype.get = function (filter) {
        return __awaiter(this, void 0, void 0, function () {
            var filter_, observaciones, year, filepath, url, records, _i, records_1, record, estacion_id, series_id, _a, _b, variable;
            return __generator(this, function (_c) {
                switch (_c.label) {
                    case 0:
                        if (!filter || !filter.timestart || !filter.timeend) {
                            throw ("Missing timestart and/or timeend");
                        }
                        filter_ = Client.setFilterValuesToArray(filter);
                        observaciones = [];
                        year = filter_.timestart.getUTCFullYear();
                        _c.label = 1;
                    case 1:
                        if (!(year <= filter_.timeend.getUTCFullYear())) return [3 /*break*/, 5];
                        filepath = this.config.file_pattern.replace("%YYYY%", year.toString());
                        url = "".concat(this.config.url).concat(filepath);
                        return [4 /*yield*/, (0, accessor_utils_1.fetch)(url, undefined, this.config.output_file, function () { return null; })];
                    case 2:
                        _c.sent();
                        return [4 /*yield*/, Client.readParquetFile(this.config.output_file)];
                    case 3:
                        records = _c.sent();
                        for (_i = 0, records_1 = records; _i < records_1.length; _i++) {
                            record = records_1[_i];
                            if (record.din_instante < filter_.timestart || record.din_instante > filter_.timeend) {
                                continue;
                            }
                            estacion_id = this.getEstacionId(record.id_reservatorio);
                            if (!estacion_id) {
                                console.warn("estacion_id not found for id_reservatorio '".concat(record.id_reservatorio));
                            }
                            else {
                                if (filter_.estacion_id && filter_.estacion_id.indexOf(estacion_id) < 0) {
                                    continue;
                                }
                                for (_a = 0, _b = this.config.var_map; _a < _b.length; _a++) {
                                    variable = _b[_a];
                                    series_id = this.getSeriesId(estacion_id, variable.var_id);
                                    observaciones.push(Client.parseDadosHidrologicosRecord(record, variable.field_name, series_id));
                                }
                            }
                        }
                        _c.label = 4;
                    case 4:
                        year++;
                        return [3 /*break*/, 1];
                    case 5: return [2 /*return*/, observaciones];
                }
            });
        });
    };
    Client._get_is_multiseries = true;
    return Client;
}(abstract_accessor_engine_1.AbstractAccessorEngine));
exports.Client = Client;
