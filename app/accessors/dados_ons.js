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
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
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
var CRUD_1 = require("../CRUD");
var Client = /** @class */ (function (_super) {
    __extends(Client, _super);
    function Client(config) {
        if (config === void 0) { config = {}; }
        var _this = _super.call(this, config) || this;
        _this.default_config = {
            url: "https://ons-aws-prod-opendata.s3.amazonaws.com/",
            sites_file: "dataset/reservatorio/RESERVATORIOS.parquet",
            file_pattern: "dataset/dados_hidrologicos_di/DADOS_HIDROLOGICOS_RES_%YYYY%.parquet",
            output_file: "/tmp/dados_ons.parquet",
            sites_output_file: "/tmp/reservatorios_ons.parquet",
            sites_map: [],
            var_map: [
                { field_name: "val_volumeutilcon", var_id: 26 },
                { field_name: "val_vazaoafluente", var_id: 22 },
                { field_name: "val_vazaovertida", var_id: 24 },
                { field_name: "val_vazaodefluente", var_id: 23 }
            ],
            series_map: [],
            unit_map: [
                {
                    var_id: 26,
                    unit_id: 10
                },
                {
                    var_id: 22,
                    unit_id: 10
                },
                {
                    var_id: 24,
                    unit_id: 10
                },
                {
                    var_id: 23,
                    unit_id: 10
                }
            ],
            tabla: "dados_ons",
            pais: "Brasil",
            propietario: "ONS",
            proc_id: 1
        };
        _this.var_map = [];
        _this.unit_map = [];
        _this.setConfig(config);
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
                            // return rows.map(r => r as DadosHidrologicosRecord)
                        ];
                    case 2:
                        rows = _a.sent();
                        // console.log(rows);
                        // return rows.map(r => r as DadosHidrologicosRecord)
                        return [2 /*return*/, rows];
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
        for (var _i = 0, _a = ["series_id", "estacion_id", "var_id", "id_externo"]; _i < _a.length; _i++) {
            var key = _a[_i];
            if (filter_[key] != undefined) {
                if (!Array.isArray(filter_[key])) {
                    filter_[key] = [filter_[key]];
                }
            }
            else {
                filter_[key] = [];
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
                        records = (_c.sent()).map(function (r) { return r; });
                        for (_i = 0, records_1 = records; _i < records_1.length; _i++) {
                            record = records_1[_i];
                            record = record;
                            if (record.din_instante < filter_.timestart || record.din_instante > filter_.timeend) {
                                continue;
                            }
                            estacion_id = this.getEstacionId(record.id_reservatorio);
                            if (!estacion_id) {
                                console.warn("estacion_id not found for id_reservatorio '".concat(record.id_reservatorio));
                            }
                            else {
                                if (filter_.estacion_id.length && filter_.estacion_id.indexOf(estacion_id) < 0) {
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
    Client.prototype.parseReservatorioRecord = function (record, estacion_id, url) {
        return {
            id: estacion_id,
            nombre: record.nom_reservatorio,
            id_externo: record.id_reservatorio,
            tabla: this.config.tabla,
            geom: {
                type: "Point",
                coordinates: [
                    record.val_longitude,
                    record.val_latitude
                ]
            },
            pais: this.config.pais,
            rio: record.nom_rio,
            has_obs: true,
            tipo: "E",
            automatica: false,
            habilitar: true,
            propietario: this.config.propietario,
            abreviatura: record.id_reservatorio,
            URL: url,
            real: true,
            public: true
        };
    };
    Client.prototype.getSites = function (filter) {
        return __awaiter(this, void 0, void 0, function () {
            var filter_, estaciones, url, records, _i, records_2, record, estacion_id;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.loadSitesMap()];
                    case 1:
                        _a.sent();
                        filter_ = Client.setFilterValuesToArray(filter);
                        estaciones = [];
                        url = "".concat(this.config.url).concat(this.config.sites_file);
                        return [4 /*yield*/, (0, accessor_utils_1.fetch)(url, undefined, this.config.sites_output_file, function () { return null; })];
                    case 2:
                        _a.sent();
                        return [4 /*yield*/, Client.readParquetFile(this.config.sites_output_file)];
                    case 3:
                        records = (_a.sent()).map(function (r) { return r; });
                        if (filter_.id_externo.length) {
                            records = records.filter(function (r) { return filter_.id_externo.indexOf(r.id_reservatorio) >= 0; });
                        }
                        for (_i = 0, records_2 = records; _i < records_2.length; _i++) {
                            record = records_2[_i];
                            estacion_id = this.getEstacionId(record.id_reservatorio);
                            if (filter_.estacion_id.length) {
                                if (!estacion_id) {
                                    continue;
                                }
                                if (filter_.estacion_id.indexOf(estacion_id) < 0) {
                                    continue;
                                }
                            }
                            estaciones.push(this.parseReservatorioRecord(record, estacion_id, url));
                        }
                        return [2 /*return*/, estaciones];
                }
            });
        });
    };
    Client.prototype.getUnidades = function (var_id) {
        for (var _i = 0, _a = this.unit_map; _i < _a.length; _i++) {
            var unit = _a[_i];
            if (unit.var_id == var_id) {
                return unit.unidades;
            }
        }
        throw (new Error("Unidades for var_id=" + var_id + " not found"));
    };
    Client.prototype.loadSitesMap = function () {
        return __awaiter(this, void 0, void 0, function () {
            var estaciones;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, CRUD_1.estacion.read({
                            tabla: this.config.tabla
                        })];
                    case 1:
                        estaciones = _a.sent();
                        this.config.sites_map = estaciones.map(function (estacion) {
                            return {
                                estacion_id: estacion.id,
                                id_reservatorio: estacion.id_externo,
                                estacion: estacion
                            };
                        });
                        return [2 /*return*/];
                }
            });
        });
    };
    /** Loads variables defined in config.var_map from database and sets this.var_map */
    Client.prototype.loadVarMap = function () {
        return __awaiter(this, void 0, void 0, function () {
            var variables, _i, _a, mapped_var, i;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0: return [4 /*yield*/, CRUD_1["var"].read({
                            id: this.config.var_map.map(function (v) { return v.var_id; })
                        })];
                    case 1:
                        variables = _b.sent();
                        for (_i = 0, _a = this.config.var_map; _i < _a.length; _i++) {
                            mapped_var = _a[_i];
                            i = variables.map(function (v) { return v.id; }).indexOf(mapped_var.var_id);
                            if (i < 0) {
                                throw (new Error("Variable with id=" + mapped_var.var_id + " not found in database"));
                            }
                            this.var_map.push(__assign({ variable: variables[i] }, mapped_var));
                        }
                        return [2 /*return*/];
                }
            });
        });
    };
    Client.prototype.loadProc = function () {
        return __awaiter(this, void 0, void 0, function () {
            var proc;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, CRUD_1.procedimiento.read({
                            id: this.config.proc_id
                        })];
                    case 1:
                        proc = _a.sent();
                        if (!proc) {
                            throw (new Error("Procedimiento with id=" + this.config.proc_id + " not found in database"));
                        }
                        this.procedimiento = proc;
                        return [2 /*return*/];
                }
            });
        });
    };
    /** Loads units defined in config.unit_map from database and sets this.unit_map */
    Client.prototype.loadUnitMap = function () {
        return __awaiter(this, void 0, void 0, function () {
            var units, _i, _a, mapped_unit, i;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0: return [4 /*yield*/, CRUD_1.unidades.read({
                            id: this.config.unit_map.map(function (u) { return u.unit_id; })
                        })];
                    case 1:
                        units = _b.sent();
                        for (_i = 0, _a = this.config.unit_map; _i < _a.length; _i++) {
                            mapped_unit = _a[_i];
                            i = units.map(function (u) { return u.id; }).indexOf(mapped_unit.unit_id);
                            if (i < 0) {
                                throw (new Error("Unidades with id=" + mapped_unit.unit_id + " not found in database"));
                            }
                            this.unit_map.push(__assign({ unidades: units[i] }, mapped_unit));
                        }
                        return [2 /*return*/];
                }
            });
        });
    };
    Client.prototype.getSeries = function (filter) {
        if (filter === void 0) { filter = {}; }
        return __awaiter(this, void 0, void 0, function () {
            var filter_, estaciones, series, _i, estaciones_1, estacion, _a, _b, variable;
            return __generator(this, function (_c) {
                switch (_c.label) {
                    case 0: return [4 /*yield*/, this.loadSitesMap()];
                    case 1:
                        _c.sent();
                        return [4 /*yield*/, this.loadVarMap()];
                    case 2:
                        _c.sent();
                        return [4 /*yield*/, this.loadProc()];
                    case 3:
                        _c.sent();
                        return [4 /*yield*/, this.loadUnitMap()];
                    case 4:
                        _c.sent();
                        filter_ = Client.setFilterValuesToArray(filter);
                        return [4 /*yield*/, this.getSites({
                                estacion_id: filter_.estacion_id,
                                id_externo: filter_.id_externo
                            })];
                    case 5:
                        estaciones = _c.sent();
                        series = [];
                        for (_i = 0, estaciones_1 = estaciones; _i < estaciones_1.length; _i++) {
                            estacion = estaciones_1[_i];
                            for (_a = 0, _b = this.var_map; _a < _b.length; _a++) {
                                variable = _b[_a];
                                if (filter_.var_id.length && filter_.var_id.indexOf(variable.var_id) < 0) {
                                    continue;
                                }
                                series.push({
                                    tipo: "puntual",
                                    estacion: estacion,
                                    "var": variable.variable,
                                    procedimiento: this.procedimiento,
                                    unidades: this.getUnidades(variable.var_id)
                                });
                            }
                        }
                        return [2 /*return*/, series];
                }
            });
        });
    };
    Client._get_is_multiseries = true;
    return Client;
}(abstract_accessor_engine_1.AbstractAccessorEngine));
exports.Client = Client;
