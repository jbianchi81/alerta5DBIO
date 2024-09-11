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
exports.Client = void 0;
const abstract_accessor_engine_1 = require("./abstract_accessor_engine");
// import { Database, RowData } from "duckdb-lambda-x86" 
const duckdb_async_1 = require("../duckdb_async");
const accessor_utils_1 = require("../accessor_utils");
const CRUD_1 = require("../CRUD");
class Client extends abstract_accessor_engine_1.AbstractAccessorEngine {
    constructor(config) {
        super(config);
        this.default_config = {
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
                { field_name: "val_vazaodefluente", var_id: 23 },
                { field_name: "val_vazaotransferida", var_id: 25 },
                { field_name: "val_vazaoturbinada", var_id: 92 },
                { field_name: "val_nivelmontante", var_id: 93 },
                { field_name: "val_niveljusante", var_id: 94 }
            ],
            series_map: [],
            unit_map: [
                {
                    var_id: 26,
                    unit_id: 15
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
                },
                {
                    var_id: 25,
                    unit_id: 10
                },
                {
                    var_id: 92,
                    unit_id: 10
                },
                {
                    var_id: 93,
                    unit_id: 11
                },
                {
                    var_id: 94,
                    unit_id: 11
                }
            ],
            tabla: "dados_ons",
            pais: "Brasil",
            propietario: "ONS",
            proc_id: 1
        };
        this.var_map = [];
        this.unit_map = [];
        this.sites_map = [];
        this.series_map = [];
        this.setConfig(config);
    }
    static readParquetFile(filename, limit = 1000000, offset = 0, output = undefined) {
        return __awaiter(this, void 0, void 0, function* () {
            // const db : Database = await Database.create(":memory:");
            // const rows : Array<RowData> = await db.all(`SELECT * FROM READ_PARQUET('${filename}') LIMIT ${limit} OFFSET ${offset}`)
            const rows = yield (0, duckdb_async_1.queryAsync)(`SELECT * FROM READ_PARQUET('${filename}') LIMIT ${limit} OFFSET ${offset}`);
            if (output) {
                // await db.all(`COPY (SELECT * FROM READ_PARQUET('${filename}') LIMIT ${limit} OFFSET ${offset}) TO '${output}' (HEADER, DELIMITER ',')`)
                yield (0, duckdb_async_1.queryAsync)(`COPY (SELECT * FROM READ_PARQUET('${filename}') LIMIT ${limit} OFFSET ${offset}) TO '${output}' (HEADER, DELIMITER ',')`);
            }
            return rows;
        });
    }
    static parseDadosHidrologicosRecord(record, field, series_id) {
        // assumes daily timestep
        var start_date = new Date(record.din_instante.getUTCFullYear(), record.din_instante.getUTCMonth(), record.din_instante.getUTCDate());
        var end_date = new Date(start_date);
        end_date.setUTCDate(end_date.getUTCDate() + 1);
        return {
            timestart: start_date,
            timeend: end_date,
            valor: record[field],
            series_id: series_id
        };
    }
    getEstacionId(id_reservatorio) {
        for (const estacion of this.sites_map) {
            if (estacion.id_reservatorio == id_reservatorio) {
                return estacion.estacion_id;
            }
        }
        return;
    }
    getSeriesId(estacion_id, var_id) {
        for (const serie of this.series_map) {
            if (serie.estacion_id == estacion_id && serie.var_id == var_id) {
                return serie.series_id;
            }
        }
        console.warn("Series id for estacion_id=" + estacion_id + ", var id=" + var_id + " not found in series_map. Please run updateSeries");
        return;
    }
    static setFilterValuesToArray(filter) {
        const filter_ = Object.assign({}, filter);
        for (const key of ["series_id", "estacion_id", "var_id", "id_externo"]) {
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
    }
    getSerieById(series_id) {
        for (const serie_map of this.series_map) {
            if (serie_map.series_id == series_id) {
                return serie_map.serie;
            }
        }
        throw (new Error("Series id " + series_id + " not found in series_map"));
    }
    get(filter, options = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!filter || !filter.timestart || !filter.timeend) {
                throw ("Missing timestart and/or timeend");
            }
            if (!this.sites_map.length) {
                yield this.loadSitesMap();
            }
            if (!this.series_map.length) {
                yield this.loadSeriesMap();
            }
            const filter_ = Client.setFilterValuesToArray(filter);
            const observaciones = [];
            for (var year = filter_.timestart.getUTCFullYear(); year <= filter_.timeend.getUTCFullYear(); year++) {
                const filepath = this.config.file_pattern.replace("%YYYY%", year.toString());
                const url = `${this.config.url}${filepath}`;
                yield (0, accessor_utils_1.fetch)(url, undefined, this.config.output_file, () => null);
                const records = (yield Client.readParquetFile(this.config.output_file)).map(r => r);
                for (var record of records) {
                    record = record;
                    const record_timestamp = new Date(record.din_instante.getUTCFullYear(), record.din_instante.getUTCMonth(), record.din_instante.getUTCDate());
                    if (record_timestamp < filter_.timestart || record_timestamp > filter_.timeend) {
                        continue;
                    }
                    // filter by id_externo
                    if (filter_.id_externo.length && filter_.id_externo.indexOf(record.id_reservatorio) < 0) {
                        continue;
                    }
                    const estacion_id = this.getEstacionId(record.id_reservatorio);
                    var series_id;
                    if (!estacion_id) {
                        // console.warn(`estacion_id not found for id_reservatorio ${record.id_reservatorio}`)
                        continue;
                    }
                    else {
                        // filter by estacion_id
                        if (filter_.estacion_id.length && filter_.estacion_id.indexOf(estacion_id) < 0) {
                            continue;
                        }
                        for (const variable of this.config.var_map) {
                            //filter by var_id
                            if (filter_.var_id.length && filter_.var_id.indexOf(variable.var_id) < 0) {
                                continue;
                            }
                            // filter out nulls
                            if (record[variable.field_name] == undefined || parseFloat(record[variable.field_name]).toString() == 'NaN') {
                                continue;
                            }
                            // foreach var, find series_id and push obs into array
                            series_id = this.getSeriesId(estacion_id, variable.var_id);
                            observaciones.push(Client.parseDadosHidrologicosRecord(record, variable.field_name, series_id));
                        }
                    }
                }
            }
            if (options.return_series) {
                // classify observaciones into series. Warning: observaciones with missing series_id will be ignored
                const series = {};
                for (const observacion of observaciones) {
                    if (!observacion.series_id) {
                        continue;
                    }
                    if (series[observacion.series_id]) {
                        series[observacion.series_id].observaciones.push(observacion);
                    }
                    else {
                        const serie = this.getSerieById(observacion.series_id);
                        serie.observaciones = [observacion];
                        series[observacion.series_id] = serie;
                    }
                }
                return Object.keys(series).map(series_id => {
                    return series[series_id];
                });
            }
            return observaciones;
        });
    }
    update(filter, options = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            const series = yield this.get(filter, Object.assign(Object.assign({}, options), { return_series: true }));
            const updated = [];
            for (var serie of series) {
                const c_serie = new CRUD_1.serie(serie);
                yield c_serie.createObservaciones();
                updated.push(c_serie);
            }
            return updated;
        });
    }
    parseReservatorioRecord(record, estacion_id, url) {
        if (!record.nom_reservatorio) {
            throw (new Error("Invalid site: missing name (nom_reservatorio"));
        }
        if (!record.id_reservatorio) {
            throw (new Error("Invalid site: missing id (id_reservatorio"));
        }
        if (!record.val_latitude || !record.val_longitude) {
            throw (new Error("Invalid site: missing latitude or longitude (val_latitude, val_longitude)"));
        }
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
    }
    getSites(filter) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.loadSitesMap();
            const filter_ = Client.setFilterValuesToArray(filter);
            var estaciones = [];
            const url = `${this.config.url}${this.config.sites_file}`;
            yield (0, accessor_utils_1.fetch)(url, undefined, this.config.sites_output_file, () => null);
            var records = (yield Client.readParquetFile(this.config.sites_output_file)).map(r => r);
            if (filter_.id_externo.length) {
                records = records.filter(r => filter_.id_externo.indexOf(r.id_reservatorio) >= 0);
            }
            for (const record of records) {
                var estacion_id = this.getEstacionId(record.id_reservatorio);
                if (filter_.estacion_id.length) {
                    if (!estacion_id) {
                        continue;
                    }
                    if (filter_.estacion_id.indexOf(estacion_id) < 0) {
                        continue;
                    }
                }
                try {
                    var estacion = this.parseReservatorioRecord(record, estacion_id, url);
                }
                catch (e) {
                    console.error("parseReservatorioRecord error: " + e.toString());
                    continue;
                }
                estaciones.push(estacion);
            }
            if (filter_.geom) {
                estaciones = (0, accessor_utils_1.filterSites)(estaciones, { geom: filter_.geom });
            }
            return estaciones.map(e => new CRUD_1.estacion(e));
        });
    }
    getUnidades(var_id) {
        for (var unit of this.unit_map) {
            if (unit.var_id == var_id) {
                return unit.unidades;
            }
        }
        throw (new Error("Unidades for var_id=" + var_id + " not found"));
    }
    loadSitesMap() {
        return __awaiter(this, void 0, void 0, function* () {
            const estaciones = yield CRUD_1.estacion.read({
                tabla: this.config.tabla
            });
            this.sites_map = estaciones.map(estacion => {
                return {
                    estacion_id: estacion.id,
                    id_reservatorio: estacion.id_externo,
                    estacion: estacion
                };
            });
        });
    }
    loadSeriesMap() {
        return __awaiter(this, void 0, void 0, function* () {
            const series = yield CRUD_1.serie.read({
                tabla_id: this.config.tabla
            });
            this.series_map = series.map(serie => {
                return {
                    series_id: serie.id,
                    estacion_id: serie.estacion.id,
                    var_id: serie.var.id,
                    serie: serie
                };
            });
        });
    }
    /** Loads variables defined in config.var_map from database and sets this.var_map */
    loadVarMap() {
        return __awaiter(this, void 0, void 0, function* () {
            const variables = yield CRUD_1.var.read({
                id: this.config.var_map.map(v => v.var_id)
            });
            for (var mapped_var of this.config.var_map) {
                const i = variables.map(v => v.id).indexOf(mapped_var.var_id);
                if (i < 0) {
                    throw (new Error("Variable with id=" + mapped_var.var_id + " not found in database"));
                }
                this.var_map.push(Object.assign({ variable: variables[i] }, mapped_var));
            }
        });
    }
    loadProc() {
        return __awaiter(this, void 0, void 0, function* () {
            const proc = yield CRUD_1.procedimiento.read({
                id: this.config.proc_id
            });
            if (!proc) {
                throw (new Error("Procedimiento with id=" + this.config.proc_id + " not found in database"));
            }
            this.procedimiento = proc;
        });
    }
    /** Loads units defined in config.unit_map from database and sets this.unit_map */
    loadUnitMap() {
        return __awaiter(this, void 0, void 0, function* () {
            const units = yield CRUD_1.unidades.read({
                id: this.config.unit_map.map(u => u.unit_id)
            });
            for (var mapped_unit of this.config.unit_map) {
                const i = units.map(u => u.id).indexOf(mapped_unit.unit_id);
                if (i < 0) {
                    throw (new Error("Unidades with id=" + mapped_unit.unit_id + " not found in database"));
                }
                this.unit_map.push(Object.assign({ unidades: units[i] }, mapped_unit));
            }
        });
    }
    getSeries(filter = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.loadSitesMap();
            yield this.loadVarMap();
            yield this.loadProc();
            yield this.loadUnitMap();
            const filter_ = Client.setFilterValuesToArray(filter);
            const estaciones = yield this.getSites({
                estacion_id: filter_.estacion_id,
                id_externo: filter_.id_externo
            });
            const series = [];
            for (var estacion of estaciones) {
                for (var variable of this.var_map) {
                    if (filter_.var_id.length && filter_.var_id.indexOf(variable.var_id) < 0) {
                        continue;
                    }
                    series.push({
                        tipo: "puntual",
                        estacion: estacion,
                        var: variable.variable,
                        procedimiento: this.procedimiento,
                        unidades: this.getUnidades(variable.var_id)
                    });
                }
            }
            return series;
        });
    }
}
Client._get_is_multiseries = true;
exports.Client = Client;
