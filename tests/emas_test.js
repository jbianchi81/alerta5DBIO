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
const node_test_1 = __importDefault(require("node:test"));
const assert_1 = __importDefault(require("assert"));
process.env.NODE_ENV = "test";
const accessors_1 = require("../app/accessors");
const CRUD_1 = require("../app/CRUD");
(0, node_test_1.default)('emas accessor sequence', (t) => __awaiter(void 0, void 0, void 0, function* () {
    const client = new accessors_1.Accessor({ class: "emas", name: "emas" });
    assert_1.default.ok("engine" in client);
    assert_1.default.ok("config" in client.engine);
    assert_1.default.ok("url" in client.engine.config);
    assert_1.default.ok("variable_lists" in client.engine.config);
    assert_1.default.ok("variable_map" in client.engine.config);
    assert_1.default.ok("station_map" in client.engine.config);
    // get all data from station
    const data = yield client.engine.getData(928);
    assert_1.default.ok("station_id" in data);
    assert_1.default.ok("rows" in data);
    assert_1.default.ok(Array.isArray(data.rows));
    assert_1.default.ok(data.rows.length >= 10);
    assert_1.default.ok(928 in client.engine.config.station_map);
    assert_1.default.ok("variable_list_key" in client.engine.config.station_map[928]);
    const variable_list_key = client.engine.config.station_map[928].variable_list_key;
    assert_1.default.ok(variable_list_key in client.engine.config.variable_lists);
    const VARIABLES = client.engine.config.variable_lists[variable_list_key];
    assert_1.default.ok(Array.isArray(VARIABLES));
    for (var i = 0; i < 10; i++) {
        const row = data.rows[i];
        assert_1.default.ok("date_time" in row);
        assert_1.default.ok("values" in row);
        const keys = Object.keys(row.values);
        assert_1.default.ok(keys.length);
        for (const key of keys) {
            assert_1.default.ok(VARIABLES.indexOf(key) >= 0, `key ${key} missing from variable list: ${VARIABLES}`);
        }
    }
    // get sites
    const estaciones = yield client.getSites();
    assert_1.default.ok(Array.isArray(estaciones));
    assert_1.default.ok(estaciones.length >= 4);
    for (var i = 0; i < 4; i++) {
        const estacion = estaciones[i];
        assert_1.default.ok("nombre" in estacion);
        assert_1.default.ok("geom" in estacion);
        assert_1.default.ok("coordinates" in estacion.geom);
        assert_1.default.ok(Array.isArray(estacion.geom.coordinates));
        assert_1.default.ok(estacion.geom.coordinates.length >= 2);
        assert_1.default.ok(Number.isFinite(estacion.geom.coordinates[0]));
        assert_1.default.ok(Number.isFinite(estacion.geom.coordinates[1]));
    }
    // get metadata
    const series = yield client.getMetadata();
    assert_1.default.ok(Array.isArray(series));
    assert_1.default.ok(series.length >= 24);
    for (var i = 0; i < 24; i++) {
        const serie = series[i];
        assert_1.default.ok("tipo" in serie);
        assert_1.default.equal(serie.tipo, "puntual");
        assert_1.default.ok("estacion" in serie);
        assert_1.default.ok("id" in serie.estacion);
        assert_1.default.ok("var" in serie);
        assert_1.default.ok("id" in serie.var);
        assert_1.default.ok("procedimiento" in serie);
        assert_1.default.ok("id" in serie.procedimiento);
        assert_1.default.ok("unidades" in serie);
        assert_1.default.ok("id" in serie.unidades);
    }
    // update metadata
    // stations
    const stations_upd = yield client.updateSites();
    assert_1.default.ok(Array.isArray(stations_upd));
    assert_1.default.ok(stations_upd.length >= 4);
    for (var i = 0; i < 4; i++) {
        const estacion = stations_upd[i];
        assert_1.default.ok("id" in estacion);
        assert_1.default.ok(Number.isFinite(estacion.id));
        assert_1.default.ok("nombre" in estacion);
        assert_1.default.ok("geom" in estacion);
        assert_1.default.ok("coordinates" in estacion.geom);
        assert_1.default.ok(Array.isArray(estacion.geom.coordinates));
        assert_1.default.ok(estacion.geom.coordinates.length >= 2);
        assert_1.default.ok(Number.isFinite(estacion.geom.coordinates[0]));
        assert_1.default.ok(Number.isFinite(estacion.geom.coordinates[1]));
    }
    const st_ids = new Set(stations_upd.map((s) => s.id));
    assert_1.default.equal(st_ids.size, stations_upd.length);
    // series
    const series_upd = yield client.updateMetadata();
    for (var i = 0; i < 24; i++) {
        const serie = series_upd[i];
        assert_1.default.ok("tipo" in serie);
        assert_1.default.equal(serie.tipo, "puntual");
        assert_1.default.ok("id" in serie);
        assert_1.default.ok(Number.isFinite(serie.id));
    }
    const ids = new Set(series_upd.map((s) => s.id));
    assert_1.default.equal(ids.size, series_upd.length);
    // get between dates (1-day period)
    const timestart = new Date(new Date().getTime() - 86400 * 1000);
    const timeend = new Date();
    const series_ = yield client.getSeries({ estacion_id: 928, timestart: timestart, timeend: timeend });
    assert_1.default.ok(Array.isArray(series_));
    assert_1.default.ok(series_.length >= 4);
    let obs_count = 0;
    for (var i = 0; i < 4; i++) {
        const serie = series_[i];
        assert_1.default.ok("estacion_id" in serie);
        assert_1.default.equal(serie.estacion_id, 928);
        assert_1.default.ok("var_id" in serie);
        assert_1.default.ok("observaciones" in serie);
        obs_count = obs_count + serie.observaciones.length;
        for (var j = 0; j < Math.min(100, serie.observaciones.length); j++) {
            const obs = serie.observaciones[j];
            assert_1.default.ok("timestart" in obs);
            const ts = new Date(obs.timestart);
            assert_1.default.ok(ts.toString() != 'NaN');
            assert_1.default.ok(ts.getTime() >= timestart.getTime());
            assert_1.default.ok("timeend" in obs);
            const te = new Date(obs.timestart);
            assert_1.default.ok(te.toString() != 'NaN');
            assert_1.default.ok(te.getTime() <= timeend.getTime());
            assert_1.default.ok("valor" in obs);
            assert_1.default.ok(obs["valor"] !== undefined && obs["valor"] !== null);
        }
        assert_1.default.ok(obs_count > 100);
    }
    // create accessor
    const emas_accessor = yield client.create();
    assert_1.default.ok("name" in emas_accessor);
    assert_1.default.equal(emas_accessor.name, "emas");
    assert_1.default.ok("class" in emas_accessor);
    assert_1.default.equal(emas_accessor.class, "emas");
    assert_1.default.ok("config" in emas_accessor);
    assert_1.default.ok("url" in emas_accessor.config);
    assert_1.default.ok("variable_lists" in emas_accessor.config);
    assert_1.default.ok("variable_map" in emas_accessor.config);
    assert_1.default.ok("station_map" in emas_accessor.config);
    // instantiate accessor
    const accessor_instance = yield (0, accessors_1.new)("emas");
    assert_1.default.ok("name" in accessor_instance);
    assert_1.default.equal(accessor_instance.name, "emas");
    assert_1.default.ok("clase" in accessor_instance);
    assert_1.default.equal(accessor_instance.clase, "emas");
    assert_1.default.ok("config" in accessor_instance);
    assert_1.default.ok("url" in accessor_instance.config);
    assert_1.default.ok("variable_lists" in accessor_instance.config);
    assert_1.default.ok("variable_map" in accessor_instance.config);
    assert_1.default.ok("station_map" in accessor_instance.config);
    assert_1.default.ok("engine" in accessor_instance);
    // delete accessor
    const accessor_deleted = yield client.delete();
    assert_1.default.ok("name" in accessor_deleted);
    assert_1.default.equal(accessor_deleted.name, "emas");
    assert_1.default.ok("class" in accessor_deleted);
    assert_1.default.equal(accessor_deleted.class, "emas");
    assert_1.default.ok("config" in accessor_deleted);
    assert_1.default.ok("url" in accessor_deleted.config);
    assert_1.default.ok("variable_lists" in accessor_deleted.config);
    assert_1.default.ok("variable_map" in accessor_deleted.config);
    assert_1.default.ok("station_map" in accessor_deleted.config);
    // delete series
    const series_del = yield CRUD_1.serie.delete({ tipo: "puntual", id: [...ids] });
    assert_1.default.equal(series_del.length, ids.size);
    // delete stations
    const stations_del = yield CRUD_1.estacion.delete({ id: stations_upd.map(s => s.id) });
    assert_1.default.equal(stations_del.length, stations_upd.length);
}));
