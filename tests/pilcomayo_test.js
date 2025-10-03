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
(0, node_test_1.default)('pilcomayo accessor sequence', (t) => __awaiter(void 0, void 0, void 0, function* () {
    const client = yield (0, accessors_1.new)("pilcomayo");
    const estaciones = yield client.getSites();
    (0, assert_1.default)(estaciones.length > 0);
    console.log(`got ${estaciones.length} estaciones`);
    for (const estacion of estaciones) {
        assert_1.default.notEqual(parseInt(estacion.id_externo).toString(), "NaN");
        assert_1.default.equal(typeof estacion.nombre, "string");
    }
    const series = yield client.engine.getSeries();
    (0, assert_1.default)(series.length > 0);
    console.log(`got ${series.length} series`);
    console.log(`mapped ${Object.keys(client.engine.series_map).length} series`);
    (0, assert_1.default)(Object.keys(client.engine.series_map).length >= 6);
    const ts = new Date();
    ts.setHours(0, 0, 0, 0);
    const te = new Date();
    const observaciones = yield client.engine.get({
        series_id: 42293,
        timestart: ts,
        timeend: te
    });
    (0, assert_1.default)(observaciones.length > 0);
    // same date
    for (const o of observaciones) {
        assert_1.default.equal(o.timestart.getFullYear(), ts.getFullYear());
        assert_1.default.equal(o.timestart.getMonth(), ts.getMonth());
        assert_1.default.equal(o.timestart.getDate(), ts.getDate());
        assert_1.default.notEqual(o.valor.toString(), "NaN");
        assert_1.default.equal(o.series_id, 42293);
        (0, assert_1.default)(o.timestart.getTime() >= ts.getTime());
        (0, assert_1.default)(o.timestart.getTime() <= te.getTime());
    }
    const series_d = yield client.getSeries({
        timestart: ts,
        timeend: te,
        var_id: 2
    });
    assert_1.default.notEqual(series_d.length, 0);
    for (const serie of series_d) {
        console.log("got serie " + serie.id + " with " + serie.observaciones.length + " observaciones");
        assert_1.default.equal(serie.var.id, 2);
        for (const o of serie.observaciones) {
            assert_1.default.equal(o.timestart.getFullYear(), ts.getFullYear());
            assert_1.default.equal(o.timestart.getMonth(), ts.getMonth());
            assert_1.default.equal(o.timestart.getDate(), ts.getDate());
            assert_1.default.notEqual(o.valor.toString(), "NaN");
            assert_1.default.equal(o.series_id, serie.id);
            (0, assert_1.default)(o.timestart.getTime() >= ts.getTime());
            (0, assert_1.default)(o.timestart.getTime() <= te.getTime());
        }
    }
    // get last month
    const ts_m = new Date();
    ts_m.setDate(1);
    ts_m.setHours(0, 0, 0, 0);
    const series_m = yield client.getSeries({
        timestart: ts_m,
        timeend: te,
        var_id: 27
    });
    assert_1.default.notEqual(series_m.length, 0);
    for (const serie of series_m) {
        console.log("got serie " + serie.id + " with " + serie.observaciones.length + " observaciones");
        assert_1.default.equal(serie.var.id, 27);
        for (const o of serie.observaciones) {
            assert_1.default.equal(o.timestart.getFullYear(), ts_m.getFullYear());
            assert_1.default.equal(o.timestart.getMonth(), ts_m.getMonth());
            assert_1.default.notEqual(o.valor.toString(), "NaN");
            assert_1.default.equal(o.series_id, serie.id);
            (0, assert_1.default)(o.timestart.getTime() >= ts_m.getTime());
            (0, assert_1.default)(o.timestart.getTime() <= te.getTime());
        }
    }
}));
