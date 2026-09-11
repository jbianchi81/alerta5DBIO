import test from 'node:test'
import assert from 'assert'
process.env.NODE_ENV = "test"
import { Client } from "../app/accessors/gefs_atmos.js"
import accessor_pkg from "../app/accessors.js"
const Accessor = accessor_pkg.Accessor
// import { serie as CrudSerie } from "../app/CRUD"
import {readFile, writeFile} from 'fs/promises' 
import {parseUtcDateTime} from "../app/accessors/accessor_utils.js"

test('gefs atmos create serie', async(t) => {
    const client = new Client({})
    const serie = await client.createSerie()
    assert.equal(serie.id, 55)
    assert.equal(serie.estacion.id,25)
    assert.equal(serie.fuente.id, 55)
})


test('gefs atmos get', async(t) => {
    const client = new Client({ens: 2, end_hour: 24})
    const result = await client.get()
    assert.ok(Array.isArray(result))
    assert.ok(result.length)
})

test('gefs atmos getPronostico', async(t) => {
    const client = new Client({ens: 2, end_hour: 24})
    const corrida = await client.getPronostico()
    assert.ok("cal_id" in corrida)
    assert.equal(corrida.cal_id, 721)
    assert.ok(Array.isArray(corrida.series))
    assert.equal(corrida.series.length, 2)
    assert.ok(corrida.series.map(s => s.qualifier).indexOf("gep01") >= 0)
    assert.ok(corrida.series.map(s => s.qualifier).indexOf("gep02") >= 0)
    for(const s of corrida.series) {
        assert.equal(s.series_id, 55)
        assert.equal(s.series_table, "series_rast")
    }
})

test('gefs atmos prono update', async(t) => {
const client = new Client({ens: 2, end_hour: 24})
    const corrida = await client.updatePronostico()
    assert.ok("cal_id" in corrida)
    assert.equal(corrida.cal_id, 721)
    assert.ok(Array.isArray(corrida.series))
    assert.equal(corrida.series.length, 2)
    // assert.ok(corrida.series.map(s => s.qualifier).indexOf("gep01") >= 0)
    // assert.ok(corrida.series.map(s => s.qualifier).indexOf("gep02") >= 0)
    for(const s of corrida.series) {
        assert.equal(s.series_id, 55)
        assert.equal(s.series_table, "series_rast")
    }
    assert.ok(corrida.id)
})

test('gefs atmos prono update w/qualifiers', async(t) => {
    const client = new Client({end_hour: 24})
    const corrida = await client.getPronostico({qualifiers: ["gec00", "geavg"]})
    assert.ok("cal_id" in corrida)
    assert.equal(corrida.cal_id, 721)
    assert.ok(Array.isArray(corrida.series))
    assert.equal(corrida.series.length, 2)
    assert.ok(corrida.series.map(s => s.qualifier).indexOf("gec00") >= 0)
    assert.ok(corrida.series.map(s => s.qualifier).indexOf("geavg") >= 0)
    for(const s of corrida.series) {
        assert.equal(s.series_id, 55)
        assert.equal(s.series_table, "series_rast")
    }
    assert.ok(corrida.forecast_date instanceof Date)
    assert.equal(corrida.forecast_date.getMinutes(), 0)
    assert.equal(corrida.forecast_date.getSeconds(), 0)
    assert.equal(corrida.forecast_date.getMilliseconds(), 0)
})

test('gefs atmos prono update w accessor class', async(t) => {
const client = new Accessor({class: "gefs_atmos", config: {end_hour: 24}})
    const corrida = await client.updatePronostico({qualifiers: ["gec00", "geavg", "gep01", "gep02"]})
    assert.ok("cal_id" in corrida)
    assert.equal(corrida.cal_id, 721)
    assert.ok(Array.isArray(corrida.series))
    assert.equal(corrida.series.length, 4)
    // assert.ok(corrida.series.map(s => s.qualifier).indexOf("gep01") >= 0)
    // assert.ok(corrida.series.map(s => s.qualifier).indexOf("gep02") >= 0)
    for(const s of corrida.series) {
        assert.equal(s.series_id, 55)
        assert.equal(s.series_table, "series_rast")
    }
    assert.ok(corrida.id)
})
