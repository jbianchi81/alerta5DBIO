import test from 'node:test'
import assert from 'assert'
process.env.NODE_ENV = "test"
import { Client } from "../app/accessors/pilcomayo"
import {new as Accessor} from "../app/accessors"

test('pilcomayo accessor sequence', async(t) => {
    const client = await Accessor("pilcomayo")
    
    const estaciones = await client.getSites()
    assert(estaciones.length > 0)
    console.log(`got ${estaciones.length} estaciones`)
    for(const estacion of estaciones) {
        assert.notEqual(parseInt(estacion.id_externo).toString(),"NaN")
        assert.equal(typeof estacion.nombre, "string")
    }

    const series = await client.engine.getSeries()
    assert(series.length > 0)
    console.log(`got ${series.length} series`)
    console.log(`mapped ${Object.keys(client.engine.series_map).length} series`)
    assert(Object.keys(client.engine.series_map).length >= 6)

    const ts = new Date()
    ts.setHours(0,0,0,0)
    const te = new Date()
    const observaciones = await client.engine.get({
        series_id: 42293,
        timestart: ts,
        timeend: te
    })
    assert(observaciones.length > 0)
    // same date
    for(const o of observaciones) {
        assert.equal(o.timestart.getFullYear(),ts.getFullYear())
        assert.equal(o.timestart.getMonth(),ts.getMonth())
        assert.equal(o.timestart.getDate(),ts.getDate())
        assert.notEqual(o.valor.toString(),"NaN")
        assert.equal(o.series_id, 42293)
        assert(o.timestart.getTime() >= ts.getTime())
        assert(o.timestart.getTime() <= te.getTime())
    }

    const series_d = await client.getSeries({
        timestart: ts,
        timeend: te,
        var_id: 2
    })
    assert.notEqual(series_d.length,0)
    for(const serie of series_d) {
        console.log("got serie " + serie.id + " with " + serie.observaciones.length + " observaciones")
        assert.equal(serie.var.id,2)
        for(const o of serie.observaciones) {
            assert.equal(o.timestart.getFullYear(),ts.getFullYear())
            assert.equal(o.timestart.getMonth(),ts.getMonth())
            assert.equal(o.timestart.getDate(),ts.getDate())
            assert.notEqual(o.valor.toString(),"NaN")
            assert.equal(o.series_id, serie.id)
            assert(o.timestart.getTime() >= ts.getTime())
            assert(o.timestart.getTime() <= te.getTime())
        }
    }


    // get last month

    const ts_m = new Date()
    ts_m.setDate(1)
    ts_m.setHours(0,0,0,0)

    const series_m = await client.getSeries({
        timestart: ts_m,
        timeend: te,
        var_id: 27
    })
    assert.notEqual(series_m.length,0)
    for(const serie of series_m) {
        console.log("got serie " + serie.id + " with " + serie.observaciones.length + " observaciones")
        assert.equal(serie.var.id,27)
        for(const o of serie.observaciones) {
            assert.equal(o.timestart.getFullYear(),ts_m.getFullYear())
            assert.equal(o.timestart.getMonth(),ts_m.getMonth())
            assert.notEqual(o.valor.toString(),"NaN")
            assert.equal(o.series_id, serie.id)
            assert(o.timestart.getTime() >= ts_m.getTime())
            assert(o.timestart.getTime() <= te.getTime())
        }
    }
})
