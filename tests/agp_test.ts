import test from 'node:test'
import assert from 'assert'
process.env.NODE_ENV = "test"
import { Client } from "../app/accessors/agp"
import {new as Accessor} from "../app/accessors"

test('agp accessor engine.get w series_id', async(t) => {
    const client = await Accessor("agp")
    
    const series = await client.engine.getSeries({var_id:2, proc_id:1, unit_id:11})
    assert(series.length >= 58)
    console.debug(`got ${series.length} series`)
    console.debug(`mapped ${Object.keys(client.engine.series_map).length} series`)
    assert(Object.keys(client.engine.series_map).length >= 58)

    const ts = new Date(2024,4,16) // 2024-05-16 17:40:00
    const te = new Date(2026,4,27)
    const observaciones = await client.engine.get({
        series_id: 44046, // RECONQUISTA
        timestart: ts,
        timeend: te
    })
    assert(observaciones.length > 0)
    console.debug("got " + observaciones.length + " observaciones")

    for(const o of observaciones) {
        assert(o.timestart.getTime() >= ts.getTime())
        assert(o.timestart.getTime() <= te.getTime())
        assert(parseFloat(o.valor).toString() != "NaN")
    }
})

test('agp accessor client.getSeries w estacion_id arr', async(t) => {
    const ts = new Date(2024,4,16) // 2024-05-16 17:40:00
    const te = new Date(2026,4,27)

    const client = await Accessor("agp")

    const series_d = await client.getSeries({
        timestart: ts,
        timeend: te,
        estacion_id: [8354, 8381]

    })
    assert.equal(series_d.length,2)
    for(const serie of series_d) {
        console.debug("got serie " + serie.id + " with " + serie.observaciones.length + " observaciones")
        assert.equal(serie.var.id,2)
        for(const o of serie.observaciones) {
            assert.equal(o.series_id, serie.id)
            assert(o.timestart.getTime() >= ts.getTime())
            assert(o.timestart.getTime() <= te.getTime())
        }
    }
})
