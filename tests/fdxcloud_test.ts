import test from 'node:test'
import assert from 'assert'
process.env.NODE_ENV = "test"
import { Client, Measurement, MeasuresResponse } from "../app/accessors/fdxcloud"
import {new as Accessor} from "../app/accessors"
import { serie as CrudSerie } from "../app/CRUD"

// test('fdxcloud accessor getMeasuresLatest', async(t) => {
//     const client = await Accessor("fdxcloud")
    
//     const measure : Measurement = await client.engine.getMeasuresLatest(
//         10
//     )
//     assert("device" in measure)
//     assert("id" in measure.device)
//     console.debug(`device.id: ${measure.device.id}`)
//     assert("measurementPoint" in measure)
//     assert("id" in measure.measurementPoint)
//     assert.equal(measure.measurementPoint.id, 10)
//     assert("measureDate" in measure)
//     console.debug(`measureDate: ${measure.measureDate}`)
//     assert("interpretedValue" in measure)
//     console.debug(`interpretedValue: ${measure.interpretedValue}`)
// })

// test('fdxcloud accessor getMeasures', async(t) => {
//     const client = await Accessor("fdxcloud")
    
//     const measures_response : MeasuresResponse = await client.engine.getMeasures(
//         10,
//         undefined,
//         new Date(2026,5,1).toISOString().substring(0,10),
//         new Date(2026,5,3).toISOString().substring(0,10),
//         100
//     )
//     assert("rows" in measures_response)
//     assert(Array.isArray(measures_response.rows))
//     console.debug(`measures_response.rows.length = ${measures_response.rows.length}`)
//     assert.equal(measures_response.rows.length, 100)
//     for(const measure of measures_response.rows) {
//         assert("device" in measure)
//         assert("id" in measure.device)
//         console.debug(`device.id: ${measure.device.id}`)
//         assert("measurementPoint" in measure)
//         assert("id" in measure.measurementPoint)
//         assert.equal(measure.measurementPoint.id, 10)
//         assert("measureDate" in measure)
//         console.debug(`measureDate: ${measure.measureDate}`)
//         assert("interpretedValue" in measure)
//         console.debug(`interpretedValue: ${measure.interpretedValue}`)
//     }
// })

test('series mapping', async(t) => {
    const client = await Accessor("fdxcloud")  
    const series = await client.engine.getSeries()
    assert(series.length >= 19)
    for(const serie of series) {
        assert.equal(serie.estacion.tabla, "ina_delta")
        assert.equal(serie.var.id, 2)
        assert.equal(serie.procedimiento.id, 1)
        assert.equal(serie.unidades.id, 11)
        // series_map
        assert(serie.estacion.id in client.engine.series_map)
    }
})

// test('getMeasuresWithPagination', async(t) => {
//     const client = await Accessor("fdxcloud")  
//     const measurements = await client.engine.getMeasuresWithPagination(
//         10,
//         undefined,
//         new Date(2026,5,1).toISOString().substring(0,10),
//         new Date(2026,5,3).toISOString().substring(0,10),
//         100
//     )
//     assert(Array.isArray(measurements))
//     console.debug(`measurements.length = ${measurements.length}`)
//     assert(measurements.length > 100)
//     for(const measure of measurements) {
//         assert("device" in measure)
//         assert("id" in measure.device)
//         assert("measurementPoint" in measure)
//         assert("id" in measure.measurementPoint)
//         assert.equal(measure.measurementPoint.id, 10)
//         assert("measureDate" in measure)
//         assert("interpretedValue" in measure)
//     }
// })

test('get obs', async(t) => {
    const client = await Accessor("fdxcloud")  
    const obs = await client.engine.get(
        {
            estacion_id: 8173,
            timestart: new Date(2026,5,1),
            timeend: new Date(2026,5,2)
        }
    )
    console.debug(`series_id: ${client.engine.series_map[8173].series_id}`)
    assert(Array.isArray(obs))
    console.debug(`obs.length = ${obs.length}`)
    assert.equal(obs.length, 24)
    for(const o of obs) {
        assert("timestart" in o)
        assert(o.timestart >= new Date(new Date(2026,5,1).getTime()))
        assert("timeend" in o)
        assert(o.timeend <= new Date(2026,5,3))
        assert("series_id" in o)
        assert.equal(o.series_id, client.engine.series_map[8173].series_id)
        assert("valor" in o)
        assert(parseFloat(o.valor).toString() != "NaN")
    }
})

test('client.getSeries', async(t) => {
    const client = await Accessor("fdxcloud")  
    const series = await client.getSeries({
        estacion_id: 8173,
        timestart: new Date(2026,5,1),
        timeend: new Date(2026,5,2)
    })
    assert.equal(series.length,1)
    assert.equal(series[0].id, client.engine.series_map[8173].series_id)
    assert.equal(series[0].observaciones.length, 24)
    for(const o of series[0].observaciones) {
        assert("timestart" in o)
        assert(o.timestart >= new Date(new Date(2026,5,1).getTime()))
        assert("timeend" in o)
        assert(o.timeend <= new Date(2026,5,3))
        assert("series_id" in o)
        assert.equal(o.series_id, client.engine.series_map[8173].series_id)
        assert("valor" in o)
        assert(parseFloat(o.valor).toString() != "NaN")
    }
})

test('client.updateSeries', async(t) => {
    const client = await Accessor("fdxcloud")  
    const series = await client.updateSeries({
        estacion_id: 8173,
        timestart: new Date(2026,5,1),
        timeend: new Date(2026,5,2)
    })
    assert.equal(series.length,1)
    assert.equal(series[0].id, client.engine.series_map[8173].series_id)
    assert.equal(series[0].observaciones.length,24)
})