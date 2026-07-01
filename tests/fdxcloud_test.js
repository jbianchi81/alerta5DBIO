const test = require('node:test')
const assert = require('assert')
const {serie: Serie, observacion: Observacion, observaciones: Observaciones, estacion: Estacion} = require('../app/CRUD')
const axios = require('axios')
const {Client} = require("../app/accessors/fdxcloud")
const accessors = require("../app/accessors")

test('fdxcloud accessor get timeseries', async(t) => {

    await t.test("get timeseries hidro", async (t) => {
        const timeend = new Date()
        const timestart = new Date(timeend.getTime() - 2 * 24 * 3600 * 1000)
        const accessor = await accessors.new("fdxcloud_quilmes")
        try {
            var timeseries = await accessor.engine.getTimeseries(
                48,
                timestart.toISOString().substring(0,19).replace("T"," "),
                timeend.toISOString().substring(0,19).replace("T"," "))
        } catch (e) {
            console.error(e)
            var timeseries = undefined
        }
        assert(Array.isArray(timeseries))
    })
})


test('fdxcloud accessor get measures', async(t) => {
    await t.test("get measures with pagination", async(t) => {
        const timeend = new Date()
        const timestart = new Date(timeend.getTime() - 2 * 24 * 3600 * 1000)
        const accessor = await accessors.new("fdxcloud_quilmes")
        try {
            var measures = await accessor.engine.getMeasuresWithPagination(
                49,
                undefined,
                timestart.toISOString().substring(0,10),
                timeend.toISOString().substring(0,10))
        } catch (e) {
            console.error(e)
            var measures = undefined
        }
        assert(Array.isArray(measures))
    })
})

test('fdxcloud accessor get', async(t) => {
    await t.test("get timeseries meteo", async (t) => {
        const timeend = new Date()
        const timestart = new Date(timeend.getTime() - 2 * 24 * 3600 * 1000)
        const accessor = await accessors.new("fdxcloud_quilmes")
        try {
            var series = await accessor.getSeries(
                {
                    estacion_id: 8413,
                    timestart: timestart,
                    timeend: timeend
                })
        } catch (e) {
            console.error(e)
            var series = undefined
        }
        assert(Array.isArray(series))
        assert.equal(series.length, 1)
        assert.equal(series[0].estacion_id, 8413)
        assert.equal(series[0].id, 44198)
        assert(Array.isArray(series[0].observaciones))
        assert(series[0].observaciones.length > 0)
    })
})