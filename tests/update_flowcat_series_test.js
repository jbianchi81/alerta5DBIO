const test = require('node:test')
const assert = require('assert')
// process.env.NODE_ENV = "test"
// const {serie: Serie, observacion: Observacion, observaciones: Observaciones, estacion: Estacion} = require('../app/CRUD')
const {updateFlowcatSeries} = require('../app/update_flowcat_series')

test('update flowcat series', async(t) => {
    const timestart = new Date(1990,0,1)
    const timeend = new Date(2025,9,1)
    const series_flowcat = await updateFlowcatSeries(timestart, timeend)
    assert(series_flowcat.length > 0)
})