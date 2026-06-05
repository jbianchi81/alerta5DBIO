const test = require('node:test')
const assert = require('assert')
// process.env.NODE_ENV = "test"
const {SerieTemporalSim} = require('../app/CRUD')
// const {CreateProcedure} = require('../app/crud_procedures')
const fs = require('fs')

test('read serie temporal sim rast', async(t) => {
    const series = await SerieTemporalSim.read(
        {
            tipo: "raster",
            series_id: 16,
            cal_id: 676,
            cor_id: 954778
        },{
            includeProno: true
        })
    assert.equal(series.length, 1)
    const serie = series[0]
    assert.equal(serie.pronosticos.length, 64)
    await serie.toRaster("/tmp/a5rast_16_676.tif")
    assert(fs.existsSync("/tmp/a5rast_16_676.tif"))
})
