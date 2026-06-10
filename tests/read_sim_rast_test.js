const test = require('node:test')
const assert = require('assert')
// process.env.NODE_ENV = "test"
const {SerieTemporalSim, pronostico} = require('../app/CRUD')
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
    assert(fs.existsSync("/tmp/a5rast_16_676.tif_index.csv"))
    const index_csv = fs.readFileSync("/tmp/a5rast_16_676.tif_index.csv",{encoding: "utf-8"})
    const index = index_csv.split("\n").map(r=>new Date(r))
    assert.equal(index.length, 64)
    for(const d of index) {
        const date = new Date(d)
        assert(date.toString() != "Invalid Date")
    }
})


test('read upsert pronosticos areales', async(t) => {
    const pronosticos = await pronostico.readFile("/tmp/gfs_areal.json")
    assert.equal(pronosticos.length, 7680)
    for(const p of pronosticos) {
        assert(p instanceof pronostico)
    }
    const upserted = await pronostico.create(pronosticos, {cor_id: 954778, tipo: "areal"})
    assert.equal(upserted.length, 7680)
})

