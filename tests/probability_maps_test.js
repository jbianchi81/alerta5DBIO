const test = require('node:test')
const assert = require('assert')
// process.env.NODE_ENV = "test"
const {CRUD: CRUD} = require('../app/CRUD')
const fs = require('fs')

test('make probability maps test', async(t) => {

    // await t.test("make probability maps", async (t) => {
    //     await CRUD.makeProbabilityMaps(
    //         "/tmp/a5dbio-prob/sum-*.GTiff",
    //         "/tmp/a5dbio-prob/geotiff",
    //         "/tmp/a5dbio-prob/png",
    //         undefined,
    //         undefined,
    //         undefined,
    //         true
    //     )
    // })

    await t.test("get ensemble probability", async (t) => {
        const ts = new Date()
        ts.setUTCHours(12)
        ts.setMinutes(0)
        ts.setSeconds(0)
        ts.setMilliseconds(0)
        const te = new Date()
        te.setUTCDate(te.getUTCDate()+5)
        te.setUTCHours(12)
        te.setMinutes(0)
        te.setSeconds(0)
        te.setMilliseconds(0)
        const serie_sim = await CRUD.getEnsembleProbabilities(
            55,
            ts,
            te,
            {funcion: "sum"},
            undefined,
            undefined,
            721,
            undefined,
            undefined,
            ["gep01","gep02","gep03","gep04","gep05"],
            undefined,
            "data/gefs_atmos/gdal",
            "data/gefs_atmos/png",
            undefined,
            undefined,
            undefined,
            true,
            undefined,
            99999,
            "GEFS"
        )
        assert.equal(serie_sim.series_id,99999)
        assert.equal(serie_sim.pronosticos.length,3)
        for(const p of serie_sim.pronosticos) {
            assert.equal(p.timestart.getTime(), ts.getTime())
            assert.equal(p.timeend.getTime(), te.getTime())
        }
        const gdal_files = fs.readdirSync("data/gefs_atmos/gdal")
        assert.equal(gdal_files.length,3)
        const png_files = fs.readdirSync("data/gefs_atmos/png")
        assert.equal(png_files.length,3)
    })
})
