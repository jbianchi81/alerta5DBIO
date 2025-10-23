const test = require('node:test')
const assert = require('assert')
// process.env.NODE_ENV = "test"
// const {serie: Serie, observacion: Observacion, observaciones: Observaciones, estacion: Estacion} = require('../app/CRUD')
const {Client, ncToPostgisRaster, parseDatesFromNc} = require('../app/accessors/thredds.js')
const {Pool} = require('pg')
const {fuente, serie, observacion}  = require('../app/CRUD.js')
const fs = require('fs')

test('create table', async(t) => {
    const client = new Client({
        url: "https://ds.nccs.nasa.gov/thredds",
        bbox: [-70, -10, -40, -40],
        var: "pr",
        horizStride: true
    })
    await client.createThreddsRastersTable() 
})

test('parse dates', async(t) => {
    const dates = await parseDatesFromNc("data/thredds/pr_day_ACCESS-CM2_historical_r1i1p1f1_gn_1950_v2.0.nc")
    assert.equal(dates.length, 365)
    assert.equal(dates[0].band,1)
    assert.equal(dates[0].date.toISOString(),new Date(Date.UTC(1950,0,1)).toISOString())
    assert.equal(dates[364].band,365)
    assert.equal(dates[364].date.toISOString(),new Date(Date.UTC(1950,11,31)).toISOString())
})

test('ncToPostgres', async(t) => {

    await ncToPostgisRaster(
        "data/thredds/pr_day_ACCESS-CM2_historical_r1i1p1f1_gn_1950_v2.0.nc",
        "public",
        "thredds_rasters",
        "rast",
        {"host": "localhost", "port": 5432, "dbname": "memeology", "user": "test", "password": "test"},
        4326,
        "filename"
    )
    const pool = new Pool({"host": "localhost", "port": 5432, "database": "memeology", "user": "test", "password": "test"})
    const result = await pool.query("SELECT filename, ST_NumBands(rast) AS numbands FROM thredds_rasters WHERE filename=$1",["pr_day_ACCESS-CM2_historical_r1i1p1f1_gn_1950_v2.0.nc"])
    assert.equal(result.rows.length, 1)
    assert.equal(result.rows[0].numbands, 365)
})

test("multibandToObservacionesRast", async(t) => {
    const client = new Client({
        url: "https://ds.nccs.nasa.gov/thredds",
        bbox: [-70, -10, -40, -40],
        var: "pr",
        horizStride: true
    })
    const f = JSON.parse(fs.readFileSync("data/thredds/fuente.json"))
    const fuente_thredds = await fuente.create(f)
    const s = JSON.parse(fs.readFileSync("data/thredds/serie_rast.json"))
    const serie_rast_thredds = await serie.create(s)
    const dates = await parseDatesFromNc("data/thredds/pr_day_ACCESS-CM2_historical_r1i1p1f1_gn_1950_v2.0.nc")
    const observaciones = await client.multibandToObservacionesRast(
        s.id,
        "pr_day_ACCESS-CM2_historical_r1i1p1f1_gn_1950_v2.0.nc",
        dates,
        "public",
        "thredds_rasters",
        "rast",
        "filename",
        "1 day"
    )
    assert.equal(observaciones.length,365)
    assert.equal(observaciones[0].timestart.toISOString(), new Date(Date.UTC(1950,0,1)).toISOString())
    assert.equal(observaciones[0].timeend.toISOString(), new Date(Date.UTC(1950,0,2)).toISOString())
    assert.equal(observaciones[364].timestart.toISOString(), new Date(Date.UTC(1950,11,31)).toISOString())
    assert.equal(observaciones[364].timeend.toISOString(), new Date(Date.UTC(1951,0,1)).toISOString())
    const serie_read = await serie.read({tipo: "raster", id: s.id, timestart:new Date(1949,11,31,21), timeend: new Date(1950,11,31,21)})
    assert.equal(serie_read.observaciones.length,365)
})

test("nc2obs", async(t) => {
    const pool = new Pool({"host": "localhost", "port": 5432, "database": "memeology", "user": "test", "password": "test"})
    await pool.query("DELETE FROM thredds_rasters WHERE filename=$1",["pr_day_ACCESS-CM2_historical_r1i1p1f1_gn_1950_v2.0.nc"])
    const client = new Client({
        url: "https://ds.nccs.nasa.gov/thredds",
        bbox: [-70, -10, -40, -40],
        var: "pr",
        horizStride: true
    })
    const deleted = await observacion.delete({tipo:"raster", series_id: 23, timestart:new Date(1949,11,31,21), timeend: new Date(1950,11,31,21)}) 
    const observaciones = await client.nc2ObservacionesRaster(
        23,
        "data/thredds/pr_day_ACCESS-CM2_historical_r1i1p1f1_gn_1950_v2.0.nc",
        "public",
        "thredds_rasters",
        "rast",
        "filename"
    )
    assert.equal(observaciones.length,365)
    assert.equal(observaciones[0].timestart.toISOString(), new Date(Date.UTC(1950,0,1)).toISOString())
    assert.equal(observaciones[0].timeend.toISOString(), new Date(Date.UTC(1950,0,2)).toISOString())
    assert.equal(observaciones[364].timestart.toISOString(), new Date(Date.UTC(1950,11,31)).toISOString())
    assert.equal(observaciones[364].timeend.toISOString(), new Date(Date.UTC(1951,0,1)).toISOString())
    const serie_read = await serie.read({tipo: "raster", id: 23, timestart:new Date(1949,11,31,21), timeend: new Date(1950,11,31,21)})
    assert.equal(serie_read.observaciones.length,365)

})

test("download", async(t) => {
    const client = new Client({
        url: "https://ds.nccs.nasa.gov/thredds",
        bbox: [-70, -10, -40, -40],
        var: "pr",
        horizStride: true
    })
    await client.downloadNC(
        "ncss/grid/AMES/NEX/GDDP-CMIP6/ACCESS-CM2/historical/r1i1p1f1/pr/pr_day_ACCESS-CM2_historical_r1i1p1f1_gn_1951_v2.0.nc",
        new Date(Date.UTC(1951,0,1)),
        new Date(Date.UTC(1951,11,31)),
        "data/thredds/pr_day_ACCESS-CM2_historical_r1i1p1f1_gn_1951_v2.0.nc")
    assert(fs.existsSync("data/thredds/pr_day_ACCESS-CM2_historical_r1i1p1f1_gn_1951_v2.0.nc"))
})

test("dir2obs", async(t) => {
    const client = new Client({
        url: "https://ds.nccs.nasa.gov/thredds",
        bbox: [-70, -10, -40, -40],
        var: "pr",
        horizStride: true
    })
    const observaciones = await client.importFromDir(
        23,
        "data/thredds/historical"
    )
    assert.equal(observaciones.length,4 * 365 + 366)
})