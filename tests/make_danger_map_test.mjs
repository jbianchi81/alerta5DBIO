import test from 'node:test'
import assert from 'assert'
process.env.NODE_ENV = "test"
import CRUD from "../app/CRUD.js"

test('qpf match test', async(t) => {
    const forecast_date = new Date(Date.UTC(2026,10,17,0))
    const timestart = new Date(forecast_date)
    timestart.setUTCHours(12)
    const timeend = new Date(timestart)
    timeend.setDate(timeend.getDate() + 7)
    const result = await CRUD.CRUD.makeDangerMap(
        28, // cosmo
        3, // gfs smn
        724,
        undefined,
        forecast_date,
        undefined,
        timestart,
        timeend,
        "main",
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        "Match QPF INA (COSMO+GFS)*",
        "/home/juan-bianchi/alerta5DBIO/data/match/danger_map.tif"
    )
    
    // assert.equal(result.filename, "/home/juan-bianchi/alerta5DBIO/data/match/danger_map.tif")
    // assert.equal(typeof result.valor,  'object')
    // assert.equal(result.forecast_date.getTime(), forecast_date.getTime())
})


test('qpf match create obs test', async(t) => {
    const forecast_date = new Date(Date.UTC(2026,10,17,0))
    const timestart = new Date(forecast_date)
    timestart.setUTCHours(12)
    const timeend = new Date(timestart)
    timeend.setDate(timeend.getDate() + 7)
    const result = await CRUD.CRUD.makeDangerMap(
        28, // cosmo
        3, // gfs smn
        724,
        undefined,
        forecast_date,
        undefined,
        timestart,
        timeend,
        "main",
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        "Match QPF INA (COSMO+GFS)*",
        "/home/juan-bianchi/alerta5DBIO/data/match/danger_map.tif",
        undefined,
        29
    )
    
    assert.equal(result.series_id, 29)
    assert.equal(typeof result.valor,  'object')
    assert.equal(result.timestart.getTime(), timestart.getTime())
    assert.equal(result.timeend.getTime(), timeend.getTime())
    assert.equal(result.tipo, "raster")
})
