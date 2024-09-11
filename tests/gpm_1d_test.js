// modificando gpm_3h para 1d
const test = require('node:test')
const { Client } = require('../app/accessors/gpm')
const assert = require('assert')
const fs = require('fs')
const {Accessor} = require('../app/accessors')

const default_config = {
    "url":"https://pmmpublisher.pps.eosdis.nasa.gov/opensearch",
    "local_path":"data/gpm/3h/",
    "dia_local_path":"data/gpm/dia",
    "search_params":{"q":"precip_1d","lat":-25,"lon":-45,"limit":64, "area": 0.25},
    "bbox":[-70,-10,-40,-40],
    "tmpfile":"/tmp/gpm_transformed.tif",
    "series_id": 4,
    "scale": 0.1,
    "dia_series_id": 13,
    "escena_id": 11
}

const client = new Client(
    default_config
)

test('get files list last week', async (t) => {
    var timeend = new Date()
    var timestart = new Date(timeend.getTime() - 8 * 24 * 3600 * 1000)
    const result = await client.getFilesList(
        {
            timestart: timestart, 
            timeend: timeend
        }
    )
    console.info("got " + result.data.items.length + " items")
    assert(result.data.items.length >= 7, "expected 7 items")
    fs.writeFileSync("/tmp/gpm_files_list.json",JSON.stringify(result.data.items, undefined, 2))
})

test('get sites', async (t) => {
    const result = await client.getSites()
    console.info("got " + result.length + " items")
    assert(result.length == 1, "expected 1 items")
    fs.writeFileSync("/tmp/gpm_sites.json",JSON.stringify(result, undefined, 2))
})

test('get series', async (t) => {
    const result = await client.getSeries()
    console.info("got " + result.length + " items")
    assert(result.length == 2, "expected 2 items")
    fs.writeFileSync("/tmp/gpm_series.json",JSON.stringify(result, undefined, 2))
})

test('get data last week', async (t) => {
    var timeend = new Date()
    var timestart = new Date(timeend.getTime() - 3 * 24 * 3600 * 1000)
    timestart.setUTCHours(0,0,0,0)
    const result = await client.get({
        timestart: timestart,
        timeend: timeend
    })
    console.info("got " + result.length + " items")
    assert(client.downloaded_files.length >= 2, "expected at least 2 items")
    assert(client.subset_files.length >= 2, "expected at least 2 items")
    assert(result.length >= 2, "expected at least 2 items")
    for(const obs of result) {
        console.debug({obs_timestart: obs.timestart, obs_timeend: obs.timeend})
        assert(obs.timestart.getTime() >= timestart.getTime())
        assert(obs.timestart.getTime() <= timeend.getTime())
        assert(obs.timeend.getTime() - 24 * 3600 * 1000 == obs.timestart.getTime()) // assert daily time support
    }
    for(const file of client.subset_files) {
        assert(fs.existsSync(file), "file " + file + " not found")
    }
})

test("update sites", async (t)=> {
    const client = new Accessor(
        {
            class: "gpm",
            config: default_config
        }
    )
    const sites = await client.updateSites()
    assert(sites.length == 1, "1 updated site expected")
})

test("update series", async (t) => {
    const client = new Accessor(
        {
            class: "gpm",
            config: default_config
        }
    )
    const series = await client.updateMetadata()
    assert(series.length == 2, "2 updated series expected")
    fs.writeFileSync("/tmp/gpm_series.json",JSON.stringify(series, undefined, 2))
    assert(series.map(s => s.fuente.nombre).indexOf("gpm") >= 0, "Expected fuente.nombre = gpm")
    assert(series.map(s => s.fuente.nombre).indexOf("pp_gpm_3h") >= 0, "Expected fuente.nombre = pp_gpm_3h")
    assert(series.map(s => s.id).indexOf(client.config.series_id) >= 0, "3h series id not as expected")
    assert(series.map(s => s.id).indexOf(client.config.dia_series_id) >= 0, "daily series id not as expected")
})

test('get series by id', async (t) => {
    const result = await client.getSeries(
        {
            series_id: client.config.dia_series_id
        }
    )
    console.info("got " + result.length + " items")
    assert(result.length == 1, "expected 1 items")
    assert(result[0].id == client.config.dia_series_id, "expected series.id == " + client.config.dia_series_id)
    fs.writeFileSync("/tmp/gpm_series.json",JSON.stringify(result, undefined, 2))
})

test("update data", async (t) => {
    const client = new Accessor(
        {
            class: "gpm",
            config: default_config
        }
    )
    var timeend = new Date()
    var timestart = new Date(timeend.getTime() - 3 * 24 * 3600 * 1000)
    timestart.setUTCHours(0,0,0,0)
    const series = await client.updateSeries(
        {
            series_id: client.config.dia_series_id,
            timestart: timestart, 
            timeend: timeend            
        }
    )
    assert(series.length, "Expected one updated serie")
    const observaciones = series[0].observaciones
    assert(observaciones.length >= 2, "at least 2 updated obs expected")
    console.info("got " + observaciones.length + " items")
    assert(client.engine.downloaded_files.length >= 2, "expected at least 2 items")
    assert(client.engine.subset_files.length >= 2, "expected at least 2 items")
    assert(observaciones.length >= 2, "expected at least 2 items")
    for(const obs of observaciones) {
        console.debug({obs_timestart: obs.timestart, obs_timeend: obs.timeend})
        assert(obs.timestart.getTime() >= timestart.getTime())
        assert(obs.timestart.getTime() <= timeend.getTime())
        assert(obs.timeend.getTime() - 24 * 3600 * 1000 == obs.timestart.getTime()) // assert daily time support
    }
    for(const file of client.engine.subset_files) {
        assert(fs.existsSync(file), "file " + file + " not found")
    }
})