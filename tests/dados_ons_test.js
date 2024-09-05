const test = require('node:test')
const fs = require('fs')
const assert = require('assert')

const { red: Red, estacion: Estacion } = require('../app/CRUD')
const { Accessor } = require('../app/accessors')
const { readParquetFile } = require('../app/accessors/dados_ons').Client

test('dados_ons accessor get sites', async(t) => {
    const accessor = new Accessor({
       class: "dados_ons" 
    })
    const estaciones = await accessor.getSites()
    fs.writeFileSync("/tmp/ons_sites.json", JSON.stringify(estaciones, undefined, 2))
    assert(estaciones.length > 100)

    await readParquetFile(accessor.engine.config.sites_output_file, 1000, 0, "/tmp/ons_sites_raw.csv")
})

test('dados_ons accessor get sites with filter.id_externo of type string', async(t) => {
    const accessor = new Accessor({
       class: "dados_ons" 
    })
    const estaciones = await accessor.getSites({
        id_externo: "AMUHBB"
    })
    fs.writeFileSync("/tmp/ons_sites.json", JSON.stringify(estaciones, undefined, 2))
    assert(estaciones.length == 1)
    assert(estaciones[0].id_externo == "AMUHBB")
})

test('dados_ons accessor get sites with filter.id_externo of type array ', async(t) => {
    const accessor = new Accessor({
       class: "dados_ons" 
    })
    const estaciones = await accessor.getSites({
        id_externo: [
            "AMUHBB",
            "AAUCCA"
        ]
    })
    fs.writeFileSync("/tmp/ons_sites.json", JSON.stringify(estaciones, undefined, 2))
    assert(estaciones.length == 2)
    assert(estaciones.map(e=>e.id_externo).indexOf("AMUHBB") >= 0)
    assert(estaciones.map(e=>e.id_externo).indexOf("AAUCCA") >= 0)
})

test('dados_ons accessor update sites', async(t) => {
    const red = await Red.create({
        nombre: "dados_ons",
        tabla_id: "dados_ons"
    })
    assert(red[0] instanceof Red)
    assert(red[0].tabla_id == "dados_ons")
    const accessor = new Accessor({
       class: "dados_ons" 
    })
    const estaciones = await accessor.updateSites()
    fs.writeFileSync("/tmp/ons_sites.json", JSON.stringify(estaciones, undefined, 2))
    assert(estaciones.length > 100)
})

test('dados_ons accessor get series', async(t) => {
    const accessor = new Accessor({
       class: "dados_ons" 
    })
    const series = await accessor.getMetadata()
    fs.writeFileSync("/tmp/ons_series.json", JSON.stringify(series, undefined, 2))
    assert(series.length > 400)
})

test('dados_ons accessor get series with filter.id_externo of string type', async(t) => {
    const accessor = new Accessor({
       class: "dados_ons" 
    })
    const series = await accessor.getMetadata({
        id_externo: "AMUHBB"
    })
    fs.writeFileSync("/tmp/ons_series.json", JSON.stringify(series, undefined, 2))
    assert(series.length == accessor.engine.config.var_map.length)
})

test('dados_ons accessor get series with filter.id_externo of array type and filter.var_id of number type', async(t) => {
    const accessor = new Accessor({
       class: "dados_ons" 
    })
    const series = await accessor.getMetadata({
        id_externo: [
            "AMUHBB",
            "AAUCCA"
        ],
        var_id: 23
    })
    fs.writeFileSync("/tmp/ons_series.json", JSON.stringify(series, undefined, 2))
    assert(series.length == 2)
    assert(series.filter(s=>s.var.id == 23).length == 2)
})


// // update series

test('dados_ons accessor get series with filter.id_externo of array type and filter.var_id of number type', async(t) => {
    const accessor = new Accessor({
       class: "dados_ons" 
    })
    const series = await accessor.updateMetadata({
        id_externo: [
            "AMUHBB",
            "AAUCCA"
        ],
        var_id: 23
    })
    fs.writeFileSync("/tmp/ons_series.json", JSON.stringify(series, undefined, 2))
    assert(series.length == 2)
    assert(series.filter(s=>s.var.id == 23).length == 2)
})

test('dados_ons accessor get series with filter.id_externo of array type and filter.var_id of array type', async(t) => {
    const accessor = new Accessor({
       class: "dados_ons" 
    })
    const series = await accessor.updateMetadata({
        id_externo: [
            "AMUHBB",
            "AAUCCA"
        ],
        var_id: [
            23,
            26
        ]
    })
    fs.writeFileSync("/tmp/ons_series.json", JSON.stringify(series, undefined, 2))
    assert(series.length == 4)
    assert(series.filter(s=>s.var.id == 23).length == 2)
    assert(series.filter(s=>s.var.id == 26).length == 2)
})



// // observaciones

test('dados_ons accessor get observations last week, one station, one variable', async(t) => {
    const accessor = new Accessor({
       class: "dados_ons" 
    })
    var timestart = new Date()
    timestart.setUTCDate(timestart.getUTCDate() - 7)
    var timeend = new Date()
    const series = await accessor.getSeries({
        timestart: timestart,
        timeend: timeend,
        id_externo: "AMUHBB",
        var_id: 23
    })
    fs.writeFileSync("/tmp/ons_series.json", JSON.stringify(series, undefined, 2))
    assert(series.length == 1)
    assert(series[0].observaciones.length > 5)
})

test('dados_ons accessor get observations last week, two stations, two variables', async(t) => {
    const accessor = new Accessor({
       class: "dados_ons" 
    })
    var timestart = new Date()
    timestart.setUTCDate(timestart.getUTCDate() - 7)
    var timeend = new Date()
    const series = await accessor.getSeries({
        timestart: timestart,
        timeend: timeend,
        id_externo: [
            "AMUHBB",
            "AAUCCA"
        ],
        var_id: [
            23,
            26
        ]
    })
    fs.writeFileSync("/tmp/ons_series.json", JSON.stringify(series, undefined, 2))
    assert(series.length == 4)
    for(const serie of series){
        assert(serie.observaciones.length > 5)
        for(const observacion of serie.observaciones) {
            // assert that time is 00:00:00 local time
            assert(observacion.timestart.getHours() == 0)
            assert(observacion.timestart.getMinutes() == 0)
            assert(observacion.timestart.getSeconds() == 0)
        }
    }
})

/** Update observaciones */
test('dados_ons accessor update observations last week, two stations, two variables', async(t) => {
    const accessor = new Accessor({
       class: "dados_ons" 
    })
    var timestart = new Date()
    timestart.setUTCDate(timestart.getUTCDate() - 7)
    var timeend = new Date()
    const series = await accessor.updateSeries({
        timestart: timestart,
        timeend: timeend,
        id_externo: [
            "AMUHBB",
            "AAUCCA"
        ],
        var_id: [
            23,
            26
        ]
    })
    fs.writeFileSync("/tmp/ons_series.json", JSON.stringify(series, undefined, 2))
    assert(series.length == 4)
    for(const serie of series){
        assert(serie.observaciones.length > 5)
        for(const observacion of serie.observaciones) {
            // assert that time is 00:00:00 local time
            assert(observacion.timestart.getHours() == 0)
            assert(observacion.timestart.getMinutes() == 0)
            assert(observacion.timestart.getSeconds() == 0)
            // assert that id is defined
            assert(observacion.id != undefined)
        }
    }
})


// restore db

test('delete all inserts', async(t) => {
    const deleted_estaciones = await Estacion.delete({tabla: "dados_ons"})
    assert(deleted_estaciones.length >= 100, "Failed to delete stations")
    const deleted_red = await Red.delete({tabla_id: "dados_ons"})
    assert(deleted_red.length == 1, "Failed to restore database state")
})