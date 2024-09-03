const test = require('node:test')
const fs = require('fs')
const assert = require('assert')

const { red: Red } = require('../app/CRUD')
const { Accessor } = require('../app/accessors')

test('dados_ons accessor get sites', async(t) => {
    const accessor = new Accessor({
       class: "dados_ons" 
    })
    const estaciones = await accessor.getSites()
    fs.writeFileSync("/tmp/ons_sites.json", JSON.stringify(estaciones, undefined, 2))
    assert(estaciones.length > 100)
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
    const series = await accessor.getSeries()
    fs.writeFileSync("/tmp/ons_series.json", JSON.stringify(series, undefined, 2))
    assert(series.length > 400)
})

test('dados_ons accessor get series with filter.id_externo of string type', async(t) => {
    const accessor = new Accessor({
       class: "dados_ons" 
    })
    const series = await accessor.getSeries({
        id_externo: "AMUHBB"
    })
    fs.writeFileSync("/tmp/ons_series.json", JSON.stringify(series, undefined, 2))
    assert(series.length == accessor.engine.config.var_map.length)
})

test('dados_ons accessor get series with filter.id_externo of array type and filter.var_id of number type', async(t) => {
    const accessor = new Accessor({
       class: "dados_ons" 
    })
    const series = await accessor.getSeries({
        id_externo: [
            "AMUHBB",
            "AAUCCA"
        ],
        var_id: 23
    })
    fs.writeFileSync("/tmp/ons_series.json", JSON.stringify(series, undefined, 2))
    assert(series.length == 2)
    assert(series.filter(s=>s.var_id == 23).length == 2)
})