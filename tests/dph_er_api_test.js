const test = require('node:test')
const assert = require('assert')
const {serie: Serie, observacion: Observacion, observaciones: Observaciones, estacion: Estacion, red: Red} = require('../app/CRUD')
const axios = require('axios')
const {Client} = require("../app/accessors/dph_er_api")
const accessors = require("../app/accessors")

test('create red', async(t) => {

    await t.test("create red", async (t) => {
        const client = new Client({tabla_id:"dph_er_api"})
        var ok=true
        try {
            await client.createRed()
        } catch (e) {
            console.error(e)
            ok = false
        }
        assert(ok)
    })
})

test('get estaciones', async(t) => {

    await t.test("get estaciones", async (t) => {
        
        try {
            const accessor = await accessors.new("dph_er_api")
            var response = await accessor.engine.get_estaciones()
        } catch (e) {
            console.error(e)
            var response = undefined
        }
        assert.equal(response.status, "ok")
        const data = response.data
        assert(data)
        assert.equal(typeof data.total,"number")
        assert(data.total > 0)
        const estaciones = data.estaciones
        assert(estaciones)
        assert(Array.isArray(estaciones))
        assert(estaciones.length)
        for(const e of estaciones) {
            assert.equal(typeof e.id, "number")
            assert.equal(typeof e.nombre, "string")
            assert.equal(typeof e.latitud, "number")
            assert.equal(typeof e.longitud, "number")
            assert.equal(typeof e.propietario, "string")
            assert.equal(typeof e.departamento, "string")
            assert.equal(typeof e.cuenca, "string")
        }
    })
})

test('get sites', async(t) => {

    await t.test("get sites", async (t) => {
        
        try {
            var accessor = await accessors.new("dph_er_api")
            var estaciones = await accessor.getSites()
        } catch (e) {
            console.error(e)
            var estaciones = undefined
        }
        assert(estaciones)
        assert(Array.isArray(estaciones))
        assert(estaciones.length)
        for(const e of estaciones) {
            assert.equal(typeof e.id_externo, "string")
            assert.equal(typeof e.nombre, "string")
            assert.equal(e.tabla, accessor.engine.tabla_id)
            assert(e.geom)
            assert.equal(e.geom.type, "Point")
            assert(Array.isArray(e.geom.coordinates))
            assert.equal(e.geom.coordinates.length, 2)
        }
    })
})


test('create sites', async(t) => {

    await t.test("create sites", async (t) => {
        
        try {
            var accessor = await accessors.new("dph_er_api")
            var estaciones = await accessor.updateSites()
        } catch (e) {
            console.error(e)
            var estaciones = undefined
        }
        assert(estaciones)
        assert(Array.isArray(estaciones))
        assert(estaciones.length)
        for(const e of estaciones) {
            assert.equal(typeof e.id, "number")
            assert.equal(typeof e.id_externo, "string")
            assert.equal(typeof e.nombre, "string")
            assert.equal(e.tabla, accessor.engine.tabla_id)
            assert(e.geom)
            assert.equal(e.geom.type, "Point")
            assert(Array.isArray(e.geom.coordinates))
            assert.equal(e.geom.coordinates.length, 2)
        }
    })
})

test('get series', async(t) => {

    await t.test("get series", async (t) => {
        
        try {
            var accessor = await accessors.new("dph_er_api")
            var series = await accessor.getMetadata()
        } catch (e) {
            console.error(e)
            var series = undefined
        }
        assert(series)
        assert(Array.isArray(series))
        assert(series.length)
        for(const s of series) {
            // assert.equal(typeof e.id, "number")
            assert.equal(s.tipo, "puntual")
            assert(s.estacion)
            assert.equal(s.estacion.tabla, accessor.engine.tabla_id)
            assert(s.var)
            assert.equal(s.var.id, 1)
            assert(s.procedimiento)
            assert.equal(s.procedimiento.id, 1)
            assert(s.unidades)
            assert.equal(s.unidades.id, 22)
        }
    })
})


test('create series', async(t) => {

    await t.test("create series", async (t) => {
        
        try {
            var accessor = await accessors.new("dph_er_api")
            var series = await accessor.updateMetadata()
        } catch (e) {
            console.error(e)
            var series = undefined
        }
        assert(series)
        assert(Array.isArray(series))
        assert(series.length)
        for(const s of series) {
            // assert.equal(typeof e.id, "number")
            assert.equal(s.tipo, "puntual")
            assert.equal(typeof s.id, "number")
            assert(s.estacion)
            assert.equal(typeof s.estacion.id, "number")
            assert.equal(s.estacion.tabla, accessor.engine.tabla_id)
            assert(s.var)
            assert.equal(s.var.id, 1)
            assert(s.procedimiento)
            assert.equal(s.procedimiento.id, 1)
            assert(s.unidades)
            assert.equal(s.unidades.id, 22)
        }
    })
})

test('get precipitaciones global', async(t) => {

    await t.test("get precipitaciones global", async (t) => {
        
        try {
            const accessor = await accessors.new("dph_er_api")
            var response = await accessor.engine.get_precipitaciones_global()
        } catch (e) {
            console.error(e)
            var response = undefined
        }
        assert.equal(response.status, "ok")
        const data = response.data
        assert(data)
        assert.equal(typeof data.total,"number")
        assert(data.total > 0)
        const precipitaciones = data.precipitaciones
        assert(precipitaciones)
        assert(Array.isArray(precipitaciones))
        assert(precipitaciones.length)
        for(const e of precipitaciones) {
            assert.equal(typeof e.estacion_id, "number")
            assert.equal(typeof e.estacion, "string")
            assert.equal(typeof e.fecha, "string")
            assert(new Date(e.fecha).toString() != "Invalid Date")
            assert.equal(typeof e.medicion_realizada, "boolean")
            if(e.medicion_realizada) {
                assert.equal(typeof e.precipitacion_mm, "number")
            } else {
                assert.equal(e.precipitacion_mm, null)
            }
        }
    })
})


test('get precipitaciones', async(t) => {

    await t.test("get precipitaciones", async (t) => {
        const timeend = new Date()
        const timestart = new Date(timeend.getTime() - 7 * 24 * 3600 * 1000)
        try {
            const accessor = await accessors.new("dph_er_api")
            var response = await accessor.engine.get_precipitaciones(
                101,
                timestart.toISOString().substring(0,10),
                timeend.toISOString().substring(0,10)
            )
        } catch (e) {
            console.error(e)
            var response = undefined
        }
        assert.equal(response.status, "ok")
        const data = response.data
        assert(data)
        assert.equal(typeof data.total,"number")
        assert(data.total > 0)
        assert.equal(data.estacion_id, 101)
        assert.equal(data.desde, timestart.toISOString().substring(0,10))
        assert.equal(data.hasta, timeend.toISOString().substring(0,10))
        const precipitaciones = data.precipitaciones
        assert(precipitaciones)
        assert(Array.isArray(precipitaciones))
        assert(precipitaciones.length)
        for(const e of precipitaciones) {
            assert.equal(typeof e.fecha, "string")
            assert(new Date(e.fecha).toString() != "Invalid Date")
            assert.equal(typeof e.medicion_realizada, "boolean")
            if(e.medicion_realizada) {
                assert.equal(typeof e.precipitacion_mm, "number")
            } else {
                assert.equal(e.precipitacion_mm, null)
            }
        }
    })
})

test('get', async(t) => {

    await t.test("get", async (t) => {
        const timeend = new Date()
        const timestart = new Date(timeend.getTime() - 6 * 24 * 3600 * 1000)        
        // const timestart = new Date(2026,5,25)
        // const timeend = new Date(2026,6,2)
        const ts = new Date(timestart)
        ts.setHours(9,0,0,0)
        const te = new Date(timeend)
        te.setHours(9,0,0,0)
        te.setDate(te.getDate()+1)
        try {
            const accessor = await accessors.new("dph_er_api")
            var observaciones = await accessor.getSeries({timestart:timestart, timeend:timeend})
        } catch (e) {
            console.error(e)
            var observaciones = undefined
        }
        assert(observaciones)
        assert(Array.isArray(observaciones))
        assert(observaciones.length)
        for(const o of observaciones) {
            assert("timestart" in o)
            assert(o.timestart instanceof Date)
            assert(o.timestart.getTime() >= ts.getTime())
            assert("timeend" in o)
            assert(o.timeend instanceof Date)
            assert(o.timeend.getTime() <= te.getTime())
            assert.equal(typeof o.valor, "number")
            assert.equal(typeof o.series_id, "number")
        }
    })
})

test('get with estacion_id[]', async(t) => {

    await t.test("get with estacion_id[]", async (t) => {
        const timeend = new Date()
        const timestart = new Date(timeend.getTime() - 30 * 24 * 3600 * 1000)        
        // const timestart = new Date(2026,5,25)
        // const timeend = new Date(2026,6,2)
        const ts = new Date(timestart)
        ts.setHours(9,0,0,0)
        const te = new Date(timeend)
        te.setHours(9,0,0,0)
        te.setDate(te.getDate()+1)
        try {
            const accessor = await accessors.new("dph_er_api")
            var observaciones = await accessor.getSeries(
                {
                    timestart:timestart, 
                    timeend:timeend,
                    estacion_id: [8525,8526,8527,8528,8529,8530]
                })
        } catch (e) {
            console.error(e)
            var observaciones = undefined
        }
        assert(observaciones)
        assert(Array.isArray(observaciones))
        assert(observaciones.length)
        for(const o of observaciones) {
            assert("timestart" in o)
            assert(o.timestart instanceof Date)
            assert(o.timestart.getTime() >= ts.getTime())
            assert("timeend" in o)
            assert(o.timeend instanceof Date)
            assert(o.timeend.getTime() <= te.getTime())
            assert.equal(typeof o.valor, "number")
            assert.equal(typeof o.series_id, "number")
        }
    })
})


test('get with estacion_id', async(t) => {

    await t.test("get with estacion_id", async (t) => {
        const timeend = new Date()
        const timestart = new Date(timeend.getTime() - 30 * 24 * 3600 * 1000)        
        // const timestart = new Date(2026,5,25)
        // const timeend = new Date(2026,6,2)
        const ts = new Date(timestart)
        ts.setHours(9,0,0,0)
        const te = new Date(timeend)
        te.setHours(9,0,0,0)
        te.setDate(te.getDate()+1)
        try {
            const accessor = await accessors.new("dph_er_api")
            var observaciones = await accessor.getSeries(
                {
                    timestart:timestart, 
                    timeend:timeend,
                    estacion_id: 8525
                })
        } catch (e) {
            console.error(e)
            var observaciones = undefined
        }
        assert(observaciones)
        assert(Array.isArray(observaciones))
        assert(observaciones.length)
        for(const o of observaciones) {
            assert("timestart" in o)
            assert(o.timestart instanceof Date)
            assert(o.timestart.getTime() >= ts.getTime())
            assert("timeend" in o)
            assert(o.timeend instanceof Date)
            assert(o.timeend.getTime() <= te.getTime())
            assert.equal(typeof o.valor, "number")
            assert.equal(typeof o.series_id, "number")
        }
    })
})

test('get with series_id', async(t) => {

    await t.test("get with series_id", async (t) => {
        const timeend = new Date()
        const timestart = new Date(timeend.getTime() - 30 * 24 * 3600 * 1000)        
        // const timestart = new Date(2026,5,25)
        // const timeend = new Date(2026,6,2)
        const ts = new Date(timestart)
        ts.setHours(9,0,0,0)
        const te = new Date(timeend)
        te.setHours(9,0,0,0)
        te.setDate(te.getDate()+1)
        try {
            const accessor = await accessors.new("dph_er_api")
            var observaciones = await accessor.getSeries(
                {
                    timestart:timestart, 
                    timeend:timeend,
                    series_id: 44339
                })
        } catch (e) {
            console.error(e)
            var observaciones = undefined
        }
        assert(observaciones)
        assert(Array.isArray(observaciones))
        assert(observaciones.length)
        for(const o of observaciones) {
            assert("timestart" in o)
            assert(o.timestart instanceof Date)
            assert(o.timestart.getTime() >= ts.getTime())
            assert("timeend" in o)
            assert(o.timeend instanceof Date)
            assert(o.timeend.getTime() <= te.getTime())
            assert.equal(typeof o.valor, "number")
            assert.equal(typeof o.series_id, "number")
        }
    })
})

test('update with series_id', async(t) => {

    await t.test("update with series_id", async (t) => {
        const timeend = new Date()
        const timestart = new Date(timeend.getTime() - 30 * 24 * 3600 * 1000)        
        // const timestart = new Date(2026,5,25)
        // const timeend = new Date(2026,6,2)
        const ts = new Date(timestart)
        ts.setHours(9,0,0,0)
        const te = new Date(timeend)
        te.setHours(9,0,0,0)
        te.setDate(te.getDate()+1)
        try {
            const accessor = await accessors.new("dph_er_api")
            var observaciones = await accessor.updateSeries(
                {
                    timestart:timestart, 
                    timeend:timeend,
                    series_id: 44339
                },
                {
                    no_update_date_range: true
                }
            )
        } catch (e) {
            console.error(e)
            var observaciones = undefined
        }
        assert(observaciones)
        assert(Array.isArray(observaciones))
        assert(observaciones.length)
        for(const o of observaciones) {
            assert("timestart" in o)
            assert(o.timestart instanceof Date)
            assert(o.timestart.getTime() >= ts.getTime())
            assert("timeend" in o)
            assert(o.timeend instanceof Date)
            assert(o.timeend.getTime() <= te.getTime())
            assert.equal(typeof o.valor, "number")
            assert.equal(typeof o.series_id, "number")
            assert.equal(o.series_id, 44339)
        }
    })
})

test('update with global', async(t) => {

    await t.test("update with global", async (t) => {
        const timeend = new Date()
        const timestart = new Date(timeend.getTime() - 6 * 24 * 3600 * 1000)        
        // const timestart = new Date(2026,5,25)
        // const timeend = new Date(2026,6,2)
        const ts = new Date(timestart)
        ts.setHours(9,0,0,0)
        const te = new Date(timeend)
        te.setHours(9,0,0,0)
        te.setDate(te.getDate()+1)
        try {
            const accessor = await accessors.new("dph_er_api")
            var observaciones = await accessor.updateSeries(
                {
                    timestart:timestart, 
                    timeend:timeend
                },
                {
                    no_update_date_range: true
                }
            )
        } catch (e) {
            console.error(e)
            var observaciones = undefined
        }
        assert(observaciones)
        assert(Array.isArray(observaciones))
        assert(observaciones.length)
        const series_ids = new Set()
        for(const o of observaciones) {
            assert("timestart" in o)
            assert(o.timestart instanceof Date)
            assert(o.timestart.getTime() >= ts.getTime())
            assert("timeend" in o)
            assert(o.timeend instanceof Date)
            assert(o.timeend.getTime() <= te.getTime())
            assert.equal(typeof o.valor, "number")
            assert.equal(typeof o.series_id, "number")
            series_ids.add(o.series_id)
        }
        assert(series_ids.size > 10)
    })
})
