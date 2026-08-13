import test from "node:test";
import assert from "node:assert/strict";
import {Client} from "../app/accessors/ctm_ws.js"
import accessors from "../app/accessors.js"
import fs from 'fs'
// import {observaciones as A5Observaciones} from '../app/CRUD.js'

test('ctm_ws accessor get data', async() => {
    const client = new Client({
        url: "https://www.saltogrande.org/ws.php",
        tabla_id: "estaciones_salto_grande"
    })

    const hasta = new Date()
    const desde = new Date()
    desde.setDate(desde.getDate() - 3)

    const observaciones = await client.getData({
        idEstacion: "A5000F4A",
        variable: "P",
        fechaDesde: desde,
        fechaHasta: hasta
    },
    {
        save_raw_result: "/tmp/ctm_response.xml"   
    })
    assert(Array.isArray(observaciones))
    assert(observaciones.length)
    for(const o of observaciones) {
        assert("idEstacion" in o)
        assert("variable" in o)
        assert("fecha" in o)
        assert("valor" in o)
        assert(o.fecha instanceof Date)
    }
})

test('ctm_ws accessor get sites', async() => {
    const client = new Client({
        url: "https://www.saltogrande.org/ws.php",
        tabla_id: "estaciones_salto_grande"
    })

    const estaciones = await client.getListaEstacionesTelemetricas(
    {
        save_raw_result: "/tmp/ctm_sites_response.xml"   
    })
    assert(Array.isArray(estaciones))
    assert(estaciones.length)
    for(const o of estaciones) {
        assert("id" in o)
        assert("name" in o)
        assert("lat" in o)
        assert("date" in o)
        assert("variables" in o)
        assert(Array.isArray(o.variables))
        for(const v of o.variables) {
            assert(typeof v == "string")
        }
    }
})

test('ctm_ws accessor get series', async() => {
    const client = new Client({
        url: "https://www.saltogrande.org/ws.php",
        tabla_id: "estaciones_salto_grande"
    })

    const series = await client.getSeries(
        {},
        {
            save_raw_result: "/tmp/ctm_sites_response.xml"
        }
    )
    assert(Array.isArray(series))
    assert(series.length)
    for(const o of series) {
        assert("id" in o)
        assert("estacion" in o)
        assert("geom" in o.estacion)
        assert("coordinates" in o.estacion.geom)
        assert(Array.isArray(o.estacion.geom.coordinates))
        assert.equal(o.estacion.geom.coordinates.length, 2)
        for(const c of o.estacion.geom.coordinates) {
            assert(c.toString() != "NaN")
        }
        assert(o.estacion.id_externo != '[object Object]')
        assert(o.estacion.nombre != '[object Object]')
        assert(o.estacion.tabla == 'estaciones_salto_grande')
        assert("var" in o)
        assert("procedimiento" in o)
        assert("unidades" in o)
    }
    fs.writeFileSync("/tmp/ctm_sites.json", JSON.stringify(series, undefined, 2))
})

test('ctm_ws accessor update series', async() => {
    const client = new Client({
        url: "https://www.saltogrande.org/ws.php",
        tabla_id: "estaciones_salto_grande"
    })

    const series = await client.updateSeries(
        {},
        {
            save_raw_result: "/tmp/ctm_sites_response.xml",
            no_update: true
        }
    )
    assert(Array.isArray(series))
    assert(series.length)
    for(const o of series) {
        assert("id" in o)
        assert("estacion" in o)
        assert("geom" in o.estacion)
        assert("coordinates" in o.estacion.geom)
        assert(Array.isArray(o.estacion.geom.coordinates))
        assert.equal(o.estacion.geom.coordinates.length, 2)
        for(const c of o.estacion.geom.coordinates) {
            assert(c.toString() != "NaN")
        }
        assert(o.estacion.id_externo != '[object Object]')
        assert(o.estacion.nombre != '[object Object]')
        assert(o.estacion.tabla == 'estaciones_salto_grande')
        assert("var" in o)
        assert("procedimiento" in o)
        assert("unidades" in o)
    }
    fs.writeFileSync("/tmp/ctm_sites_.json", JSON.stringify(series, undefined, 2))
})


test('ctm_ws accessor get series filtered', async() => {
    const client = new Client({
        url: "https://www.saltogrande.org/ws.php",
        tabla_id: "estaciones_salto_grande"
    })

    const series = await client.getSeries(
        {
            var_id: 4,
            estacion_id: 1016
        },
        {
            save_raw_result: "/tmp/ctm_sites_response.xml"
        }
    )
    assert(Array.isArray(series))
    assert.equal(series.length, 1)
    const serie = series[0]
    assert("id" in serie)
    assert.equal(serie.id, 45281)
    assert("estacion" in serie)
    assert("geom" in serie.estacion)
    assert("coordinates" in serie.estacion.geom)
    assert(Array.isArray(serie.estacion.geom.coordinates))
    assert.equal(serie.estacion.geom.coordinates.length, 2)
    for(const c of serie.estacion.geom.coordinates) {
        assert(c.toString() != "NaN")
    }
    assert.equal(serie.estacion.id_externo, 'A50012EE')
    assert.equal(serie.estacion.nombre, 'Puerto Concordia')
    assert.equal(serie.estacion.tabla, 'estaciones_salto_grande')
    assert("var" in serie)
    assert.equal(serie.var.id, 4)
    assert("procedimiento" in serie)
    assert.equal(serie.procedimiento.id, 1)
    assert("unidades" in serie)
    assert.equal(serie.unidades.id, 10)
    fs.writeFileSync("/tmp/ctm_concordia_q.json", JSON.stringify(series, undefined, 2))
})


test('ctm_ws accessor get data one serie', async() => {
    const client = new Client({
        url: "https://www.saltogrande.org/ws.php",
        tabla_id: "estaciones_salto_grande"
    })
    const hasta = new Date()
    const desde = new Date()
    desde.setDate(desde.getDate() - 3)

    const series = await client.get(
        {
            series_id: 45281,
            timestart: desde,
            timeend: hasta
        },
        {
            save_raw_result: "/tmp/ctm_data_response.xml",
            return_series: true
        }
    )
    assert(Array.isArray(series))
    assert.equal(series.length, 1)
    const serie = series[0]
    assert("id" in serie)
    assert.equal(serie.id, 45281)
    assert("estacion" in serie)
    assert("geom" in serie.estacion)
    assert("coordinates" in serie.estacion.geom)
    assert(Array.isArray(serie.estacion.geom.coordinates))
    assert.equal(serie.estacion.geom.coordinates.length, 2)
    for(const c of serie.estacion.geom.coordinates) {
        assert(c.toString() != "NaN")
    }
    assert.equal(serie.estacion.id_externo, 'A50012EE')
    assert.equal(serie.estacion.nombre, 'Puerto Concordia')
    assert.equal(serie.estacion.tabla, 'estaciones_salto_grande')
    assert("var" in serie)
    assert.equal(serie.var.id, 4)
    assert("procedimiento" in serie)
    assert.equal(serie.procedimiento.id, 1)
    assert("unidades" in serie)
    assert.equal(serie.unidades.id, 10)
    assert("observaciones" in serie)
    assert(serie.observaciones.length > 0)
    for(const o of serie.observaciones) {
        assert("timestart" in o)
        assert(o.timestart instanceof Date)
        assert("timeend" in o)
        assert(o.timeend instanceof Date)
        assert("valor" in o)
        assert(typeof o.valor == 'number')
    }
    fs.writeFileSync("/tmp/ctm_concordia_q_data.json", JSON.stringify(series, undefined, 2))
})


test('ctm_ws accessor get data one serie return observaciones', async() => {
    const client = new Client({
        url: "https://www.saltogrande.org/ws.php",
        tabla_id: "estaciones_salto_grande"
    })
    const hasta = new Date()
    const desde = new Date()
    desde.setDate(desde.getDate() - 3)

    const observaciones = await client.get(
        {
            series_id: 45283,
            timestart: desde,
            timeend: hasta
        },
        {
            save_raw_response: "/tmp/ctm_data_response.xml",
            return_series: false
        }
    )
    assert(Array.isArray(observaciones))
    assert(observaciones.length > 0)
    for(const o of observaciones) {
        assert.equal(o.series_id, 45283)
        assert("timestart" in o)
        assert(o.timestart instanceof Date)
        assert("timeend" in o)
        assert(o.timeend instanceof Date)
        assert("valor" in o)
        assert(typeof o.valor == 'number')
    }
    fs.writeFileSync("/tmp/ctm_aguapey_h_data.json", JSON.stringify(observaciones, undefined, 2))
})


test('ctm_ws accessor.getSeries() one serie', async() => {
    const client = new accessors.Accessor(
        {
            class: "ctm_ws",
            config: {
                url: "https://www.saltogrande.org/ws.php",
                tabla_id: "estaciones_salto_grande"
            }
        }
    )
    const hasta = new Date()
    const desde = new Date()
    desde.setDate(desde.getDate() - 3)

    const series = await client.getSeries(
        {
            series_id: 45281,
            timestart: desde,
            timeend: hasta
        }
    )
    assert(Array.isArray(series))
    assert.equal(series.length, 1)
    const serie = series[0]
    assert("id" in serie)
    assert.equal(serie.id, 45281)
    assert("estacion" in serie)
    assert("geom" in serie.estacion)
    assert("coordinates" in serie.estacion.geom)
    assert(Array.isArray(serie.estacion.geom.coordinates))
    assert.equal(serie.estacion.geom.coordinates.length, 2)
    for(const c of serie.estacion.geom.coordinates) {
        assert(c.toString() != "NaN")
    }
    assert.equal(serie.estacion.id_externo, 'A50012EE')
    assert.equal(serie.estacion.nombre, 'Puerto Concordia')
    assert.equal(serie.estacion.tabla, 'estaciones_salto_grande')
    assert("var" in serie)
    assert.equal(serie.var.id, 4)
    assert("procedimiento" in serie)
    assert.equal(serie.procedimiento.id, 1)
    assert("unidades" in serie)
    assert.equal(serie.unidades.id, 10)
    assert("observaciones" in serie)
    assert(serie.observaciones.length > 0)
    for(const o of serie.observaciones) {
        assert("timestart" in o)
        assert(o.timestart instanceof Date)
        assert("timeend" in o)
        assert(o.timeend instanceof Date)
        assert("valor" in o)
        assert(typeof o.valor == 'number')
    }
    fs.writeFileSync("/tmp/ctm_concordia_q_data.json", JSON.stringify(series, undefined, 2))
})


test('ctm_ws accessor.getSeries() multiple series', async() => {
    const client = new accessors.Accessor(
        {
            class: "ctm_ws",
            config: {
                url: "https://www.saltogrande.org/ws.php",
                tabla_id: "estaciones_salto_grande"
            }
        }
    )
    const hasta = new Date()
    const desde = new Date()
    desde.setDate(desde.getDate() - 3)

    const series = await client.getSeries(
        {
            var_id: 4,
            timestart: desde,
            timeend: hasta
        }
    )
    assert(Array.isArray(series))
    assert.equal(series.length, 9)
    for( const serie of series) {
        assert(serie.observaciones.length > 0)
        for(const o of serie.observaciones) {
            assert("timestart" in o)
            assert(o.timestart instanceof Date)
            assert("timeend" in o)
            assert(o.timeend instanceof Date)
            assert("valor" in o)
            assert(typeof o.valor == 'number')
        }
    }
    fs.writeFileSync("/tmp/ctm_q_data.json", JSON.stringify(series, undefined, 2))
})


test('ctm_ws accessor.updateSeries() multiple series', async() => {
    const client = new accessors.Accessor(
        {
            class: "ctm_ws",
            config: {
                url: "https://www.saltogrande.org/ws.php",
                tabla_id: "estaciones_salto_grande"
            }
        }
    )
    const hasta = new Date()
    const desde = new Date()
    desde.setDate(desde.getDate() - 3)

    const series = await client.updateSeries(
        {
            var_id: 4,
            timestart: desde,
            timeend: hasta
        },
        {
            no_update_date_range: true
        }
    )
    assert(Array.isArray(series))
    assert.equal(series.length, 9)
    for( const serie of series) {
        assert(serie.observaciones.length > 0)
        for(const o of serie.observaciones) {
            assert("timestart" in o)
            assert(o.timestart instanceof Date)
            assert("timeend" in o)
            assert(o.timeend instanceof Date)
            assert("valor" in o)
            assert(typeof o.valor == 'number')
        }
    }
    fs.writeFileSync("/tmp/ctm_q_data.json", JSON.stringify(series, undefined, 2))
})
