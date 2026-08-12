import test from "node:test";
import assert from "node:assert/strict";
import {Client} from "../app/accessors/ctm_ws.js"
import accessors from "../app/accessors.js"

test('ctm_ws accessor get data', async() => {
    const client = new Client({url: "https://www.saltogrande.org/ws.php"})

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
    const client = new Client({url: "https://www.saltogrande.org/ws.php"})

    const estaciones = await client.getSites(
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
    const client = new Client({url: "https://www.saltogrande.org/ws.php"})

    const estaciones = await client.getSeries(
    {
        save_raw_result: "/tmp/ctm_sites_response.xml",
        tabla_id: "estaciones_salto_grande"
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