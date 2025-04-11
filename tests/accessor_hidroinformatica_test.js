const test = require('node:test')
const assert = require('assert')
process.env.NODE_ENV = "test"
// const {serie: Serie, observacion: Observacion, observaciones: Observaciones, estacion: Estacion} = require('../app/CRUD')
const {Client} = require("../app/accessors/hidroinformatica")
// const accessors = require("../app/accessors")

test('hidroinformatica accessor get page', async(t) => {

    const client = new Client({})
    client.setDefaultSeriesMap()

    await t.test("default series map", async(t)=> {
        assert.equal(client.series_map.length, client.sites_id_map.length)
    })

    await t.test("get code", async (t) => {
        const serie = client.getCode(5946, 39)
        assert.equal(serie.code, 3)
    })

    await t.test("get", async (t) => {
        const observaciones = await client.get({
            estacion_id: 5946,
            var_id: 39,
            timestart: new Date(2025,0,1),
            timeend: new Date(2025,0,31)
        })
        assert.equal(observaciones.length, 31)
        assert.equal(client.last_request.params.code,3)
        for(const observacion of observaciones) {
            assert.equal(observacion.timestart.getHours(),0)
            assert.equal(observacion.timeend.getHours(),0)
            et = new Date(observacion.timestart)
            et.setDate(et.getDate()+1)
            assert.equal(et.getTime(),observacion.timeend.getTime())
        }
    })

})
