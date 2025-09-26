const test = require('node:test')
const assert = require('assert')
// process.env.NODE_ENV = "test"
const {CRUD: Crud, serie: Serie, observacion: Observacion, observaciones: Observaciones, escena: Escena, fuente: Fuente} = require('../app/CRUD')

test('getCampo test', async(t) => {

    await t.test("getCampo", async t => {
        const campo = await Crud.getCampo(
            1,
            new Date(2024,0,1,9),
            new Date(2024,0,2,9),
            {
                proc_id: 6,
                tabla: "ctp"
            },
        {})
        assert.equal(campo.variable.id == 1)
        assert.equal(campo.procedimiento.id == 6)
        assert.true(campo.series.length > 0)
        for(const serie of campo.series) {
            assert.true(serie.estacion.tabla == "ctp")
        }
    })
})