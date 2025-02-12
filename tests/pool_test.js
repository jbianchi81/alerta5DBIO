const test = require('node:test')
const assert = require('assert')
process.env.NODE_ENV = "test"
const {serie: Serie, observacion: Observacion, observaciones: Observaciones, estacion: Estacion, var: Variable} = require('../app/CRUD')

test('check no clients left open', async(t) => {

    const vars = await Variable.read()
    assert.ok(vars.length > 0)
    await global.pool.end()
    assert.ok(global.pool.totalCount == 0)
    assert.ok(global.pool.idleCount == 0)
})
