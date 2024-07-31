const test = require('node:test')
const assert = require('assert')
process.env.NODE_ENV = "test"
const {corrida: Corrida, observacionPivot: ObservacionPivot} = require('../app/CRUD')
const a5_samples = require('./a5_samples')

test('pivot corrida', (t) => {

    const corrida = new Corrida(a5_samples.corridas[0])

    const pivoted = corrida.pivot()
    for (const p of pivoted) {
        assert(p instanceof ObservacionPivot)
        assert(p.hasOwnProperty("1505"))
        assert(p.hasOwnProperty("1520"))
        assert.equal(p.toCSV().split(",").length, 4)
        assert.equal(Object.keys(p.toJSON()).filter(k =>
            (/^_/.test(k))
        ).length, 0)
    }
    assert.equal(pivoted.length, corrida.series[0].pronosticos.length)

    const respawned = JSON.parse(JSON.stringify(pivoted)).map(p=>
        new ObservacionPivot(p, {string_keys: true})
    )
    assert.equal(pivoted.length, respawned.length)
    assert.equal(JSON.stringify(pivoted), JSON.stringify(respawned))
    

})