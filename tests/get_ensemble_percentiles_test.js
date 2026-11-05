const test = require('node:test')
const assert = require('assert')
// process.env.NODE_ENV = "test"
const {CRUD: CRUD} = require('../app/CRUD')
const fs = require('fs')

test('get ensemble percentiles test', async(t) => {
    const pronosticos = await CRUD.getEnsemblePercentiles(
		undefined,
		721,
		undefined,
		[0.1,0.9],
		"areal",
		undefined,
		["gep01", "gep02", "gep03", "gep04", "gep05", "gep06", "gep07", "gep08", "gep09", "gep10", "gep11", "gep12", "gep13", "gep14", "gep15", "gep16", "gep17", "gep18", "gep19", "gep20", "gep21", "gep22", "gep23", "gep24", "gep25", "gep26", "gep27", "gep28", "gep29", "gep30"],
		undefined,
		undefined,
		91,
		undefined,
		undefined,
		undefined,
		undefined,
		undefined
	)
    assert.equal(pronosticos.length, 19040 * 2)
    var count_p10 = 0
    var count_p90 = 0
    for(const p of pronosticos) {
        assert.ok(["p10","p90"].indexOf(p.qualifier >= 0))
        assert.notEqual(parseFloat(p.valor).toString(), "NaN")
        if(p.qualifier == "p10") {
            count_p10 = count_p10 + 1
        } else if(p.qualifier == "p90") {
            count_p90 = count_p90 + 1
        } else {
            throw new assert.AssertionError("bad qualifier")
        }
    }
    assert.equal(count_p10, 19040)
    assert.equal(count_p90, 19040)
})

test('update ensemble percentiles test', async(t) => {
    const pronosticos = await CRUD.getEnsemblePercentiles(
		undefined,
		721,
		undefined,
		[0.1,0.9],
		"areal",
		undefined,
		["gep01", "gep02", "gep03", "gep04", "gep05", "gep06", "gep07", "gep08", "gep09", "gep10", "gep11", "gep12", "gep13", "gep14", "gep15", "gep16", "gep17", "gep18", "gep19", "gep20", "gep21", "gep22", "gep23", "gep24", "gep25", "gep26", "gep27", "gep28", "gep29", "gep30"],
		undefined,
		undefined,
		91,
		undefined,
		undefined,
		undefined,
		undefined,
		true,
        undefined
	)
    assert.equal(pronosticos.length, 19040 * 2)
    var count_p10 = 0
    var count_p90 = 0
    for(const p of pronosticos) {
        assert.ok(["p10","p90"].indexOf(p.qualifier >= 0))
        assert.notEqual(parseFloat(p.valor).toString(), "NaN")
        if(p.qualifier == "p10") {
            count_p10 = count_p10 + 1
        } else if(p.qualifier == "p90") {
            count_p90 = count_p90 + 1
        } else {
            throw new assert.AssertionError("bad qualifier")
        }
    }
    assert.equal(count_p10, 19040)
    assert.equal(count_p90, 19040)
})