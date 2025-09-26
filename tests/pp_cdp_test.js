const test = require('node:test')
const assert = require('assert')
// process.env.NODE_ENV = "test"
const {CRUD: Crud} = require('../app/CRUD')

test('get pp cdp batch', async(t) => {
    await t.test("get_pp_cdp_batch", async t => {
        const result = await Crud.get_pp_cdp_batch(
            new Date(2025,0,1,9),
            new Date(2025,0,5,9),
            {},
            {},
            false)
        assert.equal(result.length,5)
        for(const obs of result) {
            assert.equal(obs.type, "pp_cdp_diario")
            assert("files" in obs)
            assert("nearest_tif" in obs.files)
        }
    })
})

test('get pp cdp batch con insert', async(t) => {
    await t.test("get_pp_cdp_batch", async t => {
        const result = await Crud.get_pp_cdp_batch(
            new Date(2025,0,1,9),
            new Date(2025,0,5,9),
            {},
            {},
            true)
        assert.equal(result.length,5)
        for(const obs of result) {
            assert.equal(obs.type, "pp_cdp_diario")
            assert("files" in obs)
            assert("nearest_tif" in obs.files)
        }
    })
})