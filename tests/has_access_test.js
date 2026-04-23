const {CRUD} = require("../app/CRUD")
const test = require('node:test')
const assert = require('assert')

test("test_has_access", async () => {
    // test user with assigned write access to red
    const has_access = await CRUD.hasAccess(undefined,'estaciones_virtuales', 8, true,undefined,undefined,undefined)
    assert(has_access)
})