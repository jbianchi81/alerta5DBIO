import test from 'node:test'
import assert from 'assert'
process.env.NODE_ENV = "test"
import { Client } from "../app/accessors/gefs_atmos.js"
import accessor_pkg from "../app/accessors.js"
const Accessor = accessor_pkg.Accessor
// import { serie as CrudSerie } from "../app/CRUD"
import {readFile, writeFile} from 'fs/promises' 
import {parseUtcDateTime} from "../app/accessors/accessor_utils.js"

test('gefs atmos create serie', async(t) => {
    const client = new Client({})
    const serie = await client.createSerie()
    assert.equal(serie.id, 55)
    assert.equal(serie.estacion.id,25)
    assert.equal(serie.fuente.id, 55)
})


test('gefs atmos get', async(t) => {
    const client = new Client({})
    const result = await client.get()
    assert.ok(Array.isArray(result))
    assert.ok(result.length)
})