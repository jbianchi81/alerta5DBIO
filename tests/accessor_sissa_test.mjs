import test from 'node:test'
import assert from 'assert'
process.env.NODE_ENV = "test"
import accessors from "../app/accessors.js"
import {readFileSync} from "fs"

test('sissa accessor get sites', async(t) => {
    const accessor = JSON.parse(readFileSync("tmp/accessor_sissa.json"))
    const client = new accessors.sissa(accessor.config)
    const sites = await client.getSites()
    assert.ok(Array.isArray(sites))
})
