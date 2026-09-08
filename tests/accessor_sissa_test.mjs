import test from 'node:test'
import assert from 'assert'
process.env.NODE_ENV = "test"
import accessors from "../app/accessors.js"
import {readFileSync, writeFileSync} from "fs"
import { AssertionError } from 'assert/strict'

test('sissa accessor get sites', async(t) => {
    const accessor = JSON.parse(readFileSync("../tmp/accessor_sissa.json"))
    const client = new accessors.sissa(accessor.config)
    const sites = await client.getSites()
    assert.ok(Array.isArray(sites))
    writeFileSync("../tmp/sites_sissa.json", JSON.stringify(sites, undefined, 2))
})

test('sissa get_registros_diarios all variables', async(t) => {
    const accessor = JSON.parse(readFileSync("../tmp/accessor_sissa.json"))
    const client = new accessors.sissa(accessor.config)
    const ts = new Date()
    ts.setDate(ts.getDate() - 7)
    const te = new Date()
    try {
        const registros_diarios = await client.get_registros_diarios(
            87586, // 6000030,// 6000029, 
            undefined, 
            ts.toISOString().substring(0,10), 
            te.toISOString().substring(0,10)
        )
        assert.ok(Array.isArray(registros_diarios))
        writeFileSync(
            "../tmp/registros_diarios_sissa.json", 
            JSON.stringify(registros_diarios, undefined, 2)
        )
    } catch (e) {
        console.error(e)
        throw AssertionError("Request failed")
    }
})
