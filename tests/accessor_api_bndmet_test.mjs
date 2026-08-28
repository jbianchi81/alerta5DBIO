import test from 'node:test'
import assert from 'assert'
process.env.NODE_ENV = "test"
import { Client } from "../app/accessors/api_bndmet.js"
import {readFile, writeFile} from 'fs/promises' 
import {parseUtcDateTime} from "../app/accessors/accessor_utils.js"


test('api-bndmet a5 accessor getSites getSeries', async(t) => {
    const accessor_ = await readFile("tmp/accessor-api-bndmet.json", {encoding: "utf-8"})
    const accessor = JSON.parse(accessor_)
    const client = new Client(accessor.config)
    
    const sites = await client.getSites({})

    const series = await client.getSeries({})
})
