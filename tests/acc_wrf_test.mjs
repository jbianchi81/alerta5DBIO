import test from 'node:test'
import assert from 'assert'
process.env.NODE_ENV = "test"
import { Client } from "../app/accessors/wrf.js"

test('wrf accessor client.getPronostico', async(t) => {
    const client = new Client({
        host: "0.0.0.0",
        user: "user",
        password: "password",
        filepath: "/filepath.grb",
        cal_id: 722,
        series_id: 88
    })
    const corrida = await client.updatePronostico()
    assert.equal(corrida.cal_id, 875)
    assert.equal(corrida.pronosticos.length, 73)
    assert.equal(corrida.forecast_date.getTime(), new Date(2026,9,6,3).getTime())
})    