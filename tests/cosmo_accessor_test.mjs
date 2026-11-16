import test from 'node:test'
import assert from 'assert'
process.env.NODE_ENV = "test"
import { Client } from "../app/accessors/cosmo.js"

test('cosmo accessor client.getPronostico', async(t) => {
    const client = new Client({
        url: "/home/alerta7/git_clones/tomas_parana/data/cosmo",
        dir_1d: "/home/alerta7/git_clones/alerta5DBIO/data/cosmo/1d",
        dir_3h: "/home/alerta7/git_clones/alerta5DBIO/data/cosmo/3h",
        series_id_1d: 28,
        series_id_3h: 27,
        cal_id: 724
    })
    const corrida = await client.getPronostico()
    assert.equal(corrida.cal_id, 724)
    assert.equal(corrida.series.length, 2)
    assert.equal(corrida.forecast_date.getTime(), new Date(Date.UTC(2026,10,16,0)).getTime())
})    