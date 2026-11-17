import test from 'node:test'
import assert from 'assert'
process.env.NODE_ENV = "test"
import { Client } from "../app/accessors/cosmo.js"

test('cosmo accessor client.getPronostico', async(t) => {
    const client = new Client({
        url: "/home/juan-bianchi/tomas_parana/data/cosmo",
        dir_1d: "/home/juan-bianchi/alerta5DBIO/data/cosmo/1d",
        dir_3h: "/home/juan-bianchi/alerta5DBIO/data/cosmo/3h",
        series_id_1d: 28,
        series_id_3h: 27,
        cal_id: 724
    })
    const corrida = await client.getPronostico( {
        forecast_date: new Date(Date.UTC(2026,10, 13, 0))
    })
    assert.equal(corrida.cal_id, 724)
    assert.equal(corrida.series.length, 2)
    for(const serie of corrida.series) {
        assert.ok([27,28].indexOf(serie.series_id) >= 0)
        assert.ok(serie.pronosticos.length)
        if(serie.series_id == 27) {
            assert.equal(serie.pronosticos.length, 58)
        } else if (serie.series_id == 28) {
            assert.equal(serie.pronosticos.length, 6)
        } 
    }
    assert.equal(corrida.forecast_date.getTime(), new Date(Date.UTC(2026,10,13,0)).getTime())
})

test('cosmo accessor client.updatePronostico', async(t) => {
    const client = new Client({
        url: "/home/juan-bianchi/tomas_parana/data/cosmo",
        dir_1d: "/home/juan-bianchi/alerta5DBIO/data/cosmo/1d",
        dir_3h: "/home/juan-bianchi/alerta5DBIO/data/cosmo/3h",
        series_id_1d: 28,
        series_id_3h: 27,
        cal_id: 724
    })
    const corrida = await client.updatePronostico({
        forecast_date: new Date(Date.UTC(2026,10, 13, 0))
    })
    assert.equal(corrida.cal_id, 724)
    assert.equal(corrida.series.length, 2)
    for(const serie of corrida.series) {
        assert.ok([27,28].indexOf(serie.series_id) >= 0)
        assert.ok(serie.pronosticos.length)
        if(serie.series_id == 27) {
            assert.equal(serie.pronosticos.length, 58)
        } else if (serie.series_id == 28) {
            assert.equal(serie.pronosticos.length, 6)
        } 
    }
    assert.equal(corrida.forecast_date.getTime(), new Date(Date.UTC(2026,10,13,0)).getTime())
})