const test = require('node:test')
const assert = require('assert')
process.env.NODE_ENV = "test"
const {serie: Serie, observacion: Observacion, estacion: Estacion} = require('../app/CRUD')
const a5_samples = require('./a5_samples')

test('observacion crud sequence', async(t) => {
    await Serie.delete({
        id: 3281
    })
    var serie
    await t.test("create serie", async(t) => {
        const series = await Serie.create(a5_samples.series[0],{
            upsert_estacion: true
        })
        assert.equal(series.length, 1, "Length of created series must equal 1")
        serie = series[0]
        assert.equal(serie.id, 3281, "id of created serie must be 3281")
        assert.equal(serie.tipo, "puntual", "tipo of serie must be puntual")
        assert.equal(serie.observaciones.length, 3, "length of serie.observaciones must be 3")
    })

    await t.test("read observaciones", async(t) => {
        const observaciones = await Observacion.read({
            series_id: 3281,
            tipo: "puntual"
        })
        assert.equal(observaciones.length, 3, "Length of read observaciones must equal 3")
        for(const observacion of observaciones) {
            assert.equal(observacion.series_id, 3281, "id of observacion must be 3281")
            assert.equal(observacion.tipo, "puntual", "tipo of observacion must be puntual")
            assert.notEqual(parseFloat(observacion.valor).toString(), "NaN", "valor of observacion must be a float")
        }
    })

    await t.test("Delete observaciones", async(t) => {
        const deleted = await Observacion.delete(
            {
                series_id: 3281,
                timestart: new Date("2027-08-13T03:00:00.000Z"),
                timeend: new Date("2027-08-13T03:00:00.000Z")
            }
        )
        assert.equal(deleted.length,1, "Deleted observaciones must be of length 1")
        for(const obs of deleted) {
            assert.equal(obs.series_id,3281, "deleted observaciones must have series_id=3281")
            assert(obs.timestart.getTime() >= new Date("2027-08-13T03:00:00.000Z").getTime(), "deleted observaciones must have timestart>='2027-08-13T03:00:00.000Z'")
            assert(obs.timestart.getTime() <= new Date("2027-08-13T03:00:00.000Z").getTime(), "deleted observaciones must have timestart<='2027-08-13T03:00:00.000Z'")
        }
    })

    await t.test("Read remaining obs", async(t) => {
        const observaciones = await Observacion.read({
            series_id: 3281
        })
        assert.equal(observaciones.length, 2, "Length of remaining obs must be 2")
    })

    await t.test("Delete with fake tabla, no obs", async(t) => {
        const deleted = await Observacion.delete(
            {
                tabla: "fake",
                timestart: new Date("2027-03-03T03:08:00.000Z"),
                timeend: new Date("2030-02-16T15:21:00.000Z")
            }
        )
        assert.equal(deleted.length, 0, "Deleted observaciones must be of length 0")
    })

    await t.test("Delete with tabla, 2 obs", async(t) => {
        const deleted = await Observacion.delete(
            {
                tabla: "red_acumar",
                timestart: new Date("2027-03-03T03:08:00.000Z"),
                timeend: new Date("2030-02-16T15:21:00.000Z")
            }
        )
        assert.equal(deleted.length, 2, "Deleted observaciones must be of length 2")
    })

    await t.test("delete estacion", async(t)=> {
        const deleted = await Estacion.delete({
            "tabla": "red_acumar",
            "id_externo": "http://www.bdh.acumar.gov.ar/bdh3/meteo/boca/downld08.txt"
        })
        assert.equal(deleted.length, 1, "One station deleted")
    })

})