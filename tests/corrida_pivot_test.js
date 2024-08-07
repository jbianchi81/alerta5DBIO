const test = require('node:test')
const assert = require('assert')
process.env.NODE_ENV = "test"
const {corrida: Corrida, observacionPivot: ObservacionPivot, modelo: Modelo, calibrado: Calibrado, serie: Serie} = require('../app/CRUD')
const a5_samples = require('./a5_samples')
const {ReadProcedure} = require('../app/procedures')

test('pivot corrida', (t) => {

    const corrida = new Corrida(a5_samples.corridas[0])

    const pivoted = corrida.pivot()
    for (const p of pivoted) {
        assert(p instanceof ObservacionPivot)
        assert(p.hasOwnProperty("1505"))
        assert(p.hasOwnProperty("1520"))
        assert.equal(p.toCSV().split(",").length, 4)
        assert.equal(Object.keys(p.toJSON()).filter(k =>
            (/^_/.test(k))
        ).length, 0)
    }
    assert.equal(pivoted.length, corrida.series[0].pronosticos.length)

    const respawned = JSON.parse(JSON.stringify(pivoted)).map(p=>
        new ObservacionPivot(p, {string_keys: true})
    )
    assert.equal(pivoted.length, respawned.length)
    assert.equal(JSON.stringify(pivoted), JSON.stringify(respawned))
    

})

test("read corrida pivot test", async(t)=>{
    
    await t.test("create series",async (t) => {
        const series = await Serie.create(
            [
                a5_samples.series[2],
                a5_samples.series[3]
            ],{
                upsert_estacion: true
            }
        )
        assert.equal(series.length, 2, "Expected 2 results")
    })

    await t.test("create modelo", async(t) => {
        const modelos = await Modelo.create([
            a5_samples.modelos[0]
        ])
        assert.equal(modelos.length, 1, "Expected 1 result")
        assert.equal(modelos[0].id, 27, "expected id = 27")
        assert.equal(modelos[0].nombre, "sacramento", "expected nombre = 'sacramento'")
        assert.equal(modelos[0].parametros.length, 10, "expected 10 parametros")
        assert.equal(modelos[0].forzantes.length, 6, "expected 6 forzantes")
        assert.equal(modelos[0].estados.length, 4, "expected 4 estados")
        assert.equal(modelos[0].outputs.length, 1, "expected 1 outputs")
    })

    await t.test("create calibrado", async(t) => {
        const calibrados = await Calibrado.create([
            a5_samples.calibrados[0]
        ])
        assert.equal(calibrados.length, 1, "Expected 1 result")
        assert.equal(calibrados[0].id, 32, "expected id = 32")
        assert.equal(calibrados[0].nombre, "sac", "expected nombre = 'sac'")
        assert.equal(calibrados[0].parametros.length, 10, "expected 10 parametros")
        assert.equal(calibrados[0].forzantes.length, 2, "expected 2 forzantes")
        assert.equal(calibrados[0].estados.length, 4, "expected 4 estados")
        assert.equal(calibrados[0].outputs.length,1, "expected 1 output")
    })

    await t.test("create corrida", async(t) => {
        const corridas = await Corrida.create([
            a5_samples.corridas[0]
        ])
        assert.equal(corridas.length,1, "expected 1 corrida")
        assert.equal(corridas[0].forecast_date.getTime(), new Date(a5_samples.corridas[0].forecast_date).getTime(), "bad corrida.forecast_date")
        assert.equal(corridas[0].cal_id, a5_samples.corridas[0].cal_id, "Bad corrida.cal_id")
        assert.equal(corridas[0].series.length, 2, "expected 2 series")
        for(const i in corridas[0].series) {
            assert.equal(corridas[0].series[i].pronosticos.length, a5_samples.corridas[0].series[i].pronosticos.length, "Bad series length at index " + i)
        }
    })

    await t.test("read corrida pivot", async(t) => {
        const procedure = new ReadProcedure({
            class_name: "corrida",
            filter: {
                cal_id: a5_samples.corridas[0].cal_id,
                forecast_date: a5_samples.corridas[0].forecast_date
            },
            options: {
                pivot: true,
                format: "csv"
            },
            output: "/tmp/corridas.csv"
        })
        const corridas = await procedure.run()

        console.debug({corridas:corridas})

        assert.equal(corridas.length, 1, "expecting 1 item")
        assert(corridas[0].pronosticos != undefined, "Expected defined pronosticos property")
        assert(Array.isArray(corridas[0].pronosticos))
        assert.equal(corridas[0].pronosticos.length, a5_samples.corridas[0].series[0].pronosticos.length, `Expected pronosticos length not matched (expected:${a5_samples.corridas[0].series[0].pronosticos.length}, found: ${corridas[0].pronosticos.length}`)
        for (const p of corridas[0].pronosticos) {
            assert(p instanceof ObservacionPivot)
            for(const serie of a5_samples.corridas[0].series) {
                const column_name = serie.series_id.toString()
                assert(p.hasOwnProperty(column_name), "expected property " + column_name)
            }
            assert.equal(p.toCSV().split(",").length, 4, "expected 4 csv columns")
            assert.equal(Object.keys(p.toJSON()).filter(k =>
                (/^_/.test(k))
            ).length, 0, "Underscore properties found")
        }
    
        const respawned = JSON.parse(JSON.stringify(corridas[0].pronosticos)).map(p=>
            new ObservacionPivot(p, {string_keys: true})
        )
        assert.equal(corridas[0].pronosticos.length, respawned.length, "Unexpected respawned array length")
        assert.equal(JSON.stringify(corridas[0].pronosticos), JSON.stringify(respawned), "Unexpected respawned json stringification")
    })

    await t.test("delete corrida", async(t) => {
        const corridas = await Corrida.delete({
            cal_id: a5_samples.corridas[0].cal_id,
            forecast_date: a5_samples.corridas[0].forecast_date
        })
        assert.equal(corridas.forecast_date.getTime(), new Date(a5_samples.corridas[0].forecast_date).getTime(), "bad corrida.forecast_date")
        assert.equal(corridas.cal_id, a5_samples.corridas[0].cal_id, "Bad corrida.cal_id")
    })

    await t.test("delete calibrado", async(t) => {
        const calibrados = await Calibrado.delete({id: 32})
        assert.equal(calibrados.length, 1, "expected 1 item")
        assert.equal(calibrados[0].id, 32, "expected id = 32")
    })

    await t.test("delete modelo", async(t) => {
        const modelos = await Modelo.delete({id: 27})
        assert.equal(modelos.length, 1, "expected 1 item")
        assert.equal(modelos[0].id, 27, "expected id = 27")
    })

    await t.test("delete series", async(t) => {
        const series = await Serie.delete({id: [1505, 1520]})
        assert.equal(series.length, 2, "expected 2 item")
        assert(series.map(s => s.id).indexOf(1505) >= 0, "expected to find id = 1505")
        assert(series.map(s => s.id).indexOf(1520) >= 0, "expected to find id = 1520")
    })
})