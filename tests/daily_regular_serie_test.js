const test = require('node:test')
const assert = require('assert')
process.env.NODE_ENV = "test"
const {CRUD, serie: Serie, observaciones: Observaciones, estacion: Estacion} = require('../app/CRUD')

function timeout(ms = 500) {
    return new Promise((_, reject) => 
        setTimeout(() => reject(new Error(`Operation timed out after ${ms}ms`)), ms)
    );
}

function timeoutResolve(ms = 500) {
    return new Promise((resolve, _) => 
        setTimeout(() => resolve(`Operation resolved after ${ms}ms`), ms)
    );
}


test('insert daily', async(t) => {

    await t.test("test timeout", async(t) => {
        var ms = 100
        try {
            await timeout(ms)
        } catch(e) {
            console.log("Catched: " + e.toString())
            assert.equal(e.toString(), `Error: Operation timed out after ${ms}ms`)
        }

        assert.rejects(timeout)
    })

    await t.test("test timeoutResolve", async()=> {

        var message =  await timeoutResolve(1000)
        console.log(message)
        assert.equal(message, "Operation resolved after 1000ms")

        assert.doesNotReject(timeoutResolve)
    })

    await t.test("test race", async ()=> {
        var ms = 5
        try {
            await Promise.race([
                timeoutResolve(5000),
                timeout(ms)
            ])
            assert.ok(false, "Should have thrown")
        } catch(e) {
            console.log("Catched: " + e.toString())
            assert.equal(e.toString(), `Error: Operation timed out after ${ms}ms`)
        }

        assert.rejects(async () => Promise.race([
            timeoutResolve(5000),
            timeout(5)
        ]))
        console.log("test race ok")
    })

    var serie
    await t.test("create serie", async(t) => {
        console.log("test console log")

        const series = await Serie.create({
            "tipo": "puntual",
            "id": 1111,
            "estacion": {
                "id": 1111,
                "nombre": "Test",
                "id_externo": "1111",
                "geom": {
                    "type": "Point",
                    "coordinates": [
                        -1.1,
                        -2.2
                    ]
                },
                "tabla": "alturas_varios",
            },
            "var_id": 2,
            "proc_id": 1,
            "unit_id": 11
        },{
            upsert_estacion: true
        })
        assert.equal(series.length, 1, "Length of created series must equal 1")
        serie = series[0]
        assert.equal(serie.id, 1111, "id of created serie must be 1111")
        assert.equal(serie.tipo, "puntual", "tipo of serie must be puntual")
    })

    var serie_dia
    await t.test("create serie diaria", async(t) => {

        const series = await Serie.create({
            "tipo": "puntual",
            "id": 1112,
            "estacion_id": 1111,
            "var_id": 39,
            "proc_id": 1,
            "unit_id": 11
        })
        assert.equal(series.length, 1, "Length of created series must equal 1")
        serie_dia = series[0]
        assert.equal(serie_dia.id, 1112, "id of created serie must be 1111")
        assert.equal(serie_dia.tipo, "puntual", "tipo of serie must be puntual")
    })


    await t.test("create sub-daily observations", async t=>{
        const sequence = Array.from({ length: 365 * 4 }, (_, i) => {
            return {
                tipo: "puntual",
                series_id: 1111,
                timestart: new Date(2008,9,15,i * 6),
                timeend: new Date(2008,9,15,i * 6),
                valor: Math.random()
            }
        })

        console.log("test console log")
        
        const observaciones = await Observaciones.create(sequence)

        assert.equal(observaciones.length, 365 * 4)

    })

    await t.test("create regular daily observations", async t=>{

        const daily_obs = await CRUD.getRegularSeries(
                "puntual",
                1111,
                "1 days",
                new Date(2008,9,15,0),
                new Date(2009,9,15,0),
                {
                    agg_function: "mean", 
                    t_offset: "00:00:00", 
                    inst: true,
                    insertSeriesId: 1112
                }
            )

        assert.ok(daily_obs)

        assert.equal(daily_obs.length, 365)
        // for(var i=0;i<daily_obs.length;i++) {
            // assert.equal(daily_obs[i].timestart.getHours(), 0, `Bad hours for i: ${i}, timestart: ${daily_obs[i].timestart.toISOString()}`)
            // daily_obs[i].series_id = 1112
            // daily_obs[i].tipo = "puntual"
        // }

        // const upserted = await Observaciones.create(daily_obs)

        // assert.equal(upserted.length, 365)


    })

    await t.test("delete all", async ()=> {
        await Serie.delete({tipo:"puntual",id:1111})
        await Serie.delete({tipo:"puntual",id:1112})
        await Estacion.delete({id:1111})
    })
})
