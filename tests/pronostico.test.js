const test = require('node:test')
const assert = require('assert')
process.env.NODE_ENV = "test"
const {serie: Serie, modelo: Modelo, calibrado: Calibrado, pronostico: Pronostico, corrida: Corrida, estacion: Estacion} = require('../app/CRUD')
// const {CreateProcedure} = require('../app/crud_procedures')

test('pronostico crud sequence', async(t) => {

    try {
        await Calibrado.delete({id:4563466})
        await Calibrado.delete({id: 544})
        await Modelo.delete({id: 305}) 
        await Serie.delete({
            id: 3281
        })
    } catch(e) {
        // console.debug(e.toString())
    }


    var serie
    await t.test("create serie", async(t) => {
        const series = await Serie.create({
            "tipo": "puntual",
            "id": 3281,
            "estacion": {
                "nombre": "La Boca",
                "id_externo": "http://www.bdh.acumar.gov.ar/bdh3/meteo/boca/downld08.txt",
                "geom": {
                    "type": "Point",
                    "coordinates": [
                        -58.358055556,
                        -34.636666667
                    ]
                },
                "tabla": "red_acumar",
                "pais": "Argentina",
                "rio": "null",
                "has_obs": true,
                "tipo": "M",
                "automatica": true,
                "habilitar": false,
                "propietario": "ACUMAR",
                "abreviatura": "LABOCA",
                "localidad": "null",
                "real": true,
                "nivel_alerta": null,
                "nivel_evacuacion": null,
                "nivel_aguas_bajas": null,
                "altitud": null,
                "public": true,
                "cero_ign": null
            },
            "var": {"id":31,"var":"Ph","nombre":"precipitación horaria","abrev":"precip_horaria","type":"num","datatype":"Succeeding Total","valuetype":"Field Observation","GeneralCategory":"Climate","VariableName":"Precipitation","SampleMedium":"Precipitation","def_unit_id":"9","timeSupport":{"years":0,"months":0,"days":0,"hours":1,"minutes":0,"seconds":0,"milliseconds":0}},
            "procedimiento": {"id":4,"nombre":"Simulado","abrev":"sim","descripcion":"Simulado mediante un modelo"},
            "unidades": {
                "id": 9,
                "nombre": "milímetros",
                "abrev": "mm",
                "UnitsID": 54,
                "UnitsType": "Length"
            },
            "date_range": {
                "timestart": null,
                "timeend": null,
                "count": null,
                "data_availability": "N"
            }
        },{
            upsert_estacion: true
        })
        assert.equal(series.length, 1, "Length of created series must equal 1")
        serie = series[0]
        assert.equal(serie.id, 3281, "id of created serie must be 3281")
        assert.equal(serie.tipo, "puntual", "tipo of serie must be puntual")
    })

    await t.test("create modelo y calibrado", async(t) => {
        const modelos = await Modelo.create({id: 305, nombre: "test", tipo: "T","def_var_id": 31, "def_unit_id": 9}) 
        assert.equal(modelos.length,1)
        const modelo = modelos[0]
        assert.equal(modelo.id, 305)
        assert.equal(modelo.nombre, "test")
        const calibrados = await Calibrado.create({id: 544, model_id: modelo.id, nombre: "test"})
        assert.equal(calibrados.length,1)
        const calibrado = calibrados[0]
        assert.equal(calibrado.id, 544)
        assert.equal(calibrado.model_id, modelo.id)
        assert.equal(calibrado.nombre, "test")
    })

    // await t.test("create corrida", async(t) => {
    //     const procedure = new CreateProcedure({
    //         class_name: "corrida",
    //         csvfile: "tests/data/csv/corrida.csv"})
    //     await procedure.run()
    //     assert.equal(procedure.result.length,1)
    //     const corrida = procedure.result[0]
    //     assert.equal(corrida.cal_id, 544)
    //     assert.equal(corrida.forecast_date.getTime(), Date(2023,3,23,0,0,0).getTime())
    //     assert.equal(corrida.series.length,1)
    //     assert.equal(corrida.series[0].series_table, "series")
    //     assert.equal(corrida.series[0].series_id, 3281)
    //     assert.equal(corrida.series[0].pronosticos.length,74)
    //     assert.equal(corrida.series[0].pronosticos[3].timestart.getTime(), Date(2023,3,23,0,0,0).getTime())
    // })

    // var corrida
    // await t.test("read corrida", async(t) => {
    //     const corridas = await Corrida.read({
    //         cal_id: 544,
    //         forecast_date: Date(2023,3,23,0,0,0),
    //         series_id: 3281,
    //         tipo: "puntual"
    //     })
    //     assert.equal(corridas.length, 1)
    //     corrida = corridas[0]
    //     assert.equal(corrida.cal_id, 544)
    //     assert.equal(corrida.forecast_date.getTime(), Date(2023,3,23,0,0,0).getTime())
    //     assert.equal(corrida.series.length,1)
    //     assert.equal(corrida.series[0].series_table, "series")
    //     assert.equal(corrida.series[0].series_id, 3281)
    //     assert.equal(corrida.series[0].pronosticos.length,74)
    //     assert.equal(corrida.series[0].pronosticos[3].timestart.getTime(), Date(2023,3,23,0,0,0).getTime())
    // })

    // await t.test("Delete pronosticos", async(t) => {
    //     const deleted = await Pronostico.delete(
    //         {
    //             cal_id: 544,
    //             forecast_date: Date(2023,3,23,0,0,0),
    //             series_id: 3281,
    //             timestart: new Date("2023-04-23T00:00:00.000Z"),
    //             timeend: new Date("2023-04-23T05:00:00.000Z")
    //         }
    //     )
    //     assert.equal(deleted.length,6)
    //     for(const prono of deleted) {
    //         assert.equal(prono.series_id,3281)
    //         assert.equal(prono.cor_id, corrida.id)
    //         assert(prono.timestart.getTime() >= new Date("2023-04-23T00:00:00.000Z").getTime())
    //         assert(prono.timestart.getTime() <= new Date("2023-04-23T05:00:00.000Z").getTime())
    //     }
    // })

    // await t.test("Read remaining pronos", async(t) => {
    //     const pronosticos = await Pronostico.read({
    //         cal_id: 544,
    //         forecast_date: Date(2023,3,23,0,0,0),
    //         series_id: 3281
    //     })
    //     assert.equal(pronosticos.length, 68)
    // })

    // await t.test("Delete with fake cal_id, no obs", async(t) => {
    //     const deleted = await Pronostico.delete(
    //         {
    //             cal_id: 4563466
    //         }
    //     )
    //     assert.equal(deleted.length, 0)
    // })

    // await t.test("Delete corrida fake id, nothing deleted", async(t) => {
    //     const deleted = await Corrida.delete({id: 65745745})
    //     assert.equal(deleted.length, 0)
    // })

    // await t.test("Delete corrida", async(t) => {
    //     const deleted = await Corrida.delete({id: corrida.id})
    //     assert.equal(deleted.length, 1)
    //     assert.equal(deleted[0].id, corrida.id)
    //     assert.equal(deleted[0].cal_id, 544)
    //     assert.equal(deleted[0].forecast_date.getTime(), Date(2023,3,23,0,0,0).getTime())
    // })

    //   await t.test("delete modelo y calibrado", async(t) => {
    //     const deleted_calibrado = await Calibrado.delete({id: 544})
    //     assert.equal(deleted_calibrado.length, 1)
    //     const deleted_modelo = await Modelo.create({id: 305}) 
    //     assert.equal(deleted_modelo.length,1)
    //     assert.equal(deleted_calibrado.model_id, deleted_modelo.id)
    // })
  
    // await t.test("delete estacion", async(t)=> {
    //     const deleted = await Estacion.delete({
    //         "tabla": "red_acumar",
    //         "id_externo": "http://www.bdh.acumar.gov.ar/bdh3/meteo/boca/downld08.txt"
    //     })
    //     assert.equal(deleted.length, 1, "One station deleted")
    // })

})