const test = require('node:test')
const assert = require('assert')
process.env.NODE_ENV = "test"
const {serie: Serie, observacion: Observacion, observaciones: Observaciones, estacion: Estacion} = require('../app/CRUD')

test('observacion crud sequence', async(t) => {
    await Serie.delete({
        id: 3281
    })
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
            "var": {
                "id": 27,
                "var": "Pi",
                "nombre": "Precipitación a intervalo nativo",
                "abrev": "precip_inst",
                "type": "num",
                "datatype": "Incremental",
                "valuetype": "Field Observation",
                "GeneralCategory": "Climate",
                "VariableName": "Precipitation",
                "SampleMedium": "Precipitation",
                "def_unit_id": "9",
                "timeSupport": null,
                "def_hora_corte": null
            },
            "procedimiento": {
                "id": 1,
                "nombre": "medición directa",
                "abrev": "medicion",
                "descripcion": "Medición directa"
            },
            "unidades": {
                "id": 9,
                "nombre": "milímetros",
                "abrev": "mm",
                "UnitsID": 54,
                "UnitsType": "Length"
            },
            "fuente": {},
            "date_range": {
                "timestart": null,
                "timeend": null,
                "count": null,
                "data_availability": "N"
            },
            "observaciones": [
                {
                    "timestart": "2027-03-03T03:08:00.000Z",
                    "timeend": "2027-03-03T03:08:00.000Z",
                    "valor": 398.53
                },
                {
                    "timestart": "2027-08-13T03:00:00.000Z",
                    "timeend": "2027-08-13T03:00:00.000Z",
                    "valor": 0
                },
                {
                    "timestart": "2030-02-16T15:21:00.000Z",
                    "timeend": "2030-02-16T15:21:00.000Z",
                    "valor": 310.13
                }
            ],
            "pronosticos": null
        },{
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

    await t.test("create obs, delete in batches", async() => {
        const sequence = Array.from({ length: 10 }, (_, i) => {
            return {
                tipo: "puntual",
                series_id: 3281,
                timestart: new Date(2000,0,i + 1),
                timeend: new Date(2000,0,i + 1),
                valor: Math.random()
            }
        })
        const observaciones = await Observaciones.create(sequence)
        assert.equal(observaciones.length, 10)
        
        const deleted = await Observacion.delete({
            tipo: "puntual",
            series_id: 3281
        },{
            batch_size: 5
        })
        assert.equal(deleted.length, 10)
    })

    await t.test("create obs, delete in batches, no_send_data (return count)", async() => {
        const sequence = Array.from({ length: 1000 }, (_, i) => {
            return {
                tipo: "puntual",
                series_id: 3281,
                timestart: new Date(2000,0,i + 1),
                timeend: new Date(2000,0,i + 1),
                valor: Math.random()
            }
        })
        const observaciones = await Observaciones.create(sequence)
        assert.equal(observaciones.length, 1000)
        
        const deleted = await Observacion.delete({
            tipo: "puntual",
            series_id: 3281
        },{
            batch_size: 50,
            no_send_data: true
        })
        assert.equal(deleted, 1000)
    })

    await t.test("filter by time", async(t)=> {
        const sequence = Array.from({ length: 24 * 10 }, (_, i) => {
            return {
                tipo: "puntual",
                series_id: 3281,
                timestart: new Date(2000,0,1,i),
                timeend: new Date(2000,0,1,i),
                valor: Math.random()
            }
        })
        const observaciones = await Observaciones.create(sequence)
        assert.equal(observaciones.length, 24 * 10)

        const get_filtered = await Observaciones.read({
            tipo: "puntual",
            series_id: 3281,
            time: '00:00'
        })
        assert.equal(get_filtered.length, 10)
        for(var i=0;i<get_filtered.length;i++) {
            assert.equal(get_filtered[i].timestart.getHours(), 0)
        }

        const deleted = await Observacion.delete({
            tipo: "puntual",
            series_id: 3281,
            time: '00:00'
        },{
            batch_size: 50
        })
        assert.equal(deleted.length, 10)
        for(var i=0;i<deleted.length;i++) {
            assert.equal(deleted[i].timestart.getHours(), 0)
        }

        const get_filtered_w_list = await Observaciones.read({
            tipo: "puntual",
            series_id: 3281,
            time: ['09:00', '12:00']
        })
        assert.equal(get_filtered_w_list.length, 20)
        for(var i=0;i<get_filtered_w_list.length;i++) {
            assert.notEqual([9,12].indexOf(get_filtered_w_list[i].timestart.getHours()), -1)
        }

        const deleted_filtered_w_list = await Observacion.delete({
            tipo: "puntual",
            series_id: 3281,
            time: ['09:00','12:00']
        },{
            batch_size: 50
        })
        assert.equal(deleted_filtered_w_list.length, 20)
        for(var i=0;i<deleted_filtered_w_list.length;i++) {
            assert.notEqual([9,12].indexOf(deleted_filtered_w_list[i].timestart.getHours()), -1)
        }

        const get_filtered_w_list_not = await Observaciones.read({
            tipo: "puntual",
            series_id: 3281,
            time_not: ['10:00', '13:00','16:00']
        })
        assert.equal(get_filtered_w_list_not.length, 10 * 18)
        for(var i=0;i<get_filtered_w_list_not.length;i++) {
            assert.notEqual(get_filtered_w_list_not[i].timestart.getHours(), 10)
            assert.notEqual(get_filtered_w_list_not[i].timestart.getHours(), 13)
            assert.notEqual(get_filtered_w_list_not[i].timestart.getHours(), 16)
        }

        const deleted_filtered_w_list_not = await Observacion.delete({
            tipo: "puntual",
            series_id: 3281,
            time_not: ['10:00','13:00','16:00']
        },{
            batch_size: 50
        })
        assert.equal(deleted_filtered_w_list_not.length, 10 * 18)
        for(var i=0;i<deleted_filtered_w_list_not.length;i++) {
            assert.notEqual(deleted_filtered_w_list_not[i].timestart.getHours(), 10)
            assert.notEqual(deleted_filtered_w_list_not[i].timestart.getHours(), 13)
            assert.notEqual(deleted_filtered_w_list_not[i].timestart.getHours(), 16)
        }

        const get_filtered_time_not = await Observaciones.read({
            tipo: "puntual",
            series_id: 3281,
            time_not: '10:00'
        })
        assert.equal(get_filtered_time_not.length, 10 * 2)
        for(var i=0;i<get_filtered_time_not.length;i++) {
            assert.notEqual(get_filtered_time_not[i].timestart.getHours(), 10)
        }

        const deleted_time_not = await Observacion.delete({
            tipo: "puntual",
            series_id: 3281,
            time_not: '10:00'
        },{
            batch_size: 50
        })
        assert.equal(deleted_time_not.length, 10 * 2)
        for(var i=0;i<deleted_time_not.length;i++) {
            assert.notEqual(deleted_time_not[i].timestart.getHours(), 10)
        }

        await Observacion.delete({
            tipo: "puntual",
            series_id: 3281
        })
    })

    await t.test("delete estacion", async(t)=> {
        const deleted = await Estacion.delete({
            "tabla": "red_acumar",
            "id_externo": "http://www.bdh.acumar.gov.ar/bdh3/meteo/boca/downld08.txt"
        })
        assert.equal(deleted.length, 1, "One station deleted")
    })

})