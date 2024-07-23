const test = require('node:test')
const assert = require('assert')
const {serie: Serie, observacion: Observacion} = require('../app/CRUD')

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
                "id": 872,
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
                    "id": 10,
                    "tipo": "puntual",
                    "series_id": 3281,
                    "timestart": "2027-03-03T03:08:00.000Z",
                    "timeend": "2027-03-03T03:08:00.000Z",
                    "nombre": "upsertObservacionesPuntual",
                    "descripcion": null,
                    "unit_id": null,
                    "timeupdate": "2024-07-23T18:21:46.563Z",
                    "valor": 398.53,
                    "stats": null
                },
                {
                    "id": 11,
                    "tipo": "puntual",
                    "series_id": 3281,
                    "timestart": "2027-08-13T03:00:00.000Z",
                    "timeend": "2027-08-13T03:00:00.000Z",
                    "nombre": "upsertObservacionesPuntual",
                    "descripcion": null,
                    "unit_id": null,
                    "timeupdate": "2024-07-23T18:21:46.563Z",
                    "valor": 0,
                    "stats": null
                },
                {
                    "id": 12,
                    "tipo": "puntual",
                    "series_id": 3281,
                    "timestart": "2030-02-16T15:21:00.000Z",
                    "timeend": "2030-02-16T15:21:00.000Z",
                    "nombre": "upsertObservacionesPuntual",
                    "descripcion": null,
                    "unit_id": null,
                    "timeupdate": "2024-07-23T18:21:46.563Z",
                    "valor": 310.13,
                    "stats": null
                }
            ],
            "pronosticos": null
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

    await t.test("Delete observaciones", {TODO: true}, async(t) => {
        return
    })
})