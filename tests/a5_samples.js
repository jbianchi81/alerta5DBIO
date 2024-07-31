const internal = {}

internal.series = [
    {
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
    },
    {
        "tipo": "puntual",
        "id": 3282,
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
            "tabla": "red_acumar"
        },
        "var": {
            "id": 31
        },
        "procedimiento": {
            "id": 1
        },
        "unidades": {
            "id": 9
        }    
    }
]

internal.asociaciones = [
    {
        "source_tipo": "puntual",
        "source_series_id": 3281,
        "dest_tipo": "puntual",
        "dest_series_id": 3282,
        "dt": {
            "hours": 1
        },
        "agg_func": "sum",
        "source_is_inst": true
    }
]

internal.corridas = [
    {
        "id": 772021,
        "forecast_date": "2024-07-31T14:36:27.000Z",
        "series": [
            {
                "series_table": "series",
                "series_id": 1505,
                "cor_id": 772021,
                "qualifier": null,
                "pronosticos": [
                    {
                        "id": null,
                        "timestart": "2024-07-31T12:00:00.000Z",
                        "timeend": "2024-07-31T12:00:00.000Z",
                        "valor": 9.2441,
                        "qualifier": "main",
                        "cor_id": null,
                        "series_id": null,
                        "series_table": null
                    },
                    {
                        "id": null,
                        "timestart": "2024-08-01T12:00:00.000Z",
                        "timeend": "2024-08-01T12:00:00.000Z",
                        "valor": 8.6327,
                        "qualifier": "main",
                        "cor_id": null,
                        "series_id": null,
                        "series_table": null
                    }
                ],
                "var_id": null,
                "begin_date": null,
                "end_date": null,
                "qualifiers": null,
                "count": null,
                "estacion_id": null
            },
            {
                "series_table": "series",
                "series_id": 1520,
                "cor_id": 772021,
                "qualifier": null,
                "pronosticos": [
                    {
                        "id": null,
                        "timestart": "2024-07-31T12:00:00.000Z",
                        "timeend": "2024-07-31T12:00:00.000Z",
                        "valor": 0.8,
                        "qualifier": "main",
                        "cor_id": null,
                        "series_id": null,
                        "series_table": null
                    },
                    {
                        "id": null,
                        "timestart": "2024-08-01T12:00:00.000Z",
                        "timeend": "2024-08-01T12:00:00.000Z",
                        "valor": 0.77,
                        "qualifier": "main",
                        "cor_id": null,
                        "series_id": null,
                        "series_table": null
                    }
                ],
                "var_id": null,
                "begin_date": null,
                "end_date": null,
                "qualifiers": null,
                "count": null,
                "estacion_id": null
            }
        ],
        "cal_id": 32
    }
]

module.exports = internal