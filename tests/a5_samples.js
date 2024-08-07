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
    },
    {
        "tipo": "puntual",
        "id": 1520,
        "estacion": {
            "id": 2,
            "nombre": "Rosario del Tala",
            "id_externo": "3004",
            "geom": {
                "type": "Point",
                "coordinates": [
                    -59.0768055555556,
                    -32.3085
                ]
            },
            "tabla": "alturas_bdhi",
            "pais": "Argentina",
            "rio": "GUALEGUAY",
            "has_obs": true,
            "tipo": "H",
            "automatica": false,
            "habilitar": true,
            "propietario": null,
            "abreviatura": "TALA",
            "localidad": null,
            "real": true,
            "nivel_alerta": 7,
            "nivel_evacuacion": 8.3,
            "nivel_aguas_bajas": null,
            "altitud": null,
            "public": true,
            "cero_ign": null
        },
        "var": {
            "id": 2,
            "var": "H",
            "nombre": "Altura hidrométrica",
            "abrev": "altura",
            "type": "num",
            "datatype": "Continuous",
            "valuetype": "Field Observation",
            "GeneralCategory": "Hydrology",
            "VariableName": "Gage height",
            "SampleMedium": "Surface Water",
            "def_unit_id": "11",
            "timeSupport": {
                "years": 0,
                "months": 0,
                "days": 0,
                "hours": 0,
                "minutes": 0,
                "seconds": 0,
                "milliseconds": 0
            },
            "def_hora_corte": null
        },
        "procedimiento": {
            "id": 4,
            "nombre": "Simulado",
            "abrev": "sim",
            "descripcion": "Simulado mediante un modelo"
        },
        "unidades": {
            "id": 11,
            "nombre": "metros",
            "abrev": "m",
            "UnitsID": 52,
            "UnitsType": "Length"
        },
    },
    {
        "tipo": "puntual",
        "id": 1505,
        "estacion": {
            "id": 2,
            "nombre": "Rosario del Tala",
            "id_externo": "3004",
            "geom": {
                "type": "Point",
                "coordinates": [
                    -59.0768055555556,
                    -32.3085
                ]
            },
            "tabla": "alturas_bdhi",
            "pais": "Argentina",
            "rio": "GUALEGUAY",
            "has_obs": true,
            "tipo": "H",
            "automatica": false,
            "habilitar": true,
            "propietario": null,
            "abreviatura": "TALA",
            "localidad": null,
            "real": true,
            "nivel_alerta": 7,
            "nivel_evacuacion": 8.3,
            "nivel_aguas_bajas": null,
            "altitud": null,
            "public": true,
            "cero_ign": null
        },
        "var": {
            "id": 4,
            "var": "Q",
            "nombre": "Caudal",
            "abrev": "caudal",
            "type": "num",
            "datatype": "Continuous",
            "valuetype": "Derived Value",
            "GeneralCategory": "Hydrology",
            "VariableName": "Discharge",
            "SampleMedium": "Surface Water",
            "def_unit_id": "10",
            "timeSupport": {
                "years": 0,
                "months": 0,
                "days": 0,
                "hours": 0,
                "minutes": 0,
                "seconds": 0,
                "milliseconds": 0
            },
            "def_hora_corte": null
        },
        "procedimiento": {
            "id": 4,
            "nombre": "Simulado",
            "abrev": "sim",
            "descripcion": "Simulado mediante un modelo"
        },
        "unidades": {
            "id": 10,
            "nombre": "metros cúbicos por segundo",
            "abrev": "m^3/s",
            "UnitsID": 36,
            "UnitsType": "Flow"
        },
        "fuente": {},
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

internal.modelos = [
    {
        "id": 27,
        "nombre": "sacramento",
        "tipo": "P-Q",
        "def_var_id": 4,
        "def_unit_id": 10,
        "parametros": [
            {
                "orden": 1,
                "nombre": "x1_0",
                "lim_inf": 1,
                "lim_sup": null,
                "range_min": 35,
                "range_max": 200
            },
            {
                "orden": 2,
                "nombre": "x2_0",
                "lim_inf": 1,
                "lim_sup": null,
                "range_min": 33,
                "range_max": 248
            },
            {
                "orden": 3,
                "nombre": "m1",
                "lim_inf": 1e-9,
                "lim_sup": null,
                "range_min": 1,
                "range_max": 3
            },
            {
                "orden": 4,
                "nombre": "c1",
                "lim_inf": 1e-9,
                "lim_sup": null,
                "range_min": 0.01,
                "range_max": 0.03
            },
            {
                "orden": 5,
                "nombre": "c2",
                "lim_inf": 1e-9,
                "lim_sup": null,
                "range_min": 150,
                "range_max": 500
            },
            {
                "orden": 6,
                "nombre": "c3",
                "lim_inf": 1e-9,
                "lim_sup": null,
                "range_min": 0.00044,
                "range_max": 0.002
            },
            {
                "orden": 7,
                "nombre": "mu",
                "lim_inf": 1e-9,
                "lim_sup": null,
                "range_min": 0.4,
                "range_max": 6
            },
            {
                "orden": 8,
                "nombre": "alfa",
                "lim_inf": 1e-9,
                "lim_sup": null,
                "range_min": 0.2,
                "range_max": 0.3
            },
            {
                "orden": 9,
                "nombre": "m2",
                "lim_inf": 1e-9,
                "lim_sup": null,
                "range_min": 1,
                "range_max": 2.2
            },
            {
                "orden": 10,
                "nombre": "m3",
                "lim_inf": 1e-9,
                "lim_sup": null,
                "range_min": 1,
                "range_max": 5
            }
        ],
        "forzantes": [
            {
                "orden": 1,
                "nombre": "Qout",
                "var_id": 4,
                "unit_id": 10,
                "inst": true,
                "tipo": "areal"
            },
            {
                "orden": 2,
                "nombre": "P",
                "var_id": 1,
                "unit_id": 22,
                "inst": false,
                "tipo": "areal"
            },
            {
                "orden": 3,
                "nombre": "ETP",
                "var_id": 15,
                "unit_id": 22,
                "inst": false,
                "tipo": "areal"
            },
            {
                "orden": 4,
                "nombre": "QPF",
                "var_id": 1,
                "unit_id": 22,
                "inst": false,
                "tipo": "areal"
            },
            {
                "orden": 5,
                "nombre": "SM",
                "var_id": 20,
                "unit_id": 23,
                "inst": true,
                "tipo": "areal"
            },
            {
                "orden": 6,
                "nombre": "PA",
                "var_id": 1,
                "unit_id": 22,
                "inst": false,
                "tipo": "areal"
            }
        ],
        "estados": [
            {
                "orden": 1,
                "nombre": "x1",
                "range_min": 0,
                "range_max": "Infinity",
                "def_val": 0
            },
            {
                "orden": 2,
                "nombre": "x2",
                "range_min": 0,
                "range_max": "Infinity",
                "def_val": 0
            },
            {
                "orden": 3,
                "nombre": "x3",
                "range_min": 0,
                "range_max": "Infinity",
                "def_val": 0
            },
            {
                "orden": 4,
                "nombre": "x4",
                "range_min": 0,
                "range_max": "Infinity",
                "def_val": 0
            }
        ],
        "outputs": [
            {
                "orden": 1,
                "nombre": "caudal saliente",
                "var_id": 4,
                "unit_id": 10,
                "inst": true,
                "series_table": "series"
            }
        ]
    }
]

internal.calibrados = [
    {
        "id": 32,
        "nombre": "sac",
        "model_id": 27,
        "activar": true,
        "selected": true,
        "out_id": [
            {
                "estacion_id": 2,
                "var_id": 2,
                "proc_id": 4,
                "unit_id": 11
            },
            {
                "estacion_id": 2,
                "var_id": 4,
                "proc_id": 4,
                "unit_id": 10
            }
        ],
        "area_id": null,
        "tramo_id": null,
        "dt": {
            "years": 0,
            "months": 0,
            "days": 1,
            "hours": 0,
            "minutes": 0,
            "seconds": 0,
            "milliseconds": 0
        },
        "t_offset": {
            "years": 0,
            "months": 0,
            "days": 0,
            "hours": 9,
            "minutes": 0,
            "seconds": 0,
            "milliseconds": 0
        },
        "modelo": "sacramento",
        "parametros": [
            {
                "orden": 1,
                "valor": 66.1803,
                "cal_id": null,
                "id": null
            },
            {
                "orden": 2,
                "valor": 67.348,
                "cal_id": null,
                "id": null
            },
            {
                "orden": 3,
                "valor": 4.2853,
                "cal_id": null,
                "id": null
            },
            {
                "orden": 4,
                "valor": 0.0118488,
                "cal_id": null,
                "id": null
            },
            {
                "orden": 5,
                "valor": 182.861,
                "cal_id": null,
                "id": null
            },
            {
                "orden": 6,
                "valor": 0.00253727,
                "cal_id": null,
                "id": null
            },
            {
                "orden": 7,
                "valor": 2.70204,
                "cal_id": null,
                "id": null
            },
            {
                "orden": 8,
                "valor": 0.227675,
                "cal_id": null,
                "id": null
            },
            {
                "orden": 9,
                "valor": 1.2897,
                "cal_id": null,
                "id": null
            },
            {
                "orden": 10,
                "valor": 6.73939,
                "cal_id": null,
                "id": null
            }
        ],
        "forzantes": [
            {
                "orden": 1,
                "series_id": 1505,
                "series_table": "series",
                "cal_id": null,
                "id": null,
                "model_id": null,
                "cal": null
            },
            {
                "orden": 2,
                "series_id": 1520,
                "series_table": "series",
                "cal_id": null,
                "id": null,
                "model_id": null,
                "cal": null
            }
        ],
        "estados": [
            {
                "orden": 1,
                "valor": 0.1,
                "cal_id": null,
                "id": null
            },
            {
                "orden": 2,
                "valor": 0.1,
                "cal_id": null,
                "id": null
            },
            {
                "orden": 3,
                "valor": 0.1,
                "cal_id": null,
                "id": null
            },
            {
                "orden": 4,
                "valor": 0.1,
                "cal_id": null,
                "id": null
            }
        ],
        "outputs": [
            {
                "orden": 1,
                "series_id": 1505,
                "series_table": "series",
                "cal_id": null,
                "id": null,
                "model_id": null,
                "cal": null
            }
        ],
        "stats": null,
        "corrida": null
    }
]

module.exports = internal