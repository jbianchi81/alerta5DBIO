const test = require('node:test')
const assert = require('assert')
process.env.NODE_ENV = "test"
const {serie: Serie, observacion: Observacion, observaciones: Observaciones, escena: Escena, fuente: Fuente} = require('../app/CRUD')

test('observacion rast crud sequence', async(t) => {

    await t.test("create fuente", async t => {
        const fuentes = await Fuente.create({
            "id": 7,
            "nombre": "pp_emas",
            "data_table": "pp_emas",
            "data_column": "rast",
            "tipo": "QPE",
            "def_proc_id": 3,
            "def_dt": {
                "days": 1
            },
            "hora_corte": {
                "hours": 9
            },
            "def_unit_id": 22,
            "def_var_id": 1,
            "fd_column": null,
            "mad_table": "pmad_emas",
            "scale_factor": null,
            "data_offset": null,
            "def_pixel_height": 0.248963,
            "def_pixel_width": 0.248756,
            "def_srid": 4326,
            "def_extent": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [
                            -70,
                            -10
                        ],
                        [
                            -40,
                            -10
                        ],
                        [
                            -40,
                            -40
                        ],
                        [
                            -70,
                            -40
                        ],
                        [
                            -70,
                            -10
                        ]
                    ]
                ]
            }
        }, {
            create_cube_table: true
        })

        assert.equal(fuentes.length, 1)
        assert.ok(fuentes[0] instanceof Fuente)
    })

    await t.test("create escena", async t => {

        const escenas = await Escena.create({
            "id": 15,
            "nombre": "gfs_diario",
            "geom": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [
                            -90,
                            -10
                        ],
                        [
                            -40,
                            -10
                        ],
                        [
                            -40,
                            -70
                        ],
                        [
                            -90,
                            -70
                        ],
                        [
                            -90,
                            -10
                        ]
                    ]
                ]
            }
        })

        assert.equal(escenas.length, 1)
        assert.ok(escenas[0] instanceof Escena)
        assert.equal(escenas[0].id, 15)
    })

    await t.test("create serie", async t => {
        const series = await Serie.create({
            "id": 8,
            "tipo": "rast",
            "estacion_id": 15,
            "var_id":  1,
            "proc_id": 4,
            "unit_id": 22,
            "fuentes_id": 7
        })

        assert.equal(series.length, 1)
        assert.ok(series[0] instanceof Serie)
        assert.equal(series[0].id, 8)
    })

    await t.test("import observaciones", async (t)=> {
        const timestart = Date.UTC(2020,0,1,12)
        const observaciones = await Observaciones.importRaster(
            `${__dirname}/aux/rast_samples.tif.tgz`,
            timestart,
            {"days": 1},
            {"days": 1},
            8
        )
        assert.equal(observaciones.length, 30)
        for(var i=0;i<observaciones.length;i++) {
            assert.ok(observaciones[i] instanceof Observacion)
            assert.equal(observaciones[i].tipo, "raster")
            assert.ok(observaciones[i].valor instanceof Buffer)
            const ts = new Date(timestart)
            ts.setDate(ts.getDate() + i)
            const te = new Date(ts)
            te.setDate(te.getDate() + 1)
            assert.equal(observaciones[i].timestart.getTime(), ts.getTime())
            assert.equal(observaciones[i].timeend.getTime(), te.getTime())
            assert.equal(observaciones[i].series_id, 8)
        }
    })

    await t.test("import observaciones, pass list of filenames, use gdal metadata. Create", async (t)=> {
        const timestart = Date.UTC(2020,0,1,12)
        const observaciones = await Observaciones.importRaster(
            [
                `${__dirname}/aux/rast_samples/rast_2020-01-01.tif`,
                `${__dirname}/aux/rast_samples/rast_2020-01-02.tif`,
                `${__dirname}/aux/rast_samples/rast_2020-01-03.tif`,
                `${__dirname}/aux/rast_samples/rast_2020-01-04.tif`,
                `${__dirname}/aux/rast_samples/rast_2020-01-05.tif`
            ],
            undefined,
            undefined,
            undefined,
            undefined,
            true
        )
        assert.equal(observaciones.length, 5)
        for(var i=0;i<observaciones.length;i++) {
            assert.ok(observaciones[i] instanceof Observacion)
            assert.equal(observaciones[i].tipo, "raster")
            assert.ok(observaciones[i].valor instanceof Buffer)
            const ts = new Date(timestart)
            ts.setDate(ts.getDate() + i)
            const te = new Date(ts)
            te.setDate(te.getDate() + 1)
            assert.equal(observaciones[i].timestart.getTime(), ts.getTime())
            assert.equal(observaciones[i].timeend.getTime(), te.getTime())
            assert.equal(observaciones[i].series_id, 8)
        }
    })

    await t.test("read created obs", async t => {
        const observaciones = await Observaciones.read({
            tipo: "rast",
            series_id: 8,
            timestart: new Date(2020,0,1,9),
            timeend: new Date(2020,0,6,9)
        })
        assert.equal(observaciones.length, 5)
        for(var i=0;i<observaciones.length;i++) {
            assert.ok(observaciones[i] instanceof Observacion)
            assert.equal(observaciones[i].tipo, "raster")
            assert.ok(observaciones[i].valor instanceof Buffer)
            const ts = new Date(2020,0,1,9)
            ts.setDate(ts.getDate() + i)
            const te = new Date(ts)
            te.setDate(te.getDate() + 1)
            assert.equal(observaciones[i].timestart.getTime(), ts.getTime())
            assert.equal(observaciones[i].timeend.getTime(), te.getTime())
            assert.equal(observaciones[i].series_id, 8)
        }
    })

    await t.test("delete observaciones", async t => {
        const observaciones = await Observaciones.delete({
            tipo: "rast",
            series_id: 8,
            timestart: new Date(2020,0,1,9),
            timeend: new Date(2020,0,6,9)
        },
        {
            batch_size: 2

        })
        assert.equal(observaciones.length, 5)
        for(var i=0;i<observaciones.length;i++) {
            assert.ok(observaciones[i] instanceof Observacion)
            assert.equal(observaciones[i].tipo, "raster")
            assert.ok(observaciones[i].valor instanceof Buffer)
            const ts = new Date(2020,0,1,9)
            ts.setDate(ts.getDate() + i)
            const te = new Date(ts)
            te.setDate(te.getDate() + 1)
            assert.equal(observaciones[i].timestart.getTime(), ts.getTime())
            assert.equal(observaciones[i].timeend.getTime(), te.getTime())
            assert.equal(observaciones[i].series_id, 8)
        }
    })


    await t.test("delete serie", async t => {
        const series = await Serie.delete({
            tipo: "rast",
            id: 8
        })

        assert.equal(series.length, 1)
        assert.ok(series[0] instanceof Serie)
        assert.equal(series[0].id, 8)
    })

    await t.test("delete escena", async t => {
        const escenas = await Escena.delete({
            id: 15
        })

        assert.equal(escenas.length, 1)
        assert.ok(escenas[0] instanceof Escena)
        assert.equal(escenas[0].id, 15)
    })

    await t.test("delete fuente", async t => {
        const fuentes = await Fuente.delete({
            id: 7
        })

        assert.equal(fuentes.length, 1)
        assert.ok(fuentes[0] instanceof Fuente)
        assert.equal(fuentes[0].id, 7)
    })


})