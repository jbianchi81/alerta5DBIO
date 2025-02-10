const test = require('node:test')
const assert = require('assert')
process.env.NODE_ENV = "test"
const {serie: Serie, observacion: Observacion, observaciones: Observaciones, escena: Escena, fuente: Fuente} = require('../app/CRUD')

test('observacion rast crud sequence', async(t) => {

    // await t.test("create fuente", async(t => {
    //     const fuentes = await Fuente.create({
    //         "id": 7,
    //         "nombre": "pp_emas",
    //         "data_table": "pp_emas",
    //         "data_column": "rast",
    //         "tipo": "QPE",
    //         "def_proc_id": 3,
    //         "def_dt": {
    //             "days": 1
    //         },
    //         "hora_corte": {
    //             "hours": 9
    //         },
    //         "def_unit_id": 22,
    //         "def_var_id": 1,
    //         "fd_column": null,
    //         "mad_table": "pmad_emas",
    //         "scale_factor": null,
    //         "data_offset": null,
    //         "def_pixel_height": 0.248963,
    //         "def_pixel_width": 0.248756,
    //         "def_srid": 4326,
    //         "def_extent": {
    //             "type": "Polygon",
    //             "coordinates": [
    //                 [
    //                     [
    //                         -70,
    //                         -10
    //                     ],
    //                     [
    //                         -40,
    //                         -10
    //                     ],
    //                     [
    //                         -40,
    //                         -40
    //                     ],
    //                     [
    //                         -70,
    //                         -40
    //                     ],
    //                     [
    //                         -70,
    //                         -10
    //                     ]
    //                 ]
    //             ]
    //         }
    //     }, {
    //         create_cube_table: true
    //     })

    //     const escenas = await Escena.create({
    //         "id": 15,
    //         "nombre": "gfs_diario",
    //         "geom": {
    //             "type": "Polygon",
    //             "coordinates": [
    //                 [
    //                     [
    //                         -90,
    //                         -10
    //                     ],
    //                     [
    //                         -40,
    //                         -10
    //                     ],
    //                     [
    //                         -40,
    //                         -70
    //                     ],
    //                     [
    //                         -90,
    //                         -70
    //                     ],
    //                     [
    //                         -90,
    //                         -10
    //                     ]
    //                 ]
    //             ]
    //         }
    //     })

    //     const series = await Serie.create({
    //         "id": 3,
    //         "tipo": "rast",
    //         "estacion": {
    //             "id": 15
    //         },
    //         "var": {
    //             "id": 1
    //         },
    //         "procedimiento": {
    //             "id": 4
    //         },
    //         "unidades": {
    //             "id": 22
    //         },
    //         "fuente": {
    //             "id": 7
    //         }
    //     })

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
            assert.equal(observaciones[i] instanceof Observacion, true)
            assert.equal(observaciones[i].tipo, "raster")
            assert.equal(observaciones[i].valor instanceof Buffer, true)
            const ts = new Date(timestart)
            ts.setDate(ts.getDate() + i)
            const te = new Date(ts)
            te.setDate(te.getDate() + 1)
            assert.equal(observaciones[i].timestart.getTime(), ts.getTime())
            assert.equal(observaciones[i].timeend.getTime(), te.getTime())
            assert.equal(observaciones[i].series_id, 8)
        }
    })
})