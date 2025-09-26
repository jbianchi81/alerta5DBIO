const test = require('node:test')
const assert = require('assert')
// process.env.NODE_ENV = "test"
const {CRUD: Crud} = require('../app/CRUD')

test('getCampo test', async(t) => {

    await t.test("getCampo", async t => {
        const campo = await Crud.getCampo(
            1,
            new Date(2024,0,1,9),
            new Date(2024,0,2,9),
            {
                proc_id: 6,
                tabla: "ctp"
            },
        {})
        assert.equal(campo.variable.id, 1)
        assert.equal(campo.procedimiento.id, 6)
        assert(campo.series.length >= 9)
        for(const serie of campo.series) {
            assert.equal(serie.estacion.tabla,"ctp")
        }
    })
})


test('getCampo test', async(t) => {

    let campo
    let geojson
    let obs_rast
    let upserted

    await t.test("getCampo", async t => {
        campo = await Crud.getCampo(
            1,
            new Date(2024,0,1,9),
            new Date(2024,0,2,9),
            {
                proc_id: 6,
                tabla: "ctp"
            },
        {})
        assert.equal(campo.variable.id, 1)
        assert.equal(campo.procedimiento.id, 6)
        assert(campo.series.length >= 9)
        for(const serie of campo.series) {
            assert.equal(serie.estacion.tabla,"ctp")
        }
    })

    await t.test("to geojson", async t => {
        // to geojson
        geojson = campo.toGeoJSON()
        assert("features" in geojson)
        assert(geojson.features.length >= 9)
    })

    await t.test("points2rast", async t => {
        // points2rast
        obs_rast = await Crud.points2rast(
            geojson,
            {
                series_id:22,
                timestart:new Date(2024,0,1,9),
                timeend: new Date(2024,0,2,9)
            },
            {
                outputdir: "/tmp",
                nmin: 9,
                radius1: 1.0,
                radius2: 1.0,
                out_x: 208,
                out_y: 192,
                nullvalue: -9999.0,
                method: "nearest",
                target_extent: [[-67.4,-23.3],[-62.2,-18.5]],
                roifile: "data/vect/CDP_polygon.shp",
                srs: 4326,
                makepng: true,
                geojson_file: "ctp_campo.geojson",
                output: "ctp_campo.tif",                
                geojsonfile: "ctp_campo2.geojson",
            },
            false)
        // {tipo:"rast",series_id:metadata.series_id,timeupdate:metadata.timeupdate,timestart: metadata.timestart, timeend: metadata.timeend, valor: data}
        assert.equal(obs_rast.tipo, "rast")
        assert.equal(obs_rast.series_id, 22)
        assert.equal(obs_rast.timestart.toISOString(),new Date(2024,0,1,9).toISOString())
        assert.equal(obs_rast.timeend.toISOString(),new Date(2024,0,2,9).toISOString())
        assert(obs_rast.valor instanceof Buffer)
    })

    await t.test("upsert", async t=> {
        // upsert
        upserted = await Crud.upsertObservacion(obs_rast)
        assert.equal(upserted.tipo, "raster")
        assert.equal(upserted.series_id, 22)
        assert.equal(upserted.timestart.toISOString(),new Date(2024,0,1,9).toISOString())
        assert.equal(upserted.timeend.toISOString(),new Date(2024,0,2,9).toISOString())
        assert(upserted.valor instanceof Buffer)
    })
    
    await t.test("areal",async t => {
        // areal
        for(const area_id of [
                715, 716, 717, 718,
                719, 720, 721, 722,
                723, 724, 725, 726,
                727, 728, 447
            ]) {
            const obs_areales = await Crud.rast2areal(
                22, 
                new Date(2024,0,1,9), 
                new Date(2024,0,2,9),
                area_id,
                {
                    no_insert: true
                }
            )
            assert.equal(obs_areales.length,1)
            assert.equal(obs_areales[0].timestart.toISOString(),new Date(2024,0,1,9).toISOString())
            assert.equal(obs_areales[0].timeend.toISOString(),new Date(2024,0,2,9).toISOString())
        }
    })

    await t.test("areal con insert",async t => {
        // areal
        for(const area_id of [
                715, 716, 717, 718,
                719, 720, 721, 722,
                723, 724, 725, 726,
                727, 728, 447
            ]) {
            const obs_areales = await Crud.rast2areal(
                22, 
                new Date(2024,0,1,9), 
                new Date(2024,0,2,9),
                area_id,
                {}
            )
            assert.equal(obs_areales.length,1)
            assert.equal(obs_areales[0].timestart.toISOString(),new Date(2024,0,1,9).toISOString())
            assert.equal(obs_areales[0].timeend.toISOString(),new Date(2024,0,2,9).toISOString())
        }
    })
})

test('campo2rast test', async(t) => {
    let obs_rast
    await t.test("campo2rast", async t => {
        obs_rast = await Crud.campo2rast(
            new Date(2024,0,2,9),
            new Date(2024,0,3,9),
            1,
            {
                proc_id: 6,
                tabla: "ctp"
            },
            {
                outputdir: "/tmp",
                nmin: 9,
                radius1: 1.0,
                radius2: 1.0,
                out_x: 208,
                out_y: 192,
                nullvalue: -9999.0,
                method: "nearest",
                target_extent: [[-67.4,-23.3],[-62.2,-18.5]],
                roifile: "data/vect/CDP_polygon.shp",
                srs: 4326,
                makepng: true,
                geojson_file: "ctp_campo.geojson",
                output: "ctp_campo.tif",                
                geojsonfile: "ctp_campo2.geojson"              
            },
            22,
            [
                715, 716, 717, 718,
                719, 720, 721, 722,
                723, 724, 725, 726,
                727, 728, 447
            ]
        )
        assert.equal(obs_rast.tipo, "raster")
        assert.equal(obs_rast.series_id, 22)
        assert.equal(obs_rast.timestart.toISOString(),new Date(2024,0,2,9).toISOString())
        assert.equal(obs_rast.timeend.toISOString(),new Date(2024,0,3,9).toISOString())
        assert(obs_rast.valor instanceof Buffer)
    })
})


test('seriescampo2rast test', async(t) => {
    let obs_rast
    await t.test("seriescampo2rast", async t => {
        obs_rast = await Crud.seriescampo2rast(
            new Date(2024,0,1,9),
            new Date(2024,0,10,9),
            1,
            {
                proc_id: 6,
                tabla: "ctp"
            },
            {
                outputdir: "/tmp",
                nmin: 9,
                radius1: 1.0,
                radius2: 1.0,
                out_x: 208,
                out_y: 192,
                nullvalue: -9999.0,
                method: "nearest",
                target_extent: [[-67.4,-23.3],[-62.2,-18.5]],
                roifile: "data/vect/CDP_polygon.shp",
                srs: 4326,
                makepng: true,
                geojson_file: "ctp_campo.geojson",
                geojsonfile: "ctp_campo2.geojson"              
            },
            22,
            undefined, 
            {days:1}, 
            {hours:9}
        )
        assert.equal(obs_rast.length,9)
        for(const obs of obs_rast) {
            assert.equal(obs.tipo, "raster")
            assert.equal(obs.series_id, 22)
            assert(obs.timestart.getTime() >= new Date(2024,0,1,9).getTime())
            assert(obs.timeend.getTime() <= new Date(2024,0,11,9).getTime())
            assert(obs.valor instanceof Buffer)
        }
    })
})

test('seriescampo2rast con areales test', async(t) => {
    let obs_rast
    await t.test("seriescampo2rast", async t => {
        obs_rast = await Crud.seriescampo2rast(
            new Date(2024,0,1,9),
            new Date(2024,0,10,9),
            1,
            {
                proc_id: 6,
                tabla: "ctp"
            },
            {
                outputdir: "/tmp",
                nmin: 9,
                radius1: 1.0,
                radius2: 1.0,
                out_x: 208,
                out_y: 192,
                nullvalue: -9999.0,
                method: "nearest",
                target_extent: [[-67.4,-23.3],[-62.2,-18.5]],
                roifile: "data/vect/CDP_polygon.shp",
                srs: 4326,
                makepng: true,
                geojson_file: "ctp_campo.geojson",
                geojsonfile: "ctp_campo2.geojson"              
            },
            22,
            [
                715, 716, 717, 718,
                719, 720, 721, 722,
                723, 724, 725, 726,
                727, 728, 447
            ], 
            {days:1}, 
            {hours:9}
        )
        assert.equal(obs_rast.length,9)
        for(const obs of obs_rast) {
            assert.equal(obs.tipo, "raster")
            assert.equal(obs.series_id, 22)
            assert(obs.timestart.getTime() >= new Date(2024,0,1,9).getTime())
            assert(obs.timeend.getTime() <= new Date(2024,0,11,9).getTime())
            assert(obs.valor instanceof Buffer)
        }
    })
})