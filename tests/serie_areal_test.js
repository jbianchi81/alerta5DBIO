const test = require('node:test')
const assert = require('assert')
process.env.NODE_ENV = "test"
const {serie: Serie} = require('../app/CRUD')

test('serie areal sequence', async(t) => {
    await Serie.delete({
        tipo: "areal",
        id: 7657
    })

    await t.test("create serie", async(t) => {
        const series = await Serie.create({
            "tipo":"areal",
            "id":7657,
            "estacion":{"id":740,"nombre":"test4949494","geom":{"type":"Polygon","coordinates":[[[-48.9045833333333,-16.4020833333333],[-48.89875,-16.4079166666667],[-48.8979166666667,-16.4170833333333],[-48.8929166666667,-16.4195833333333],[-48.9045833333333,-16.4020833333333]]]},"exutorio":null,"ae":null,"rho":null,"wp":null,"activar":true,"mostrar":null,"area":28860203609.9227},
            "var":{"id":95,"var":"Pd0z","nombre":"precipitación diaria 00Z","abrev":"precip_diaria_00z","type":"num","datatype":"Succeeding Total","valuetype":"Field Observation","GeneralCategory":"Climate","VariableName":"Precipitation","SampleMedium":"Precipitation","def_unit_id":"22","timeSupport":{"years":0,"months":0,"days":1,"hours":0,"minutes":0,"seconds":0,"milliseconds":0},"def_hora_corte":{"years":0,"months":0,"days":0,"hours":-3,"minutes":0,"seconds":0,"milliseconds":0}},
            "procedimiento":{"id":4,"nombre":"Simulado","abrev":"sim","descripcion":"Simulado mediante un modelo"},
            "unidades":{"id":22,"nombre":"milímetros por día","abrev":"mm/d","UnitsID":305,"UnitsType":"velocity"},
            "fuente":{"id":53,"nombre":"test_assegg","data_table":"test_dtab","data_column":"rast","tipo":"PA","def_proc_id":6,"def_dt":{"years":0,"months":0,"days":1,"hours":0,"minutes":0,"seconds":0,"milliseconds":0},"hora_corte":{"years":0,"months":0,"days":0,"hours":-3,"minutes":0,"seconds":0,"milliseconds":0},"def_unit_id":22,"def_var_id":1,"fd_column":null,"mad_table":"test_mad_tb","scale_factor":null,"data_offset":null,"def_pixel_height":0.25,"def_pixel_width":0.25,"def_srid":4326,"def_extent":{"type":"Polygon","coordinates":[[[-70,-10],[-40,-10],[-40,-40],[-70,-40],[-70,-10]]]},"date_column":"date","def_pixeltype":"32BF","abstract":"ACCESS-CM2_historical","source":"https://testsource","public":true,"constraints":[{"table_name":"test_tbnm","constraint_name":"test_tbnm","constraint_type":"u","column_names":["filename"]}]},
            "observaciones": [
                {"tipo":"areal","timestart":"1990-01-01T23:00:00.000Z","timeend":"1990-01-02T23:00:00.000Z","valor":10.15},
                {"tipo":"areal","timestart":"1990-01-02T23:00:00.000Z","timeend":"1990-01-03T23:00:00.000Z","valor":11.67},
                {"tipo":"areal","timestart":"1990-01-03T23:00:00.000Z","timeend":"1990-01-04T23:00:00.000Z","valor":7.55}
            ],
        },{
            all: true
        })
        assert.equal(series.length, 1, "Length of created series must equal 1")
        const serie = series[0]
        assert.equal(serie.id, 7657, "id of created serie must be 7657")
        assert.equal(serie.tipo, "areal", "tipo of serie must be puntual")
        assert.equal(serie.observaciones.length, 3, "length of serie.observaciones must be 3")
    })

    await t.test("read serie by id", async(t) => {
        const serie = await Serie.read({
            "tipo": "areal",
            "id": 7657,
            "timestart": new Date(1990,0,1),
            "timeend": new Date(1990,0,5)
        },{
            skip_nulls: true
        })
        assert.equal(serie.id, 7657, "id of created serie must be 7657")
        assert.equal(serie.tipo, "areal", "tipo of serie must be puntual")
        assert.equal(serie.observaciones.length, 3, "length of serie.observaciones must be 3")

    })
})