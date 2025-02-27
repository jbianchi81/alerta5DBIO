const test = require('node:test')
const assert = require('assert')
process.env.NODE_ENV = "test"
const {serie: Serie, escena: Escena, area: Area, estacion: Estacion, observacion: Observacion} = require('../app/CRUD')
// const { convert } = require('xmlbuilder2');
var parser = require('xml2json');
const {Libxml} = require('node-libxml')

test('serie rast read as gmd', async(t) => {
    await Serie.delete({
        tipo: "raster",
        id: 1
    })
    var serie
    await t.test("create serie", async(t) => {
        const series = await Serie.create({
            "tipo":"rast","id":1,"estacion":{"id":7,"nombre":"campo_splines","geom":{"type":"Polygon","coordinates":[[[-70,-10],[-40,-10],[-40,-40],[-70,-40],[-70,-10]]]}},"var":{"id":1,"var":"P","nombre":"precipitación diaria 12Z","abrev":"precip_diaria_met","type":"num","datatype":"Succeeding Total","valuetype":"Field Observation","GeneralCategory":"Climate","VariableName":"Precipitation","SampleMedium":"Precipitation","def_unit_id":"22","timeSupport":{"years":0,"months":0,"days":1,"hours":0,"minutes":0,"seconds":0,"milliseconds":0},"def_hora_corte":{"years":0,"months":0,"days":0,"hours":9,"minutes":0,"seconds":0,"milliseconds":0}},"procedimiento":{"id":6,"nombre":"Análisis","abrev":"anal","descripcion":"Análisis a partir de datos observados"},"unidades":{"id":22,"nombre":"milímetros por día","abrev":"mm/d","UnitsID":305,"UnitsType":"velocity"},"fuente":{"id":2,"nombre":"cpc","data_table":"pp_cpc","data_column":"rast","tipo":"PA","def_proc_id":6,"def_dt":{"years":0,"months":0,"days":1,"hours":0,"minutes":0,"seconds":0,"milliseconds":0},"hora_corte":{"years":0,"months":0,"days":0,"hours":-3,"minutes":0,"seconds":0,"milliseconds":0},"def_unit_id":22,"def_var_id":1,"fd_column":null,"mad_table":"pmad_cpc","scale_factor":null,"data_offset":null,"def_pixel_height":0.5,"def_pixel_width":0.5,"def_srid":4326,"def_extent":{"type":"Polygon","coordinates":[[[-70,-10],[-40,-10],[-40,-40],[-70,-40],[-70,-10]]]},"date_column":"date","def_pixeltype":"32BF","abstract":"Análisis de la precipitación a paso diario realizado sobre la base de la red internacional de estaciones meteorológicas","source":"ftp://ftp.cdc.noaa.gov/Datasets/cpc_global_precip","public":true,"constraints":[{"table_name":"pp_cpc","constraint_name":"pp_cpc_date_key","constraint_type":"u","column_names":["date"]},{"table_name":"pp_cpc","constraint_name":"pp_cpc_pkey","constraint_type":"p","column_names":["id"]}]},"date_range":{"timestart":null,"timeend":null,"count":null},"observaciones":[]
        },{
            upsert_estacion: true
        })
        assert.equal(series.length, 1, "Length of created series must equal 1")
        serie = series[0]
        assert.equal(serie.id, 1, "id of created serie must be 1")
        assert.equal(serie.tipo, "rast", "tipo of serie must be raster")
        assert.equal(serie.observaciones.length, 0, "length of serie.observaciones must be 0")
    })

    // create dummy obs 

    await t.test("crea observacion rast dummy", async(t) => {
        const values = [[0.1,1.1,0.1],[1.3,0.01,4.2],[0.3,3.6,3.0]]
        result = await Observacion.insertFromArray(
            values,
            10.0,
            -70.0,
            -40.0,
            4326,
            '32BF',
            1,
            new Date('2024-01-01'),
            new Date('2024-01-02')
        )
        assert(result instanceof Observacion, "Expected an Observacion object")
        assert.equal(result.timestart.toISOString(),new Date('2024-01-01').toISOString(), "expected timestart=2024-01-01")
        assert.equal(result.series_id,1,"Expected series_id=1")
        assert.equal(result.tipo,"raster","Expected tipo=raster")
    })


    await t.test("read serie as gmd", async(t) => {
        const series = await Serie.read({
            series_id: 1,
            tipo: "raster"
        },{
            include_geom: true
        })
        assert.equal(serie.fuente.def_pixel_width,0.5)
        const serie_gmd = series[0].toGmd()
        js_obj = parser.toJson(serie_gmd, {coerce: true, object: true})
        assert.equal(js_obj["gmi:MI_Metadata"]["gmd:spatialRepresentationInfo"]["gmd:MD_GridSpatialRepresentation"]["gmd:axisDimensionProperties"]["gmd:MD_Dimension"]["gmd:resolution"]["gco:Measure"]["$t"],0.5, "Expected resolution 0.5")
    })

    await t.test("delete estacion", async(t)=> {
        const deleted = await Escena.delete({
            "id": 7
        })
        assert.equal(deleted.length, 1, "One station deleted")
    })

})

test('serie areal read as gmd', async(t) => {
    await Serie.delete({
        tipo: "areal",
        id: 1
    })
    var serie
    await t.test("create serie", async(t) => {
        const series = await Serie.create({
            "tipo":"areal","id":1,"estacion":{"id":7,"nombre":"test","geom":{"type":"Polygon","coordinates":[[[-70,-10],[-40,-10],[-40,-40],[-70,-40],[-70,-10]]]}},"var":{"id":1,"var":"P","nombre":"precipitación diaria 12Z","abrev":"precip_diaria_met","type":"num","datatype":"Succeeding Total","valuetype":"Field Observation","GeneralCategory":"Climate","VariableName":"Precipitation","SampleMedium":"Precipitation","def_unit_id":"22","timeSupport":{"years":0,"months":0,"days":1,"hours":0,"minutes":0,"seconds":0,"milliseconds":0},"def_hora_corte":{"years":0,"months":0,"days":0,"hours":9,"minutes":0,"seconds":0,"milliseconds":0}},"procedimiento":{"id":6,"nombre":"Análisis","abrev":"anal","descripcion":"Análisis a partir de datos observados"},"unidades":{"id":22,"nombre":"milímetros por día","abrev":"mm/d","UnitsID":305,"UnitsType":"velocity"},"fuente":{"id":2,"nombre":"cpc","data_table":"pp_cpc","data_column":"rast","tipo":"PA","def_proc_id":6,"def_dt":{"years":0,"months":0,"days":1,"hours":0,"minutes":0,"seconds":0,"milliseconds":0},"hora_corte":{"years":0,"months":0,"days":0,"hours":-3,"minutes":0,"seconds":0,"milliseconds":0},"def_unit_id":22,"def_var_id":1,"fd_column":null,"mad_table":"pmad_cpc","scale_factor":null,"data_offset":null,"def_pixel_height":0.5,"def_pixel_width":0.5,"def_srid":4326,"def_extent":{"type":"Polygon","coordinates":[[[-70,-10],[-40,-10],[-40,-40],[-70,-40],[-70,-10]]]},"date_column":"date","def_pixeltype":"32BF","abstract":"Análisis de la precipitación a paso diario realizado sobre la base de la red internacional de estaciones meteorológicas","source":"ftp://ftp.cdc.noaa.gov/Datasets/cpc_global_precip","public":true,"constraints":[{"table_name":"pp_cpc","constraint_name":"pp_cpc_date_key","constraint_type":"u","column_names":["date"]},{"table_name":"pp_cpc","constraint_name":"pp_cpc_pkey","constraint_type":"p","column_names":["id"]}]},"date_range":{"timestart":null,"timeend":null,"count":null},"observaciones":[]
        },{
            upsert_estacion: true
        })
        assert.equal(series.length, 1, "Length of created series must equal 1")
        serie = series[0]
        assert.equal(serie.id, 1, "id of created serie must be 1")
        assert.equal(serie.tipo, "areal", "tipo of serie must be areal")
        assert.equal(serie.observaciones.length, 0, "length of serie.observaciones must be 0")
    })

    await t.test("read serie as gmd", async(t) => {
        const series = await Serie.read({
            series_id: 1,
            tipo: "areal"
        },{
            include_geom: true
        })
        const serie_gmd = series[0].toGmd()
        const libxml = new Libxml()
        assert(libxml.loadXmlFromString(serie_gmd),"Invalid xml")
    })

    await t.test("delete estacion", async(t)=> {
        const deleted = await Area.delete({
            "id": 7
        })
        assert.equal(deleted.length, 1, "One station deleted")
    })

})

test('serie puntual read as gmd', async(t) => {
    await Serie.delete({
        tipo: "puntual",
        id: 1
    })
    var serie
    await t.test("create serie", async(t) => {
        const series = await Serie.create({
            "tipo":"puntual","id":1,"estacion":{"id":7,"nombre":"test","geom":{"type":"Point","coordinates":[-70,-10]},"tabla":"red_acumar","id_externo":7},"var":{"id":1,"var":"P","nombre":"precipitación diaria 12Z","abrev":"precip_diaria_met","type":"num","datatype":"Succeeding Total","valuetype":"Field Observation","GeneralCategory":"Climate","VariableName":"Precipitation","SampleMedium":"Precipitation","def_unit_id":"22","timeSupport":{"years":0,"months":0,"days":1,"hours":0,"minutes":0,"seconds":0,"milliseconds":0},"def_hora_corte":{"years":0,"months":0,"days":0,"hours":9,"minutes":0,"seconds":0,"milliseconds":0}},"procedimiento":{"id":6,"nombre":"Análisis","abrev":"anal","descripcion":"Análisis a partir de datos observados"},"unidades":{"id":22,"nombre":"milímetros por día","abrev":"mm/d","UnitsID":305,"UnitsType":"velocity"},"date_range":{"timestart":null,"timeend":null,"count":null},"observaciones":[]
        },{
            upsert_estacion: true
        })
        assert.equal(series.length, 1, "Length of created series must equal 1")
        serie = series[0]
        assert.equal(serie.id, 1, "id of created serie must be 1")
        assert.equal(serie.tipo, "puntual", "tipo of serie must be puntual")
        assert.equal(serie.observaciones.length, 0, "length of serie.observaciones must be 0")
    })

    await t.test("read serie as gmd", async(t) => {
        const series = await Serie.read({
            series_id: 1,
            tipo: "puntual"
        },{
            include_geom: true
        })
        const serie_gmd = series[0].toGmd()
        const libxml = new Libxml()
        assert(libxml.loadXmlFromString(serie_gmd),"Invalid xml")
    })

    await t.test("delete estacion", async(t)=> {
        const deleted = await Estacion.delete({
            "id": 7
        })
        assert.equal(deleted.length, 1, "One station deleted")
    })

})