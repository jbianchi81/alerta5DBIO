import test from 'node:test'
import assert from 'assert'
process.env.NODE_ENV = "test"
import { describeFeatureType, getWFSCapabilities, MetadataCatalog } from "../app/metadataCatalog.js"

test('get and parse field descriptions', async(t) => {
    const fd = await describeFeatureType("http://localhost:8080/geoserver", "siyah:ultimas_alturas_reporte")
    assert(Array.isArray(fd))
    for(const field of fd) {
        assert(field.name)
        if(field.name == "unid") {
            assert(field.documentation)
        }
    }
}) 

test('get and parse field descriptions older geoserver', async(t) => {
    const fd = await describeFeatureType("https://alerta.ina.gob.ar/geoserver", "public2:tramos_condicion_params")
    assert(Array.isArray(fd))
    for(const field of fd) {
        assert(field.name)
    }
}) 


test('get capabilities', async(t) => {
    const c = await getWFSCapabilities("http://localhost:8080/geoserver")
    assert(Array.isArray(c))
    for(const i of c) {
        assert(i.name)
        if(i.name == "siyah:ultimas_alturas_reporte") {
            assert(i.title == "Condición hídrica en Cuenca del Plata (estaciones)")
        }
        assert(i.boundingbox)
        assert.equal(i.boundingbox.type, "Polygon")
    }
})

test('update catalog', async(t) => {
    const md_items = await MetadataCatalog.update("http://localhost:8080/geoserver")
    assert(Array.isArray(md_items))
    for(const item of md_items) {
        assert(item.name)
        if(item.name == "siyah:ultimas_alturas_reporte") {
            assert.equal(item.title, "Condición hídrica en Cuenca del Plata (estaciones)")
        }
    }
})

test('update catalog one item', async(t) => {
    const md_items = await MetadataCatalog.update("http://localhost:8080/geoserver", true, undefined, "siyah:ultimas_alturas_reporte")
    assert(Array.isArray(md_items))
    assert.equal(md_items.length,1)
    const item = md_items[0]
    assert(item.name)
    assert.equal(item.name,"siyah:ultimas_alturas_reporte")
    assert.equal(item.title,"Condición hídrica en Cuenca del Plata (estaciones)")
})