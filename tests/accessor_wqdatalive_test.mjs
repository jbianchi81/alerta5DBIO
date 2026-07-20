import test from 'node:test'
import assert from 'assert'
process.env.NODE_ENV = "test"
import { Client } from "../app/accessors/wqdatalive.js"
// import {new as Accessor} from "../app/accessors"
// import { serie as CrudSerie } from "../app/CRUD"
import {readFile, writeFile} from 'fs/promises' 
import {parseUtcDateTime} from "../app/accessors/accessor_utils.js"

// test('wqdatalive accessor get devices', async(t) => {
//     const accessor_ = await readFile("tmp/accessor_boyas_atucha.json", {encoding: "utf-8"})
//     const accessor = JSON.parse(accessor_)
//     const client = new Client(accessor.config)
//     assert.equal(client.url, accessor.config.url)
//     assert.equal(client.key, accessor.config.key)
//     assert.equal(client.devices.length, 1)
//     assert.equal(client.devices[0], accessor.config.devices[0])

//     const devices_response = await client.getDevices()
//     assert(Array.isArray(devices_response.devices))
//     assert.equal(devices_response.devices.length,1)
//     assert.equal(devices_response.devices[0].id, accessor.config.devices[0])
// })

// test('wqdatalive accessor get device', async(t) => {
//     const accessor_ = await readFile("tmp/accessor_boyas_atucha.json", {encoding: "utf-8"})
//     const accessor = JSON.parse(accessor_)
//     const client = new Client(accessor.config)
//     assert.equal(client.devices.length, 1)
    
//     const device = await client.getDevice(client.devices[0])
//     assert.equal(device.id, client.devices[0])
// })

// test('wqdatalive accessor get parameters', async(t) => {
//     const accessor_ = await readFile("tmp/accessor_boyas_atucha.json", {encoding: "utf-8"})
//     const accessor = JSON.parse(accessor_)
//     const client = new Client(accessor.config)
//     assert.equal(client.devices.length, 1)
    
//     const parameters_response = await client.getParameters(client.devices[0])
//     assert("parameters" in parameters_response)
//     assert(Array.isArray(parameters_response.parameters))
//     assert(parameters_response.parameters.length)
//     for(const parameter of parameters_response.parameters) {
//         assert("id" in parameter)
//         assert("name" in parameter)
//         assert("visible" in parameter)
//         assert("unit" in parameter)
//         assert("precision" in parameter)
//         // assert("sensorName" in parameter) // Not in parameter
//         assert("sensorUID" in parameter)
//         assert("isCalculated" in parameter)
//     }
//     await writeFile("/tmp/wqdatalive-parameters.json", JSON.stringify(parameters_response, undefined, 2))
// })

// test('wqdatalive accessor get parameter', async(t) => {
//     const accessor_ = await readFile("tmp/accessor_boyas_atucha.json", {encoding: "utf-8"})
//     const accessor = JSON.parse(accessor_)
//     const client = new Client(accessor.config)
//     assert.equal(client.devices.length, 1)
//     assert.equal(client.parameters.length, 1)
    
//     const parameter = await client.getParameter(client.devices[0], client.parameters[0])
//     assert("id" in parameter)
//     assert.equal(parameter.id, client.parameters[0])
//     assert("name" in parameter)
//     assert("visible" in parameter)
//     assert("unit" in parameter)
//     assert("precision" in parameter)
//     // assert("sensorName" in parameter) // Not in parameter
//     assert("sensorUID" in parameter)
//     assert("isCalculated" in parameter)
// })

// test('wqdatalive accessor get data', async(t) => {
//     const accessor_ = await readFile("tmp/accessor_boyas_atucha.json", {encoding: "utf-8"})
//     const accessor = JSON.parse(accessor_)
//     const client = new Client(accessor.config)
//     assert.equal(client.devices.length, 1)
//     assert.equal(client.parameters.length, 1)
    
//     const to = new Date()
//     const from = new Date(to)
//     from.setDate(from.getDate() - 1)
//     const data_response = await client.getData(client.devices[0], from, to)
//     assert("data" in data_response)
//     assert(Array.isArray(data_response.data))
//     for(const point of data_response.data) {
//         assert("timestamp" in point)
//         const timestamp = parseUtcDateTime(point.timestamp)
//         assert(timestamp instanceof Date)
//         assert.notEqual(timestamp.toString(), "Invalid Date")
//         assert("values" in point)
//         for(const value of point.values) {
//             assert("parameterId" in value)
//             assert("value" in value)
//         }
//     }
//     await writeFile("/tmp/wqdatalive-data.json", JSON.stringify(data_response, undefined, 2))
// })


// test('wqdatalive accessor get data one parameter', async(t) => {
//     const accessor_ = await readFile("tmp/accessor_boyas_atucha.json", {encoding: "utf-8"})
//     const accessor = JSON.parse(accessor_)
//     const client = new Client(accessor.config)
//     assert.equal(client.devices.length, 1)
//     assert.equal(client.parameters.length, 1)
    
//     const to = new Date()
//     const from = new Date(to)
//     from.setDate(from.getDate() - 1)
//     const data_response = await client.getParameterData(client.devices[0], client.parameters[0], from, to)
//     assert("data" in data_response)
//     assert(Array.isArray(data_response.data))
//     for(const point of data_response.data) {
//         assert("timestamp" in point)
//         const timestamp = parseUtcDateTime(point.timestamp)
//         assert(timestamp instanceof Date)
//         assert.notEqual(timestamp.toString(), "Invalid Date")
//         assert("value" in point)
//     }
//     await writeFile("/tmp/wqdatalive-parameter-data.json", JSON.stringify(data_response, undefined, 2))
// })


// test('wqdatalive accessor get latest data', async(t) => {
//     const accessor_ = await readFile("tmp/accessor_boyas_atucha.json", {encoding: "utf-8"})
//     const accessor = JSON.parse(accessor_)
//     const client = new Client(accessor.config)
//     assert.equal(client.devices.length, 1)
//     assert(Array.isArray(client.parameters))
    
//     const data_response = await client.getDataLatest(client.devices[0], client.parameters)
//     assert("data" in data_response)
//     assert(Array.isArray(data_response.data))
//     for(const point of data_response.data) {
//         assert("id" in point)
//         assert(client.parameters.indexOf(point.id) >= 0)
//         assert("unit" in point)
//         assert("timestamp" in point)
//         const timestamp = parseUtcDateTime(point.timestamp)
//         assert(timestamp instanceof Date)
//         assert.notEqual(timestamp.toString(), "Invalid Date")
//         assert("value" in point)
//     }
//     await writeFile("/tmp/wqdatalive-data-latest.json", JSON.stringify(data_response, undefined, 2))
// })


// test('wqdatalive accessor get latest data one parameter', async(t) => {
//     const accessor_ = await readFile("tmp/accessor_boyas_atucha.json", {encoding: "utf-8"})
//     const accessor = JSON.parse(accessor_)
//     const client = new Client(accessor.config)
//     assert.equal(client.devices.length, 1)
//     assert(Array.isArray(client.parameters))
    
//     const point = await client.getParameterDataLatest(client.devices[0], client.parameters[0])
//     assert("id" in point)
//     assert.equal(point.id, client.parameters[0])
//     assert("unit" in point)
//     assert("timestamp" in point)
//     const timestamp = parseUtcDateTime(point.timestamp)
//     assert(timestamp instanceof Date)
//     assert.notEqual(timestamp.toString(), "Invalid Date")
//     assert("value" in point)
//     await writeFile("/tmp/wqdatalive-parameter-data-latest.json", JSON.stringify(point, undefined, 2))
// })

// // a5 interface

// test('wqdatalive accessor get sites', async(t) => {
//     const accessor_ = await readFile("tmp/accessor_boyas_atucha.json", {encoding: "utf-8"})
//     const accessor = JSON.parse(accessor_)
//     const client = new Client(accessor.config)
    
//     const estaciones = await client.getSites()
//     assert(Array.isArray(estaciones))
//     for(const estacion of estaciones) {
//         assert("id_externo" in estacion)
//         assert("tabla" in estacion)
//         assert("geom" in estacion)
//         assert("coordinates" in estacion.geom)
//         assert.equal(estacion.geom.coordinates.length,2)
//     }
//     await writeFile("/tmp/wqdatalive-sites.json", JSON.stringify(estaciones, undefined, 2))
// })

test('wqdatalive accessor get series', async(t) => {
    const accessor_ = await readFile("tmp/accessor_boyas_atucha.json", {encoding: "utf-8"})
    const accessor = JSON.parse(accessor_)
    const client = new Client(accessor.config)
    
    const series = await client.getSeries()
    assert(Array.isArray(series))
    for(const serie of series) {
        assert("estacion" in serie)
        assert("id" in serie.estacion)
        assert("geom" in serie.estacion)
        assert("coordinates" in serie.estacion.geom)
        assert.equal(serie.estacion.geom.coordinates.length,2)
        assert("var" in serie)
        assert(serie.var.id)
        assert(serie.var.nombre)
        assert(serie.procedimiento.id)
        assert(serie.procedimiento.nombre)
        assert(serie.unidades.id)
        assert(serie.unidades.nombre)
        assert.equal(serie.tipo, "puntual")
    }
    await writeFile("/tmp/wqdatalive-series.json", JSON.stringify(series, undefined, 2))
})
