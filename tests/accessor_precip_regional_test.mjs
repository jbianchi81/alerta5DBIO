import test from 'node:test'
import assert from 'assert'
process.env.NODE_ENV = "test"
import { readCsv } from "../app/accessors/precip_regional.js"

test('precip_regional accessor parse csv', async(t) => {
    
    const data = readCsv("data/precip_regional.csv", "data/precip_regional.geojson")
    assert.ok("features" in data)
    assert.ok(Array.isArray(data.features))
    const values = []
    for(const feature of data.features) {
        assert.ok("geometry" in feature)
        assert.ok("coordinates" in feature.geometry)
        assert.ok(Array.isArray(feature.geometry.coordinates))
        assert.equal(feature.geometry.coordinates.length, 2)
        assert.ok("properties" in feature)
        assert.ok("date" in feature.properties)
        assert.ok(feature.properties.date instanceof Date)
        assert.notEqual(feature.properties.date.toString(), "Invalid Date")
        assert.ok("station_id" in feature.properties)
        assert.ok("value" in feature.properties)
        if(feature.properties.value != null) {
            assert.notEqual(parseFloat(feature.properties.value).toString(), "NaN")
            values.push({
                station_id: feature.properties.station_id,
                date: feature.properties.date,
                value: feature.properties.value
            })
        }
        assert.notEqual(values.length, 0)
    }
})
