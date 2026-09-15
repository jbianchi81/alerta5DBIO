import test from "node:test";
import assert from "node:assert/strict";
import request from "supertest";
import app from "../app/rest.mjs";

const token = process.env.TOKEN

test("GET /obs/puntual/series pagination", async () => {
  var last = false
  const results = []
  const limit = 5000
  var offset = 0
  var url = `/obs/puntual/series?tipo=puntual&proc_id=1&proc_id=2&proc_id=6&format=geojson&data_availability=a&limit=${limit}&red_id=&include_geom=true&pagination=true&offset=${offset}`
  const series_id = 1371
  while(last == false) {
    const res = (token) ? 
      await request(app)
        .get(url)
        .set("Authorization", `Bearer ${token}`) : 
      await request(app)
        .get(url);
    assert.equal(res.statusCode, 200);
    assert.ok("features" in res.body)
    console.debug(`offset: ${res.body.offset}`)
    assert(Array.isArray(res.body["features"]));
    last = res.body.is_last_page
    if(last) {
      assert.ok(res.body["features"].length <= limit)
    } else {
      assert.equal(res.body["features"].length,limit)
    }
    assert.equal(res.body.features.length, [...new Set(res.body.features.map(s => s.properties.id))].length)
    results.push(...res.body.features)
    
    url = `/${res.body.next_page_url}`
  }
  console.debug(`results: ${results.length}`)
  assert.equal(results.length, [...new Set(results.map(s => s.properties.id))].length)
  assert.ok(results.map(s => s.properties.id).indexOf(series_id) >= 0)
})

// 37026