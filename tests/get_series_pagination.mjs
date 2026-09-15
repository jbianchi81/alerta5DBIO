import test from "node:test";
import assert from "node:assert/strict";
import request from "supertest";
import app from "../app/rest.mjs";


test("GET /obs/puntual/series pagination", async () => {
  var last = false
  const results = []
  const limit = 5000
  var offset = 0
  var url = `/obs/puntual/series?tipo=puntual&proc_id=1&proc_id=2&proc_id=6&format=geojson&data_availability=a&limit=${limit}&red_id=&include_geom=true&pagination=true&offset=${offset}`
  const series_id = 1371
  while(last == false) {

    const res = await request(app)
      .get(url)
      // .set("Authorization", `Bearer ${token}`);
    assert.equal(res.statusCode, 200);
    assert.ok("features" in res.body)
    assert(Array.isArray(res.body["features"]));
    last = res.body.is_last_page
    if(last) {
      assert.ok(res.body["features"].length <= limit)
    } else {
      assert.equal(res.body["features"].length,limit)
    }
    results.push(...res.body.features)
    
    url = `/${res.body.next_page_url}`
  }
  assert.ok(results.map(s => s.properties.id).indexOf(series_id) >= 0)
})
