import test from "node:test";
import assert from "node:assert/strict";
import request from "supertest";
import app from "../app/rest.mjs";

test("get catalog", async () => {
    // create users
    const res = await request(app)
    .get(`/metadataCatalog`)
    assert.equal(res.statusCode, 200);
    assert(Array.isArray(res.body));
    console.log("results: " + res.body.length)
    assert(res.body.length > 10)
})


test("get item", async () => {
    // create users
    const res = await request(app)
    .get(`/metadataCatalog/siyah:ultimas_alturas_reporte`)
    assert.equal(res.statusCode, 200);
    assert.equal(res.body.name,"siyah:ultimas_alturas_reporte");
    assert.equal(res.body.boundingbox.type,"Polygon")
    assert(Array.isArray(res.body.fields))
    assert.equal(res.body.fields.length,19)
})
