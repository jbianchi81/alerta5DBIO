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
    assert(res.body.length > 50)
})


test("get item", async () => {
    // create users
    const res = await request(app)
    .get(`/metadataCatalog/public2:tramos_condicion_params`)
    assert.equal(res.statusCode, 200);
    assert.equal(res.body.name,"tramos_condicion_params");
    assert.equal(res.body.boundingbox.type,"Polygon")
})
