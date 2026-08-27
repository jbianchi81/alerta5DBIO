import test from "node:test";
import assert from "node:assert/strict";
import request from "supertest";
import app from "../app/rest.mjs";



  test("GET /obs/puntual/series/:series_id/regular prono last", async() => {
    const ts = new Date()
    ts.setDate(ts.getDate() - 15)
    const te = new Date()
    te.setDate(te.getDate() + 6)
    const res = await request(app)
      .get(`/obs/puntual/series/1470/regular`)
      .query({
        timestart: ts.toISOString(),
        timeend: te.toISOString(),
        dt: "1 day",
        cal_id: 35
      });
      // .set("Authorization", `Bearer ${writer.token}`);
    assert.equal(res.statusCode, 200);
    assert.ok(Array.isArray(res.body))
    assert.equal(res.body.length, 21)
  })

