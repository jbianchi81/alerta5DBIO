import test from "node:test";
import assert from "node:assert/strict";
import request from "supertest";
import app from "../app/rest.mjs";

const writer_token = "token" // role writer
const noaccess_token = "token_1" // role writer
const reader_token = "token_2" // role public
const admin_token = "token_3" // role admin

let estacion
let serie

const group_name = "app_series_test"
const estacion_id = 2948

// test('parent test', async (t) => {
  // preparacion

  test("crea grupo, asigna usuario y red", async ()=> {
    
    const res = await request(app)
      .post("/groups")
      .send([
        {
          name: group_name
        }
      ])
      .set("Content-Type", "application/json")
      .set("Authorization", `Bearer ${admin_token}`);
    assert.equal(res.statusCode, 201)
    const res2 = await request(app)
      .put(`/groups/${group_name}/members`)
      .send([
          {
              user_id: 5
          },{
              user_id: 7
          },{
            user_id: 8
          }]
      )
      .set("Content-Type", "application/json")
      .set("Authorization", `Bearer ${admin_token}`);
    assert.equal(res2.statusCode, 200);
    const res3 = await request(app)
      .post(`/groups/${group_name}/redes`)
      .send([
        {
          "red_id": 10,
          "access": "write"
        },
        {
          "red_id": 4,
          "access": "read"
        }
      ])
      .set("Content-Type", "application/json")
      .set("Authorization", `Bearer ${admin_token}`);
    assert.equal(res3.statusCode, 200);  
    const res4 = await request(app)
      .post(`/obs/puntual/estaciones`)
      .send([
        {
          id: estacion_id,
          tabla: "alturas_prefe",
          id_externo: "t665a443",
          nombre: "test write",
          geom: {type: "Point", coordinates: [0,0]}
        }
      ])
      .set("Content-Type", "application/json")
      .set("Authorization", `Bearer ${writer_token}`);
    assert.equal(res4.statusCode, 200);  
    assert.ok(Array.isArray(res4.body))
    assert.equal(res4.body.length,1)
    estacion = res4.body[0]
  })

  test("POST /obs/puntual/series  series_metadata w/ no admin", async () => {
    const res = await request(app)
      .post("/obs/puntual/series?series_metadata=true")
      .send([{
        estacion: {id: estacion_id},
        var: {id: 2},
        procedimiento: {id: 1},
        unidades: {id: 11}
      }])
      .set("Content-Type", "application/json")
      .set("Authorization", `Bearer ${writer_token}`);

    assert.equal(res.statusCode, 401);
    })

  test("POST /obs/puntual/series  w/ write access", async () => {
    const res = await request(app)
      .post("/obs/puntual/series")
      .send([{
        estacion: {id: estacion_id},
        var: {id: 2},
        procedimiento: {id: 1},
        unidades: {id: 11}
      }])
      .set("Content-Type", "application/json")
      .set("Authorization", `Bearer ${writer_token}`);

    assert.equal(res.statusCode, 200);
    assert(Array.isArray(res.body));
    assert.equal(res.body.length,1)
    serie = res.body[0]
    assert.ok("estacion" in serie)
    assert.ok("tabla" in serie.estacion)
    assert.equal(serie.estacion.tabla,"alturas_prefe")
    assert.ok("id_externo" in serie.estacion)
    assert.equal(serie.estacion.id_externo,"t665a443")
    assert.ok("var" in serie)
    assert.equal(serie.var.id,2)
    assert.ok("procedimiento" in serie)
    assert.equal(serie.procedimiento.id,1)
    assert.ok("unidades" in serie)
    assert.equal(serie.unidades.id,11)
  });

  test("GET /obs/puntual/series", async () => {
    const res = await request(app)
      .get("/obs/puntual/series?tabla=alturas_prefe&id_externo=t665a443&var_id=2&proc_id=1&unit_id=11")
      .set("Authorization", `Bearer ${writer_token}`);

    assert.equal(res.statusCode, 200);
    assert.ok("rows" in res.body)
    assert(Array.isArray(res.body["rows"]));
    assert.equal(res.body["rows"].length,1)
    const s = res.body["rows"][0]
    assert.ok("estacion" in s)
    assert.ok("tabla" in s.estacion)
    assert.equal(s.estacion.tabla,"alturas_prefe")
    assert.ok("id_externo" in s.estacion)
    assert.equal(s.estacion.id_externo,"t665a443")
    assert.ok("var" in s)
    assert.equal(s.var.id,2)
    assert.ok("procedimiento" in s)
    assert.equal(s.procedimiento.id,1)
    assert.ok("unidades" in s)
    assert.equal(s.unidades.id,11)
  })

  test("PUT /obs/puntual/series/{id}", async () => {
    const res2 = await request(app)
      .put(`/obs/puntual/series/${serie.id}`)
      .send({
        serie: {
          estacion: {id: estacion_id},
          var: {id: 2},
          procedimiento: {id: 1},
          unidades: {id: 9}
        }
      })
      .set("Authorization", `Bearer ${writer_token}`);

    assert.equal(res2.statusCode, 200);
    assert("id" in res2.body)
    assert.equal(res2.body.id,serie.id)
    assert("unidades" in res2.body)
    assert("id" in res2.body.unidades)
    assert.equal(res2.body.unidades.id,9)
  });

  // user with no access rights

  test("POST /obs/puntual/series  w/ no write access", async () => {
    const res = await request(app)
      .post("/obs/puntual/series")
      .send([{
        estacion: {id: estacion_id},
        var: {id: 2},
        procedimiento: {id: 1},
        unidades: {id: 11}
      }])
      .set("Content-Type", "application/json")
      .set("Authorization", `Bearer ${noaccess_token}`);
    console.log(res.text)
    assert.equal(res.statusCode, 401);
  });

  test("POST /obs/puntual/estaciones  w/ no write access 2", async () => {
    const res = await request(app)
      .post("/obs/puntual/estaciones")
      .send({
        estacion: {id: estacion_id},
        var: {id: 2},
        procedimiento: {id: 1},
        unidades: {id: 11}
      })
      .set("Content-Type", "application/json")
      .set("Authorization", `Bearer ${reader_token}`);

    assert.equal(res.statusCode, 401);
  });

  test("GET /obs/puntual/series no access", async () => {
    const res = await request(app)
      .get("/obs/puntual/series?tabla=alturas_prefe&id_externo=t665a443")
      .set("Authorization", `Bearer ${noaccess_token}`);

    assert.equal(res.statusCode, 200);
    console.log(res.text)
    assert.ok("rows" in res.body)
    assert(Array.isArray(res.body["rows"]));
    assert.equal(res.body["rows"].length,0)
  })

  test("GET /obs/puntual/series reader access", async () => {
    const res = await request(app)
      .get("/obs/puntual/series?tabla=alturas_prefe&id_externo=t665a443")
      .set("Authorization", `Bearer ${reader_token}`);

    assert.equal(res.statusCode, 200);
    console.log(res.text)
    assert.ok("rows" in res.body)
    assert(Array.isArray(res.body["rows"]));
    assert.equal(res.body["rows"].length,1)
  })


  test("GET /obs/puntual/series/:id no access not found", async () => {
    const res = await request(app)
      .get(`/obs/puntual/series/${serie.id}`)
      .set("Authorization", `Bearer ${noaccess_token}`);

    assert.equal(res.statusCode, 404);
    console.log(res.body)
  })

    test("GET /obs/puntual/series/:id reader access", async () => {
    const res = await request(app)
      .get(`/obs/puntual/series/${serie.id}`)
      .set("Authorization", `Bearer ${reader_token}`);

    assert.equal(res.statusCode, 200);
    console.log(res.body)
    assert.ok(!Array.isArray(res.body));
    assert.equal(res.body.id,serie.id)
  })

  test("DELETE /obs/puntual/series/{id} no access", async () => {
    const res = await request(app)
      .delete(`/obs/puntual/series/${serie.id}`)
      .set("Authorization", `Bearer ${noaccess_token}`);

    console.log(res.text)
    assert.equal(res.statusCode, 400);
  })

  test("DELETE /obs/puntual/series no access, not found", async () => {
    const res = await request(app)
      .delete(`/obs/puntual/series?estacion_id=${estacion_id}`)
      .set("Authorization", `Bearer ${noaccess_token}`);
    console.log(res.text)
    assert.equal(res.statusCode, 200);
    assert.ok(Array.isArray(res.body))
    assert.equal(res.body.length,0)
  })

  // delete

  test("DELETE /obs/puntual/series/{id}", async () => {
    const res = await request(app)
      .delete(`/obs/puntual/series/${serie.id}`)
      .set("Authorization", `Bearer ${writer_token}`);

    assert.equal(res.statusCode, 200);
    assert.ok(!Array.isArray(res.body));
    console.log(res.text)
    const serie_ = res.body
    assert.ok("estacion" in serie_)
    assert.ok("id" in serie_.estacion)
    assert.equal(serie_.estacion.id,estacion_id)
  })

  // restore

  test("DELETE /obs/puntual/estaciones/{id}", async () => {
    const res = await request(app)
      .delete(`/obs/puntual/estaciones/${estacion_id}`)
      .set("Authorization", `Bearer ${writer_token}`);
    console.log(res.text)
    assert.equal(res.statusCode, 200);
  })

  test("DELETE /groups/:name", async () => {
    const res = await request(app)
      .delete(`/groups/${group_name}`)
      .set("Authorization", `Bearer ${admin_token}`);
    assert.equal(res.statusCode, 200);
  })
// })