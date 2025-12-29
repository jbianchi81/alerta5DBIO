import test from "node:test";
import assert from "node:assert/strict";
import request from "supertest";
import app from "../app/rest.mjs";

const writer = {
  name: "writer_name",
  role: "writer",
  password: "writer_password",
  token: "writer_token" // role writer
}
const admin = {
  name: "admin_name",
  role: "admin",
  password: "admin_password",
  token: "admin_token" // role admin
}
const noaccess = {
  name: "noaccess_name",
  role: "writer",
  password: "noaccess_password",
  token: "noaccess_token" // role writer
}

const admin_token = "token_3" // debe preexistir

let group
const group_name = "test_app_estacion"

// prepare
test("prepare group", async () => {
  // create users
  const res0 = await request(app)
    .put(`/users/${writer.name}`)
    .send(writer)
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${admin_token}`);
  console.log(res0.body)
  const res_ad = await request(app)
    .put(`/users/${admin.name}`)
    .send(admin)
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${admin_token}`);
  console.log(res_ad.body)
  const res_no = await request(app)
    .put(`/users/${noaccess.name}`)
    .send(noaccess)
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${admin_token}`);
  console.log(res_no.body)

  // create group
  const res = await request(app)
    .post("/groups")
    .send([
      {
        name: group_name
      }
    ])
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 201);
  assert(Array.isArray(res.body))
  assert.equal(res.body.length,1)
  group = res.body[0]
  // assign membership
  const res2 = await request(app)
    .put(`/groups/${group.name}/members`)
    .send([
        {
            user_name: writer.name
        },{
            user_name: admin.name
        }]
    )
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res2.statusCode, 200);
  assert(Array.isArray(res2.body))
  assert.equal(res2.body.length,2)
  // grant access to redes
  const res3 = await request(app)
    .post(`/groups/${group.name}/redes`)
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
  assert(Array.isArray(res3.body))
  assert.equal(res3.body.length,2)
})

// redes

test("GET /obs/puntual/fuentes", async() => {
  const res = await request(app)
    .get("/obs/puntual/fuentes")
    .set("Authorization", `Bearer ${writer.token}`);
  assert.equal(res.statusCode, 200);
  assert(Array.isArray(res.body));
  assert.equal(res.body.length,2)
  assert.ok(res.body.map(r=>r.id).indexOf(10) >= 0)
  assert.ok(res.body.map(r=>r.id).indexOf(4) >= 0)
})

test("GET /obs/puntual/fuentes admin", async() => {
  const res = await request(app)
    .get("/obs/puntual/fuentes")
    .set("Authorization", `Bearer ${admin.token}`);
  assert.equal(res.statusCode, 200);
  assert(Array.isArray(res.body));
  assert.equal(res.body.length,2)
  assert.ok(res.body.map(r=>r.id).indexOf(10) >= 0)
  assert.ok(res.body.map(r=>r.id).indexOf(4) >= 0)
})

test("GET /obs/puntual/fuentes no access", async() => {
  const res = await request(app)
    .get("/obs/puntual/fuentes")
    .set("Authorization", `Bearer ${noaccess.token}`);
  assert.equal(res.statusCode, 200);
  assert(Array.isArray(res.body));
  assert.equal(res.body.length,0)
})


let estacion

test("POST /obs/puntual/estaciones  w/ write access", async () => {
  const res = await request(app)
    .post("/obs/puntual/estaciones")
    .send({
      tabla: "alturas_prefe",
      id_externo: "t665a443",
      nombre: "test write",
      geom: {type: "Point", coordinates: [0,0]}
    })
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${writer.token}`);

  assert.equal(res.statusCode, 200);
  assert(Array.isArray(res.body));
  assert.equal(res.body.length,1)
  assert.equal(res.body[0].tabla,"alturas_prefe")
  assert.equal(res.body[0].id_externo,"t665a443")
  estacion = res.body[0]
});

test("GET /obs/puntual/estaciones", async () => {
  const res = await request(app)
    .get("/obs/puntual/estaciones?tabla=alturas_prefe&id_externo=t665a443")
    .set("Authorization", `Bearer ${writer.token}`);

  assert.equal(res.statusCode, 200);
  assert(Array.isArray(res.body));
  assert.equal(res.body.length,1)
  assert.equal(res.body[0].tabla,"alturas_prefe")
  assert.equal(res.body[0].id_externo,"t665a443")
  assert.equal(estacion.id,res.body[0].id)
})

test("GET /obs/puntual/estaciones with pagination", async () => {
  const res = await request(app)
    .get("/obs/puntual/estaciones?tabla=alturas_prefe&id_externo=t665a443&pagination=true")
    .set("Authorization", `Bearer ${writer.token}`);

  assert.equal(res.statusCode, 200);
  assert.ok(!Array.isArray(res.body));
  assert.ok("estaciones" in res.body)
  assert.ok(Array.isArray(res.body.estaciones))
  assert.equal(res.body.estaciones.length,1)
  assert.equal(res.body.estaciones[0].tabla,"alturas_prefe")
  assert.equal(res.body.estaciones[0].id_externo,"t665a443")
  assert.equal(estacion.id,res.body.estaciones[0].id)
})


test("PUT /obs/puntual/estaciones/{id}", async () => {
  const res2 = await request(app)
    .put(`/obs/puntual/estaciones/${estacion.id}`)
    .send({
      estacion: {
        habilitar: true
      }
    })
    .set("Authorization", `Bearer ${writer.token}`);

  assert.equal(res2.statusCode, 200);
  assert("tabla" in res2.body)
  assert.equal(res2.body.tabla,"alturas_prefe")
  assert.equal(res2.body.id_externo,"t665a443")
  assert.equal(res2.body.habilitar,true)
});


test("PUT /obs/puntual/estaciones/{id}?", async () => {
  const res2 = await request(app)
    .put(`/obs/puntual/estaciones/${estacion.id}?propietario=pna`)
    .set("Authorization", `Bearer ${writer.token}`);

  assert.equal(res2.statusCode, 200);
  assert("tabla" in res2.body)
  assert.equal(res2.body.tabla,"alturas_prefe")
  assert.equal(res2.body.id_externo,"t665a443")
  assert.equal(res2.body.propietario,"pna")
});

// user with no access rights

test("POST /obs/puntual/estaciones  w/ no write access", async () => {
  const res = await request(app)
    .post("/obs/puntual/estaciones")
    .send({
      tabla: "red_ana_pluvio",
      id_externo: "poj365io",
      nombre: "test no write",
      geom: {type: "Point", coordinates: [0,0]}
    })
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${writer.token}`);

  assert.equal(res.statusCode, 401);
});

test("POST /obs/puntual/estaciones  w/ no write access 2", async () => {
  const res = await request(app)
    .post("/obs/puntual/estaciones")
    .send({
      tabla: "alturas_prefe",
      id_externo: "poj365io",
      nombre: "test no write",
      geom: {type: "Point", coordinates: [0,0]}
    })
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${noaccess.token}`);

  assert.equal(res.statusCode, 401);
});

test("GET /obs/puntual/estaciones no access", async () => {
  const res = await request(app)
    .get("/obs/puntual/estaciones?tabla=alturas_prefe&id_externo=t665a443")
    .set("Authorization", `Bearer ${noaccess.token}`);

  assert.equal(res.statusCode, 200);
  assert(Array.isArray(res.body));
  assert.equal(res.body.length,0)
})

test("PUT /obs/puntual/estaciones/{id}? no access", async () => {
  const res2 = await request(app)
    .put(`/obs/puntual/estaciones/${estacion.id}?propietario=pna`)
    .set("Authorization", `Bearer ${noaccess.token}`);
  console.log(res2.body)
  assert.equal(res2.statusCode, 401);
});

test("DELETE /obs/puntual/estaciones/{id} no access", async () => {
  const res = await request(app)
    .delete(`/obs/puntual/estaciones/${estacion.id}`)
    .set("Authorization", `Bearer ${noaccess.token}`);
  console.log(res.body)
  assert.equal(res.statusCode, 401);
})


// delete

test("DELETE /obs/puntual/estaciones/{id}", async () => {
  const res = await request(app)
    .delete(`/obs/puntual/estaciones/${estacion.id}`)
    .set("Authorization", `Bearer ${writer.token}`);
  console.log(res.body)
  assert.equal(res.statusCode, 200);
  assert("tabla" in res.body)
  assert.equal(res.body.tabla,"alturas_prefe")
  assert.equal(res.body.id_externo,"t665a443")
  assert.equal(res.body.id,estacion.id)
})

test("DELETE /groups/:name", async () => {
  const res = await request(app)
    .delete(`/groups/${group_name}`)
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 200);
})
