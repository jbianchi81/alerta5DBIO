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
const reader = {
  name: "reader_name",
  role: "writer",
  password: "reader_password",
  token: "reader_token" // role writer
}
const noaccess = {
  name: "noaccess_name",
  role: "writer",
  password: "noaccess_password",
  token: "noaccess_token" // role writer
}

const admin_token = "token_3" // debe preexistir

const group_name = "gr"
const read_group_name = "read_gr"

test("crea usuarios", async ()=> {
  const res0 = await request(app)
    .put(`/users/${writer.name}`)
    .send(writer)
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${admin_token}`);
  console.log(res0.body)
  const res_r = await request(app)
    .put(`/users/${reader.name}`)
    .send(reader)
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${admin_token}`);
  console.log(res_r.body)
  const res_no = await request(app)
    .put(`/users/${noaccess.name}`)
    .send(noaccess)
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${admin_token}`);
  console.log(res_no.body)
})

test("crea grupo de usuarios y agrega usuario", async ()=> {
  var res0 = await request(app)
    .post("/groups")
    .send([
      {
        name: group_name
      },
      {
        name: read_group_name
      }
    ])
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${admin_token}`);
  console.log(res0.body)
  assert.equal(res0.statusCode,200)
  // add member
  res0 = await request(app)
    .put(`/groups/${group_name}/members`)
    .send(
      [{
          user_name: writer.name
      }]
    )
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${admin_token}`);
  console.log(res0.body)
  assert.equal(res0.statusCode,200)
  // add member reader
  res0 = await request(app)
    .put(`/groups/${read_group_name}/members`)
    .send(
      [{
          user_name: reader.name
      }]
    )
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${admin_token}`);
  console.log(res0.body)
  assert.equal(res0.statusCode,200)
})

test("POST areas group", async() => {
  const res = await request(app)
    .post("/obs/areal/area_groups")
    .send([
      {
        id: 123,
        name: "g1"
      }
    ])
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${writer.token}`);
  assert.equal(res.statusCode, 200);
  assert(Array.isArray(res.body))
  assert.equal(res.body.length,1)
  assert("id" in res.body[0])
  assert.equal(res.body[0].id, 123)
  assert("name" in res.body[0])
  assert.equal(res.body[0].name, "g1")  
  // ownership must be granted to creator
  assert("owner_id" in res.body[0])
  assert.equal(res.body[0].owner_id, 1)  
})

test("grant access to ag to ug", async () => {
  const res = await request(app)
    .post(`/obs/areal/area_groups/${123}/access`)
      .send([{
        name: group_name,
        access: "write"
      },{
        name: read_group_name,
        access: "read"
      }])
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${admin_token}`);
  console.log(res.body)
  assert.equal(res.statusCode,200)
  assert.ok(Array.isArray(res.body))
  assert.equal(res.body.length,2)
  for(const item of res.body) {
    assert.ok("name" in item)
    assert.ok("access" in item)
    assert.ok([group_name, read_group_name].indexOf(item.name) >= 0)
  }
})

test("list access rights", async() => {
  const res = await request(app)
    .get(`/obs/areal/area_groups/${123}/access`)
    .set("Authorization", `Bearer ${admin_token}`);
  console.log(res.body)
  assert.ok(Array.isArray(res.body))
  assert.length(res.body,2)
  for(const item of res.body) {
    assert.ok("name" in item)
    assert.ok("access" in item)
    assert.ok([group_name, read_group_name].indexOf(item.name) >= 0)
  }
})

test("read access rights", async() => {
  const res = await request(app)
    .get(`/obs/areal/area_groups/${123}/access/${group_name}`)
    .set("Authorization", `Bearer ${admin_token}`);
  console.log(res.body)
  assert.ok(!Array.isArray(res.body))
  assert.ok("name" in res.body)
  assert.equal(res.body.name,group_name)
  assert.ok("access" in res.body)
  assert.equal(res.body.access,"write") 
})

test("POST /obs/areal/areas within group user unauthorized", async() => {
  const res = await request(app)
    .post("/obs/areal/areas")
    .send([
        {
            id: 876358,
            nombre: "test write",
            geom: {type: "Polygon", coordinates: [[0,0],[1,0],[1,1],[0,1],[0,0]]},
            group_id: 123
        }
    ])
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${noaccess.token}`);
  assert.equal(res.statusCode, 401);
})

test("POST /obs/areal/areas within group", async () => {
  const res = await request(app)
    .post("/obs/areal/areas")
    .send({
      id: 876358,
      nombre: "test write",
      geom: {type: "Polygon", coordinates: [[0,0],[1,0],[1,1],[0,1],[0,0]]},
      group_id: 123
    })
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${writer.token}`);
  assert.equal(res.statusCode, 200);
  assert(Array.isArray(res.body));
  assert.equal(res.body.length,1)
  assert.equal(res.body[0].id,876358)
  assert.equal(res.body[0].nombre,"test write")
  assert.equal(res.body[0].group_id,123)
});

test("POST add member to group other user unauthorized", async() => {
  const res = await request(app)
    .post("/obs/areal/area_groups/123/areas")
    .send([
      {
        id: 876360,
        nombre: "test write 2",
        geom: {type: "Polygon", coordinates: [[0,0],[-1,0],[-1,-1],[0,-1],[0,0]]}
      }
    ])
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${noaccess.token}`);
  assert.equal(res.statusCode, 401);
})

test("POST add area to group", async() => {
  const res = await request(app)
    .post("/obs/areal/area_groups/123/areas")
    .send([
      {
        id: 876360,
        nombre: "test write 2",
        geom: {type: "Polygon", coordinates: [[0,0],[-1,0],[-1,-1],[0,-1],[0,0]]}
      }
    ])
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${writer.token}`);
  assert.equal(res.statusCode, 200);
  assert(Array.isArray(res.body))
  assert.equal(res.body.length,1)
  assert("id" in res.body[0])
  assert.equal(res.body[0].id, 876360)
  assert("nombre" in res.body[0])
  assert.equal(res.body[0].nombre, "test write 2")
  assert("geom" in res.body[0])
  assert.equal(JSON.stringify(res.body[0].geom), JSON.stringify({type: "Polygon", coordinates: [[0,0],[-1,0],[-1,-1],[0,-1],[0,0]]}))
})

test("GET areas group", async() => {
  const res = await request(app)
    .get("/obs/areal/area_groups/123")
    .set("Authorization", `Bearer ${writer.token}`);
  assert.equal(res.statusCode, 200);
  assert(!Array.isArray(res.body))
  assert("id" in res.body)
  assert.equal(res.body.id, 123)
  assert("name" in res.body)
  assert.equal(res.body.name, "g1")
  assert("access_level" in res.body)
  assert.equal(res.body.access_level, "write")
  assert("areas" in res.body)
  assert(Array.isArray(res.body.areas))
  assert.equal(res.body.areas.length,2)
  assert("id" in res.body.areas[0])
  assert.equal(res.body.areas[0].id, 876358)
  assert("nombre" in res.body.areas[0])
  assert.equal(res.body.areas[0].nombre, "test write")
  // no geom
  assert(!("geom" in res.body.areas[0]))
  assert("id" in res.body.areas[1])
  assert.equal(res.body.areas[1].id, 876360)
  assert("nombre" in res.body.areas[1])
  assert.equal(res.body.areas[1].nombre, "test write 2")
  // no geom
  assert(!("geom" in res.body.areas[1]))
})

test("GET areas group other user unauthorized", async() => {
  const res = await request(app)
    .get("/obs/areal/area_groups/123")
    .set("Authorization", `Bearer ${noaccess.token}`);
  assert.equal(res.statusCode, 401);
})

test("GET areas group fail not found", async() => {
  const res = await request(app)
    .get("/obs/areal/area_groups/124")
    .set("Authorization", `Bearer ${writer.token}`);
  assert.equal(res.statusCode, 404);
})

test("GET areas group areas", async() => {
  const res = await request(app)
    .get("/obs/areal/area_groups/123/areas")
    .set("Authorization", `Bearer ${writer.token}`);
  assert.equal(res.statusCode, 200);
  assert(Array.isArray(res.body))
  assert.equal(res.body.length,1)
  assert("id" in res.body[0])
  assert.equal(res.body[0].id, 876358)
  assert("nombre" in res.body[0])
  assert.equal(res.body[0].nombre, "test write")
  assert("geom" in res.body[0])
  assert.equal(JSON.stringify(res.body[0].geom), JSON.stringify({type: "Polygon", coordinates: [[0,0],[1,0],[1,1],[0,1],[0,0]]}))
  assert("id" in res.body[1])
  assert.equal(res.body[1].id, 876360)
  assert("nombre" in res.body[1])
  assert.equal(res.body[1].nombre, "test write 2")
  assert("geom" in res.body[1])
  assert.equal(JSON.stringify(res.body[1].geom), JSON.stringify({type: "Polygon", coordinates: [[0,0],[-1,0],[-1,-1],[0,-1],[0,0]]}))

})

test("GET areas group areas other user unauthorized", async() => {
  const res = await request(app)
    .get("/obs/areal/area_groups/123/areas")
    .set("Authorization", `Bearer ${noaccess.token}`);
  assert.equal(res.statusCode, 401);
})

// FUENTE

let fuente

test("POST fuente grant write access to creator", async() => {
    const res = await request(app)
      .post("/obs/areal/fuentes")
      .send([
        {
            nombre: "test",

        }
      ])
      .set("Content-Type", "application/json")
      .set("Authorization", `Bearer ${writer.token}`);
    assert.equal(res.statusCode, 200);
    assert(Array.isArray(res.body))
    assert.equal(res.body.length,1)
    fuente = res.body[0]
    assert("id" in fuente)
    assert("nombre" in fuente)
    assert.equal(fuente.nombre, "test")
    assert("owner_id" in fuente)
    assert.equal(fuente.owner_id, 1)
})

let serie

test("POST serie of owned fuente", async() => {
    const res = await request(app)
      .post("/obs/areal/series")
      .send([
        {
            fuentes_id: fuente.id,
            area_id: 876358,
            var_id: 1,
            proc_id: 4,
            unit_id: 22
        }
      ])
      .set("Content-Type", "application/json")
      .set("Authorization", `Bearer ${writer.token}`);
    assert.equal(res.statusCode, 200);
    assert(Array.isArray(res.body))
    assert.equal(res.body.length,1)
    serie = res.body[0]
    assert("id" in serie)
    assert("fuentes_id" in serie)
    assert.equal(serie.fuentes_id, fuente.id)
    assert("area_id" in serie)
    assert.equal(serie.area_id, 876358)
    assert("var_id" in serie)
    assert.equal(serie.var_id, 1)
})

test("POST serie of owned fuente fail unauthorized", async() => {
    const res = await request(app)
      .post("/obs/areal/series")
      .send([
        {
            fuentes_id: fuente.id,
            area_id: 876358,
            var_id: 1,
            proc_id: 4,
            unit_id: 22
        }
      ])
      .set("Content-Type", "application/json")
      .set("Authorization", `Bearer ${noaccess.token}`);
    assert.equal(res.statusCode, 401);
})

// DELETE

test("DELETE areas group member other user unauthorized", async() => {
  const res = await request(app)
    .delete("/obs/areal/area_groups/123/members/876358")
    .set("Authorization", `Bearer ${noaccess.token}`);
  assert.equal(res.statusCode, 401);
})

test("DELETE areas group member", async() => {
  const res = await request(app)
    .delete("/obs/areal/area_groups/123/members/876358")
    .set("Authorization", `Bearer ${writer.token}`);
  assert.equal(res.statusCode, 200);
  assert(!Array.isArray(res.body))
  assert("id" in res.body)
  assert.equal(res.body.id, 876358)
})

test("delete access rights", async() => {
  var res = await request(app)
    .delete(`/obs/areal/area_groups/${123}/access/${group_name}`)
    .set("Authorization", `Bearer ${admin_token}`);
  console.log(res.body)
  assert.equal(res.statusCode,200)
  assert.ok(!Array.isArray(res.body))
  assert.ok("name" in res.body)
  assert.equal(res.body.name,group_name)
  assert.ok("access" in res.body)
  assert.equal(res.body.access,"write") 
  // confirm deleted
  res = await request(app)
    .get(`/obs/areal/area_groups/${123}/access/${group_name}`)
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode,404)
})

test("DELETE areas group other user unauthorized", async() => {
  const res = await request(app)
    .delete("/obs/areal/area_groups/123")
    .set("Authorization", `Bearer ${noaccess.token}`);
  assert.equal(res.statusCode, 401);
})

test("DELETE areas group", async() => {
  const res = await request(app)
    .delete("/obs/areal/area_groups/123")
    .set("Authorization", `Bearer ${writer.token}`);
  assert.equal(res.statusCode, 200);
  assert(!Array.isArray(res.body))
  assert("id" in res.body)
  assert.equal(res.body.id, 123)
  assert("name" in res.body)
  assert.equal(res.body.name, "g1")  
})

test("GET areas group not found", async() => {
  const res = await request(app)
    .get("/obs/areal/area_groups/123")
    .set("Authorization", `Bearer ${writer.token}`);
  assert.equal(res.statusCode, 404)
})

test("DELETE /obs/areal/areas  w/ write access", async () => {
  const res = await request(app)
    .delete("/obs/areal/areas/876358")
    .set("Authorization", `Bearer ${writer.token}`);
  assert.equal(res.statusCode, 200);
  assert(!Array.isArray(res.body));
  assert.equal(res.body.id,876358)
  assert.equal(res.body.nombre,"test write")
});

test("elimina grupo de usuarios", async ()=> {
  var res = await request(app)
    .delete(`/groups/${group_name}`)
    .set("Authorization", `Bearer ${admin_token}`);
  console.log(res.body)
  assert.equal(res.statusCode, 200)
})


test("elimina usuarios", async ()=> {
  var res = await request(app)
    .delete(`/users/${writer.name}`)
    .set("Authorization", `Bearer ${admin_token}`);
  console.log(res.body)
  assert.equal(res.statusCode, 200)
  var res = await request(app)
    .delete(`/users/${reader.name}`)
    .set("Authorization", `Bearer ${admin_token}`);
  console.log(res.body)
  assert.equal(res.statusCode, 200)
  res = await request(app)
    .delete(`/users/${noaccess.name}`)
    .set("Authorization", `Bearer ${admin_token}`);
  console.log(res.body)
  assert.equal(res.statusCode, 200)
})
