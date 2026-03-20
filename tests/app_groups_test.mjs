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
const noaccess = {
  name: "noaccess_name",
  role: "writer",
  password: "noaccess_password",
  token: "noaccess_token" // role writer
}
const reader = {
  name: "reader_name",
  role: "public",
  password: "reader_password",
  token: "reader_token" // role public
}
const admin_token = "token_3" // role admin

let group

const group_name = "app_groups_test"

// prepare

test("prepare", async () => {
    const res0 = await request(app)
    .put(`/users/${writer.name}`)
    .send(writer)
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${admin_token}`);
  console.log(res0.body)
  const res1 = await request(app)
    .put(`/users/${noaccess.name}`)
    .send(noaccess)
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${admin_token}`);
  console.log(res1.body)
  const res2 = await request(app)
    .put(`/users/${reader.name}`)
    .send(reader)
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${admin_token}`);
  console.log(res2.body)
  const res3 = await request(app)
    .delete(`/groups/testgroup`)
    .set("Authorization", `Bearer ${admin_token}`);
  console.log(res3.body)
})

// create group

test("POST /groups  fail unauthorized", async () => {
  const res = await request(app)
    .post("/groups")
    .send([
      {
        name: group_name
      }
    ])
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${writer.token}`);
  assert.equal(res.statusCode, 401);
})

test("POST /groups  w/ admin access", async () => {
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
  assert("name" in group)
  assert.equal(group["name"], group_name)
})

test("GET /groups fail unauthorized", async () => {
  const res = await request(app)
    .get("/groups")
    .set("Authorization", `Bearer ${reader.token}`);
  assert.equal(res.statusCode, 401);
})

test("GET /groups", async () => {
  const res = await request(app)
    .get("/groups")
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 200);
  assert(Array.isArray(res.body))
  assert(res.body.length >= 1)
  let found = false
  for(let i=0;i<res.body.length;i++) {
    const gr = res.body[i]
    assert("name" in gr)
    if(gr.name == group_name) {
      found = true
      assert.equal(group.id, gr.id)
    }
  }
  assert(found)
})

test("GET /groups?name=", async () => {
  const res = await request(app)
    .get("/groups")
    .query({name: group_name})
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 200);
  assert.ok(Array.isArray(res.body))
  assert.equal(res.body.length, 1)
  const gr = res.body[0]
  assert.ok("name" in gr)
  assert.equal(group_name, gr.name)
})

test("GET /groups/:name fail unauthorized", async () => {
  const res = await request(app)
    .get(`/groups/${group_name}`)
    .set("Authorization", `Bearer ${writer.token}`);
  assert.equal(res.statusCode, 401);
})

test("GET /groups/:name", async () => {
  const res = await request(app)
    .get(`/groups/${group_name}`)
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 200);
  assert.ok(!Array.isArray(res.body))
  const gr = res.body
  assert.ok("name" in gr)
  assert.equal(group_name, gr.name)
})

// test("PUT /groups/:name  fail unauthorized", async () => {
//   const res = await request(app)
//     .put(`/groups/${group_name}`)
//     .send({
//         name: "test_create_edit"
//     })
//     .set("Content-Type", "application/json")
//     .set("Authorization", `Bearer ${writer.token}`);
//   assert.equal(res.statusCode, 401);
// })

// test("PUT /groups/:name", async () => {
//   const res = await request(app)
//     .put(`/groups/${group_name}`)
//     .send({
//         name: "test_create_edit"
//     })
//     .set("Content-Type", "application/json")
//     .set("Authorization", `Bearer ${admin_token}`);
//   assert.equal(res.statusCode, 200);
//   assert(!Array.isArray(res.body))
//   const gr = res.body
//   assert("name" in gr)
//   assert.equal(gr.name, "test_create_edit")
//   group = gr
// })

// members

test("POST /groups/:name/members (assign membership) unauthorized", async() => {
    const res = await request(app)
    .put(`/groups/${group_name}/members`)
    .send([
        {
            user_name: writer.name
        },{
            user_name: reader.name
        }]
    )
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${writer.token}`);
  assert.equal(res.statusCode, 401);
})

test("POST /groups/:name/members (assign membership)", async() => {
  const res = await request(app)
    .put(`/groups/${group_name}/members`)
    .send([
        {
            user_name: writer.name
        },{
            user_name: reader.name
        }]
    )
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 200);
  assert(Array.isArray(res.body))
  assert.equal(res.body.length,2)
  for(const member of res.body) {
    assert("user_name" in member)
  }
  assert.ok(res.body.map(m=>m.user_name).includes(writer.name))
  assert.ok(res.body.map(m=>m.user_name).includes(reader.name))
})

test("POST /groups/:name/members (assign membership) fail bad request user not found", async() => {
  const res = await request(app)
    .put(`/groups/${group_name}/members`)
    .send([
        {
            user_name: "5436534"
        },{
            user_name: "6346378"
        }]
    )
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 400);
})

test("GET /groups/:name/members", async() => {
  const res = await request(app)
    .get(`/groups/${group_name}/members`)
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 200);
  assert(Array.isArray(res.body))
  assert.equal(res.body.length,2)
  for(const member of res.body) {
    assert("user_name" in member)
  }
  assert.ok(res.body.map(m=>m.user_name).includes(writer.name))
  assert.ok(res.body.map(m=>m.user_name).includes(reader.name))
})

test("GET /groups/:group_name/members/:user_name unauthorized", async() => {
  const res = await request(app)
    .get(`/groups/${group_name}/members/${writer.name}`)
    .set("Authorization", `Bearer ${writer.token}`);
  assert.equal(res.statusCode, 401);
})

test("GET /groups/:group_name/members/:user_name not found", async() => {
  const res = await request(app)
    .get(`/groups/${group_name}/members/45643`)
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 404);
})

test("GET /groups/:group_name/members/:user_name", async() => {
    const res = await request(app)
    .get(`/groups/${group_name}/members/${writer.name}`)
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 200);
  assert(!Array.isArray(res.body))
  const member = res.body
  assert.ok("user_name" in member)
  assert.equal(member.user_name, writer.name)
})

// redes access

test("POST /groups/:group_name/redes unauthorized", async() =>{
  const res = await request(app)
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
    .set("Authorization", `Bearer ${writer.token}`);
  assert.equal(res.statusCode, 401);
})

test("POST /groups/:group_name/redes", async() =>{
  const res = await request(app)
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
  assert.equal(res.statusCode, 200);
  assert(Array.isArray(res.body))
  assert.equal(res.body.length,2)
  for(const member of res.body) {
    assert("red_id" in member)
  }
  assert.ok(res.body.map(m=>m.red_id).includes(10))
  assert.ok(res.body.map(m=>m.red_id).includes(4))
})

test("GET /groups/:name/redes", async() => {
  const res = await request(app)
    .get(`/groups/${group_name}/redes`)
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 200);
  assert(Array.isArray(res.body))
  assert.equal(res.body.length,2)
  for(const member of res.body) {
    assert("red_id" in member)
  }
  assert.ok(res.body.map(m=>m.red_id).includes(10))
  assert.ok(res.body.map(m=>m.red_id).includes(4))
})

test("GET /groups/:group_name/redes/:red_id unauthorized", async() => {
  const res = await request(app)
    .get(`/groups/${group_name}/redes/10`)
    .set("Authorization", `Bearer ${writer.token}`);
  assert.equal(res.statusCode, 401);
})

test("GET /groups/:group_name/redes/:red_id not found", async() => {
  const res = await request(app)
    .get(`/groups/${group_name}/redes/5876`)
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 404);
})

test("GET /groups/:group_name/redes/:red_id", async() => {
    const res = await request(app)
    .get(`/groups/${group_name}/redes/10`)
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 200);
  assert(!Array.isArray(res.body))
  const red = res.body
  assert.ok("red_id" in red)
  assert.equal(red.red_id, 10)
})

// delete

test("DELETE /groups/:group_name/members/:user_name (remove user from group)", async() => {
    const res = await request(app)
    .delete(`/groups/${group_name}/members/${writer.name}`)
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 200);
  assert(!Array.isArray(res.body))
  const member = res.body
  assert.ok("user_name" in member)
  assert.equal(member.user_name, writer.name)
  const res2 = await request(app)
    .get(`/groups/${group_name}/members/${writer.name}`)
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res2.statusCode, 404);
})

test("DELETE /groups/:group_name/redes/:red_id (remove red from group)", async() => {
    const res = await request(app)
    .delete(`/groups/${group_name}/redes/10`)
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 200);
  assert(!Array.isArray(res.body))
  const member = res.body
  assert.ok("red_id" in member)
  assert.equal(member.red_id, 10)
  const res2 = await request(app)
    .get(`/groups/${group.id}/redes/10`)
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res2.statusCode, 404);
})

test("DELETE /groups/:name  fail unauthorized", async () => {
  const res = await request(app)
    .delete(`/groups/${group_name}`)
    .set("Authorization", `Bearer ${writer.token}`);
  assert.equal(res.statusCode, 401);
})

test("DELETE /groups/:name", async () => {
  const res = await request(app)
    .delete(`/groups/${group_name}`)
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 200);
  assert(!Array.isArray(res.body))
  const gr = res.body
  assert("name" in gr)
  assert.equal(gr.name, group_name)
  const res2 = await request(app)
    .get(`/groups/${group_name}`)
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res2.statusCode, 404);
})


// restore 
test("restore", async() => {
    const res0 = await request(app)
    .delete(`/users/${writer.name}`)
    .set("Authorization", `Bearer ${admin_token}`);
  console.log(res0.body)
  assert.equal(res0.statusCode,200)
  const res1 = await request(app)
    .delete(`/users/${noaccess.name}`)
    .set("Authorization", `Bearer ${admin_token}`);
  console.log(res1.body)
  assert.equal(res1.statusCode,200)
  const res2 = await request(app)
    .delete(`/users/${reader.name}`)
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res2.statusCode,200)
  console.log(res2.body)
})