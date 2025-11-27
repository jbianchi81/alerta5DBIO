import test from "node:test";
import assert from "node:assert/strict";
import request from "supertest";
import app from "../app/rest.mjs";

const writer_token = "token" // writer user
const reader_token = "token_1" // reader user
const admin_token = "token_3" // admin user

let group

// prepare

test("DELETE /groups/:name prepare", async () => {
  const res = await request(app)
    .delete(`/groups/testgroup`)
    .set("Authorization", `Bearer ${admin_token}`);
})

// create group

test("POST /groups  fail unauthorized", async () => {
  const res = await request(app)
    .post("/groups")
    .send([
      {
        name: "test_create"
      }
    ])
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${writer_token}`);
  assert.equal(res.statusCode, 401);
})

test("POST /groups  w/ admin access", async () => {
  const res = await request(app)
    .post("/groups")
    .send([
      {
        name: "test_create"
      }
    ])
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 201);
  assert(Array.isArray(res.body))
  assert.equal(res.body.length,1)
  group = res.body[0]
  assert("name" in group)
  assert.equal(group["name"], "test_create")
})

test("GET /groups fail unauthorized", async () => {
  const res = await request(app)
    .get("/groups")
    .set("Authorization", `Bearer ${reader_token}`);
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
    if(gr.name == group.name) {
      found = true
      assert.equal(group.id, gr.id)
    }
  }
  assert(found)
})

test("GET /groups?name=", async () => {
  const res = await request(app)
    .get("/groups")
    .query({name: group.name})
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 200);
  assert(Array.isArray(res.body))
  assert.equal(res.body.length, 1)
  const gr = res.body[0]
  assert("name" in gr)
  assert(group.name, gr.name)
})

test("GET /groups/:name fail unauthorized", async () => {
  const res = await request(app)
    .get(`/groups/${group.name}`)
    .set("Authorization", `Bearer ${writer_token}`);
  assert.equal(res.statusCode, 401);
})

test("GET /groups/:name", async () => {
  const res = await request(app)
    .get(`/groups/${group.name}`)
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 200);
  assert(!Array.isArray(res.body))
  const gr = res.body
  assert("name" in gr)
  assert(group.name, gr.name)
})

// test("PUT /groups/:name  fail unauthorized", async () => {
//   const res = await request(app)
//     .put(`/groups/${group.name}`)
//     .send({
//         name: "test_create_edit"
//     })
//     .set("Content-Type", "application/json")
//     .set("Authorization", `Bearer ${writer_token}`);
//   assert.equal(res.statusCode, 401);
// })

// test("PUT /groups/:name", async () => {
//   const res = await request(app)
//     .put(`/groups/${group.name}`)
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
    .put(`/groups/${group.name}/members`)
    .send([
        {
            user_id: 5
        },{
            user_id: 6
        }]
    )
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${writer_token}`);
  assert.equal(res.statusCode, 401);
})

test("POST /groups/:name/members (assign membership)", async() => {
  const res = await request(app)
    .put(`/groups/${group.name}/members`)
    .send([
        {
            user_id: 5
        },{
            user_id: 6
        }]
    )
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 200);
  assert(Array.isArray(res.body))
  assert.equal(res.body.length,2)
  for(const member of res.body) {
    assert("user_id" in member)
  }
  assert.ok(res.body.map(m=>m.user_id).includes(5))
  assert.ok(res.body.map(m=>m.user_id).includes(6))
})

test("POST /groups/:name/members (assign membership) fail bad request user id not found", async() => {
  const res = await request(app)
    .put(`/groups/${group.name}/members`)
    .send([
        {
            user_id: 5436534
        },{
            user_id: 6346378
        }]
    )
    .set("Content-Type", "application/json")
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 400);
})

test("GET /groups/:name/members", async() => {
  const res = await request(app)
    .get(`/groups/${group.name}/members`)
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 200);
  assert(Array.isArray(res.body))
  assert.equal(res.body.length,2)
  for(const member of res.body) {
    assert("user_id" in member)
  }
  assert.ok(res.body.map(m=>m.user_id).includes(5))
  assert.ok(res.body.map(m=>m.user_id).includes(6))
})

test("GET /groups/:group_name/members/:user_id unauthorized", async() => {
  const res = await request(app)
    .get(`/groups/${group.name}/members/5`)
    .set("Authorization", `Bearer ${writer_token}`);
  assert.equal(res.statusCode, 401);
})

test("GET /groups/:group_name/members/:user_id not found", async() => {
  const res = await request(app)
    .get(`/groups/${group.name}/members/45643`)
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 404);
})

test("GET /groups/:group_name/members/:user_id", async() => {
    const res = await request(app)
    .get(`/groups/${group.name}/members/5`)
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 200);
  assert(!Array.isArray(res.body))
  const member = res.body
  assert.ok("user_id" in member)
  assert.equal(member.user_id, 5)
})

test("DELETE /groups/:group_name/members/:user_id (remove user from group)", async() => {
    const res = await request(app)
    .delete(`/groups/${group.name}/members/5`)
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 200);
  assert(!Array.isArray(res.body))
  const member = res.body
  assert.ok("user_id" in member)
  assert.equal(member.user_id, 5)
  const res2 = await request(app)
    .get(`/groups/${group.id}/members/5`)
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res2.statusCode, 404);
})


test("DELETE /groups/:name  fail unauthorized", async () => {
  const res = await request(app)
    .delete(`/groups/${group.name}`)
    .set("Authorization", `Bearer ${writer_token}`);
  assert.equal(res.statusCode, 401);
})

test("DELETE /groups/:name", async () => {
  const res = await request(app)
    .delete(`/groups/${group.name}`)
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res.statusCode, 200);
  assert(!Array.isArray(res.body))
  const gr = res.body
  assert("name" in gr)
  assert.equal(gr.name, group.name)
  const res2 = await request(app)
    .get(`/groups/${group.name}`)
    .set("Authorization", `Bearer ${admin_token}`);
  assert.equal(res2.statusCode, 404);
})
