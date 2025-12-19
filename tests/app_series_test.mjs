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
const admin = {
  name: "admin_name",
  role: "admin",
  password: "admin_password",
  token: "admin_token" // role admin
}
const reader_of_red_10 = {
  name: "other_writer",
  role: "writer",
  password: "other_writer_password",
  token: "other_writer_token" // role admin
}

const admin_token = "token_3" // debe preexistir

let estacion
let serie

const group_name = "app_series_test_writers"
const other_group_name = "app_series_test_readers"
const estacion_id = 2948

// test('parent test', async (t) => {
  // preparacion

  test("crea usuarios, crea grupo, asigna usuario y red", async ()=> {

    const res0 = await request(app)
      .put(`/users/${writer.name}`)
      .send(writer)
      .set("Content-Type", "application/json")
      .set("Authorization", `Bearer ${admin_token}`);
    console.log(res0.body)
    const res_no = await request(app)
      .put(`/users/${noaccess.name}`)
      .send(noaccess)
      .set("Content-Type", "application/json")
      .set("Authorization", `Bearer ${admin_token}`);
    console.log(res_no.body)
      // assert.equal(res0.statusCode, 201)    
    const res_re = await request(app)
      .put(`/users/${reader.name}`)
      .send(reader)
      .set("Content-Type", "application/json")
      .set("Authorization", `Bearer ${admin_token}`);
    console.log(res_re.body)
    const res_ad = await request(app)
      .put(`/users/${admin.name}`)
      .send(admin)
      .set("Content-Type", "application/json")
      .set("Authorization", `Bearer ${admin_token}`);
    console.log(res_ad.body)
    const res_or = await request(app)
      .put(`/users/${reader_of_red_10.name}`)
      .send(reader_of_red_10)
      .set("Content-Type", "application/json")
      .set("Authorization", `Bearer ${admin_token}`);
    console.log(res_or.body)

    const res = await request(app)
      .post("/groups")
      .send([
        {
          name: group_name
        },
        {
          name: other_group_name
        }
      ])
      .set("Content-Type", "application/json")
      .set("Authorization", `Bearer ${admin_token}`);
    console.log(res.body)
    // assert.equal(res.statusCode, 201)

    const res2 = await request(app)
      .put(`/groups/${group_name}/members`)
      .send([
          {
              user_name: writer.name
          },{
              user_name: reader.name
          },{
            user_name: admin.name
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
    
    // readers group
    const res11 = await request(app)
      .put(`/groups/${other_group_name}/members`)
      .send([
          {
              user_name: reader_of_red_10.name
          }]
      )
      .set("Content-Type", "application/json")
      .set("Authorization", `Bearer ${admin_token}`);
    assert.equal(res11.statusCode, 200);
    const res12 = await request(app)
      .post(`/groups/${other_group_name}/redes`)
      .send([
        {
          "red_id": 11,
          "access": "write"
        },
        {
          "red_id": 10,
          "access": "read"
        }
      ])
      .set("Content-Type", "application/json")
      .set("Authorization", `Bearer ${admin_token}`);
    assert.equal(res12.statusCode, 200);  
    
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
      .set("Authorization", `Bearer ${writer.token}`);
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
      .set("Authorization", `Bearer ${writer.token}`);

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
      .set("Authorization", `Bearer ${writer.token}`);

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
      .set("Authorization", `Bearer ${writer.token}`);

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
      .set("Authorization", `Bearer ${writer.token}`);

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
      .set("Authorization", `Bearer ${noaccess.token}`);
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
      .set("Authorization", `Bearer ${reader.token}`);

    assert.equal(res.statusCode, 401);
  });

  test("POST /obs/puntual/estaciones  w/ no write access 3", async () => {
    const res = await request(app)
      .post("/obs/puntual/estaciones")
      .send({
        estacion: {id: estacion_id},
        var: {id: 2},
        procedimiento: {id: 1},
        unidades: {id: 11}
      })
      .set("Content-Type", "application/json")
      .set("Authorization", `Bearer ${reader_of_red_10.token}`);

    assert.equal(res.statusCode, 401);
  });

  test("GET /obs/puntual/series no access", async () => {
    const res = await request(app)
      .get("/obs/puntual/series?tabla=alturas_prefe&id_externo=t665a443")
      .set("Authorization", `Bearer ${noaccess.token}`);

    assert.equal(res.statusCode, 200);
    console.log(res.text)
    assert.ok("rows" in res.body)
    assert(Array.isArray(res.body["rows"]));
    assert.equal(res.body["rows"].length,0)
  })

  test("GET /obs/puntual/series reader access", async () => {
    const res = await request(app)
      .get("/obs/puntual/series?tabla=alturas_prefe&id_externo=t665a443")
      .set("Authorization", `Bearer ${reader.token}`);

    assert.equal(res.statusCode, 200);
    console.log(res.text)
    assert.ok("rows" in res.body)
    assert(Array.isArray(res.body["rows"]));
    assert.equal(res.body["rows"].length,1)
  })

  test("GET /obs/puntual/series reader access 2", async () => {
    const res = await request(app)
      .get("/obs/puntual/series?tabla=alturas_prefe&id_externo=t665a443")
      .set("Authorization", `Bearer ${reader_of_red_10.token}`);

    assert.equal(res.statusCode, 200);
    console.log(res.text)
    assert.ok("rows" in res.body)
    assert(Array.isArray(res.body["rows"]));
    assert.equal(res.body["rows"].length,1)
  })


  test("GET /obs/puntual/series/:id no access not found", async () => {
    const res = await request(app)
      .get(`/obs/puntual/series/${serie.id}`)
      .set("Authorization", `Bearer ${noaccess.token}`);

    assert.equal(res.statusCode, 404);
    console.log(res.body)
  })

    test("GET /obs/puntual/series/:id reader access", async () => {
    const res = await request(app)
      .get(`/obs/puntual/series/${serie.id}`)
      .set("Authorization", `Bearer ${reader.token}`);

    assert.equal(res.statusCode, 200);
    console.log(res.body)
    assert.ok(!Array.isArray(res.body));
    assert.equal(res.body.id,serie.id)
  })

  // OBSERVACIONES

  test("POST /obs/puntual/series/:id/observaciones  w/ write access", async () => {
    const res = await request(app)
      .post(`/obs/puntual/series/${serie.id}/observaciones`)
      .send([{
        timestart: new Date("2000-01-01T03:00:00.000Z"),
        timeend: new Date("2000-01-01T03:00:00.000Z"),
        valor: 1.11
      },{
        timestart: new Date("2000-01-02T03:00:00.000Z"),
        timeend: new Date("2000-01-02T03:00:00.000Z"),
        valor: 2.22
      },{
        timestart: new Date("2000-01-03T03:00:00.000Z"),
        timeend: new Date("2000-01-03T03:00:00.000Z"),
        valor: 3.33
      }])
      .set("Content-Type", "application/json")
      .set("Authorization", `Bearer ${writer.token}`);

    assert.equal(res.statusCode, 200);
    assert(Array.isArray(res.body));
    assert.equal(res.body.length,3)
    const observaciones = res.body[0]
    for(const o of observaciones) {
      assert.ok("timestart" in observaciones)
      assert.ok(new Date(observaciones.timestart).toString() != "Invalid Date")
      assert.ok("timeend" in observaciones)
      assert.ok(new Date(observaciones.timeend).toString() != "Invalid Date")
      assert.ok("valor" in observaciones)
      assert.ok(Number(observaciones.valor).toString() != "NaN")
    }
  });

  test("POST /obs/puntual/series/:id/observaciones  w/ no write access", async () => {
    const res = await request(app)
      .post(`/obs/puntual/series/${serie.id}/observaciones`)
      .send([{
        timestart: new Date("2000-01-01T03:00:00.000Z"),
        timeend: new Date("2000-01-01T03:00:00.000Z"),
        valor: 1.11
      },{
        timestart: new Date("2000-01-02T03:00:00.000Z"),
        timeend: new Date("2000-01-02T03:00:00.000Z"),
        valor: 2.22
      },{
        timestart: new Date("2000-01-03T03:00:00.000Z"),
        timeend: new Date("2000-01-03T03:00:00.000Z"),
        valor: 3.33
      }])
      .set("Content-Type", "application/json")
      .set("Authorization", `Bearer ${noaccess.token}`);

    assert.equal(res.statusCode, 401);
  });

  test("POST /obs/puntual/series/:id/observaciones  w/ no write access 2", async () => {
    const res = await request(app)
      .post(`/obs/puntual/series/${serie.id}/observaciones`)
      .send([{
        timestart: new Date("2000-01-01T03:00:00.000Z"),
        timeend: new Date("2000-01-01T03:00:00.000Z"),
        valor: 1.11
      },{
        timestart: new Date("2000-01-02T03:00:00.000Z"),
        timeend: new Date("2000-01-02T03:00:00.000Z"),
        valor: 2.22
      },{
        timestart: new Date("2000-01-03T03:00:00.000Z"),
        timeend: new Date("2000-01-03T03:00:00.000Z"),
        valor: 3.33
      }])
      .set("Content-Type", "application/json")
      .set("Authorization", `Bearer ${reader_of_red_10.token}`);

    assert.equal(res.statusCode, 401);
  });

  test("GET /obs/puntual/series/:id/observaciones  w/ read access", async () => {
    const res = await request(app)
      .get(`/obs/puntual/series/${serie.id}/observaciones`)
      .query({
        timestart: "2000-01-01T03:00:00.000Z",
        timeend: "2000-01-04T03:00:00.000Z"
      })
      .set("Authorization", `Bearer ${writer.token}`);
    assert.equal(res.statusCode, 200);
    assert.ok(Array.isArray(res.body))
    assert.equal(res.body.length,3)
  });

  test("GET /obs/puntual/series/:id/observaciones  w/ read access 2", async () => {
    const res = await request(app)
      .get(`/obs/puntual/series/${serie.id}/observaciones`)
      .query({
        timestart: "2000-01-01T03:00:00.000Z",
        timeend: "2000-01-04T03:00:00.000Z"
      })
      .set("Authorization", `Bearer ${reader_of_red_10.token}`);
    assert.equal(res.statusCode, 200);
    assert.ok(Array.isArray(res.body))
    assert.equal(res.body.length,3)
  });

  test("GET /obs/puntual/series/:id/observaciones  w/ NO read access 2", async () => {
    const res = await request(app)
      .get(`/obs/puntual/series/${serie.id}/observaciones`)
      .query({
        timestart: "2000-01-01T03:00:00.000Z",
        timeend: "2000-01-04T03:00:00.000Z"
      })
      .set("Authorization", `Bearer ${noaccess.token}`);
    assert.equal(res.statusCode, 401); // or empty list?
  });
    // observaciones/{id}
  test("PUT /obs/puntual/series/:id/observaciones/:id  w/ write access", async () => {
    // read
    const res_obs = await request(app)
      .get(`/obs/puntual/series/${serie.id}/observaciones`)
      .query({
        timestart: "2000-01-01T03:00:00.000Z",
        timeend: "2000-01-04T03:00:00.000Z"
      })
      .set("Authorization", `Bearer ${writer.token}`);
    assert.equal(res.statusCode,200)
    assert.ok(Array.isArray(res.body))
    assert.ok(res.body.length)
    const obs = res.body[0]

    // update
    const res = await request(app)
      .put(`/obs/puntual/series/${serie.id}/observaciones/${obs.id}`)
      .send({
        timestart: "2000-01-01T03:00:00.000Z",
        timeend: "2000-01-01T03:00:00.000Z",
        valor: 1.01
      })
      .set("Authorization", `Bearer ${writer.token}`);
    assert.equal(res.statusCode, 200); 

    // fail update w/ reader 
    const res_fail = await request(app)
      .put(`/obs/puntual/series/${serie.id}/observaciones/${obs.id}`)
      .send({
        timestart: "2000-01-01T03:00:00.000Z",
        timeend: "2000-01-01T03:00:00.000Z",
        valor: 1.21
      })
      .set("Authorization", `Bearer ${reader_of_red_10.token}`);
    assert.equal(res_fail.statusCode, 401); 

    // read updated
    const res_read = await request(app)
    .get(`/obs/puntual/series/${serie.id}/observaciones/${obs.id}`)
      .set("Authorization", `Bearer ${writer.token}`);
    assert.equal(res_read.statusCode,200)
    assert.ok(!Array.isArray(res_read.body))
    assert.ok("valor" in res_read.body)
    assert.equal(res_read.body.valor, 1.01)
  });  

  // DELETE

    // observaciones
  test("DELETE /obs/puntual/series/{id}/observaciones no access", async () => {
    const res = await request(app)
      .delete(`/obs/puntual/series/${serie.id}/observaciones`)
      .query({
        timestart: "2000-01-01T03:00:00.000Z",
        timeend: "2000-01-03T04:00:00.000Z"
      })
      .set("Authorization", `Bearer ${noaccess.token}`);
    console.log(res.text)
    assert.equal(res.statusCode, 401);
  })

  test("DELETE /obs/puntual/series/{id}/observaciones no access 2 (reader)", async () => {
    const res = await request(app)
      .delete(`/obs/puntual/series/${serie.id}/observaciones`)
      .query({
        timestart: "2000-01-01T03:00:00.000Z",
        timeend: "2000-01-03T04:00:00.000Z"
      })
      .set("Authorization", `Bearer ${reader_of_red_10.token}`);
    console.log(res.text)
    assert.equal(res.statusCode, 401);
  })

  test("DELETE /obs/puntual/series/{id}/observaciones w/ write access", async () => {
    const res = await request(app)
      .delete(`/obs/puntual/series/${serie.id}/observaciones`)
      .query({
        timestart: "2000-01-01T03:00:00.000Z",
        timeend: "2000-01-03T04:00:00.000Z"
      })
      .set("Authorization", `Bearer ${writer.token}`);
    console.log(res.text)
    assert.equal(res.statusCode, 200);
    assert.ok(Array.isArray(res.body))
    assert.equal(res.body.length,3)
  })


    // series

  test("DELETE /obs/puntual/series/{id} no access", async () => {
    const res = await request(app)
      .delete(`/obs/puntual/series/${serie.id}`)
      .set("Authorization", `Bearer ${noaccess.token}`);

    console.log(res.text)
    assert.equal(res.statusCode, 400);
  })

  test("DELETE /obs/puntual/series/{id} no write access", async () => {
    const res = await request(app)
      .delete(`/obs/puntual/series/${serie.id}`)
      .set("Authorization", `Bearer ${reader_of_red_10.token}`);

    console.log(res.text)
    assert.equal(res.statusCode, 400);
  })

  test("DELETE /obs/puntual/series no access, not found", async () => {
    const res = await request(app)
      .delete(`/obs/puntual/series?estacion_id=${estacion_id}`)
      .set("Authorization", `Bearer ${noaccess.token}`);
    console.log(res.text)
    assert.equal(res.statusCode, 200);
    assert.ok(Array.isArray(res.body))
    assert.equal(res.body.length,0)
  })

  test("DELETE /obs/puntual/series/{id}", async () => {
    const res = await request(app)
      .delete(`/obs/puntual/series/${serie.id}`)
      .set("Authorization", `Bearer ${writer.token}`);

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
      .set("Authorization", `Bearer ${writer.token}`);
    console.log(res.text)
    assert.equal(res.statusCode, 200);
  })

  test("DELETE /groups/:name", async () => {
    const res = await request(app)
      .delete(`/groups/${group_name}`)
      .set("Authorization", `Bearer ${admin_token}`);
    assert.equal(res.statusCode, 200);
  })

  test("DELETE /users/:username", async() => {
    const res0 = await request(app)
      .delete(`/users/${writer.name}`)
      .set("Authorization", `Bearer ${admin_token}`);
    console.log(res0.body)
    assert.equal(res0.statusCode, 200)    
    const res_no = await request(app)
      .delete(`/users/${noaccess.name}`)
      .set("Authorization", `Bearer ${admin_token}`);
    console.log(res_no.body)
    const res_re = await request(app)
      .delete(`/users/${reader.name}`)
      .set("Authorization", `Bearer ${admin_token}`);
    console.log(res_re.body)
    assert.equal(res_re.statusCode, 200)    
    const res_ad = await request(app)
      .delete(`/users/${admin.name}`)
      .set("Authorization", `Bearer ${admin_token}`);
    assert.equal(res_ad.statusCode, 200)    
    console.log(res_ad.body)

  })
// })