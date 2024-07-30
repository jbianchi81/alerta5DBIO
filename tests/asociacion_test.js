const test = require('node:test')
const assert = require('assert')
process.env.NODE_ENV = "test"
const {CreateProcedure, ReadProcedure, runProcedureTests, DeleteProcedure} = require('../app/procedures')
const fs = require('promise-fs')
const a5_samples = require('./a5_samples')

test('asociacion procedure', async(t) => {

    await t.test("create series",async (t) => {
        const procedure = new CreateProcedure({
            class_name: "serie",
            elements: [
                a5_samples.series[0],
                a5_samples.series[1]
            ],
            options: {
                upsert_estacion: true
            }
        },
        [
            {
                "testName": "ResultIsInstanceOfTest",
                "arguments": {
                    "class_name": "serie"
                }
            }
        ])
        const result = await procedure.run()
        assert.equal(result.length, 2, "Expected 2 results")
        await runProcedureTests(procedure, result)
    })

    await t.test("create asociacion",async (t) => {
        const procedure = new CreateProcedure({
            class_name: "asociacion",
            elements: [
                a5_samples.asociaciones[0]
            ]
        },
        [
            {
                "testName": "ResultIsInstanceOfTest",
                "arguments": {
                    "class_name": "asociacion"
                }
            }
        ])
        const result = await procedure.run()
        assert.equal(result.length, 1, "Expected 1 results")
        await runProcedureTests(procedure, result)
    })

    await t.test("read asociacion", async(t) => {
        const procedure = new ReadProcedure({
            class_name: "asociacion",
            filter: {
                "source_tipo": "puntual",
                "source_series_id": 3281,
                "dest_tipo": "puntual",
                "dest_series_id": 3282
            },
            output: "/tmp/asociacion.json"
        },
        [
            {
                "testName": "ResultIsInstanceOfTest",
                "arguments": {
                    "class_name": "asociacion"
                }
            },
            {
                "testName": "OutputFileTest",
                arguments: {
                    class_name: "asociacion",
                    output: "/tmp/asociacion.json",
                    output_format: "json"
                }
            }
        ])
        const result = await procedure.run()
        assert.equal(result.length, 1, "Expected 1 results")
        await procedure.writeResult("json")
        await runProcedureTests(procedure, result)
    })

    await t.test("read asociacion as csv", async(t) => {
        const procedure = new ReadProcedure({
            class_name: "asociacion",
            filter: {
                "source_tipo": "puntual",
                "source_series_id": 3281,
                "dest_tipo": "puntual",
                "dest_series_id": 3282
            },
            output: "/tmp/asociacion.csv",
            output_format: "csv"
        },
        [
            {
                "testName": "ResultIsInstanceOfTest",
                "arguments": {
                    "class_name": "asociacion"
                }
            },
            {
                "testName": "OutputFileTest",
                arguments: {
                    class_name: "asociacion",
                    output: "/tmp/asociacion.csv",
                    output_format: "csv"
                }
            }
        ])
        const result = await procedure.run()
        assert.equal(result.length, 1, "Expected 1 results")
        await procedure.writeResult("csv")
        await runProcedureTests(procedure, result)
    })

    await t.test("delete asociacion", async(t) => {
        const procedure = new DeleteProcedure({
            class_name: "asociacion",
            filter: {
                "source_tipo": "puntual",
                "source_series_id": 3281,
                "dest_tipo": "puntual",
                "dest_series_id": 3282
            }
        },
        [
            {
                "testName": "ResultIsInstanceOfTest",
                "arguments": {
                    "class_name": "asociacion"
                }
            }
        ])
        const result = await procedure.run()
        assert.equal(result.length, 1, "Expected 1 results")
        await runProcedureTests(procedure, result)
    })

    await t.test("delete series",async (t) => {
        const procedure = new DeleteProcedure({
            class_name: "serie",
            filter: {
                id: [3281, 3282]
            }
        },
        [
            {
                "testName": "ResultIsInstanceOfTest",
                "arguments": {
                    "class_name": "serie"
                }
            }
        ])
        const result = await procedure.run()
        assert.equal(result.length, 2, "Expected 2 results")
        await runProcedureTests(procedure, result)
    })

    await t.test("delete estacion", async(t)=> {
        const procedure = new DeleteProcedure({
            "class_name": "estacion",
            filter: {
                "tabla": "red_acumar",
                "id_externo": "http://www.bdh.acumar.gov.ar/bdh3/meteo/boca/downld08.txt"                
            }
        },
        [
            {
                "testName": "ResultIsInstanceOfTest",
                "arguments": {
                    "class_name": "estacion"
                }
            }
        ])
        const result = await procedure.run()
        assert.equal(result.length, 1, "Exprected one station deleted")
        await runProcedureTests(procedure, result)
    })
})

