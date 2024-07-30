const test = require('node:test')
const assert = require('assert')
process.env.NODE_ENV = "test"
const {ReadProcedure, runProcedureTests} = require('../app/procedures')
const fs = require('promise-fs')

test('read procedure', async(t) => {
    const procedure = new ReadProcedure({
        class_name: "var",
        filter: {
            VariableName: "Precipitation"
        },
        output: "/tmp/var.json"
    },
    [
        {
            testName: "OutputFileTest",
            arguments: {
                class_name: "var",
                output: "/tmp/var.json",
                output_format: "json"
            }
        }
    ])
    const result = await procedure.run()
    assert.equal(result.length, 8, "expected 8 results")

    await procedure.writeResult("json")
    await runProcedureTests(procedure, result)
    // const test_results = await procedure.tests[0].run(result)
    // assert(test_results.value, test_results.reason)
})

test('read procedure, output csv', async(t) => {
    const procedure = new ReadProcedure({
        class_name: "var",
        filter: {
            VariableName: "Precipitation"
        },
        output: "/tmp/var.csv",
        output_format: "csv"
    },
    [
        {
            testName: "OutputFileTest",
            arguments: {
                class_name: "var",
                output: "/tmp/var.csv",
                output_format: "csv"
            }
        }
    ])
    const result = await procedure.run()
    assert.equal(result.length, 8, "expected 8 results")

    await procedure.writeResult("csv")
    await runProcedureTests(procedure, result)
    // const test_results = await procedure.tests[0].run(result)
    // assert(test_results.value, test_results.reason)
})

