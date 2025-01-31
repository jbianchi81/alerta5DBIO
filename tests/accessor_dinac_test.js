const test = require('node:test')
const assert = require('assert')
process.env.NODE_ENV = "test"
const {serie: Serie, observacion: Observacion, estacion: Estacion} = require('../app/CRUD')
const axios = require('axios')
const {Client} = require("../app/accessors/dinac_convencional")

test('dinac accessor get page', async(t) => {

    await t.test("predict page range", async (t) => {
        const timestart = new Date()
        timestart.setHours(0,0,0,0)
        timestart.setDate(timestart.getDate() - 45)
        const timeend = new Date()
        timeend.setHours(0,0,0,0)
        timeend.setDate(timeend.getDate() - 16)

        const client = new Client({})

        const page_range = client.predict_page_range(timestart, timeend)

        assert.equal(page_range.begin, 2, `Expected begin page: 2, got: ${page_range.begin}`)
        assert.equal(page_range.end, 4, `Expected end page: 3, got: ${page_range.end}`)
    })

    await t.test("predict page range 2", async (t) => {
        const timestart = new Date()
        timestart.setHours(0,0,0,0)
        timestart.setDate(timestart.getDate() - 44)
        const timeend = new Date()
        timeend.setHours(0,0,0,0)
        // timeend.setDate(timeend.getDate() - 16)

        const client = new Client({})

        const page_range = client.predict_page_range(timestart, timeend)

        assert.equal(page_range.begin, 1, `Expected begin page: 2, got: ${page_range.begin}`)
        assert.equal(page_range.end, 3, `Expected end page: 3, got: ${page_range.end}`)
    })


    await t.test("predict date", async(t)=> {
        const code = 2000086134
        // const page = 1
        const client = new Client({})

        const pages = [1,2,3,4,5,381]
        pages.forEach(async page => {

            const observaciones = await client.getPage(code, page) 

            const date_range = client.predict_date_range(page, observaciones.length)

            assert.equal(date_range.begin.toISOString(), observaciones[0].timestart.toISOString(), `Expected begin date: ${date_range.begin.toISOString()}, got: ${observaciones[0].timestart.toISOString()}`)

            assert.equal(date_range.end.toISOString(), observaciones[observaciones.length-1].timestart.toISOString(), `Expected end date: ${date_range.end.toISOString()}, got: ${observaciones[observaciones.length-1].timestart.toISOString()}`)
        })

    })

    await t.test("download and parse", async(t) => {
        const code = 2000086134
        const page = 381
        // const size = 15
        const url = `https://www.meteorologia.gov.py/nivel-rio/vermas_convencional.php?code=${code}&page=${page}`
        const response = await axios.get(url)
        const matches = response.data.match(/var\sphp_vars\s?=\s?(\{.*\})/)
        const data = JSON.parse(matches[1])
        const observaciones = []
        data.categories.forEach((category, i) => {
            if(data.data.length < i + 1) {
                console.warn(`Data array is shorter than categories array: skipping category ${category}`)
                return
            }
            const split_date = category.split("-").map(d=>parseInt(d))
            const date = new Date(split_date[2], split_date[1]-1, split_date[0])
            observaciones.push(new Observacion(
                {
                    timestart: date,
                    timeend: date,
                    valor: parseFloat(data.data[i])
                }
            ))
            assert.notEqual(observaciones[i].valor.toString(),"NaN", `Error parsing value as float: ${data.data[i]}`)
        })
        assert.equal(observaciones.length, 15, "Expected 15 observaciones")
    })

    await t.test("get one site, last 45 days", async(t) => {
        var timestart = new Date()
        var timeend = new Date()
        timestart.setDate(timestart.getDate() - 45)
        const client = new Client({})
        client.sites_map = [
            {
                estacion_id: 155,
                code: 2000086134,
                estacion: {
                    id: 155,
                    id_externo: 2000086134
                }
            }
        ]
        client.series_map = [
            {
                series_id: 155,
                estacion_id: 155,
                var_id: 2,
                proc_id: 1,
                unit_id: 11
            }
        ]
        result = await client.get({
            timestart: timestart,
            timeend: timeend,
            estacion_id: 155
        })
        assert.equal(result.length,45)
        
        for(var i=0;i<result.length;i++) {
            const obs = result[i]
            assert.equal(obs.series_id, 155)
            assert.equal(obs.timestart.getTime() >= timestart.getTime(), true)
            assert.equal(obs.timestart.getTime() <= timeend.getTime(), true)
            assert.notEqual(obs.valor.toString(), "NaN")
        }
    })
})


        



// data = '/* <![CDATA[ */\nvar php_vars = {"porPagina":"15","nombre":"Concepci\u00f3n","periodo":"29-05-2009 al 12-06-2009","categories":["29-05-2009","30-05-2009","31-05-2009","01-06-2009","02-06-2009","03-06-2009","04-06-2009","05-06-2009","06-06-2009","07-06-2009","08-06-2009","09-06-2009","10-06-2009","11-06-2009","12-06-2009"],"data":[2.27,2.27,2.28,2.3,2.3,2.3,2.3,2.3,2.32,2.32,2.34,2.35,2.35,2.36,2.37]};\n/* ]]> */'

// my ($match) = ($str =~ /var\sphp_vars\s?=\s?(\{.*\})/);