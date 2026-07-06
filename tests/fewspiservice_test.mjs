import test from "node:test";
import assert from "node:assert/strict";
import fewspirest from "../app/accessors/fewspirestwebservice.js"


// test('create fuente', async() => {
//     const client = new fewspirest.Client({
//         filter_id: "Mod_Hydro_Input_Historical_INA_Basins",
//         series_tipo: "areal",
//         variable_map: {
//             1: "P.obs"
//         },
//         procedure_map: {
//             1: 3
//         },
//         unit_map: {
//             1: 22
//         }
//     })

//     const fuente = await client.createFuente()
//     assert("id" in fuente)
//     assert("nombre" in fuente)
//     assert.equal(fuente.nombre, "fewspiservice")
// })

test('get timeseries areales', async() => {
    const client = new fewspirest.Client({
        filter_id: "Mod_Hydro_Input_Historical_INA_Basins",
        series_tipo: "areal",
        variable_map: {
            1: "P.obs"
        },
        procedure_map: {
            1: 3
        },
        unit_map: {
            1: 22
        },
        fuentes_id: 54
    })

    const locations = await client.getSites()
    assert(Array.isArray(locations))
    assert(locations.length)

    const series = await client.getSeriesAreales(locations, undefined, true)
    assert(Array.isArray(series))
    assert(series.length)
    for(const s of series) {
        assert(s.id in client.series_map)
    }

})

