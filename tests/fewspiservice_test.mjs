import test from "node:test";
import assert from "node:assert/strict";
import fewspirest from "../app/accessors/fewspirestwebservice.js"
import accessors from "../app/accessors.js"


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

// test('get timeseries areales', async() => {
//     const client = new fewspirest.Client({
//         url: "https://sstdfews.cicplata.org/FewsWebServices/rest/fewspiservice/v1",
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
//         },
//         fuentes_id: 54,
//         location_id_pattern: "INA.%d",
//         proc_id: 3,
//         var_id: 1,
//         unit_id: 22,
//         units_map: {
//             22: "mm"
//         }
//     })

//     const locations = await client.getSites()
//     assert(Array.isArray(locations))
//     assert(locations.length)

//     // const series = await client.getSeriesAreales(locations) //, undefined, true)
//     // assert(Array.isArray(series))
//     // assert(series.length)
//     // for(const s of series) {
//     //     assert(s.id in client.series_map)
//     // }

//     // var [location_ids, parameter_ids, series_id_map] = await client.getLocationParameterSeriesFilters()
//     // assert(location_ids.has("INA.1"))
//     // assert(parameter_ids.has("P.obs"))
//     // assert("INA.1" in series_id_map)
//     // assert("P.obs" in series_id_map["INA.1"])
//     // assert(Object.keys(series_id_map).length > 100)

//     const te = new Date()
//     const ts = new Date(te)
//     ts.setDate(ts.getDate() - 7)
//     var series = await client.get({
//         estacion_id: [1, 2],
//         timestart: ts,
//         timeend: te
//     })
//     assert(Array.isArray(series))
//     assert.equal(series.length, 2)
//     for(const s of series) {
//         assert("id" in s)
//         assert("tipo" in s)
//         assert.equal(s.tipo, "areal")
//         assert("observaciones" in s)
//         assert(Array.isArray(s.observaciones))
//         assert(s.observaciones.length >= 6)
//     }
// })

test('accessor timeseries areales', async() => {
    const accessor = await accessors.new(
        "sttdfews-ina-basins",
        "fewspirestwebservice",
        {
            url: "https://sstdfews.cicplata.org/FewsWebServices/rest/fewspiservice/v1",
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
            fuentes_id: 54,
            location_id_pattern: "INA.%d",
            proc_id: 3,
            var_id: 1,
            unit_id: 22,
            units_map: {
                22: "mm"
            },
            t_offset: "09:00:00",
            timeSupport: {days: 1}
        }
    )

    const te = new Date()
    const ts = new Date(te)
    ts.setDate(ts.getDate() - 7)
    
    // var series = await accessor.getSeries({
    //     estacion_id: [1,2,5,65,77,93,100,101,103,107,109,113,115,116,118,132,133,134,138,141,142,163,184,185,186,187,188,189,190,191,192,193,194,195,197,198,199,206,208,214,215,233,240,243,248,252,253,254,255,256,257,258,259,261,262,264,269,271,273,278,282,284,287,288,291,292,295,296,297,300,302,303,304,305,306,307,310,311,313,319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,349,350,351,352,353,354,355,356,357,358,359,360,361,362,363,364,365,366,367,368,369,370,371,372,373,374,375,377,379,380,381,382,383,384,385,386,387,388,389,390,391,392,393,394,395,396,397,398,399,400,401,402,403,404,405,406,407,408,409,410,411,412,413,414,415,416,417,418,419,420,421,422,423,424,425,426,427,428,429,430,431,432,433,434,435,436,437,438,439,440,441,442,443,444,445,446,447,448,449,450,451,452,476,477,478,479,480,481,482,483,484,485,486,487,500,501,502,504,505,601,602,603,604,605,606,607,608,609,610,611,612,613,614,615,616,617,618,619,620,621,622,623,624,625,626,627,628,629,630,631,632,633,642,643,644,645,646,647,648,649,650,651,652,653,654,655,656,657,658,659,660,661,662,663,664,665,666,667,668,669,670,671,672,673,674,675,676,677,678,679,680,681,682,683,684,685,686,687,688,689,691,692,693,694,695,696,697,698,699,700,702,704,705,706,707,708,709,710,711,713,714,715,716,717,718,719,720,721,722,723,724,725,726,727,728],
    //     timestart: ts,
    //     timeend: te
    // })
    // assert(Array.isArray(series))
    // assert.equal(series.length, 340)
    // for(const s of series) {
    //     assert("id" in s)
    //     assert("tipo" in s)
    //     assert.equal(s.tipo, "areal")
    //     assert("observaciones" in s)
    //     assert(Array.isArray(s.observaciones))
    //     assert(s.observaciones.length >= 6)
    //     for(const o of s.observaciones) {
    //         assert.equal(o.timestart.getHours(),9)
    //         assert.equal(o.timeend.getHours(),9)
    //         assert.equal(o.timeend.getTime() - o.timestart.getTime(), 24 * 3600 * 1000)
    //     }
    // }

    var series_upd = await accessor.updateSeries(
        {
            estacion_id: [1,2,5,65,77,93,100,101,103,107,109,113,115,116,118,132,133,134,138,141,142,163,184,185,186,187,188,189,190,191,192,193,194,195,197,198,199,206,208,214,215,233,240,243,248,252,253,254,255,256,257,258,259,261,262,264,269,271,273,278,282,284,287,288,291,292,295,296,297,300,302,303,304,305,306,307,310,311,313,319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,349,350,351,352,353,354,355,356,357,358,359,360,361,362,363,364,365,366,367,368,369,370,371,372,373,374,375,377,379,380,381,382,383,384,385,386,387,388,389,390,391,392,393,394,395,396,397,398,399,400,401,402,403,404,405,406,407,408,409,410,411,412,413,414,415,416,417,418,419,420,421,422,423,424,425,426,427,428,429,430,431,432,433,434,435,436,437,438,439,440,441,442,443,444,445,446,447,448,449,450,451,452,476,477,478,479,480,481,482,483,484,485,486,487,500,501,502,504,505,601,602,603,604,605,606,607,608,609,610,611,612,613,614,615,616,617,618,619,620,621,622,623,624,625,626,627,628,629,630,631,632,633,642,643,644,645,646,647,648,649,650,651,652,653,654,655,656,657,658,659,660,661,662,663,664,665,666,667,668,669,670,671,672,673,674,675,676,677,678,679,680,681,682,683,684,685,686,687,688,689,691,692,693,694,695,696,697,698,699,700,702,704,705,706,707,708,709,710,711,713,714,715,716,717,718,719,720,721,722,723,724,725,726,727,728],
            timestart: ts,
            timeend: te
        },
        {
            no_update_date_range: true
        }
    )
    assert(Array.isArray(series_upd))
    assert.equal(series_upd.length, 340)
    for(const s of series_upd) {
        assert("id" in s)
        assert("tipo" in s)
        assert.equal(s.tipo, "areal")
        assert("observaciones" in s)
        assert(Array.isArray(s.observaciones))
        assert(s.observaciones.length >= 6)
    }
})


