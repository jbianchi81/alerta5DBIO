// modificando gpm_3h para 1d
const test = require('node:test')
const {gpm_3h} = require('../app/accessors')
const assert = require('assert')
const fs = require('fs')
a = new gpm_3h({"url":"https://pmmpublisher.pps.eosdis.nasa.gov/opensearch","local_path":"data/gpm/3h/","dia_local_path":"data/gpm/dia","search_params":{"q":"precip_1d","lat":-25,"lon":-45,"limit":64, "area": 0.25},"bbox":[-70,-10,-40,-40],"tmpfile":"/tmp/gpm_transformed.tif","series_id":4,"scale":0.1})

test('get files list last week', async (t) => {
    var timeend = new Date()
    var timestart = new Date(timeend.getTime() - 8 * 24 * 3600 * 1000)
    const result = await a.getFilesList(
        {
            timestart: timestart, 
            timeend: timeend
        }
    )
    console.info("got " + result.data.items.length + " items")
    assert(result.data.items.length == 7, "expected 7 items")
    fs.writeFileSync("/tmp/gpm_files_list.json",JSON.stringify(result.data.items, undefined, 2))
})
