// modificando gpm_3h para 1d

const {gpm_3h} = require('./app/accessors')

a = new gpm_3h({"url":"https://pmmpublisher.pps.eosdis.nasa.gov/opensearch","local_path":"data/gpm/3h/","dia_local_path":"data/gpm/dia","search_params":{"q":"precip_1d","lat":-25,"lon":-45,"limit":64, "area": 0.25},"bbox":[-70,-10,-40,-40],"tmpfile":"/tmp/gpm_transformed.tif","series_id":4,"scale":0.1})

a.getFilesList({timestart: new Date("2024-09-04"), timeend: new Date("2024-09-09")}).then(r=>{
    result=r
    console.log(result.data.items.length)

    console.log(result.data.items.map(i=>i.properties.date))

}).catch(e=>console.error(e))

