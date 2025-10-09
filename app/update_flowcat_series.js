const {serie} = require('./CRUD')

const internal = {}

internal.updateFlowcatSeries = async function(timestart, timeend) {
    const series_qmm = await serie.read({
        tipo: "puntual",
        var_id: 48,
        proc_id: 1,
        timestart: timestart,
        timeend: timeend
    },{
        getWeibullPercentiles: true
    })
    const series_flowcat = await serie.read({
        tipo: "puntual",
        var_id: 104,
        proc_id: 1
    })
    const result = []
    for(const serie_qmm of series_qmm) {
        if(!serie_qmm.monthlyStats.length) {
            console.error("No monthly stats retrieved for estacion id " + serie_qmm.estacion.id)
            continue
        }
        const series_flowcat_matches = series_flowcat.filter(s => s.estacion.id == serie_qmm.estacion.id)
        if(!series_flowcat_matches.length) {
            console.error("No flowcat series for estacion id " + serie_qmm.estacion.id)
            continue
        }
        const serie_flowcat = series_flowcat_matches[0]
        const obs_cat = serie_qmm.observaciones.filter(o=> (o.stats && o.stats.percentile_category && o.stats.percentile_category.number)).map(o=> {
            return {
                timestart: o.timestart,
                timeend: o.timeend,
                valor: o.stats.percentile_category.number
            }
        })
        if(!obs_cat.length) {
            console.info("No flow category found for estacion id " + serie_qmm.estacion.id)
            continue
        }
        serie_flowcat.setObservaciones(obs_cat)
        const created = await serie_flowcat.createObservaciones()
        if(!created || !created.length) {
            console.error("No flowcat observations created")
            continue
        }
        console.log("Created " + created.length + " flowcat observations for estacion id " + serie_flowcat.estacion.id + ", series_id=" + serie_flowcat.id)
        result.push(serie_flowcat)
    }
    if(!result.length) {
        console.error("No flowcat series were obtained")
    }
    return result
}

module.exports = internal