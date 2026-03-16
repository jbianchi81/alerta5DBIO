const test = require('node:test')
const assert = require('assert')
const {CRUD} = require('../app/CRUD')

test('get_serie_by_site_and_var_test', async (t) => {

    const filter = {
        series_id: 16864,
        tipo: "puntual",
        timestart: new Date(2025,11,1),
        timeend: new Date(2026,2,15)
    }
    const options = {
        includeProno: true,
        // stats: "monthly",
        get_cal_stats: false
    }

    const result = await CRUD.getSeriesBySiteAndVar(
		filter.estacion_id,
		filter.var_id, 
		filter.timestart, 
		filter.timeend, 
		options.includeProno, 
		undefined, 
		undefined, 
		filter.proc_id,
		filter.public,
		filter.forecast_date,
		filter.series_id,
		filter.tipo,
		options.from_view,
		options.get_cal_stats
	)
    assert.equal(result.id, filter.series_id)

    const stats = await CRUD.getMonthlyStats("puntual",result.id)
    assert(stats)
    return
})