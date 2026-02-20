import test from 'node:test'
import assert from 'assert'
process.env.NODE_ENV = "test"
import { Client } from "../app/accessors/emas"
import {Accessor, new as new_accessor} from "../app/accessors"
import {serie as crud_serie, estacion as crud_estacion} from "../app/CRUD"

test('emas accessor sequence', async(t) => {
    const client = new Accessor({class:"emas", name: "emas"})
    assert.ok("engine" in client)
    assert.ok("config" in client.engine)
    assert.ok("url" in client.engine.config)
    assert.ok("variable_lists" in client.engine.config)
    assert.ok("variable_map" in client.engine.config)
    assert.ok("station_map" in client.engine.config)

    // get all data from station
    const data = await client.engine.getData(928)
    assert.ok("station_id" in data)
    assert.ok("rows" in data)
    assert.ok(Array.isArray(data.rows))
    assert.ok(data.rows.length >= 10)
    assert.ok(928 in client.engine.config.station_map)
    assert.ok("variable_list_key" in client.engine.config.station_map[928])
    const variable_list_key = client.engine.config.station_map[928].variable_list_key
    assert.ok(variable_list_key in client.engine.config.variable_lists)
    const VARIABLES = client.engine.config.variable_lists[variable_list_key]
    assert.ok(Array.isArray(VARIABLES))
    for(var i=0;i<10;i++) {
        const row = data.rows[i]
        assert.ok("date_time" in row)
        assert.ok("values" in row)
        const keys = Object.keys(row.values)
        assert.ok(keys.length)
        for(const key of keys) {
            assert.ok(VARIABLES.indexOf(key) >= 0, `key ${key} missing from variable list: ${VARIABLES}`)
        }
    }

    // get sites
    const estaciones = await client.getSites()
    assert.ok(Array.isArray(estaciones))
    assert.ok(estaciones.length >= 4)
    for(var i=0; i<4; i++) {
        const estacion = estaciones[i]
        assert.ok("nombre" in estacion)
        assert.ok("geom" in estacion)
        assert.ok("coordinates" in estacion.geom)
        assert.ok(Array.isArray(estacion.geom.coordinates))
        assert.ok(estacion.geom.coordinates.length >= 2)
        assert.ok(Number.isFinite(estacion.geom.coordinates[0]))
        assert.ok(Number.isFinite(estacion.geom.coordinates[1]))
    }

    // get metadata
    const series = await client.getMetadata()
    assert.ok(Array.isArray(series))
    assert.ok(series.length >= 24)
    for(var i=0;i < 24; i++) {
        const serie = series[i]
        assert.ok("tipo" in serie)
        assert.equal(serie.tipo, "puntual")
        assert.ok("estacion" in serie)
        assert.ok("id" in serie.estacion)
        assert.ok("var" in serie)
        assert.ok("id" in serie.var)
        assert.ok("procedimiento" in serie)
        assert.ok("id" in serie.procedimiento)
        assert.ok("unidades" in serie)
        assert.ok("id" in serie.unidades)
    }

    // update metadata
    // stations
    const stations_upd = await client.updateSites()
    assert.ok(Array.isArray(stations_upd))
    assert.ok(stations_upd.length >= 4)
    for(var i=0;i < 4; i++) {
        const estacion = stations_upd[i]
        assert.ok("id" in estacion)
        assert.ok(Number.isFinite(estacion.id))
        assert.ok("nombre" in estacion)
        assert.ok("geom" in estacion)
        assert.ok("coordinates" in estacion.geom)
        assert.ok(Array.isArray(estacion.geom.coordinates))
        assert.ok(estacion.geom.coordinates.length >= 2)
        assert.ok(Number.isFinite(estacion.geom.coordinates[0]))
        assert.ok(Number.isFinite(estacion.geom.coordinates[1]))
    }    
    const st_ids = new Set(stations_upd.map((s : any)=> s.id))
    assert.equal(st_ids.size, stations_upd.length)
    
    // series
    const series_upd = await client.updateMetadata()
    for(var i=0;i < 24; i++) {
        const serie = series_upd[i]
        assert.ok("tipo" in serie)
        assert.equal(serie.tipo, "puntual")
        assert.ok("id" in serie)
        assert.ok(Number.isFinite(serie.id))
    }
    const ids = new Set(series_upd.map((s : any)=> s.id))
    assert.equal(ids.size, series_upd.length)

    // get between dates (1-day period)
    const timestart = new Date(new Date().getTime() - 86400 * 1000)
    const timeend = new Date()
    const series_ = await client.getSeries({estacion_id: 928, timestart: timestart, timeend: timeend})
    assert.ok(Array.isArray(series_))
    assert.ok(series_.length >= 4)
    let obs_count = 0
    for(var i=0;i < 4; i++) {
        const serie = series_[i]
        assert.ok("estacion_id" in serie)
        assert.equal(serie.estacion_id, 928)
        assert.ok("var_id" in serie)
        assert.ok("observaciones" in serie)
        obs_count = obs_count + serie.observaciones.length
        for(var j=0;j<Math.min(100,serie.observaciones.length);j++) {
            const obs = serie.observaciones[j]
            assert.ok("timestart" in obs)
            const ts = new Date(obs.timestart)
            assert.ok(ts.toString() != 'NaN')
            assert.ok(ts.getTime() >= timestart.getTime())
            assert.ok("timeend" in obs)
            const te = new Date(obs.timestart)
            assert.ok(te.toString() != 'NaN')
            assert.ok(te.getTime() <= timeend.getTime())
            assert.ok("valor" in obs)
            assert.ok(obs["valor"] !== undefined && obs["valor"] !== null)
        }
        assert.ok(obs_count > 100)
    }

    // create accessor
    const emas_accessor = await client.create()
    assert.ok("name" in emas_accessor)
    assert.equal(emas_accessor.name,"emas")
    assert.ok("class" in emas_accessor)
    assert.equal(emas_accessor.class,"emas")
    assert.ok("config" in emas_accessor)
    assert.ok("url" in emas_accessor.config)
    assert.ok("variable_lists" in emas_accessor.config)
    assert.ok("variable_map" in emas_accessor.config)
    assert.ok("station_map" in emas_accessor.config)

    // instantiate accessor
    const accessor_instance = await new_accessor("emas")
    assert.ok("name" in accessor_instance)
    assert.equal(accessor_instance.name,"emas")
    assert.ok("clase" in accessor_instance)
    assert.equal(accessor_instance.clase,"emas")
    assert.ok("config" in accessor_instance)
    assert.ok("url" in accessor_instance.config)
    assert.ok("variable_lists" in accessor_instance.config)
    assert.ok("variable_map" in accessor_instance.config)
    assert.ok("station_map" in accessor_instance.config)
    assert.ok("engine" in accessor_instance)

    // delete accessor
    const accessor_deleted = await client.delete()
    assert.ok("name" in accessor_deleted)
    assert.equal(accessor_deleted.name,"emas")
    assert.ok("class" in accessor_deleted)
    assert.equal(accessor_deleted.class,"emas")
    assert.ok("config" in accessor_deleted)
    assert.ok("url" in accessor_deleted.config)
    assert.ok("variable_lists" in accessor_deleted.config)
    assert.ok("variable_map" in accessor_deleted.config)
    assert.ok("station_map" in accessor_deleted.config)

    // delete series
    const series_del = await crud_serie.delete({tipo: "puntual", id: [...ids]})
    assert.equal(series_del.length, ids.size)

    // delete stations
    const stations_del = await crud_estacion.delete({id: stations_upd.map(s=>s.id)})
    assert.equal(stations_del.length, stations_upd.length)


})