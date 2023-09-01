'use strict'

const internal = {};
// var sprintf = require('sprintf-js').sprintf, vsprintf = require('sprintf-js').vsprintf
var fs =require("promise-fs");
const { sprintf } = require("sprintf-js");
// const { exec, spawn } = require('child_process');
// const pexec = require('child-process-promise').exec;
// const ogr2ogr = require("ogr2ogr")
// var path = require('path');
// const validFilename = require('valid-filename');
// const config = require('config');
// const { client } = require('./wmlclient');
// const { assert } = require('console');

internal.informe_semanal = class {
    constructor(fecha,texto_general,contenido) {
        this.fecha = fecha // new Date(fecha)
        this.texto_general = texto_general.toString()
        if(contenido) {
            this.contenido = contenido.map(item=>{
                var c =  {
                    region_id: item.region_id.toString(),
                    texto: item.texto.toString()
                }
                if (item.tramos) {
                    c.tramos = item.tramos.map(t=>{
                        return {
                            "tramo_id": t.tramo_id.toString(),
                            "texto": t.texto.toString()
                        }
                    })
                }
                return c
            })
        }
    }
    setContenido(contenido) {
        this.contenido = contenido.map(item=>{
            var c =  {
                region_id: item.region_id.toString(),
                texto: (item.texto) ? item.texto.toString() : null
            }
            if (item.tramos) {
                c.tramos = item.tramos.map(t=>{
                    return {
                        "tramo_id": t.tramo_id.toString(),
                        "texto": (t.texto) ? t.texto.toString() : null
                    }
                })
            }
            return c
        })
    }
}

internal.crud = class {
	constructor(pool,config){
        this.pool = pool
		if(config) {
			this.config = config
			if(config.database) {
				this.dbConnectionString = "host=" + config.database.host + " user=" + config.database.user + " dbname=" + config.database.database + " password=" + config.database.password + " port=" + config.database.port
			}
            if(!this.config.informe_semanal) {
                this.config.informe_semanal = {
                    informes_limit: 100
                }
            } else if (!this.config.informe_semanal.informes_limit) {
                this.config.informe_semanal.informes_limit = 100
            }
		}
	}    
    async createRegionesFromGeoJson(geojson) {
        if(!geojson.features) {
            throw("Missing features")
        }
        const results = []
        const regiones = geojson.features.filter(f=>(!f.properties.subregion))
        const tramos = geojson.features.filter(f=>(f.properties.subregion))
        for (var i in regiones) {
            const region = {}
            region.id = regiones[i].properties.id
            region.nombre = regiones[i].properties.nombre
            region.geom = JSON.stringify(regiones[i].geometry)
            try {
                const result = await this.createRegion(region) 
                results.push(result)
            } catch(e) {
                console.error(e.toString())
            }
        }
        // tramos
        for (var i in tramos) {
            const tramo = {}
            tramo.id = tramos[i].properties.id
            tramo.nombre = tramos[i].properties.nombre
            tramo.region_id = tramos[i].properties.region
            tramo.geom = JSON.stringify(tramos[i].geometry)
            try {
                const result = await this.createTramo(tramo)
                results.push(result)
            } catch(e) {
                console.error(e.toString())
            }
        }

        if(!results.length) {
            throw("No rows inserted")
        }
        return results
    }

    async createTramosFromGeoJson(geojson) {
        if(!geojson.features) {
            throw("Missing features")
        }
        const results = []
        const tramos = geojson.features
        for (var i in tramos) {
            const tramo = {}
            tramo.id = tramos[i].properties.id
            tramo.nombre = tramos[i].properties.nombre
            tramo.region_id = tramos[i].properties.region_id
            tramo.geom = JSON.stringify(tramos[i].geometry)
            try {
                const result = await this.createTramo(tramo)
                results.push(result)
            } catch(e) {
                console.error(e.toString())
            }
        }

        if(!results.length) {
            throw("No rows inserted")
        }
        return results
    }

    async createRegion(region) {
        return this.pool.query("INSERT INTO informe_semanal_regiones (id,nombre,geom) VALUES ($1,$2,st_GeomFromGeoJSON($3)) ON CONFLICT (id) DO UPDATE SET nombre=EXCLUDED.nombre, geom=EXCLUDED.geom RETURNING *",[region.id,region.nombre,region.geom])
        .then(results=>{
            if(!results.rows.length) {
                throw("Nothing inserted")
            }
            return results.rows[0]
        })
    }
    async createTramo(tramo) {
        return this.pool.query("INSERT INTO informe_semanal_tramos (id,region_id,nombre,geom) VALUES ($1,$2,$3,st_GeomFromGeoJSON($4)) ON CONFLICT (id) DO UPDATE SET region_id=EXCLUDED.region_id,nombre=EXCLUDED.nombre, geom=EXCLUDED.geom RETURNING *",[tramo.id,tramo.region_id,tramo.nombre,tramo.geom])
        .then(results=>{
            if(!results.rows.length) {
                throw("Nothing inserted")
            }
            return results.rows[0]
        })
    }
    async readRegiones(id,geojson=true,tramos=false) {
        var result
        if(id) {
            try {
                result = await this.pool.query("SELECT id, nombre, st_asgeojson(geom) geom FROM informe_semanal_regiones WHERE id=$1",[id])
            } catch(e) {
                throw(e)
            }
        } else {
            try {
                result = await this.pool.query("SELECT id, nombre, st_asgeojson(geom) geom FROM informe_semanal_regiones")
            } catch(e) {
                throw(e)
            }
        }
        if(tramos) {
            for(var region in result.rows) {
                try {
                    var tramos = await this.pool.query("SELECT id, nombre, region_id, st_asgeojson(geom) geom FROM informe_semanal_tramos WHERE id=$1",[region.id])
                    region.tramos = tramos.rows 
                } catch(e) {
                    throw(e)
                }
            }
        }
        if(!geojson) {
            return result.rows.map(r=>{
                if(tramos) {
                    var tramos = r.tramos.map(t=>{
                        return {
                            id: t.id,
                            nombre: t.nombre
                        }
                    })
                    return {
                        id: r.id,
                        nombre: r.nombre,
                        tramos: tramos
                    }
                } else {
                    return {
                        id: r.id,
                        nombre: r.nombre
                    }
                }
            })
        }
        var regiones_geojson = {
            "type": "FeatureCollection",
            "name": "informe_semanal_regiones",
            "features": result.rows.map(r=>{
                return {
                    type: "Feature",
                    properties: {
                        id: r.id,
                        nombre: r.nombre
                    },
                    geometry: JSON.parse(r.geom)
                }
            })
        }
        if(tramos) {
            var tramos_features = []
            for(var region in result.rows) {
                if(region.tramos.length) {
                    tramos_features.push(...region.tramos.map(tramo=>{
                        return {
                            type: "Feature",
                            properties: {
                                id: tramo.id,
                                nombre: tramo.nombre,
                                region_id: tramo.region_id
                            },
                            geometry: JSON.parse(tramo.geom)
                        }
                    }))
                }
            }
            regiones_geojson.features.push(...tramos_features)
        }
    }
    async deleteRegiones(id) {
        var result
        if(id) {
            if(Array.isArray(id)) {
                var id_list = stringOrArrayFilter(id)
                try {
                    result = await this.pool.query(`DELETE FROM informe_semanal_regiones WHERE id IN (${id_list}) RETURNING *`)
                } catch(e) {
                    throw(e)
                }
            } else {
                try {
                    result = await this.pool.query("DELETE FROM informe_semanal_regiones WHERE id=$1  RETURNING *",[id])
                } catch(e) {
                    throw(e)
                }
            }
        } else {
            try {
                result = await this.pool.query("DELETE FROM informe_semanal_regiones RETURNING *")
            } catch(e) {
                throw(e)
            }
        }
        return result.rows
    }
    async readTramos(id,region_id,geojson=true) {
        var result
        if(id) {
            try {
                result = await this.pool.query("SELECT id, nombre, region_id, st_asgeojson(geom) geom FROM informe_semanal_tramos WHERE id=$1",[id])
            } catch(e) {
                throw(e)
            }
        } else if (region_id) {
            try {
                result = await this.pool.query("SELECT id, nombre, region_id, st_asgeojson(geom) geom FROM informe_semanal_tramos WHERE region_id=$1",[region_id])
            } catch(e) {
                throw(e)
            }
        } else {
            try {
                result = await this.pool.query("SELECT id, nombre, region_id, st_asgeojson(geom) geom FROM informe_semanal_tramos")
            } catch(e) {
                throw(e)
            }
        }
        if(!geojson) {
            return result.rows.map(r=>{
                return {
                    id: r.id,
                    nombre: r.nombre,
                    region_id: r.region_id
                }
            })
        }
        return {
            "type": "FeatureCollection",
            "name": "informe_semanal_tramos",
            "features": result.rows.map(r=>{
                return {
                    type: "Feature",
                    properties: {
                        id: r.id,
                        nombre: r.nombre,
                        region_id: r.region_id
                    },
                    geometry: JSON.parse(r.geom)
                }
            })
        }
    }

    async createInforme(fecha,texto_general,contenido) {
        var result
        var client
        async function abort (err){
            client.query("ROLLBACK")
            client.release()
            throw(err)
        }
        try {
            client = await this.pool.connect()
        } catch(e) {
            throw(e)
        }
        try {
            await client.query("BEGIN")
            result = await client.query("INSERT INTO informe_semanal (fecha,texto_general) VALUES ($1,$2) ON CONFLICT (fecha) DO UPDATE SET texto_general=EXCLUDED.texto_general RETURNING *",[fecha, texto_general])
        } catch(e) {
            await abort(e)
        }
        if(!result.rows.length) {
            throw(`createInforme: nothing inserted`)
        }
        const informe_semanal = new internal.informe_semanal(result.rows[0].fecha,result.rows[0].texto_general)
            if(contenido) {
            try {
                var inserted_contenido = await this.createContenido(informe_semanal.fecha,contenido,client)
                informe_semanal.setContenido(inserted_contenido)
            } catch(e) {
                throw(e)
            }
        }
        try {
            await client.query("COMMIT")
            client.release()
            // .then(()=>{
            //     console.log("client disconnected")
            //     return
            // }).catch(err=>{
            //     console.log("error during disconnection")
            //     return
            // })
        } catch(e) {
            throw(e)
        }
        // console.log("client ended")
        return informe_semanal
    }
    async readInforme(fecha,contenido=true) {
        var informe
        if(fecha) {
            try {
                informe = await this.pool.query("SELECT informe_semanal.fecha,informe_semanal.texto_general from informe_semanal WHERE informe_semanal.fecha = $1::date;",[fecha])
                // WITH tramos AS (SELECT informe_semanal_tramos.id tramo_id, informe_semanal_tramos.region_id region_id, informe_semanal_contenido_tramo.texto FROM informe_semanal_tramos, informe_semanal_contenido_tramo WHERE informe_semanal_tramos.id=informe_semanal_contenido_tramo.tramo_id AND informe_semanal_contenido_tramo.fecha=$1::date) SELECT informe_semanal_contenido.fecha,informe_semanal_contenido.region_id,informe_semanal_contenido.texto,json_agg(json_build_object('tramo_id',tramos.tramo_id, 'texto', tramos.texto)) FILTER (WHERE tramos.tramo_id IS NOT NULL) AS tramos FROM informe_semanal_contenido LEFT JOIN tramos ON (informe_semanal_contenido.region_id = tramos.region_id ) WHERE informe_semanal_contenido.fecha=$1 GROUP BY informe_semanal_contenido.region_id,informe_semanal_contenido.fecha,informe_semanal_contenido.texto ORDER BY informe_semanal_contenido.region_id;",[fecha])
                // result = await this.pool.query("SELECT informe_semanal.fecha, texto_general, json_agg(json_build_object('region_id', informe_semanal_regiones.id, 'region_nombre',informe_semanal_regiones.nombre,'texto', texto) ORDER BY informe_semanal_regiones.id) as contenido FROM informe_semanal JOIN informe_semanal_regiones ON (informe_semanal.fecha=$1::date) LEFT OUTER JOIN informe_semanal_contenido ON (informe_semanal.fecha=informe_semanal_contenido.fecha AND informe_semanal_regiones.id=informe_semanal_contenido.region_id) GROUP BY informe_semanal.fecha;",[fecha])
            } catch(e) {
                throw(e)
            }
        } else {
            try {
                informe = await this.pool.query("WITH last AS (SELECT max(fecha) as fecha FROM informe_semanal) SELECT informe_semanal.fecha,informe_semanal.texto_general from informe_semanal JOIN last ON (last.fecha=informe_semanal.fecha)")
                // informe = await this.pool.query("WITH last AS (SELECT max(fecha) as fecha FROM informe_semanal),tramos AS (SELECT informe_semanal_tramos.id tramo_id, informe_semanal_tramos.region_id region_id, informe_semanal_contenido_tramo.texto FROM informe_semanal_tramos, informe_semanal_contenido_tramo,last WHERE informe_semanal_tramos.id=informe_semanal_contenido_tramo.tramo_id AND informe_semanal_contenido_tramo.fecha=last.fecha)  SELECT informe_semanal_contenido.fecha,informe_semanal_contenido.region_id,informe_semanal_contenido.texto,json_agg(json_build_object('tramo_id',tramos.tramo_id, 'texto', tramos.texto)) FILTER (WHERE tramos.tramo_id IS NOT NULL) AS tramos FROM informe_semanal_contenido JOIN last ON (informe_semanal_contenido.fecha = last.fecha) LEFT JOIN tramos ON (informe_semanal_contenido.region_id = tramos.region_id ) GROUP BY informe_semanal_contenido.region_id,informe_semanal_contenido.fecha,informe_semanal_contenido.texto ORDER BY informe_semanal_contenido.region_id")
                // result = await this.pool.query("WITH last AS (SELECT max(fecha) as fecha FROM informe_semanal) SELECT informe_semanal.fecha, texto_general, json_agg(json_build_object('region_id', informe_semanal_regiones.id, 'region_nombre',informe_semanal_regiones.nombre, 'texto', texto) ORDER BY informe_semanal_regiones.id) as contenido FROM informe_semanal JOIN last ON (informe_semanal.fecha = last.fecha) JOIN informe_semanal_regiones ON (1=1) LEFT OUTER JOIN informe_semanal_contenido ON (informe_semanal.fecha=informe_semanal_contenido.fecha AND informe_semanal_regiones.id=informe_semanal_contenido.region_id) GROUP BY informe_semanal.fecha;")
            } catch(e) {
                throw(e)
            }
        }
        if(!informe.rows.length) {
            console.error("infome_semanal.readInforme: nothing found")
            informe = {
                fecha: null,
                texto_general: null
            }
        } else {
            informe = informe.rows[0]
        }
        try {
            // get mapa anomalias
            var fecha_pp = await this.pool.query("select max(date) date from pp_emas")
            fecha_pp = new Date(fecha_pp.rows[0].date)
            var y = fecha_pp.getFullYear()
            var m = fecha_pp.getMonth()+1
            var d = fecha_pp.getDate()
            var fecha_pp_inicio = new Date(fecha_pp)
            fecha_pp_inicio.setTime(fecha_pp_inicio.getTime() - 7*24*3600*1000)
            var y_i = fecha_pp_inicio.getFullYear()
            var m_i = fecha_pp_inicio.getMonth()+1
            var d_i = fecha_pp_inicio.getDate()
            informe.mapa_anomalias = sprintf("https://alerta.ina.gob.ar/ina/13-SYNOP/mapas_anomalia_semanal/%04d/%02d/ppanom_%04d%02d%02d_%04d%02d%02d\.png",y,m,y_i,m_i,d_i,y,m,d)
            informe.texto_mapa_anomalias = "Se calcula la anomalía como la diferencia entre el valor acumulado durante la ventana temporal considerada y el valor considerado como normal (período de los últimos 30 años) correspondiente a la misma ventana temporal (semanal)"
        } catch(e) {
            throw(e)
        }
        if(contenido) {
            try {
                informe.contenido = await this.readContenido(informe.fecha)
            } catch(e) {
                throw(e)
            }
        }
        return informe
    }
    async readInformes(fecha_inicio,fecha_fin,contenido=true) {
        var informes
        if(fecha_inicio && fecha_fin) {
            try {
                informes = await this.pool.query("SELECT informe_semanal.fecha,informe_semanal.texto_general from informe_semanal WHERE informe_semanal.fecha BETWEEN $1 AND $2 ORDER BY informe_semanal.fecha LIMIT $3;",[fecha_inicio,fecha_fin, this.config.informe_semanal.informes_limit])
                    // "SELECT informe_semanal.fecha, texto_general, json_agg(json_build_object('region_id', informe_semanal_regiones.id, 'region_nombre',informe_semanal_regiones.nombre, 'texto', texto) ORDER BY informe_semanal_regiones.id) as contenido FROM informe_semanal JOIN informe_semanal_regiones ON (informe_semanal.fecha BETWEEN $1 AND $2) LEFT OUTER JOIN informe_semanal_contenido ON (informe_semanal.fecha=informe_semanal_contenido.fecha AND informe_semanal_regiones.id=informe_semanal_contenido.region_id) GROUP BY informe_semanal.fecha ORDER BY informe_semanal.fecha LIMIT $3;",[fecha_inicio,fecha_fin, this.config.informe_semanal.informes_limit])
            } catch(e) {
                throw(e)
            }
        } else if(fecha_inicio) {
            try {
                informes = await this.pool.query("SELECT informe_semanal.fecha,informe_semanal.texto_general from informe_semanal WHERE informe_semanal.fecha >= $1 ORDER BY informe_semanal.fecha LIMIT $2;",[fecha_inicio, this.config.informe_semanal.informes_limit])
                // "SELECT informe_semanal.fecha, texto_general, json_agg(json_build_object('region_id', informe_semanal_regiones.id, 'region_nombre',informe_semanal_regiones.nombre, 'texto', texto) ORDER BY informe_semanal_regiones.id) as contenido FROM informe_semanal JOIN informe_semanal_regiones ON (informe_semanal.fecha >= $1) LEFT OUTER JOIN informe_semanal_contenido ON (informe_semanal.fecha=informe_semanal_contenido.fecha AND informe_semanal_regiones.id=informe_semanal_contenido.region_id) GROUP BY informe_semanal.fecha ORDER BY informe_semanal.fecha LIMIT $2;",[fecha_inicio, this.config.informe_semanal.informes_limit])
            } catch(e) {
                throw(e)
            }
        } else if(fecha_fin) {
            try {
                informes = await this.pool.query("SELECT informe_semanal.fecha,informe_semanal.texto_general from informe_semanal WHERE informe_semanal.fecha <= $1 ORDER BY informe_semanal.fecha LIMIT $2;",[fecha_fin, this.config.informe_semanal.informes_limit])
                    // "SELECT informe_semanal.fecha, texto_general, json_agg(json_build_object('region_id', informe_semanal_regiones.id, 'region_nombre',informe_semanal_regiones.nombre, 'texto', texto) ORDER BY informe_semanal_regiones.id) as contenido FROM informe_semanal JOIN informe_semanal_regiones ON (informe_semanal.fecha <= $1) LEFT OUTER JOIN informe_semanal_contenido ON (informe_semanal.fecha=informe_semanal_contenido.fecha AND informe_semanal_regiones.id=informe_semanal_contenido.region_id) GROUP BY informe_semanal.fecha ORDER BY informe_semanal.fecha LIMIT $2;",[fecha_fin,this.config.informe_semanal.informes_limit])
            } catch(e) {
                throw(e)
            }
        } else {
            try {
                informes = await this.pool.query("SELECT informe_semanal.fecha,informe_semanal.texto_general from informe_semanal ORDER BY informe_semanal.fecha LIMIT $1;",[this.config.informe_semanal.informes_limit])
                // result = await this.pool.query("SELECT informe_semanal.fecha, texto_general, json_agg(json_build_object('region_id', informe_semanal_regiones.id, 'region_nombre',informe_semanal_regiones.nombre, 'texto', texto) ORDER BY informe_semanal_regiones.id) as contenido FROM informe_semanal JOIN informe_semanal_regiones ON (1=1) LEFT OUTER JOIN informe_semanal_contenido ON (informe_semanal.fecha=informe_semanal_contenido.fecha AND informe_semanal_regiones.id=informe_semanal_contenido.region_id) GROUP BY informe_semanal.fecha ORDER BY informe_semanal.fecha LIMIT $1;",[this.config.informe_semanal.informes_limit])
            } catch(e) {
                throw(e)
            }
        }
        if(!informes.rows.length) {
            throw("Nothing found")
        }
        informes = informes.rows
        if(contenido) {
            for(var i in informes) {
                try {
                    informes[i].contenido = await this.readContenido(informes[i].fecha)
                } catch(e) {
                    throw(e)
                }
            }
        }
        return informes
    }
    
    async deleteInforme(fecha) {
        var result
        if(!fecha) { // DELETE last
            try {
                result = await this.pool.query("WITH last AS (SELECT max(fecha) as fecha FROM informe_semanal) DELETE FROM informe_semanal USING last WHERE informe_semanal.fecha=last.fecha RETURNING *",[fecha])
            } catch(e) {
                throw(e)
            }
        } else {
            try {
                result = await this.pool.query("DELETE FROM informe_semanal WHERE fecha=$1::date RETURNING *",[fecha])
            } catch(e) {
                throw(e)
            }
        }
        if(!result.rows.length) {
            throw("deleteInforme: no se encontró la fecha seleccionada")
        }
        return result.rows[0]
    }

    async deleteInformes(fecha_inicio,fecha_fin) {
        var result
        if(fecha_inicio && fecha_fin) {
            try {
                result = await this.pool.query("DELETE FROM informe_semanal WHERE informe_semanal.fecha BETWEEN $1 AND $2 RETURNING *",[fecha_inicio,fecha_fin])
            } catch(e) {
                throw(e)
            }
        } else if(fecha_inicio) {
            try {
                result = await this.pool.query("DELETE FROM informe_semanal WHERE informe_semanal.fecha >= $1 RETURNING *",[fecha_inicio])
            } catch(e) {
                throw(e)
            }
        } else if(fecha_fin) {
            try {
                result = await this.pool.query("DELETE FROM informe_semanal WHERE informe_semanal.fecha <= $1 RETURNING *",[fecha_fin])
            } catch(e) {
                throw(e)
            }
        } else {
            try {
                result = await this.pool.query("DELETE FROM informe_semanal RETURNING *")
            } catch(e) {
                throw(e)
            }
        }
        if(!result.rows.length) {
            throw("deleteInformes: Nonthing deleted")
        }
        return result.rows
    }

    async createContenidoRegion(fecha,region,texto) {
        var contenido = {
            region_id: region,
            texto: texto
        }
        if(fecha) {
            return this.createContenido(fecha,contenido)
        } else {
            return this.readInforme()
            .then(informe=>{
                return this.createContenido(informe.fecha,contenido)
            })
        }
    } 

    async createContenidoTramo(fecha,tramo_id,texto) {
        if(!fecha) {
            try {
                fecha = await this.readInforme()
                .then(informe=>informe.fecha)
            }catch(e) {
                throw(e)
            }
        }
        return this.pool.query("INSERT INTO informe_semanal_contenido_tramo (fecha,tramo_id,texto) VALUES ($1::date,$2,$3) ON CONFLICT (fecha,tramo_id) DO UPDATE SET texto=EXCLUDED.texto RETURNING *",[fecha,tramo_id,texto])
        .then(result=>{
            if(!result.rows.length) {
                throw("Nothing inserted")
            }
            return result.rows[0]
        })
    } 

    async createContenido(fecha,contenido,client) {
        var flag_commit_at_end = false
        if(!client) {
            flag_commit_at_end = true
            try {
                client = await this.pool.connect()
                var date_exists = await client.query("SELECT 1 FROM informe_semanal WHERE fecha=$1::date",[fecha])
                if(!date_exists.rows.length) {
                    throw("createContenido: no existe el informe de la fecha indicada")
                }
                await client.query("BEGIN")
            } catch(e) {
                throw(e)
            }
        }
        const abort = err =>{
            try {
                client.query("ROLLBACK")
            } catch(e) {
                client.release()
                throw(e)
            }
            client.release()
            throw(err)
        }
        if(!Array.isArray(contenido)) {
            contenido = [contenido]
        }
        var inserted_contenido = []
        for(var i in contenido) {
            var inserted
            if(contenido[i].texto && contenido[i].texto.trim() != "") {
                try {
                    inserted = await client.query("INSERT INTO informe_semanal_contenido (fecha,region_id,texto) VALUES ($1::date,$2,$3) ON CONFLICT (fecha,region_id) DO UPDATE SET texto=EXCLUDED.texto RETURNING *",[fecha,contenido[i].region_id,contenido[i].texto])
                } catch(e) {
                    abort(e)
                }
                if(!inserted.rows.length) {
                    abort(`createContenido: row ${i}: nothing inserted`)
                }
            } else {
                inserted = {
                    rows:[
                        {fecha: fecha, region_id: contenido[i].region_id, texto: null}
                    ]
                }
            }
            if(contenido[i].tramos && contenido[i].tramos.length) {
                var inserted_tramos = []
                for(var j in contenido[i].tramos) {
                    if(contenido[i].tramos[j].texto && contenido[i].tramos[j].texto.trim() != "") {
                        try {
                            var inserted_tramo = await client.query("INSERT INTO informe_semanal_contenido_tramo (fecha,tramo_id,texto) VALUES ($1::date,$2,$3) ON CONFLICT (fecha,tramo_id) DO UPDATE SET texto=EXCLUDED.texto RETURNING *",[fecha,contenido[i].tramos[j].tramo_id,contenido[i].tramos[j].texto])
                        } catch(e) {
                            abort(e)
                        }
                        if(!inserted_tramo.rows.length) {
                            abort(`createContenido: tramo row ${i} ${j}: nothing inserted`)
                        }
                        inserted_tramos.push(inserted_tramo.rows[0])
                    }
                }
                inserted.rows[0].tramos = inserted_tramos
            }
            inserted_contenido.push(inserted.rows[0])
        }
        if(flag_commit_at_end) {
            try {
                await client.query("COMMIT")
                client.release()
            } catch(e) {
                throw(e)
            }
        }
        return inserted_contenido
    }
    async readContenido(fecha,region_id) {
        var result
        if(!fecha) { // LAST FECHA
            if(!region_id) { // ALL REGIONS
                try {
                    result = await this.pool.query("WITH last AS (SELECT max(fecha) as fecha FROM informe_semanal), tramos AS (SELECT informe_semanal_tramos.id tramo_id, informe_semanal_tramos.nombre, informe_semanal_tramos.region_id region_id, informe_semanal_contenido_tramo.texto FROM informe_semanal_tramos JOIN last ON (1=1) LEFT JOIN  informe_semanal_contenido_tramo ON (informe_semanal_tramos.id=informe_semanal_contenido_tramo.tramo_id AND informe_semanal_contenido_tramo.fecha=last.fecha)) SELECT last.fecha,informe_semanal_regiones.id AS region_id, informe_semanal_regiones.nombre AS region_nombre, informe_semanal_contenido.texto,json_agg(json_build_object('tramo_id',tramos.tramo_id,'tramo_nombre',tramos.nombre,'texto',tramos.texto)) FILTER (WHERE tramos.tramo_id IS NOT NULL) AS tramos FROM last JOIN informe_semanal_regiones ON (1=1) LEFT JOIN informe_semanal_contenido ON (last.fecha=informe_semanal_contenido.fecha AND informe_semanal_regiones.id=informe_semanal_contenido.region_id) LEFT JOIN tramos ON (tramos.region_id = informe_semanal_regiones.id) GROUP BY last.fecha, informe_semanal_regiones.id, informe_semanal_contenido.texto ORDER BY region_id;")
                } catch(e) {
                    throw(e)
                }
            } else {
                var region_id_list = stringOrArrayFilter(region_id)
                try {
                    result = await this.pool.query(`WITH last AS (SELECT max(fecha) as fecha FROM informe_semanal), tramos AS (SELECT informe_semanal_tramos.id tramo_id, informe_semanal_tramos.nombre, informe_semanal_tramos.region_id region_id, informe_semanal_contenido_tramo.texto FROM informe_semanal_tramos JOIN last ON (informe_semanal_tramos.region_id IN (${region_id_list})) LEFT JOIN  informe_semanal_contenido_tramo ON (informe_semanal_tramos.id=informe_semanal_contenido_tramo.tramo_id AND informe_semanal_contenido_tramo.fecha=last.fecha)) SELECT last.fecha,informe_semanal_regiones.id AS region_id, informe_semanal_regiones.nombre AS region_nombre,informe_semanal_contenido.texto,json_agg(json_build_object('tramo_id',tramos.tramo_id,'tramo_nombre',tramos.nombre,'texto',tramos.texto)) FILTER (WHERE tramos.tramo_id IS NOT NULL) AS tramos FROM last JOIN informe_semanal_regiones ON (informe_semanal_regiones.id IN (${region_id_list})) LEFT JOIN informe_semanal_contenido ON (last.fecha=informe_semanal_contenido.fecha AND informe_semanal_regiones.id=informe_semanal_contenido.region_id) LEFT JOIN tramos ON (tramos.region_id = informe_semanal_regiones.id) GROUP BY last.fecha, informe_semanal_regiones.id, informe_semanal_contenido.texto ORDER BY region_id;`)
                    // WITH last AS (SELECT max(fecha) as fecha FROM informe_semanal), tramos AS (SELECT informe_semanal_tramos.id tramo_id, informe_semanal_tramos.region_id region_id, informe_semanal_contenido_tramo.texto FROM informe_semanal_tramos, informe_semanal_contenido_tramo,last WHERE informe_semanal_tramos.id=informe_semanal_contenido_tramo.tramo_id AND informe_semanal_contenido_tramo.fecha=last.fecha AND informe_semanal_tramos.region_id IN (${region_id_list})) SELECT informe_semanal_contenido.fecha,informe_semanal_contenido.region_id,informe_semanal_contenido.texto,json_agg(json_build_object('tramo_id',tramos.tramo_id,'texto',tramos.texto)) FILTER (WHERE tramos.texto IS NOT NULL) AS tramos FROM informe_semanal_contenido JOIN last ON (last.fecha= informe_semanal_contenido.fecha) LEFT JOIN tramos ON (tramos.region_id = informe_semanal_contenido.region_id) WHERE informe_semanal_contenido.region_id IN (${region_id_list}) GROUP BY informe_semanal_contenido.fecha, informe_semanal_contenido.region_id, informe_semanal_contenido.texto ORDER BY region_id`)
                } catch(e) {
                    throw(e)
                }
            }
        } else {
            if(!region_id) { // ALL REGIONS
                try {
                    result = await this.pool.query("WITH informe AS (SELECT fecha FROM informe_semanal WHERE fecha=$1::date), tramos AS (SELECT informe_semanal_tramos.id tramo_id, informe_semanal_tramos.nombre, informe_semanal_tramos.region_id region_id, informe_semanal_contenido_tramo.texto FROM informe_semanal_tramos JOIN informe ON (1=1) LEFT JOIN informe_semanal_contenido_tramo ON (informe_semanal_tramos.id=informe_semanal_contenido_tramo.tramo_id AND informe_semanal_contenido_tramo.fecha=informe.fecha)) SELECT informe.fecha,informe_semanal_regiones.id AS region_id, informe_semanal_regiones.nombre AS region_nombre,informe_semanal_contenido.texto,json_agg(json_build_object('tramo_id',tramos.tramo_id,'tramo_nombre',tramos.nombre,'texto',tramos.texto)) FILTER (WHERE tramos.tramo_id IS NOT NULL) AS tramos FROM informe JOIN informe_semanal_regiones ON (1=1) LEFT JOIN informe_semanal_contenido ON (informe.fecha=informe_semanal_contenido.fecha AND informe_semanal_regiones.id=informe_semanal_contenido.region_id) LEFT JOIN tramos ON (tramos.region_id = informe_semanal_regiones.id) GROUP BY informe.fecha, informe_semanal_regiones.id, informe_semanal_contenido.texto ORDER BY region_id;",[fecha])
                    // WITH tramos AS (SELECT informe_semanal_tramos.id tramo_id, informe_semanal_tramos.region_id region_id, informe_semanal_contenido_tramo.texto FROM informe_semanal_tramos, informe_semanal_contenido_tramo WHERE informe_semanal_tramos.id=informe_semanal_contenido_tramo.tramo_id AND informe_semanal_contenido_tramo.fecha=$1::date) SELECT informe_semanal_contenido.fecha,informe_semanal_contenido.region_id,informe_semanal_contenido.texto,json_agg(json_build_object('tramo_id',tramos.tramo_id,'texto',tramos.texto)) FILTER (WHERE tramos.texto IS NOT NULL) AS tramos FROM informe_semanal_contenido LEFT JOIN tramos ON (tramos.region_id = informe_semanal_contenido.region_id) WHERE informe_semanal_contenido.fecha=$1::date GROUP BY informe_semanal_contenido.fecha, informe_semanal_contenido.region_id, informe_semanal_contenido.texto ORDER BY region_id;",[fecha])
                } catch(e) {
                    throw(e)
                }
            } else {
                var region_id_list = stringOrArrayFilter(region_id)
                try {
                    result = await this.pool.query(`WITH informe AS (SELECT fecha FROM informe_semanal WHERE fecha=$1::date), tramos AS (SELECT informe_semanal_tramos.id tramo_id, informe_semanal_tramos.nombre, informe_semanal_tramos.region_id region_id, informe_semanal_contenido_tramo.texto FROM informe_semanal_tramos JOIN informe ON (informe_semanal_tramos.region_id IN (${region_id_list})) LEFT JOIN informe_semanal_contenido_tramo ON (informe_semanal_tramos.id=informe_semanal_contenido_tramo.tramo_id AND informe_semanal_contenido_tramo.fecha=informe.fecha)) SELECT informe.fecha,informe_semanal_regiones.id AS region_id, informe_semanal_regiones.nombre AS region_nombre,informe_semanal_contenido.texto,json_agg(json_build_object('tramo_id',tramos.tramo_id,'tramo_nombre',tramos.nombre,'texto',tramos.texto)) FILTER (WHERE tramos.tramo_id IS NOT NULL) AS tramos FROM informe JOIN informe_semanal_regiones ON (informe_semanal_regiones.id IN (${region_id_list})) LEFT JOIN informe_semanal_contenido ON (informe.fecha=informe_semanal_contenido.fecha AND informe_semanal_regiones.id=informe_semanal_contenido.region_id) LEFT JOIN tramos ON (tramos.region_id = informe_semanal_regiones.id) GROUP BY informe.fecha, informe_semanal_regiones.id, informe_semanal_contenido.texto ORDER BY region_id;`,[fecha])
                        // `WITH tramos AS (SELECT informe_semanal_tramos.id tramo_id, informe_semanal_tramos.region_id region_id, informe_semanal_contenido_tramo.texto FROM informe_semanal_tramos, informe_semanal_contenido_tramo WHERE informe_semanal_tramos.id=informe_semanal_contenido_tramo.tramo_id AND informe_semanal_contenido_tramo.fecha=$1::date AND informe_semanal_tramos.region_id IN (${region_id_list})) SELECT informe_semanal_contenido.fecha,informe_semanal_contenido.region_id,informe_semanal_contenido.texto,json_agg(json_build_object('tramo_id',tramos.tramo_id,'texto',tramos.texto)) FILTER (WHERE tramos.texto IS NOT NULL) AS tramos FROM informe_semanal_contenido LEFT JOIN tramos ON (tramos.region_id = informe_semanal_contenido.region_id) WHERE informe_semanal_contenido.fecha=$1::date AND informe_semanal_contenido.region_id IN (${region_id_list}) GROUP BY informe_semanal_contenido.fecha, informe_semanal_contenido.region_id, informe_semanal_contenido.texto ORDER BY region_id`,[fecha])
                } catch(e) {
                    throw(e)
                }
            }
        }
        if(!result.rows.length) {
            console.error("readContenido: nothing found. Returning template")
            return this.readRegiones(region_id,false,true)
            .then(regiones=>{
                return regiones.map(region=>{
                    return {
                        fecha: null,
                        region_id: region.id,
                        nombre: region.nombre,
                        texto: null,
                        tramos: region.tramos.map(tramo=>{
                            return {
                                tramo_id: tramo.id,
                                tramo_nombre: tramo.nombre,
                                texto: null
                            }
                        })
                    }
                })
            })
        } else {
            return result.rows
        }
    }

    // DELETE CONTENIDO
    async deleteContenido(fecha,region_id) {
        var result
        if(region_id) {
            region_id = stringOrArrayFilter(region_id)
            try {
                result = await this.pool.query(`DELETE FROM informe_semanal_contenido WHERE fecha::date=$1 AND region_id IN (${region_id}) RETURNING *`,[fecha])
            } catch(e) {
                throw(e)
            }
        } else {
            try {
                result = await this.pool.query(`DELETE FROM informe_semanal_contenido WHERE fecha::date=$1 RETURNING *`,[fecha])
            } catch(e) {
                throw(e)
            }
        }
        if(!result.rows.length) {
            throw("deleteContenido: nothing deleted")
        }
        return result.rows
    } 
    async deleteContenidoTramo(fecha,tramo_id) {
        var result
        if(tramo_id) {
            tramo_id = stringOrArrayFilter(tramo_id)
            try {
                result = await this.pool.query(`DELETE FROM informe_semanal_contenido_tramo WHERE fecha::date=$1 AND tramo_id IN (${tramo_id}) RETURNING *`,[fecha])
            } catch(e) {
                throw(e)
            }
        } else {
            try {
                result = await this.pool.query(`DELETE FROM informe_semanal_contenido_tramo WHERE fecha::date=$1 RETURNING *`,[fecha])
            } catch(e) {
                throw(e)
            }
        }
        if(!result.rows.length) {
            throw("deleteContenidoTramo: nothing deleted")
        }
        return result.rows
    } 

}

internal.rest = class {
    constructor(pool,config) {
        // console.log("Instantiating informe_semanal.rest")
        this.crud = new internal.crud(pool,config)
    }
    getRegiones (req,res) { // todas
        var geojson = true
        if(req.query && req.query.no_geom) {
            geojson = false
        }
        this.crud.readRegiones(undefined,geojson)
        .then(result=>{
            res.send(result)
        })
        .catch(e=>{
            console.error(e)
            res.status(400).send({"message":e.toString()})
        })
    }
    getRegionById (req,res) {
        var geojson = true
        if(req.query && req.query.no_geom) {
            geojson = false
        }
        this.crud.readRegiones(req.params.region_id,geojson)
        .then(result=>{
            res.send(result)
        })
        .catch(e=>{
            console.error(e)
            res.status(400).send({"message":e.toString()})
        })
    }
    getTramos (req,res) { // todas
        var geojson = true
        var region_id
        if(req.query && req.query.no_geom) {
            geojson = false
        }
        if(req.params && req.params.region_id) {
            region_id = parseInt(req.params.region_id)
        }
        this.crud.readTramos(undefined,region_id,geojson)
        .then(result=>{
            res.send(result)
        })
        .catch(e=>{
            console.error(e)
            res.status(400).send({"message":e.toString()})
        })
    }
    getTramoById (req,res) {
        var geojson = true
        if(req.query && req.query.no_geom) {
            geojson = false
        }
        this.crud.readTramos(req.params.tramo_id,geojson)
        .then(result=>{
            res.send(result)
        })
        .catch(e=>{
            console.error(e)
            res.status(400).send({"message":e.toString()})
        })
    }
    getInforme (req,res) { // last full 
        this.crud.readInforme()
        .then(result=>{
            res.send(result)
        })
        .catch(e=>{
            console.error(e)
            res.status(400).send({"message":e.toString()})
        })
    }
    getInformeByFecha (req,res) { // all regions
        this.crud.readInforme(req.params.fecha)
        .then(result=>{
            res.send(result)
        })
        .catch(e=>{
            console.error(e)
            res.status(400).send({"message":e.toString()})
        })
    }
    getContenidoByFechaByRegion (req,res) {
        this.crud.readContenido(req.params.fecha,req.params.region_id)
        .then(result=>{
            res.send(result)
        })
        .catch(e=>{
            console.error(e)
            res.status(400).send({"message":e.toString()})
        })
    }
    getContenidoByRegion (req,res) { // last date, 1 region
        this.crud.readContenido(undefined,req.params.region_id)
        .then(result=>{
            res.send(result)
        })
        .catch(e=>{
            console.error(e)
            res.status(400).send({"message":e.toString()})
        })
    }

    postInformeJSON(req,res) {
        if(!req.body || !req.body.fecha || !req.body.texto_general) {
            res.status(400).send({message:"La solicitud es incorrecta. El cuerpo del mensaje (JSON) debe contener: fecha, texto_general, contenido (opcional)"})
            res.end()
            return
        } 
        var informe_semanal = {
            fecha: req.body.fecha,
            texto_general: req.body.texto_general,
            contenido: req.body.contenido
        }
        console.log("solicitud post informe, informe_semanal:" + JSON.stringify(informe_semanal).substring(0,100) + "...")
        this.crud.createInforme(informe_semanal.fecha, informe_semanal.texto_general, informe_semanal.contenido)
        .then(result=>{
            res.send(result)
            res.end()
            return
        })
        .catch(e=>{
            console.error(e)
            res.status(400).send({"message":e.toString()})
            res.end()
            return
        })
    }

    postInforme (res,err,fields,files) {
        var informe_semanal
        console.log({fields:fields,files:files})
        if (err) {
            console.error('Error', err)
            res.status(500).send({message:"parse error",error:err})
            return
        }
        if(files.informe_semanal) {
            if(files.informe_semanal.size <=0) {
                console.error("archivo vacío o faltante")
                res.status(400).send("archivo vacío o faltante")
                res.end()
                return
            } else if(!fs.existsSync(files.informe_semanal.path)) {
                console.error("File not found")
                res.status(400).send("File not found")
                res.end()
                return
            } else {
                try {
                    informe_semanal = JSON.parse(fs.readFileSync(files.informe_semanal.path,{encoding:'utf8'}))
                } catch(e) {
                    res.status(400).send(e.toString())
                    res.end()
                    return
                }
            }
        } else {
            console.error("falta archivo informe_semanal")
            res.status(400).send("falta archivo informe_semanal")
            res.end()
        }
        console.log("solicitud post informe, informe_semanal:" + JSON.stringify(informe_semanal).substring(0,100) + "...")
        this.crud.createInforme(informe_semanal.fecha, informe_semanal.texto_general, informe_semanal.contenido)
        .then(result=>{
            res.send(result)
            res.end()
            return
        })
        .catch(e=>{
            console.error(e)
            res.status(400).send({"message":e.toString()})
            res.end()
            return
        })
    }

    postInformeFecha (req,res) {
        if(!req.body) {
            res.status(400).send({message:"La solicitud es incorrecta. Falta el cuerpo del mensaje (JSON)"})
            res.end()
            return
        }
        if(!req.params.fecha || ! req.body.texto_general) {
            res.status(400).send({message:"La solicitud es incorrecta. El cuerpo del mensaje (JSON) 		debe contener: texto_general, contenido (opcional)"})
            res.end()
            return
        }
        this.crud.createInforme(req.params.fecha, req.body.texto_general, req.body.contenido)
        .then(result=>{
            res.send(result)
        })
        .catch(e=>{
            console.error(e)
            res.status(400).send({"message":e.toString()})
        })
    }
    postContenidoRegion(req,res) { // última fecha de informe o toma de body.fecha o params.fecha
        if(!req.params.region_id) {
            res.status(400).send({message:"La solicitud es incorrecta. Falta region_id"})
            res.end()
            return
        }
        // console.log({body:req.body})
        if(!req.body || !req.body.texto) {
            res.status(400).send({message:"La solicitud es incorrecta. Falta texto"})
            res.end()
            return
        }
        this.crud.createContenidoRegion((req.params && req.params.fecha) ? req.params.fecha : (req.body.fecha) ? req.body.fecha : undefined,req.params.region_id,req.body.texto)
        .then(result=>{
            res.send(result)
        })
        .catch(e=>{
            console.error(e)
            res.status(400).send({"message":e.toString()})
        })
    }
    postContenidoTramo(req,res) {
        if(!req.params.tramo_id) {
            res.status(400).send({message:"La solicitud es incorrecta. Falta tramo_id"})
            res.end()
            return
        }
        // console.log({body:req.body})
        if(!req.body || !req.body.texto) {
            res.status(400).send({message:"La solicitud es incorrecta. Falta texto"})
            res.end()
            return
        }
        this.crud.createContenidoTramo((req.params && req.params.fecha) ? req.params.fecha : (req.body.fecha) ? req.body.fecha : undefined,req.params.tramo_id,req.body.texto)
        .then(result=>{
            res.send(result)
        })
        .catch(e=>{
            console.error(e)
            res.status(400).send({"message":e.toString()})
        })
    }
    deleteInformeFecha(req,res) {
        if(!req.params || !req.params.fecha) {
            res.status(400).send({message:"La solicitud es incorrecta. Falta fecha"})
            res.end()
            return
        }
        this.crud.deleteInforme(req.params.fecha)
        .then(result=>{
            res.send(result)
        })
        .catch(e=>{
            console.error(e)
            res.status(400).send({"message":e.toString()})
        })
    }
    deleteContenido(req,res) {
        if(!req.params || !req.params.fecha || !req.params.region_id) {
            res.status(400).send({message:"La solicitud es incorrecta. Falta fecha y/o region_id"})
            res.end()
            return
        }
        this.crud.deleteContenido(req.params.fecha,req.params.region_id)
        .then(result=>{
            res.send(result)
        })
        .catch(e=>{
            console.error(e)
            res.status(400).send({"message":e.toString()})
        })
    }
    deleteContenidoTramo(req,res) {
        if(!req.params || !req.params.fecha || !req.params.tramo_id) {
            res.status(400).send({message:"La solicitud es incorrecta. Falta fecha y/o tramo_id"})
            res.end()
            return
        }
        this.crud.deleteContenidoTramo(req.params.fecha,req.params.tramo_id)
        .then(result=>{
            res.send(result)
        })
        .catch(e=>{
            console.error(e)
            res.status(400).send({"message":e.toString()})
        })
    }
    renderForm(req,res) {
        this.crud.readInforme()
        .then(result=>{
            if(!result.fecha) {
                // render empty form
                result.fecha = new Date()
            }
            result.fecha = result.fecha.toISOString().substring(0,10)
            res.render('informe_semanal_form',result)
        })
        .catch(e=>{
            console.error(e)
            res.status(400).send({"message":e.toString()})
        })
    }

    getInformeMd(req,res) {
        this.crud.readInforme()
        .then(result=>{
            if(!result.fecha) {
                // render empty form
                result.fecha = new Date()
            }
            result.fecha = result.fecha.toISOString().substring(0,10)
            result.layout = 'empty'
            res.set({"content-type": "text/plain; charset=utf-8"})
            res.render('web_semanal_md',result)
        })
        .catch(e=>{
            console.error(e)
            res.status(400).send({"message":e.toString()})
        })
    }
}

function stringOrArrayFilter(arg) {
    if(!Array.isArray(arg)) {
        arg = [arg]
    }
    return arg.map(i=> {
        if(/[';]/.test(i)) {
            throw("Invalid characters in string filter")
        }
        return `'${i.toString()}'`
    }).join(",")
}

internal.createRegiones = function (filename='public/json/regiones_semanal.json') {
    const { Pool } = require('pg')
    const config = require('config');
    const pool = new Pool(config.database)
    var crud = new internal.crud(pool,config)
    var fs = require('fs')

    var geojson = fs.readFileSync(filename,{encoding: "utf-8"})
    geojson = JSON.parse(geojson)
    return crud.createRegionesFromGeoJson(geojson)
}

internal.createTramos = function (filename='public/json/tramos_semanal.json') {
    const { Pool } = require('pg')
    const config = require('config');
    const pool = new Pool(config.database)
    var crud = new internal.crud(pool,config)
    var fs = require('fs')

    var geojson = fs.readFileSync(filename,{encoding: "utf-8"})
    geojson = JSON.parse(geojson)
    return crud.createTramosFromGeoJson(geojson)
}


module.exports = internal
