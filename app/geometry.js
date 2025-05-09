const Wkt = require('wicket')
var wkt = new Wkt.Wkt()
const {bbox} = require("@turf/bbox")
var turfHelpers = require("@turf/helpers")
const geojsonValidation = require('geojson-validation');

class Geometry {
	constructor() {
        // super()
		// console.log(JSON.stringify({geom_arguments:arguments}))
		switch(arguments.length) {
			case 1:
				if(typeof(arguments[0]) === "string") {   // WKT
					// if(config.verbose) {
					// 	console.log("reading wkt string geometry")
					// }
					try {
						var geom = wkt.read(arguments[0]).toJson()
					} catch(e) {
						throw new Error(e)
					}
					this.type = geom.type
					this.coordinates = geom.coordinates
				} else {
					this.type = arguments[0].type
					this.coordinates = arguments[0].coordinates
				}
				break;
			default:
				this.type = arguments[0]
				this.coordinates = arguments[1]
				break;
		}
		if(!this.type) {
			throw new Error("Invalid geometry: missing type")
		}
		if(!this.coordinates) {
			throw new Error("Invalid geometry: missing coordinates")
		}
		
		if(this.type.toUpperCase() == "BOX") {
			this.type = "Polygon"
			var coords = Array.isArray(this.coordinates) ? this.coordinates : this.coordinates.split(",").map(c=>parseFloat(c))
			if(coords.length<2) {
				console.error("Faltan coordenadas")
				throw new Error("Faltan coordenadas")
			} 
			for(var i=0;i<coords.length;i++) {
				if(coords[i].toString() == "NaN") {
					throw new Error("Coordenadas incorrectas")
				}
			}
			if(coords.length<4) {
				this.type = "Point"
				this.coordinates = [ coords[0], coords[1] ]
			} else {
				this.coordinates =  [ [ [ coords[0], coords[1] ], [ coords[0], coords[3] ], [ coords[2], coords[3] ], [ coords[2], coords[1] ], [ coords[0], coords[1] ] ] ]
			}
			// console.log(JSON.stringify(this))
		} 
		if(!geojsonValidation.isGeometryObject({type: capitalize_initial(this.type), coordinates: this.coordinates})) {
			throw new Error("Invalid geometry")
		}

	}
	toString() {  // WKT
		return wkt.fromObject(this).write()
	}
	toCSV() {
		return wkt.fromObject(this).write() // this.type + "," + this.coordinates.join(",")
	}
	toSQL() {
		//~ return "ST_GeomFromText('" + this.toString() + "', 4326)"
		if(this.type.toUpperCase() == "POINT") {
			return "ST_SetSRID(ST_Point(" + this.coordinates.join(",") + "),4326)"
		} else if (this.type.toUpperCase() == "POLYGON") {
			//return "st_geomfromtext('" + this.toString()+ "',4326)" 
			//  "ST_Polygon('LINESTRING(" + this.coordinates.map(it=> it.join(" ")).join(",") + ")'::geometry,4326)"
			return "st_geomfromtext('POLYGON((" + this.coordinates[0].map(p=> p.join(" ")).join(",")+ "))',4326)"
		} else if (this.type.toUpperCase() == "LINESTRING") {
			//~ return "st_geomfromtext('" + this.toString()+ "',4326)" // "ST_GeomFromText('LINESTRING(" + this.coordinates.map(it=> it.join(" ")).join(",") + ")',4326)"
			return "st_geomfromtext('LINESTRING((" + this.coordinates.map(p=> p.join(" ")).join(",")+ "))',4326)"
		} else {
			console.error("Unknown geometry type")
			return null
		}
	}
	toGeoJSON(properties) {
		return turfHelpers.feature(this,properties)
		// var geojson
		// switch(this.geom.type.toLowerCase()) {
		// 	case "point":
		// 		geojson = turfHelpers.point(this.geom.coordinates)
		// 		break;
		// 	case "polygon":
		// 		geojson = turfHelpers.polygon(this.geom.coordinates)
		// 		break;
		// 	case "line":
		// 		geojson = turfHelpers.line()
		// }
		// return geojson
	}
	distance(feature) {
		if(feature instanceof internal.geometry) {
			feature = feature.toGeoJSON()
		}
		if(this.type != "Point" || feature.geometry.type != "Point") {
			console.error("distance only works for points")
			return
		}
		return this.distance(feature,this.toGeoJSON(),{units:"kilometers"})
	}
	bbox() {
		// bbox extent in [minX, minY, maxX, maxY] order
		const geom = this.toGeoJSON()
		return bbox(geom)
	}
}

function capitalize_initial(str) {
	if (!str) return str;
	return str.charAt(0).toUpperCase() + str.slice(1);
}
  

module.exports = {
    Geometry: Geometry
}