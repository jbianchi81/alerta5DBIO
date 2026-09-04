import {readFileSync, writeFileSync} from 'fs' 
import {Feature, FeatureCollection } from '../geometry_types'
import {parse} from 'csv-string'

// row:
// 2026-09-03	82022	BOA VISTA AEROPORTO                     	5	-2,5	60,42	361
// columns:
// 0: date (yyyy-mm-dd) : str
// 1: station_id : int
// 2: station_name : str
// 3: country_code : int
// 4: latitude * -1 : float
// 5: longitude * -1 : float
// 6: unknown : int
// 7: value : float | empty

export function readCsv(filename: string, output?: string) : FeatureCollection {
    const data = readFileSync(filename, {encoding: "utf-8"})
    const rows = parse(data,{output:"tuples"})
    const features : Feature[] = []
    for(const [i, r] of rows.entries()) {
        const date = new Date(`${r[0]}T09:00`)
        if(date.toString() == "Invalid Date") {
            console.error(`Invalid date in csv row ${i} column 0: ${r[0]}`)
            continue
        }
        const station_id =parseInt(r[1])
        if(station_id.toString() == "NaN") {
            console.error(`Invalid integer in csv row ${i} column 1: ${r[1]}`)
            continue
        }
        const station_name = r[2].trim()
        const country_code = parseInt(r[3])
        const value = (r[7] == "") ? 0: parseFloat(r[7])
        if(value.toString() == "NaN"){
            console.error(`Invalid float in csv row ${i} column 7: ${r[7]}`)
            continue
        }
        const feature : Feature = {
            type: "Feature",
            geometry: {
                "type": "Point",
                "coordinates": [
                    parseFloat(r[5]) * -1,
                    parseFloat(r[4]) * -1,
                ]
            },
            properties: {
                date: date,
                station_id: station_id,
                station_name: station_name,
                country_code: country_code,
                value: value
            }
        }
        features.push(feature)
    }
    const feature_collection : FeatureCollection = {
        type: "FeatureCollection",
        features: features
    }
    if(output) {
        writeFileSync(output, JSON.stringify(feature_collection, undefined, 2))
    }
    return feature_collection
}