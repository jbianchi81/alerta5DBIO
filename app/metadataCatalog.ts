import setGlobal from 'a5base/setGlobal'
import {Interval} from './a5_types'
import {Geometry} from './geometry_types'
const g = setGlobal()

export type MetadataItem = {
    gid: number
    name: string
    location : string
    sourcetype : string
    title : string
    abstract : string
    description : string
    keywords : string[]
    defaultcrs : string
    // xmlelement
    metadata_creation_date : Date
    boundingbox : Geometry
    units : string
    frequency : Interval
    resolution : number
    data_sources : string[]
    services_provided : []
}

export type MetadataItemSmall = {
    gid: number
    name: string
}

export class MetadataCatalog {
    
    static async list() : Promise<MetadataItemSmall[]> {
        const pool = (g as any).pool
        const result = await pool.query(`select gid,name from metadata_catalog`)
        return result.rows
    }
    
    static async readOne(name : string) : Promise<MetadataItem> {
        const pool = (g as any).pool
        const result = await pool.query(`
            SELECT
                gid,
                name,
                location,
                sourcetype,
                title,
                abstract,
                description,
                keywords,
                defaultcrs,
                metadata_creation_date,
                ST_asGeoJSON(boundingbox)::json AS boundingbox,
                units,
                frequency,
                resolution,
                data_sources,
                services_provided    
            FROM metadata_catalog
            WHERE name=$1`, [name])
        if(!result.rowCount) {
            throw new Error("Metadata item not found")
        }
        return result.rows[0]        
    }
}