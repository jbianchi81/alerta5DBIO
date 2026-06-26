import setGlobal from 'a5base/setGlobal'
import {Interval} from './a5_types'
import {Geometry} from './geometry_types'
import axios from "axios";
import { XMLParser } from "fast-xml-parser";
import pg from "pg";
import { withTransaction } from './db';

const g = setGlobal()

export type MetadataItem = {
    gid?: number
    name: string
    location : string
    sourcetype : string
    title : string
    abstract : string
    description? : string
    keywords : string[]
    defaultcrs : string
    // xmlelement
    metadata_creation_date? : Date
    boundingbox? : Geometry
    units? : string
    frequency? : Interval
    resolution? : number
    data_sources? : string[]
    services_provided? : []
    fields?: FieldDefinition[]
}

export type MetadataItemSmall = {
    gid: number
    name: string
}

export interface FieldDefinition {
    name: string;
    type: string;
    minOccurs?: number;
    maxOccurs?: number | "unbounded";
    nillable?: boolean;
    documentation?: string;
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
        const md_item = result.rows[0]
        const fields = await pool.query(`
            SELECT
                field_name,
                field_type,
                min_occurs,
                max_occurs,
                nillable,
                documentation
            FROM layer_fields
            WHERE layer_name=$1`, [name])
        md_item.fields = fields.rows
        return md_item
    }

    
    static async harvestCapabilities(
        geoserverUrl : string,
        harvest_field_definitions : boolean=true
    ) : Promise<MetadataItem[]> {
        const md_items = await getWFSCapabilities(geoserverUrl)

        for(const item of md_items) {
            if(harvest_field_definitions) {
                item.fields = await describeFeatureType(geoserverUrl, item.name)
            }
        }
        return md_items
    }

    static async update(
        geoserverUrl : string,
        harvest_field_definitions : boolean=true,
        client : pg.PoolClient|null=null,
        layer_name? : string|string[]
    ) : Promise<MetadataItem[]> {
        var md_items = await this.harvestCapabilities(geoserverUrl, harvest_field_definitions)
        if(layer_name ) {
            layer_name = (Array.isArray(layer_name)) ? layer_name : [layer_name]
            md_items = md_items.filter(item => layer_name!.indexOf(item.name) >= 0)
        }
        return withTransaction(client, async (client : pg.PoolClient) => {
            const results = []
            for(const item of md_items) {
                const result = await upsertMetadataItem(client, item)
                const fields = await saveFields(client, item.name, item.fields!)
                result.fields = fields
                results.push(result)
            }
            return results
        })
    }
}

export async function describeFeatureType(
    geoserverUrl: string,
    layer: string // including namespace
): Promise<FieldDefinition[]> {

    const url =
        `${geoserverUrl}/wfs` +
        `?service=WFS` +
        `&version=2.0.0` +
        `&request=DescribeFeatureType` +
        `&typeNames=${layer}`;

    const response = await axios.get(url);

    const parser = new XMLParser({
        ignoreAttributes: false,
        attributeNamePrefix: ""
    });

    const xsd = parser.parse(response.data);

    const schema = xsd["xsd:schema"] ?? xsd.schema;

    const complexType =
        schema["xsd:complexType"] ??
        schema.complexType;

    if(!complexType) {
        console.warn("No attribute metadata found for layer " + layer)
        return []
    }

    const complexContent =
        complexType["xsd:complexContent"] ??
        complexType.complexContent;
    
    const extension =
        complexContent["xsd:extension"] ??
        complexContent.extension;

    const sequence =
        extension["xsd:sequence"] ??
        extension.sequence;

    const elements =
        sequence["xsd:element"] ??
        sequence.element;

    return (Array.isArray(elements) ? elements : [elements])
        .map((e: any) => (parseAttributeTableElement(e)));
}

function parseAttributeTableElement(e : any) : FieldDefinition {
    const field_definition : FieldDefinition = {
        name: e.name,
        type: e.type,
        minOccurs: e.minOccurs
            ? Number(e.minOccurs)
            : undefined,
        maxOccurs: e.maxOccurs === "unbounded"
            ? "unbounded"
            : e.maxOccurs
                ? Number(e.maxOccurs)
                : undefined,
        nillable: e.nillable === true || e.nillable === "true"
    }
    const annotation = e["xsd:annotation"] ?? e.annotation
    if(annotation) {
        const documentation = annotation["xsd:documentation"] ?? annotation.documentation
        if(documentation) {
            field_definition.documentation = documentation
        }
    }
    return field_definition
}

export async function saveFields(
    client: pg.PoolClient,
    layer: string, // includes workspace
    fields: FieldDefinition[]
) : Promise<FieldDefinition[]> {
    const results : FieldDefinition[] = []

    for (const field of fields) {

        const result = await client.query(
            `
            INSERT INTO layer_fields (
                layer_name,
                field_name,
                field_type,
                min_occurs,
                max_occurs,
                nillable,
                documentation
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7)

            ON CONFLICT (
                layer_name,
                field_name
            )
            DO UPDATE SET
                field_type = COALESCE(EXCLUDED.field_type, layer_fields.field_type),
                min_occurs = COALESCE(EXCLUDED.min_occurs, layer_fields.min_occurs),
                max_occurs = COALESCE(EXCLUDED.max_occurs, layer_fields.max_occurs),
                nillable = COALESCE(EXCLUDED.nillable, layer_fields.nillable),
                documentation = COALESCE(NULLIF(EXCLUDED.documentation, ''), layer_fields.documentation)
            RETURNING *
            `,
            [
                layer,
                field.name,
                field.type,
                field.minOccurs ?? null,
                field.maxOccurs?.toString() ?? null,
                field.nillable ?? null,
                field.documentation ?? null
            ]
        );
        if(result.rowCount) {
            results.push(result.rows[0])
        }
    }
    return results
}

export async function getWFSCapabilities(
    geoserverUrl: string
): Promise<MetadataItem[]> {

    const response = await axios.get(
        `${geoserverUrl}/wfs?service=WFS&request=GetCapabilities`
    );

    const parser = new XMLParser({
        ignoreAttributes: false,
        attributeNamePrefix: ""
    });

    const xml = parser.parse(response.data);

    const featureTypes =
        xml["wfs:WFS_Capabilities"]
           ["FeatureTypeList"]
           ["FeatureType"];

    const list = Array.isArray(featureTypes)
        ? featureTypes
        : [featureTypes];

    return list.map((ft: any) => {

        const bbox = ft["ows:WGS84BoundingBox"];

        let geometry: Geometry | undefined;

        if (bbox) {

            const lower =
                bbox["ows:LowerCorner"]
                    .split(/\s+/)
                    .map(Number);

            const upper =
                bbox["ows:UpperCorner"]
                    .split(/\s+/)
                    .map(Number);

            geometry = {
                type: "Polygon",
                coordinates: [[
                    [lower[0], lower[1]],
                    [upper[0], lower[1]],
                    [upper[0], upper[1]],
                    [lower[0], upper[1]],
                    [lower[0], lower[1]]
                ]]
            };
        }

        const keywords =
            ft["ows:Keywords"]?.["ows:Keyword"];

        return {
            name: ft["Name"],
            title: ft["Title"] ?? "",
            abstract: ft["Abstract"] ?? "",
            keywords:
                keywords == null
                    ? []
                    : Array.isArray(keywords)
                        ? keywords
                        : [keywords],
            defaultcrs:
                ft["wfs:DefaultCRS"] ??
                ft["DefaultCRS"] ??
                "",
            location: geoserverUrl,
            sourcetype: "geoserver",
            boundingbox: geometry
        };
    });
}

export async function upsertMetadataItem(
    client: pg.PoolClient,
    item: MetadataItem
): Promise<MetadataItem> {

    const geojson =
        item.boundingbox == null
            ? null
            : JSON.stringify(item.boundingbox);

    const result = await client.query(
        `
        INSERT INTO metadata_catalog (
            name,
            location,
            sourcetype,
            title,
            abstract,
            description,
            keywords,
            defaultcrs,
            metadata_creation_date,
            boundingbox,
            units,
            frequency,
            resolution,
            data_sources,
            services_provided
        )
        VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,
            CASE
                WHEN $10::text IS NULL THEN NULL
                ELSE ST_SetSRID(
                    ST_GeomFromGeoJSON($10::text),
                    4326
                )
            END,
            $11,$12,$13,$14,$15
        )
        ON CONFLICT (
            name
        )
        DO UPDATE SET
            sourcetype = COALESCE(EXCLUDED.sourcetype, metadata_catalog.sourcetype),
            location = COALESCE(EXCLUDED.location, metadata_catalog.location),
            title = COALESCE(NULLIF(EXCLUDED.title, ''), metadata_catalog.title),
            abstract = COALESCE(NULLIF(EXCLUDED.abstract, ''), metadata_catalog.abstract),
            description = COALESCE(NULLIF(EXCLUDED.description, ''), metadata_catalog.description),
            keywords = COALESCE(EXCLUDED.keywords, metadata_catalog.keywords),
            defaultcrs = COALESCE(EXCLUDED.defaultcrs, metadata_catalog.defaultcrs),
            metadata_creation_date =
                COALESCE(EXCLUDED.metadata_creation_date, metadata_catalog.metadata_creation_date),
            boundingbox = COALESCE(EXCLUDED.boundingbox, metadata_catalog.boundingbox),
            units = COALESCE(EXCLUDED.units, metadata_catalog.units),
            frequency = COALESCE(EXCLUDED.frequency, metadata_catalog.frequency),
            resolution = COALESCE(EXCLUDED.resolution, metadata_catalog.resolution),
            data_sources = COALESCE(EXCLUDED.data_sources, metadata_catalog.data_sources),
            services_provided = COALESCE(EXCLUDED.services_provided, metadata_catalog.services_provided)
        RETURNING *
        `,
        [
            item.name,
            item.location,
            item.sourcetype,
            item.title,
            item.abstract,
            item.description,
            item.keywords,
            item.defaultcrs,
            item.metadata_creation_date,
            geojson,
            item.units,
            item.frequency,
            item.resolution,
            item.data_sources,
            item.services_provided
        ]
    );
    if(!result.rowCount) {
        throw new Error("No metadata_catalog records inserted")
    }
    return result.rows[0]

}