"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.upsertMetadataItem = exports.getWFSCapabilities = exports.saveFields = exports.describeFeatureType = exports.MetadataCatalog = void 0;
const setGlobal_1 = __importDefault(require("a5base/setGlobal"));
const axios_1 = __importDefault(require("axios"));
const fast_xml_parser_1 = require("fast-xml-parser");
const db_1 = require("./db");
const g = (0, setGlobal_1.default)();
class MetadataCatalog {
    static list() {
        return __awaiter(this, void 0, void 0, function* () {
            const pool = g.pool;
            const result = yield pool.query(`select gid,name from metadata_catalog`);
            return result.rows;
        });
    }
    static readOne(name) {
        return __awaiter(this, void 0, void 0, function* () {
            const pool = g.pool;
            const result = yield pool.query(`
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
            WHERE name=$1`, [name]);
            if (!result.rowCount) {
                throw new Error("Metadata item not found");
            }
            const md_item = result.rows[0];
            const fields = yield pool.query(`
            SELECT
                field_name,
                field_type,
                min_occurs,
                max_occurs,
                nillable,
                documentation
            FROM layer_fields
            WHERE layer_name=$1`, [name]);
            md_item.fields = fields.rows;
            return md_item;
        });
    }
    static harvestCapabilities(geoserverUrl, harvest_field_definitions = true) {
        return __awaiter(this, void 0, void 0, function* () {
            const md_items = yield getWFSCapabilities(geoserverUrl);
            for (const item of md_items) {
                if (harvest_field_definitions) {
                    item.fields = yield describeFeatureType(geoserverUrl, item.name);
                }
            }
            return md_items;
        });
    }
    static update(geoserverUrl, harvest_field_definitions = true, client = null, layer_name) {
        return __awaiter(this, void 0, void 0, function* () {
            var md_items = yield this.harvestCapabilities(geoserverUrl, harvest_field_definitions);
            if (layer_name) {
                layer_name = (Array.isArray(layer_name)) ? layer_name : [layer_name];
                md_items = md_items.filter(item => layer_name.indexOf(item.name) >= 0);
            }
            return (0, db_1.withTransaction)(client, (client) => __awaiter(this, void 0, void 0, function* () {
                const results = [];
                for (const item of md_items) {
                    const result = yield upsertMetadataItem(client, item);
                    const fields = yield saveFields(client, item.name, item.fields);
                    result.fields = fields;
                    results.push(result);
                }
                return results;
            }));
        });
    }
}
exports.MetadataCatalog = MetadataCatalog;
function describeFeatureType(geoserverUrl, layer // including namespace
) {
    var _a, _b;
    return __awaiter(this, void 0, void 0, function* () {
        const url = `${geoserverUrl}/wfs` +
            `?service=WFS` +
            `&version=2.0.0` +
            `&request=DescribeFeatureType` +
            `&typeNames=${layer}`;
        const response = yield axios_1.default.get(url);
        const parser = new fast_xml_parser_1.XMLParser({
            ignoreAttributes: false,
            attributeNamePrefix: ""
        });
        const xsd = parser.parse(response.data);
        const schema = (_a = xsd["xsd:schema"]) !== null && _a !== void 0 ? _a : xsd.schema;
        const complexType = (_b = schema["xsd:complexType"]) !== null && _b !== void 0 ? _b : schema.complexType;
        const sequence = complexType["xsd:complexContent"]["xsd:extension"]["xsd:sequence"];
        const elements = sequence["xsd:element"];
        return (Array.isArray(elements) ? elements : [elements])
            .map((e) => (parseAttributeTableElement(e)));
    });
}
exports.describeFeatureType = describeFeatureType;
function parseAttributeTableElement(e) {
    var _a, _b;
    const field_definition = {
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
    };
    const annotation = (_a = e["xsd:annotation"]) !== null && _a !== void 0 ? _a : e.annotation;
    if (annotation) {
        const documentation = (_b = annotation["xsd:documentation"]) !== null && _b !== void 0 ? _b : annotation.documentation;
        if (documentation) {
            field_definition.documentation = documentation;
        }
    }
    return field_definition;
}
function saveFields(client, layer, // includes workspace
fields) {
    var _a, _b, _c, _d, _e;
    return __awaiter(this, void 0, void 0, function* () {
        const results = [];
        for (const field of fields) {
            const result = yield client.query(`
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
            `, [
                layer,
                field.name,
                field.type,
                (_a = field.minOccurs) !== null && _a !== void 0 ? _a : null,
                (_c = (_b = field.maxOccurs) === null || _b === void 0 ? void 0 : _b.toString()) !== null && _c !== void 0 ? _c : null,
                (_d = field.nillable) !== null && _d !== void 0 ? _d : null,
                (_e = field.documentation) !== null && _e !== void 0 ? _e : null
            ]);
            if (result.rowCount) {
                results.push(result.rows[0]);
            }
        }
        return results;
    });
}
exports.saveFields = saveFields;
function getWFSCapabilities(geoserverUrl) {
    return __awaiter(this, void 0, void 0, function* () {
        const response = yield axios_1.default.get(`${geoserverUrl}/wfs?service=WFS&request=GetCapabilities`);
        const parser = new fast_xml_parser_1.XMLParser({
            ignoreAttributes: false,
            attributeNamePrefix: ""
        });
        const xml = parser.parse(response.data);
        const featureTypes = xml["wfs:WFS_Capabilities"]["FeatureTypeList"]["FeatureType"];
        const list = Array.isArray(featureTypes)
            ? featureTypes
            : [featureTypes];
        return list.map((ft) => {
            var _a, _b, _c, _d, _e;
            const bbox = ft["ows:WGS84BoundingBox"];
            let geometry;
            if (bbox) {
                const lower = bbox["ows:LowerCorner"]
                    .split(/\s+/)
                    .map(Number);
                const upper = bbox["ows:UpperCorner"]
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
            const keywords = (_a = ft["ows:Keywords"]) === null || _a === void 0 ? void 0 : _a["ows:Keyword"];
            return {
                name: ft["Name"],
                title: (_b = ft["Title"]) !== null && _b !== void 0 ? _b : "",
                abstract: (_c = ft["Abstract"]) !== null && _c !== void 0 ? _c : "",
                keywords: keywords == null
                    ? []
                    : Array.isArray(keywords)
                        ? keywords
                        : [keywords],
                defaultcrs: (_e = (_d = ft["wfs:DefaultCRS"]) !== null && _d !== void 0 ? _d : ft["DefaultCRS"]) !== null && _e !== void 0 ? _e : "",
                location: geoserverUrl,
                sourcetype: "geoserver",
                boundingbox: geometry
            };
        });
    });
}
exports.getWFSCapabilities = getWFSCapabilities;
function upsertMetadataItem(client, item) {
    return __awaiter(this, void 0, void 0, function* () {
        const geojson = item.boundingbox == null
            ? null
            : JSON.stringify(item.boundingbox);
        const result = yield client.query(`
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
        `, [
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
        ]);
        if (!result.rowCount) {
            throw new Error("No metadata_catalog records inserted");
        }
        return result.rows[0];
    });
}
exports.upsertMetadataItem = upsertMetadataItem;
