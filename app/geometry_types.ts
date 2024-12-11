export type GeometryType = "Point" | "MultiPoint" | "LineString" | "MultiLineString" | "Polygon" | "MultiPolygon" | "GeometryCollection"

export type Position = [ number, number] | [number, number, number ]

export type LineString = [Position, Position, ...Array<Position>]

export type Polygon = Array<LineString>

export type MultiPolygon = Array<Polygon>

export type Geometry = {
    bbox(): [ number, number, number, number ],
    type: GeometryType,
    coordinates: Position | LineString | Polygon | MultiPolygon
}

