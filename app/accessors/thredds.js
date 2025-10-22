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
Object.defineProperty(exports, "__esModule", { value: true });
exports.ncToPostgisRaster = exports.Client = void 0;
const abstract_accessor_engine_1 = require("./abstract_accessor_engine");
const dateutils_1 = require("./dateutils");
const child_process_1 = require("child_process");
class Client extends abstract_accessor_engine_1.AbstractAccessorEngine {
    constructor() {
        super(...arguments);
        this.api_url = "https://ds.nccs.nasa.gov/thredds";
    }
    downloadNC(product, var_, north, west, east, south, horizStride, timestart, timeend, accept = "netcdf3", addLatLon, output) {
        return __awaiter(this, void 0, void 0, function* () {
            // https://ds.nccs.nasa.gov/thredds/ncss/grid/AMES/NEX/GDDP-CMIP6/ACCESS-CM2/historical/r1i1p1f1/pr/pr_day_ACCESS-CM2_historical_r1i1p1f1_gn_1950_v2.0.nc?var=pr&north=-10&west=-70&east=-40&south=-40&horizStride=1&time_start=1950-01-01T12:00:00Z&time_end=1950-12-31T12:00:00Z&&&accept=netcdf3&addLatLon=true
            const url = `${this.api_url}/${product}`;
            const params = {
                var: var_,
                north: north,
                west: west,
                east: east,
                south: south,
                horizStride: (horizStride) ? 1 : 0,
                timestart: timestart.toISOString(),
                timeend: timeend.toISOString(),
                accept: accept,
                addLatLon: (addLatLon) ? "true" : "false"
            };
            yield (0, dateutils_1.downloadFile)(url, output, params);
        });
    }
}
exports.Client = Client;
function ncToPostgisRaster(nc_file, variable_name, schema = "public", table_name, column_name = "rast", dbconnectionparams) {
    const cmd = `gdal_translate \
        NETCDF:"${nc_file}":${variable_name} \
        "PG:host=${dbconnectionparams.host} user=${dbconnectionparams.user} dbname=${dbconnectionparams.dbname} password=${dbconnectionparams.password} schema=${schema} table=${table_name} column=${column_name}" \
        -of PostGISRaster \
        -ot Float32 \
        -co BLOCKXSIZE=100 -co BLOCKYSIZE=100`;
    (0, child_process_1.exec)(cmd, (err, stdout, stderr) => {
        if (err) {
            throw new Error("Error:" + stderr);
        }
        else {
            console.log("Success:", stdout);
        }
    });
}
exports.ncToPostgisRaster = ncToPostgisRaster;
