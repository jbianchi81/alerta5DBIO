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
exports.Client = void 0;
exports.parseMJD = parseMJD;
exports.parseDatesFromNc = parseDatesFromNc;
exports.ncToPostgisRaster = ncToPostgisRaster;
exports.downloadNC = downloadNC;
const abstract_accessor_engine_1 = require("./abstract_accessor_engine");
const dateutils_1 = require("./dateutils");
const child_process_promise_1 = require("child-process-promise");
const path_1 = __importDefault(require("path"));
const utils2_1 = require("../utils2");
class Client extends abstract_accessor_engine_1.AbstractAccessorEngine {
    constructor(config) {
        super(config);
        this.api_url = "https://ds.nccs.nasa.gov/thredds";
        this.setConfig(config);
        this.api_url = this.config.url;
    }
    createThreddsRastersTable() {
        return __awaiter(this, arguments, void 0, function* (schema = "public", table_name = "thredds_rasters", column_name = "rast", filename_column = "filename") {
            yield global.pool.query(`CREATE TABLE "${schema}"."${table_name}" (
            "${filename_column}" varchar UNIQUE,
            "${column_name}" raster,
            gid serial
            );`);
        });
    }
    downloadNC(product, timestart, timeend, output, bbox, // W N E S
    var_) {
        return __awaiter(this, void 0, void 0, function* () {
            // https://ds.nccs.nasa.gov/thredds/ncss/grid/AMES/NEX/GDDP-CMIP6/ACCESS-CM2/historical/r1i1p1f1/pr/pr_day_ACCESS-CM2_historical_r1i1p1f1_gn_1950_v2.0.nc?var=pr&north=-10&west=-70&east=-40&south=-40&horizStride=1&time_start=1950-01-01T12:00:00Z&time_end=1950-12-31T12:00:00Z&&&accept=netcdf3&addLatLon=true
            var_ = var_ !== null && var_ !== void 0 ? var_ : this.config.var;
            bbox = bbox !== null && bbox !== void 0 ? bbox : this.config.bbox;
            yield downloadNC(this.api_url, product, var_, (bbox) ? bbox[1] : undefined, (bbox) ? bbox[0] : undefined, (bbox) ? bbox[2] : undefined, (bbox) ? bbox[3] : undefined, true, timestart, timeend, "netcdf3", true, output);
        });
    }
    importFromDir(series_id_1, dir_path_1) {
        return __awaiter(this, arguments, void 0, function* (series_id, dir_path, schema = "public", table_name = "thredds_rasters", column_name = "rast", filename_column = "filename", return_values = false, interval
        // variable_name : string = this.config.var
        ) {
            const nc_files = (0, utils2_1.listFilesSync)(dir_path);
            const observaciones = [];
            for (const nc_file of nc_files) {
                if (!/\.nc$/.test(nc_file)) {
                    console.debug("Skipping file " + nc_file);
                    continue;
                }
                const obs = yield this.nc2ObservacionesRaster(series_id, nc_file, schema, table_name, column_name, filename_column, return_values, interval);
                observaciones.push(...obs);
            }
            return observaciones;
        });
    }
    /**
     * Parses yearly multiband netcdf file into daily observations and inserts into observaciones_rast
     * @param series_id
     * @param nc_file
     * @param schema
     * @param table_name
     * @param column_name
     * @param filename_column
     * @param return_values
     * @param interval?
     */
    nc2ObservacionesRaster(series_id_1, nc_file_1) {
        return __awaiter(this, arguments, void 0, function* (series_id, nc_file, schema = "public", table_name = "thredds_rasters", column_name = "rast", filename_column = "filename", return_values = false, interval
        // variable_name : string = this.config.var
        ) {
            yield ncToPostgisRaster(nc_file, 
            // variable_name,
            schema, table_name, column_name, {
                user: global.config.database.user,
                host: global.config.database.host,
                dbname: global.config.database.database,
                password: global.config.database.password,
                port: global.config.database.port
            }, 4326, filename_column);
            const filename = path_1.default.basename(nc_file);
            // const begin_date = this.getBeginDate(filename)
            const dates = yield parseDatesFromNc(nc_file);
            return this.multibandToObservacionesRast(series_id, filename, dates, schema, table_name, column_name, filename_column, interval !== null && interval !== void 0 ? interval : this.config.interval, return_values);
        });
    }
    getBeginDate(filename) {
        // pr_day_ACCESS-CM2_historical_r1i1p1f1_gn_1950_v2.0.nc
        const y = parseInt(filename.split("_")[6]);
        return new Date(y, 0, 1);
    }
    multibandToObservacionesRast(series_id_1, filename_1, dates_1) {
        return __awaiter(this, arguments, void 0, function* (series_id, filename, dates, schema = "public", table_name = "thredds_rasters", column_name = "rast", filename_column = "filename", interval = "1 day", return_values = false) {
            const dates_dict = {};
            for (const d of dates) {
                dates_dict[d.band] = d.date.toISOString();
            }
            const stmt = `WITH dates AS (
            SELECT * from json_each($1)
          )
        
          INSERT INTO observaciones_rast (series_id, timestart, timeend, valor)
            SELECT
                $2 AS series_id,
                dates.value::varchar::timestamptz AS timestart,
                dates.value::varchar::timestamptz + $3::interval AS timeend,
                ST_Band(${table_name}.${column_name}, dates.key) AS valor
            FROM ${schema}.${table_name}, dates 
            WHERE ${table_name}.${filename_column}=$4
            ON CONFLICT (series_id,timestart, timeend) DO UPDATE SET valor=excluded.valor, timeupdate=excluded.timeupdate
            RETURNING series_id, timestart, timeend, timeupdate${(return_values) ? ", valor" : ""};
        `;
            const result = yield global.pool.query(stmt, [JSON.stringify(dates_dict), series_id, interval, filename]);
            return result.rows;
        });
    }
    multibandToObservacionesRast_(series_id_1, filename_1, begin_date_1) {
        return __awaiter(this, arguments, void 0, function* (series_id, filename, begin_date, schema = "public", table_name = "climate_rasters", column_name = "rast", filename_column = "filename", interval = "1 day") {
            const stmt = `INSERT INTO observaciones_rast (series_id, timestart, valor)
            SELECT
            $1,
            $2 + (g.i - 1) * interval $3 AS timestart,
            $2 + (g.i - 1) * interval $3 + interval $3 AS timeend,
            ST_Band(${table_name}.${column_name}, g.i) AS valor,
            FROM ${schema}.${table_name}, generate_series(1, ST_NumBands(${table_name}.${column_name})) AS g(i) 
            WHERE ${table_name}.${filename_column}=$4
            ON CONFLICT (series_id,timestart, timeend) DO UPDATE SET valor=excluded.valor, timeudpdate=excluded.timeupdate;
        `;
            const result = yield global.pool.query(stmt, [series_id, begin_date, interval, filename]);
        });
    }
}
exports.Client = Client;
function parseMJD(mjd) {
    const origin = Date.UTC(1850, 0, 1);
    const date = new Date(origin);
    date.setUTCDate(date.getUTCDate() + mjd);
    return date;
}
function parseDatesFromNc(nc_file) {
    return __awaiter(this, void 0, void 0, function* () {
        const md = yield (0, utils2_1.runCommandAndParseJSON)(`gdalinfo ${nc_file} -json`);
        return md.bands.map((b) => {
            return {
                band: b.band,
                date: parseMJD(parseFloat(b.metadata[""].NETCDF_DIM_time))
            };
        });
    });
}
function ncToPostgisRaster(nc_file_1) {
    return __awaiter(this, arguments, void 0, function* (nc_file, 
    // variable_name : string,
    schema = "public", table_name, column_name = "rast", dbconnectionparams, srid = 4326, file_id_column = "filename") {
        const cmd = `raster2pgsql -s ${srid} -a ${nc_file} -f ${column_name} -F -n ${file_id_column} ${schema}.${table_name} | PGPASSWORD=${dbconnectionparams.password} psql ${dbconnectionparams.dbname} ${dbconnectionparams.user} -p ${dbconnectionparams.port} -h ${dbconnectionparams.host}`;
        // const cmd = `gdal_translate \
        //     NETCDF:"${nc_file}":${variable_name} \
        //     "PG:host=${dbconnectionparams.host} user=${dbconnectionparams.user} dbname=${dbconnectionparams.dbname} password=${dbconnectionparams.password} schema=${schema} table=${table_name} column=${column_name}" \
        //     -of PostGISRaster \
        //     -ot Float32`
        try {
            yield (0, child_process_promise_1.exec)(cmd);
        }
        catch (e) {
            throw new Error(e);
        }
    });
}
function downloadNC(base_url_1, product_1, var_1, north_1, west_1, east_1, south_1, horizStride_1, timestart_1, timeend_1) {
    return __awaiter(this, arguments, void 0, function* (base_url, product, var_, north, west, east, south, horizStride, timestart, timeend, accept = "netcdf3", addLatLon, output) {
        // https://ds.nccs.nasa.gov/thredds/ncss/grid/AMES/NEX/GDDP-CMIP6/ACCESS-CM2/historical/r1i1p1f1/pr/pr_day_ACCESS-CM2_historical_r1i1p1f1_gn_1950_v2.0.nc?var=pr&north=-10&west=-70&east=-40&south=-40&horizStride=1&time_start=1950-01-01T12:00:00Z&time_end=1950-12-31T12:00:00Z&&&accept=netcdf3&addLatLon=true
        const url = `${base_url}/${product}`;
        const params = {
            var: var_,
            north: north,
            west: west,
            east: east,
            south: south,
            horizStride: (horizStride) ? 1 : 0,
            time_start: timestart.toISOString(),
            time_end: timeend.toISOString(),
            accept: accept,
            addLatLon: (addLatLon) ? "true" : "false"
        };
        yield (0, dateutils_1.downloadFile)(url, output, params);
    });
}
