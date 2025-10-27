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
exports.parseOrigin = parseOrigin;
exports.parseDatesFromNc = parseDatesFromNc;
exports.ncToPostgisRaster = ncToPostgisRaster;
exports.downloadNC = downloadNC;
exports.readTifDate = readTifDate;
exports.setTifMetadata = setTifMetadata;
exports.tifToObservacionRaster = tifToObservacionRaster;
exports.tifDirToObservacionesRaster = tifDirToObservacionesRaster;
exports.createSeriesAreal = createSeriesAreal;
const abstract_accessor_engine_1 = require("./abstract_accessor_engine");
const dateutils_1 = require("./dateutils");
const child_process_promise_1 = require("child-process-promise");
const path_1 = __importDefault(require("path"));
const utils2_1 = require("../utils2");
const CRUD_1 = require("../CRUD");
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
    /**
     * Download yearly files
     * @param product - string template for inserting year YYYY. I.e.: ncss/grid/AMES/NEX/GDDP-CMIP6/ACCESS-CM2/historical/r1i1p1f1/pr/pr_day_ACCESS-CM2_historical_r1i1p1f1_gn_{year}_v2.0.nc
     * @param timestart
     * @param timeend
     * @param dir_path - output dir
     * @param bbox
     * @param var_
     */
    downloadNCYears(product, timestart, timeend, dir_path, bbox, // W N E S
    var_) {
        return __awaiter(this, void 0, void 0, function* () {
            const results = [];
            let error_count = 0;
            for (let date = new Date(timestart); date < timeend; date.setUTCFullYear(date.getUTCFullYear() + 1)) {
                const y = date.getUTCFullYear();
                console.debug("Year: " + y);
                const filename = product.replace("{year}", y.toString());
                const ts = new Date(Date.UTC(y, 0, 1));
                const te = new Date(Date.UTC(y, 11, 31, 23, 59, 59));
                const output = path_1.default.join(dir_path, path_1.default.basename(filename));
                try {
                    yield this.downloadNC(filename, ts, te, output, bbox, var_);
                    results.push(output);
                }
                catch (e) {
                    console.error(e);
                    error_count = error_count + 1;
                    continue;
                }
            }
            console.debug("Finished download with " + results.length + " downloaded files and " + error_count + " errors");
            return results;
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
        return __awaiter(this, arguments, void 0, function* (series_id, dir_path, schema = "public", table_name = "thredds_rasters", column_name = "rast", filename_column = "filename", return_values = false, interval, conversion_factor, origin, noleap, timestart, timeend
        // variable_name : string = this.config.var
        ) {
            const nc_files = (0, utils2_1.listFilesSync)(dir_path);
            const observaciones = [];
            for (const nc_file of nc_files) {
                if (!/\.nc$/.test(nc_file)) {
                    console.debug("Skipping file " + nc_file);
                    continue;
                }
                const dates = yield parseDatesFromNc(nc_file, origin, noleap);
                if (timestart || timeend) {
                    if (timestart && dates[dates.length - 1].date.getTime() < timestart.getTime()) {
                        console.debug("Skipping file " + nc_file);
                        continue;
                    }
                    if (timeend && dates[0].date.getTime() > timeend.getTime()) {
                        console.debug("Skipping file " + nc_file);
                        continue;
                    }
                }
                console.debug("Reading file " + nc_file);
                const obs = yield this.nc2ObservacionesRaster(series_id, nc_file, schema, table_name, column_name, filename_column, return_values, interval, conversion_factor, origin, noleap, dates);
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
        return __awaiter(this, arguments, void 0, function* (series_id, nc_file, schema = "public", table_name = "thredds_rasters", column_name = "rast", filename_column = "filename", return_values = false, interval, conversion_factor, origin, noleap, dates
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
            dates = (dates) ? dates : yield parseDatesFromNc(nc_file, origin, noleap);
            return this.multibandToObservacionesRast(series_id, filename, dates, schema, table_name, column_name, filename_column, interval !== null && interval !== void 0 ? interval : this.config.interval, return_values, conversion_factor);
        });
    }
    getBeginDate(filename) {
        // pr_day_ACCESS-CM2_historical_r1i1p1f1_gn_1950_v2.0.nc
        const y = parseInt(filename.split("_")[6]);
        return new Date(y, 0, 1);
    }
    multibandToObservacionesRast(series_id_1, filename_1, dates_1) {
        return __awaiter(this, arguments, void 0, function* (series_id, filename, dates, schema = "public", table_name = "thredds_rasters", column_name = "rast", filename_column = "filename", interval = "1 day", return_values = false, conversion_factor) {
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
                ${(conversion_factor) ? `ST_MapAlgebra(${table_name}.${column_name}, dates.key::integer, '32BF'::text, '[rast]*${conversion_factor}')` : `ST_Band(${table_name}.${column_name}, dates.key)`} AS valor
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
function countLeapDays(fromYear, toYear) {
    let count = 0;
    for (let y = fromYear; y < toYear; y++) {
        if ((y % 4 === 0 && y % 100 !== 0) || (y % 400 === 0))
            count++;
    }
    return count;
}
function parseMJD(mjd, origin = new Date(Date.UTC(1850, 0, 1)), noleap) {
    const msPerDay = 24 * 60 * 60 * 1000;
    const date = new Date(origin.getTime() + mjd * msPerDay);
    if (noleap) {
        const leapdays = countLeapDays(origin.getUTCFullYear(), date.getUTCFullYear());
        const leapdate = new Date(origin.getTime() + (mjd + leapdays) * msPerDay);
        return new Date(Date.UTC(leapdate.getUTCFullYear(), leapdate.getUTCMonth(), leapdate.getUTCDate(), date.getUTCHours()));
    }
    else {
        return date;
    }
}
function parseOrigin(o) {
    // "days since 1850-1-1"
    const m = o.match(/\d{4}-\d{1,2}-\d{1,2}/);
    if (!m) {
        return;
    }
    const s = m[0].split("-").map(i => parseInt(i));
    const origin = new Date(Date.UTC(s[0], s[1] - 1, s[2]));
    origin.setUTCFullYear(s[0]);
    return origin;
}
function parseDatesFromNc(nc_file, origin, noleap) {
    return __awaiter(this, void 0, void 0, function* () {
        const md = yield (0, utils2_1.runCommandAndParseJSON)(`gdalinfo ${nc_file} -json`);
        const md_origin = (md.metadata[""]["time#units"]) ? parseOrigin(md.metadata[""]["time#units"]) : undefined;
        origin = (origin) ? origin : md_origin;
        noleap = (noleap) ? noleap : (md.metadata[""]["time#calendar"] && md.metadata[""]["time#calendar"] == "365_day") ? true : false;
        return md.bands.map((b) => {
            return {
                band: b.band,
                date: parseMJD(parseFloat(b.metadata[""].NETCDF_DIM_time), origin, noleap)
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
function readTifDate(file) {
    return __awaiter(this, void 0, void 0, function* () {
        const gdalinfo = yield (0, utils2_1.runCommandAndParseJSON)(`gdalinfo ${file} -json`);
        const year = parseInt(gdalinfo.metadata[""].year);
        const day = parseInt(gdalinfo.metadata[""].day);
        return new Date(Date.UTC(year, 0, day));
    });
}
function setTifMetadata(file_1, timestart_1, series_id_1) {
    return __awaiter(this, arguments, void 0, function* (file, timestart, series_id, interval = { "days": 1 }) {
        var _a, _b;
        const timeend = new Date(timestart);
        timeend.setDate(timeend.getDate() + ((_a = interval.days) !== null && _a !== void 0 ? _a : 0));
        timeend.setHours(timeend.getHours() + ((_b = interval.hours) !== null && _b !== void 0 ? _b : 0));
        const cmd = `gdal_edit.py ${file} -mo series_id=${series_id} -mo timestart=${timestart.toISOString()} -mo timeend=${timeend.toISOString()}`;
        try {
            yield (0, child_process_promise_1.exec)(cmd);
        }
        catch (e) {
            throw new Error(e);
        }
    });
}
function tifToObservacionRaster(file, series_id, interval, create) {
    return __awaiter(this, void 0, void 0, function* () {
        const timestart = yield readTifDate(file);
        yield setTifMetadata(file, timestart, series_id, interval);
        const obs = CRUD_1.observacion.fromRaster(file);
        if (create) {
            return obs.create();
        }
        else {
            return obs;
        }
    });
}
function tifDirToObservacionesRaster(dir_path, series_id, interval, create, return_dates, timestart, timeend) {
    return __awaiter(this, void 0, void 0, function* () {
        const files = (0, utils2_1.listFilesSync)(dir_path);
        const observaciones = [];
        var dates = [];
        for (const file of files) {
            if (timestart || timeend) {
                const date = yield readTifDate(file);
                if (timestart && date.getTime() < timestart.getTime()) {
                    console.debug("Skipping file " + file);
                    continue;
                }
                if (timeend && date.getTime() > timeend.getTime()) {
                    console.debug("Skipping file " + file);
                    continue;
                }
            }
            console.debug("Reading file " + file);
            const observacion = yield tifToObservacionRaster(file, series_id, interval, create);
            dates.push(observacion.timestart);
            if (return_dates) {
                continue;
            }
            observaciones.push(observacion);
        }
        if (return_dates) {
            return dates;
        }
        return observaciones;
    });
}
function createSeriesAreal(series_id, area_id) {
    return __awaiter(this, void 0, void 0, function* () {
        const areas_filter = (area_id) ? (Array.isArray(area_id)) ? ` AND areas_pluvio.unid IN (${area_id.map(id => id.toString()).join(",")})` : (area_id == "all") ? "" : `AND areas_pluvio.unid=${area_id}` : "";
        const result = yield global.pool.query(`INSERT INTO series_areal (area_id, var_id, proc_id, unit_id, fuentes_id)
        SELECT 
            areas_pluvio.unid,
            series_rast.var_id,
            series_rast.proc_id,
            series_rast.unit_id,
            series_rast.fuentes_id
        FROM areas_pluvio, series_rast
        WHERE series_rast.id=$1
        ${areas_filter}
        AND areas_pluvio.activar=TRUE
        ON CONFLICT (fuentes_id, proc_id, unit_id, var_id, area_id) DO NOTHING
        RETURNING *`, [series_id]);
        return result.rows.map((s) => new CRUD_1.serie(s));
    });
}
