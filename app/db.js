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
exports.queryToRaster = exports.streamQuery = exports.withTransaction = exports.withClient = void 0;
const setGlobal_1 = __importDefault(require("a5base/setGlobal"));
const pg_cursor_1 = __importDefault(require("pg-cursor"));
const promise_fs_1 = __importDefault(require("promise-fs"));
const node_os_1 = require("node:os");
const node_path_1 = require("node:path");
const child_process_promise_1 = require("child-process-promise");
const g = (0, setGlobal_1.default)();
function withClient(client, fn, pool) {
    return __awaiter(this, void 0, void 0, function* () {
        const ownClient = !client;
        if (!pool)
            pool = g.pool;
        if (!pool) {
            throw new Error("Global pool not set");
        }
        if (!client) {
            client = yield pool.connect();
        }
        try {
            return yield fn(client);
        }
        finally {
            if (ownClient)
                client.release();
        }
    });
}
exports.withClient = withClient;
function withTransaction(client, fn) {
    return __awaiter(this, void 0, void 0, function* () {
        const ownClient = !client;
        if (!client) {
            client = yield g.pool.connect();
        }
        const txClient = client;
        // track transaction state
        if (txClient._inTransaction === undefined) {
            txClient._inTransaction = false;
        }
        const isInTx = txClient._inTransaction;
        const startTx = ownClient || !isInTx;
        try {
            if (startTx) {
                yield txClient.query('BEGIN');
                txClient._inTransaction = true;
            }
            else {
                yield txClient.query('SAVEPOINT sp_tx');
            }
            const result = yield fn(txClient);
            if (startTx) {
                yield txClient.query('COMMIT');
                txClient._inTransaction = false;
            }
            else {
                yield txClient.query('RELEASE SAVEPOINT sp_tx');
            }
            return result;
        }
        catch (e) {
            if (startTx) {
                yield txClient.query('ROLLBACK');
                txClient._inTransaction = false;
            }
            else {
                yield txClient.query('ROLLBACK TO SAVEPOINT sp_tx');
            }
            throw e;
        }
        finally {
            if (ownClient)
                txClient.release();
        }
    });
}
exports.withTransaction = withTransaction;
function streamQuery(query, callback, client, max_rows = 100) {
    return __awaiter(this, void 0, void 0, function* () {
        const ownClient = client == null;
        if (ownClient) {
            client = (yield g.pool.connect());
            yield client.connect();
        }
        const cli = client;
        const results = [];
        try {
            const cursor = cli.query(new pg_cursor_1.default(query));
            try {
                while (true) {
                    const rows = yield cursor.read(max_rows);
                    if (rows.length === 0) {
                        break;
                    }
                    for (const row of rows) {
                        results.push(yield callback(row));
                    }
                }
            }
            finally {
                yield cursor.close();
            }
        }
        finally {
            if (ownClient) {
                yield cli.end();
            }
        }
        return results;
    });
}
exports.streamQuery = streamQuery;
/**
 * execute psql statement and stack results into multilayer gdal file
 * @param stmt psql statement that returns at least timestart [timestamptz] and valor [bytea]
 * @param output_file output file
 * @param write_index_file if string, writes dates index into that path. else if true, writes into '${output_file}_index.csv'
 * @param client pg client. if null, instantiates one from global.pool
 * @param max_rows read up to this number of rows at a time
 * @returns object: {cover_file: string, dates_file: string}
 */
function queryToRaster(stmt, output_file, write_index_file = true, client, max_rows) {
    return __awaiter(this, void 0, void 0, function* () {
        const ownClient = client == null;
        if (ownClient) {
            client = (yield g.pool.connect());
            yield client.connect();
        }
        const cli = client;
        const dir = promise_fs_1.default.mkdtempSync((0, node_path_1.join)((0, node_os_1.tmpdir)(), "a5dbio-"));
        yield promise_fs_1.default.chmod(dir, 0o777);
        console.debug("created tmp dir: " + dir);
        output_file = (output_file) ? output_file : (0, node_path_1.join)(dir, "cover_file.tif");
        try {
            var result = yield streamQuery(stmt, (row) => {
                const tmpfile = (0, node_path_1.join)(dir, `a5rast-${row.timestart.toISOString()}.tif`);
                promise_fs_1.default.writeFileSync(tmpfile, row.valor);
                return { filename: tmpfile, date: row.timestart };
            }, cli, max_rows);
            if (!result.length) {
                throw new Error("Cannot write gdal file: no records matched the query");
            }
        }
        finally {
            if (ownClient) {
                yield cli.end();
            }
        }
        const tmpfiles = result.map(r => r.filename);
        const dates = result.map(r => r.date.toISOString());
        try {
            const { stdout, stderr } = yield (0, child_process_promise_1.spawn)("gdal_merge.py", ["-o", output_file, "-separate", ...tmpfiles]);
            console.log(stdout);
        }
        catch (e) {
            console.error(e);
            throw new Error("Failed to run gdal_merge.py");
        }
        const index_file = (typeof write_index_file == "string") ? write_index_file : `${output_file}_index.csv`;
        if (write_index_file) {
            promise_fs_1.default.writeFileSync(index_file, dates.join("\n"));
            console.debug(`Wrote index file ${index_file} with ${result.length} dates`);
        }
        for (const f of tmpfiles) {
            promise_fs_1.default.rmSync(f);
        }
        console.debug(`Wrote file ${output_file} with ${result.length} layers`);
        return {
            cover_file: output_file,
            dates_file: index_file
        };
    });
}
exports.queryToRaster = queryToRaster;
