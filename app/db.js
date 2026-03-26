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
exports.withTransaction = exports.withClient = void 0;
const setGlobal_1 = __importDefault(require("a5base/setGlobal"));
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
