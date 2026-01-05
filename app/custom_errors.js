"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.handleCrudError = exports.ConflictError = exports.BadRequestError = exports.NotFoundError = exports.AuthError = void 0;
const pg_1 = require("pg");
class AuthError extends Error {
    constructor(message) {
        super(message);
        this.name = "AuthError";
    }
}
exports.AuthError = AuthError;
class NotFoundError extends Error {
    constructor(message) {
        super(message);
        this.name = "NotFoundError";
    }
}
exports.NotFoundError = NotFoundError;
class BadRequestError extends Error {
    constructor(message) {
        super(message);
        this.name = "BadRequestError";
    }
}
exports.BadRequestError = BadRequestError;
class ConflictError extends Error {
    constructor(message) {
        super(message);
        this.name = "ConflictError";
    }
}
exports.ConflictError = ConflictError;
function handleCrudError(e, res) {
    console.error(e);
    if (e instanceof AuthError) {
        res.status(401).send({ message: "Unauthorized", error: e.toString() });
    }
    else if (e instanceof NotFoundError) {
        res.status(404).send({ message: "Not found", error: e.toString() });
    }
    else if (e instanceof BadRequestError) {
        res.status(400).send({ message: "Bad request", error: e.toString() });
    }
    else if (e instanceof ConflictError) {
        res.status(409).send({ message: "Conflict", error: e.toString() });
    }
    else if (e instanceof pg_1.DatabaseError) {
        if (e.code) {
            switch (e.code) {
                case "23503":
                    // foreign_key_violation
                    res.status(400).send({ message: "Referenced row does not exist", error: e.toString() });
                case "23505":
                    // unique_violation
                    res.status(409).send({ message: "Duplicate key", error: e.toString() });
                default:
                    res.status(500).send({ message: "Database error", error: e.toString() });
            }
        }
        else {
            res.status(500).send({ message: "Database error", error: e.toString() });
        }
    }
    else {
        res.status(500).send({ message: "Server error", error: e.toString() });
    }
}
exports.handleCrudError = handleCrudError;
