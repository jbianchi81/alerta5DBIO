"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ConflictError = exports.BadRequestError = exports.NotFoundError = exports.AuthError = void 0;
exports.handleCrudError = handleCrudError;
exports.assertIsAdmin = assertIsAdmin;
exports.getUserId = getUserId;
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
        return res.status(401).send({ message: "Unauthorized", error: e.toString() });
    }
    else if (e instanceof NotFoundError) {
        return res.status(404).send({ message: "Not found", error: e.toString() });
    }
    else if (e instanceof BadRequestError) {
        return res.status(400).send({ message: "Bad request", error: e.toString() });
    }
    else if (e instanceof ConflictError) {
        return res.status(409).send({ message: "Conflict", error: e.toString() });
    }
    else if (e instanceof pg_1.DatabaseError) {
        if (e.code) {
            switch (e.code) {
                case "23502":
                    // not-null violation
                    return res.status(400).send({ message: "Not-null constraint violation", error: e.toString() });
                case "23503":
                    // foreign_key_violation
                    return res.status(409).send({ message: "Foreign key constraint violation", error: e.toString() });
                case "23505":
                    // unique_violation
                    return res.status(409).send({ message: "Duplicate key", error: e.toString() });
                default:
                    return res.status(500).send({ message: "Database error", error: e.toString() });
            }
        }
        else {
            return res.status(500).send({ message: "Database error", error: e.toString() });
        }
    }
    else {
        return res.status(500).send({ message: "Server error", error: e.toString() });
    }
}
function assertIsAdmin(req) {
    if (!req) {
        throw new AuthError("Unauthorized");
    }
    if (!req.user) {
        throw new AuthError("Unauthorized");
    }
    if (!req.user.role) {
        throw new AuthError("Unauthorized");
    }
    if (req.user.role != "admin") {
        throw new AuthError("Unauthorized");
    }
}
function getUserId(req) {
    return (req.user) ? req.user.id : undefined;
}
