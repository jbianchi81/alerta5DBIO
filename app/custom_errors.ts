import Express from 'express'
import {DatabaseError} from 'pg'

declare global {
  namespace Express {
    interface User {
      id: number
      name: string
      role: string
    }
    interface Request {
      user?: User
    }
  }
}

export class AuthError extends Error {
  constructor(message : string) {
    super(message);
    this.name = "AuthError";
  }
}

export class NotFoundError extends Error {
  constructor(message : string) {
    super(message);
    this.name = "NotFoundError";
  }
}

export class BadRequestError extends Error {
  constructor(message : string) {
    super(message);
    this.name = "BadRequestError";
  }
}

export class ConflictError extends Error {
  constructor(message : string) {
    super(message);
    this.name = "ConflictError";
  }
}

export function handleCrudError(e : Error, res : Express.Response) {
  console.error(e)
  if(e instanceof AuthError) {
    return res.status(401).send({message:"Unauthorized", error: e.toString()})
  } else if(e instanceof NotFoundError) {
    return res.status(404).send({message:"Not found", error: e.toString()})
  } else if (e instanceof BadRequestError) {
    return res.status(400).send({message:"Bad request", error: e.toString()})
  } else if (e instanceof ConflictError) {
    return res.status(409).send({message:"Conflict", error: e.toString()})
  } else if (e instanceof DatabaseError) {
    if(e.code) {
      switch (e.code) {
        case "23502":
          // not-null violation
          return res.status(400).send({message: "Not-null constraint violation", error: e.toString()})
        case "23503":
          // foreign_key_violation
          return res.status(409).send({message:"Foreign key constraint violation",error:e.toString()})
        case "23505":
          // unique_violation
          return res.status(409).send({message:"Duplicate key", error:e.toString()})
        default:
          return res.status(500).send({message:"Database error",error:e.toString()})
      }
    } else {
      return res.status(500).send({message:"Database error",error:e.toString()})
    }
  } else {
    return res.status(500).send({message:"Server error", error: e.toString()})
  }
}

export function assertIsAdmin(req : Express.Request) {
  if(!req) {
    throw new AuthError("Unauthorized")
  }
  if(!req.user) {
    throw new AuthError("Unauthorized")
  }
  if(!req.user.role) {
    throw new AuthError("Unauthorized")
  }
  if(req.user.role != "admin") {
    throw new AuthError("Unauthorized")
  }
}

export function getUserId(req : Express.Request) {
	return (req.user) ? req.user.id : undefined
}
