import {Response} from 'express'
import {DatabaseError} from 'pg'

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

export function handleCrudError(e : Error, res : Response) {
  console.error(e)
  if(e instanceof AuthError) {
    res.status(401).send({message:"Unauthorized", error: e.toString()})
  } else if(e instanceof NotFoundError) {
    res.status(404).send({message:"Not found", error: e.toString()})
  } else if (e instanceof BadRequestError) {
    res.status(400).send({message:"Bad request", error: e.toString()})
  } else if (e instanceof ConflictError) {
    res.status(409).send({message:"Conflict", error: e.toString()})
  } else if (e instanceof DatabaseError) {
    if(e.code) {
      switch (e.code) {
        case "23503":
          // foreign_key_violation
          res.status(400).send({message:"Referenced row does not exist",error:e.toString()})
        case "23505":
          // unique_violation
          res.status(409).send({message:"Duplicate key", error:e.toString()})
        default:
          res.status(500).send({message:"Database error",error:e.toString()})
      }
    } else {
      res.status(500).send({message:"Database error",error:e.toString()})
    }
  } else {
    res.status(500).send({message:"Server error", error: e.toString()})
  }
}
