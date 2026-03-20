import { Pool, PoolClient } from 'pg'
import setGlobal from 'a5base/setGlobal'
const g = setGlobal()

type TxClient = PoolClient & {
  _inTransaction?: boolean
}

type WithClientFn<T> = (client: PoolClient) => Promise<T>
type WithTxFn<T> = (client: TxClient) => Promise<T>

export async function withClient<T>(
    client: PoolClient | null,
    fn: WithClientFn<T>,
    pool?: Pool
  ): Promise<T> {
    const ownClient = !client
    if (!pool) pool = (g as any).pool
    if (!pool) {
        throw new Error("Global pool not set")
    }

    if (!client) {
      client = await pool.connect()
    }

    try {
      return await fn(client)
    } finally {
      if (ownClient) client.release()
    }
  }

export async function withTransaction<T>(
    client: PoolClient | null,
    fn: WithTxFn<T>
  ): Promise<T> {
    const ownClient = !client

    if (!client) {
      client = await (g as any).pool.connect()
    }

    const txClient = client as TxClient

    // track transaction state
    if (txClient._inTransaction === undefined) {
      txClient._inTransaction = false
    }

    const isInTx = txClient._inTransaction
    const startTx = ownClient || !isInTx

    try {
      if (startTx) {
        await txClient.query('BEGIN')
        txClient._inTransaction = true
      } else {
        await txClient.query('SAVEPOINT sp_tx')
      }

      const result = await fn(txClient)

      if (startTx) {
        await txClient.query('COMMIT')
        txClient._inTransaction = false
      } else {
        await txClient.query('RELEASE SAVEPOINT sp_tx')
      }

      return result
    } catch (e) {
      if (startTx) {
        await txClient.query('ROLLBACK')
        txClient._inTransaction = false
      } else {
        await txClient.query('ROLLBACK TO SAVEPOINT sp_tx')
      }

      throw e
    } finally {
      if (ownClient) txClient.release()
    }
  }
