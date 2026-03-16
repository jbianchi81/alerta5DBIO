// db.js
export async function withClient(client, fn, pool) {
    const ownClient = !client
    if (!pool) pool = global.pool
    if (!client) client = await pool.connect()

    try {
        return await fn(client)
    } finally {
        if (ownClient) client.release()
    }
}

export async function withTransaction(client, fn, { force = false } = {}) {
    const ownClient = !client
    if (!client) client = await global.pool.connect()

    const useTransaction = ownClient || force
    const useSavepoint = force && !ownClient

    try {
        if (ownClient) {
            await client.query("BEGIN")
        } else if (useSavepoint) {
            await client.query("SAVEPOINT sp_tx")
        }

        const result = await fn(client)

        if (ownClient) {
            await client.query("COMMIT")
        } else if (useSavepoint) {
            await client.query("RELEASE SAVEPOINT sp_tx")
        }

        return result

    } catch (e) {

        if (ownClient) {
            await client.query("ROLLBACK")
        } else if (useSavepoint) {
            await client.query("ROLLBACK TO SAVEPOINT sp_tx")
        }

        throw e

    } finally {
        if (ownClient) client.release()
    }
}