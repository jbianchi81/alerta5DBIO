// db.js
const internal = {
    withClient:  async function(client, fn, pool) {
        const ownClient = !client
        if (!pool) pool = global.pool
        if (!client) client = await pool.connect()

        try {
            return await fn(client)
        } finally {
            if (ownClient) client.release()
        }
    },

    withTransaction: async function(client, fn) {
        const ownClient = !client
        if (!client) client = await global.pool.connect()

        // 👇 track transaction state on client
        if (client._inTransaction === undefined) {
            client._inTransaction = false
        }

        const isInTx = client._inTransaction

        const startTx = ownClient || !isInTx

        try {
            if (startTx) {
                await client.query("BEGIN")
                client._inTransaction = true
            } else {
                await client.query("SAVEPOINT sp_tx")
            }

            const result = await fn(client)

            if (startTx) {
                await client.query("COMMIT")
                client._inTransaction = false
            } else {
                await client.query("RELEASE SAVEPOINT sp_tx")
            }

            return result

        } catch (e) {

            if (startTx) {
                await client.query("ROLLBACK")
                client._inTransaction = false
            } else {
                await client.query("ROLLBACK TO SAVEPOINT sp_tx")
            }

            throw e

        } finally {
            if (ownClient) client.release()
        }
}
}
module.exports = internal