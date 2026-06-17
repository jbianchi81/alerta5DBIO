import { Pool, PoolClient, Client } from 'pg'
import setGlobal from 'a5base/setGlobal'
import Cursor from "pg-cursor";
import fs  from "promise-fs"
import {tmpdir} from "node:os"
import {join} from "node:path"
import {spawn} from 'child-process-promise'
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

export async function streamQuery<T>(
    query: string,
    callback: (row: any) => T | Promise<T>,
    client?: Client,
    max_rows: number=100
): Promise<T[]> {
    
    const ownClient = client == null;

    if (ownClient) {
        client = await (g as any).pool.connect() as Client
        await client.connect();
    }
    const cli = client as Client

    const results: T[] = [];

    try {
        const cursor = cli.query(new Cursor(query));

        try {
            while (true) {
                const rows = await cursor.read(max_rows);

                if (rows.length === 0) {
                    break;
                }

                for (const row of rows) {
                    results.push(await callback(row));
                }
            }
        } finally {
            await cursor.close();
        }
    } finally {
        if (ownClient) {
            await cli.end();
        }
    }

    return results;
}

/**
 * execute psql statement and stack results into multilayer gdal file
 * @param stmt psql statement that returns at least timestart [timestamptz] and valor [bytea]
 * @param output_file output file
 * @param write_index_file if string, writes dates index into that path. else if true, writes into '${output_file}_index.csv' 
 * @param client pg client. if null, instantiates one from global.pool
 * @param max_rows read up to this number of rows at a time
 * @returns object: {cover_file: string, dates_file: string}
 */
export async function queryToRaster(
    stmt : string,
    output_file : string,
    write_index_file : boolean|string=true,
    client? : Client,
    max_rows? : number
  ) : Promise<{
        cover_file: string,
        dates_file: string
      }> {

    const ownClient = client == null;

    if (ownClient) {
        client = await (g as any).pool.connect() as Client
        await client.connect();
    }
    const cli = client as Client

    const dir = fs.mkdtempSync(join(tmpdir(), "a5dbio-"));
    await fs.chmod(dir, 0o777)
    console.debug("created tmp dir: " + dir)

    output_file = (output_file) ? output_file : join(dir, "cover_file.tif")

    try {
      var result = await streamQuery(
        stmt,
        (row) => {
          const tmpfile = join(dir, `a5rast-${row.timestart.toISOString()}.tif`)
          fs.writeFileSync(tmpfile, row.valor)
          return {filename: tmpfile, date: row.timestart}
        },
        cli,
        max_rows
      )
      if(!result.length) {
        throw new Error("Cannot write gdal file: no records matched the query")
      }
    } finally {
        if (ownClient) {
            await cli.end();
        }
    }
    const tmpfiles = result.map(r=>r.filename)
    const dates = result.map(r=>r.date.toISOString())
    try {
      const {stdout, stderr} = await spawn(
        "gdal_merge.py",
        ["-o", output_file, "-separate", ...tmpfiles])
      console.log(stdout)
    } catch (e) {
      console.error(e)
      throw new Error("Failed to run gdal_merge.py")
    }
    const index_file = (typeof write_index_file == "string") ? write_index_file : `${output_file}_index.csv`
    if(write_index_file) {
      fs.writeFileSync(index_file, dates.join("\n"))
      console.debug(`Wrote index file ${index_file} with ${result.length} dates`)
    }
    for(const f of tmpfiles) {
      fs.rmSync(f)
    }
    console.debug(`Wrote file ${output_file} with ${result.length} layers`)
    return {
      cover_file: output_file,
      dates_file: index_file
    }
}