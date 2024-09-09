import {Database} from 'duckdb-lambda-x86'

export async function queryAsync(stmt : string) : Promise<Array<Object>> {
    return new Promise((resolve, reject) => {
        const db : Database = new Database(":memory:")
        const connection = db.connect()
        connection.all(stmt, ( (e : Error, rows : Array<Object>) => {
            if(e) {
                reject(e)
            }
            resolve(rows)
        }))
    })
}