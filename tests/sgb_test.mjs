import test from "node:test";
import assert from "node:assert/strict";
import {Client} from "../app/accessors/sgb_sace_report.js"
import { writeFile, unlink } from "fs/promises";
import { tmpdir } from "os";
import { join } from "path";
import { randomUUID } from "crypto";
import accessors from "../app/accessors.js"
const {Accessor} = accessors

const md_table_content = `| Estação Fluviométrica | Dia Atual   | Cota Atual (cm) | Dia +7 (cm) | Dia +14 (cm) | Dia +21 (cm) | Dia +28 (cm) |
|-----------------------|-------------|-----------------|-------------|--------------|--------------|--------------|
| BARRA DO BUGRES       | 04/03/2026  | 467             | 472         | 472          | NA           | NA           |
| CUIABÁ                | 04/03/2026  | 366             | 348         | 360          | NA           | NA           |
| CÁCERES               | 04/03/2026  | 400             | 413         | 410          | 412          | 414          |
| LADÁRIO               | 04/03/2026  | 122             | 130         | 136          | 141          | 147          |
| FORTE COIMBRA         | 04/03/2026  | 6               | 15          | 22           | 29           | 38           |
| PORTO MURTINHO        | 04/03/2026  | 228             | 236         | 250          | 263          | 272          |
`

test('parse md table from file', async() => {
    const filePath = join(tmpdir(), `tmp-${randomUUID()}.tmp`);
    await writeFile(filePath, md_table_content);
    try {
        const client = new Client({file:filePath, cal_id: 712})
        const corrida = await client.getPronostico({})
        
        assert.equal(corrida.forecast_date.getTime(), new Date(2026,2,4).getTime())
        assert.equal(corrida.cal_id, 712)
        assert.equal(corrida.series.length, 6)
        assert.equal(corrida.series[0].pronosticos[2].timestart.getTime(), new Date(2026,2,18).getTime())
        assert.equal(corrida.series[0].pronosticos[2].valor, 4.72)
        assert.equal(corrida.series[5].pronosticos[4].timestart.getTime(), new Date(2026,3,1).getTime())
        assert.equal(corrida.series[5].pronosticos[4].valor, 2.72)
    } finally {
        await unlink(filePath).catch(() => {});
    }
})  

test('parse md table from file, use Accessor', async() => {
    const filePath = join(tmpdir(), `tmp-${randomUUID()}.tmp`);
    await writeFile(filePath, md_table_content);
    try {
        const client = new Accessor({class:"sgb_sace_report",config:{file:filePath, cal_id: 712}})
        const corrida = await client.getPronostico({})
        
        assert.equal(corrida.forecast_date.getTime(), new Date(2026,2,4).getTime())
        assert.equal(corrida.cal_id, 712)
        assert.equal(corrida.series.length, 6)
        assert.equal(corrida.series[0].pronosticos[2].timestart.getTime(), new Date(2026,2,18).getTime())
        assert.equal(corrida.series[0].pronosticos[2].valor, 4.72)
        assert.equal(corrida.series[5].pronosticos[4].timestart.getTime(), new Date(2026,3,1).getTime())
        assert.equal(corrida.series[5].pronosticos[4].valor, 2.72)
    } finally {
        await unlink(filePath).catch(() => {});
    }
})  