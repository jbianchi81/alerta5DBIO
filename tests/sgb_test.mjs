import test from "node:test";
import assert from "node:assert/strict";
import {Client} from "../app/accessors/sgb_sace_report.js"
import { writeFile, unlink } from "fs/promises";
import { tmpdir } from "os";
import { join } from "path";
import { randomUUID } from "crypto";

async function withTempFile(content, fn) {
  const filePath = join(tmpdir(), `tmp-${randomUUID()}.tmp`);

  await writeFile(filePath, content);

  try {
    return await fn(filePath);
  } finally {
    await unlink(filePath).catch(() => {});
  }
}

const md_table_content = `| Estação Fluviométrica | Dia Atual  | Cota Atual (cm) | Dia +7 (cm) | Dia +14 (cm) | Dia +21 (cm) | Dia +28 (cm) |
| --------------------- | ---------- | --------------- | ----------- | ------------ | ------------ | ------------ |
| BARRA DO BUGRES       | 25/02/2026 | 301             | 250         | 225          | NA           | NA           |
| CUIABÁ                | 25/02/2026 | 450             | 352         | 333          | NA           | NA           |
| CÁCERES               | 25/02/2026 | 338             | 349         | 360          | 360          | 357          |
| LADÁRIO               | 25/02/2026 | 112             | 119         | 130          | 135          | 140          |
| FORTE COIMBRA         | 25/02/2026 | -1              | 11          | 19           | 27           | 34           |
| PORTO MURTINHO        | 25/02/2026 | 223             | 229         | 238          | 252          | 267          |
`

test('parse md table from file', async() => {
    const filePath = join(tmpdir(), `tmp-${randomUUID()}.tmp`);
    await writeFile(filePath, md_table_content);
    try {
        const client = new Client({file:filePath, cal_id: 712})
        const corrida = await client.getPronostico({})
        
        assert.equal(corrida.forecast_date.getTime(), new Date(2026,1,25).getTime())
        assert.equal(corrida.cal_id, 712)
        assert.equal(corrida.series.length, 6)
    } finally {
        await unlink(filePath).catch(() => {});
    }
})  