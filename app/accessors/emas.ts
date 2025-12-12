import { AbstractAccessorEngine, AccessorEngine, ObservacionesFilter, ObservacionesFilterWithArrays, SeriesFilter, SitesFilter, SitesFilterWithArrays, SeriesFilterWithArrays } from './abstract_accessor_engine'
import { filterSites, filterSeries } from '../accessor_utils'
import { Estacion, Observacion, Procedimiento, Serie, SerieOnlyIds, Unidades, Variable } from '../a5_types'
import {observacion as crud_observacion, estacion as crud_estacion, var as crud_var, procedimiento as crud_proc, unidades as crud_unidades, serie as crud_serie, serie} from '../CRUD'

type StationParams = {
    url: string,
    variable_list_key: string
}

type ClientConfig = {
    url: string
    variable_lists: Record<string, string[]>,
    variable_map: Record<number, string>,
    station_map: Record<number, StationParams>
}

import fs from "fs";
import readline from "readline";

export interface EmasRecord {
  date_time: Date;
  values: Record<string, number | string>;
}

export interface EmasTable {
  station_id: number;
  rows: EmasRecord[];
}

const VARIABLES = [
  "tempmedia","tempmax","tempmin","humrel","puntorocio","velvientomedia","dirviento",
  "recviento","velvientomax","dirvientomax","sensterm","indcalor","indthw","indthws",
  "presion","precip","intprecip","radsolar","enersolar","maxradsolar","graddcalor",
  "graddfrio","tempint","humint","rocioint","incalint","et","humsuelo1","humsuelo2",
  "humsuelo3","tempsuelo1","tempsuelo2","tempsuelo3","humhoja","muestviento",
  "txviento","recepiss","intarc"
];

function parseDate(dateStr: string, timeStr: string): Date | null {
  // dateStr: "m/d/yy"
  // timeStr: "hh:mm"
  const [m, d, yy] = dateStr.split("/").map(Number);
  const [hh, mm] = timeStr.split(":").map(Number);

  const yyyy = 2000 + yy;
  const dt = new Date(Date.UTC(yyyy, m - 1, d, hh, mm));
  if (isNaN(dt.getTime())) return null;
  return dt

//   const pad = (n: number) => String(n).padStart(2, "0");
//   return `${dt.getUTCFullYear()}-${pad(dt.getUTCMonth() + 1)}-${pad(dt.getUTCDate())} `
//        + `${pad(dt.getUTCHours())}:${pad(dt.getUTCMinutes())}`;
}

async function* readLinesLocalFile(path: string) {
  const rl = readline.createInterface({
    input: fs.createReadStream(path),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    yield line;
  }
}

async function* readLinesUrl(url: string) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch ${url}: ${res.statusText}`);

  const text = await res.text();
  for (const line of text.split(/\r?\n/)) {
    yield line;
  }
}

/**
 * Parse EMAS file (local or URL) into a table-like object.
 */
export async function parseEmas(
  source: string,         // local file path OR http(s) URL
  stationId: number,
  variables: string[]=VARIABLES,
  timestart?: Date,
  timeend?: Date
): Promise<EmasTable> {

  const rows: EmasRecord[] = [];
  const isUrl = /^https?:\/\//i.test(source);

  const lineGenerator = isUrl
    ? readLinesUrl(source)
    : readLinesLocalFile(source);

  for await (let line of lineGenerator) {
    line = line.trim();
    if (!line) continue;

    const parts = line.split(/\s+/);
    if (!/^\d{1,2}\/\d{1,2}\/\d\d$/.test(parts[0])) continue;  // skip non-date lines

    const dateStr = parts[0];
    const timeStr = parts[1];

    const timestamp = parseDate(dateStr, timeStr);
    if (!timestamp) continue;
    if (timestart && timestamp.getTime() < timestart.getTime()) continue;
    if (timeend && timestamp.getTime() > timeend.getTime()) continue;

    const values = parts.slice(2);
    const record: Record<string, number | string> = {};

    for (let i = 0; i < values.length; i++) {
      const val = values[i];
      if (val === "---") continue;

      const name = VARIABLES[i];
      if (!name) continue;

      if (/^[+-]?\d+(?:\.\d+)?$/.test(val)) {
        record[name] = parseFloat(val);
      } else {
        record[name] = val;
      }
    }

    rows.push({
      date_time: timestamp,
      values: record
    });
  }

  return { station_id: stationId, rows };
}

type SeriesMap = {
    estacion_id : number,
    var_id : number
}


export class Client extends AbstractAccessorEngine implements AccessorEngine {

    config : ClientConfig

    series_map : Record<number, SeriesMap>

    default_config : ClientConfig = {
        "url": "https://www.hidraulica.gob.ar/ema", // unused
        "variable_lists": {
            "basabil": [
                "tempmedia","tempmax","tempmin","humrel","puntorocio","velvientomedia","dirviento",
                "recviento","velvientomax","dirvientomax","sensterm","indcalor","indthw","indthws",
                "presion","precip","intprecip","radsolar","enersolar","maxradsolar","graddcalor",
                "graddfrio","tempint","humint","rocioint","incalint","et","humsuelo1","humsuelo2",
                "humsuelo3","tempsuelo1","tempsuelo2","tempsuelo3","humhoja","muestviento",
                "txviento","recepiss","intarc"
            ],
            "bovril": [
                "tempmedia","tempmax","tempmin","humrel","puntorocio","velvientomedia","dirviento",
                "recviento","velvientomax","dirvientomax","sensterm","indcalor","indthw","indthws",
                "presion","precip","intprecip","radsolar","enersolar","maxradsolar","indiceuv",
                "dosisuv","uvmax","graddcalor","graddfrio","tempint","humint","rocioint","incalint",
                "et","humsuelo1","humsuelo2","humsuelo3","tempsuelo1","tempsuelo2","tempsuelo3",
                "humhoja","muestviento","txviento","recepiss","intarc"
            ],
            "colon": ["tempmedia","tempmax","tempmin","humrel","puntorocio","velvientomedia","dirviento","recviento","velvientomax","dirvientomax","sensterm","indcalor","indthw","indthws","presion","precip","intprecip","radsolar","enersolar","maxradsolar","graddcalor","graddfrio","tempint","humint","rocioint","incalint","et","muestviento","txviento","recepiss","intarc"],
            "galarza": ["tempmedia","tempmax","tempmin","humrel","puntorocio","velvientomedia","dirviento","recviento","velvientomax","dirvientomax","sensterm","indcalor","indthw","indthws","presion","precip","intprecip","radsolar","enersolar","maxradsolar","indiceuv","dosisuv","uvmax","graddcalor","graddfrio","tempint","humint","rocioint","incalint","emcint","densintaire","et","humsuelo1","humsuelo2","humsuelo3","tempsuelo1","tempsuelo2","tempsuelo3","humhoja","muestviento","txviento","recepiss","intarc"],
            "gualeguay": ["tempmedia","tempmax","tempmin","humrel","puntorocio","velvientomedia","dirviento","recviento","velvientomax","dirvientomax","sensterm","indcalor","indthw","indthws","presion","precip","intprecip","radsolar","enersolar","maxradsolar","indiceuv","dosisuv","uvmax","graddcalor","graddfrio","tempint","humint","rocioint","incalint","emcint","densintaire","et","humsuelo1","humsuelo2","humsuelo3","tempsuelo1","tempsuelo2","tempsuelo3","humhoja","muestviento","txviento","recepiss","intarc"],
            "gualeguaychu": ["tempmedia","tempmax","tempmin","humrel","puntorocio","velvientomedia","dirviento","recviento","velvientomax","dirvientomax","sensterm","indcalor","indthw","presion","precip","intprecip","graddcalor","graddfrio","tempint","humint","rocioint","incalint","muestviento","txviento","recepiss","intarc"]
        },
        "variable_map": {
            53: "tempmedia",
            58: "humrel",
            43: "puntorocio",
            55: "velvientomedia",
            57: "dirviento",
            68: "presion",
            27: "precip",
            14: "radsolar"
        },
        "station_map": {
            928: {
                "url": "https://www.hidraulica.gob.ar/ema/ema-curuguay/downld08.txt",
                "variable_list_key": "bovril"
            },
            930: {
                "url": "https://www.hidraulica.gob.ar/ema/ema-villaparanacito/downld08.txt",
                "variable_list_key": "bovril"
            },
            931: {
                "url": "https://www.hidraulica.gob.ar/ema/ema-basavilbaso/downld08.txt",
                "variable_list_key": "basavil"
            },
            932: {
                "url": "https://www.hidraulica.gob.ar/ema/ema-urdinarrain/downld08.txt",
                "variable_list_key": "basavil"
            },
            914: {
                "url": "https://www.hidraulica.gob.ar/ema/ema-macia/downld08.txt",
                "variable_list_key": "bovril"
            }
        }
    }

    async getData(station_id : number, timestart? : Date, timeend? : Date) : Promise<EmasTable> {
        const station_config = this.config.station_map[station_id]
        if(!station_config) {
            throw new Error("station_id = " + station_id + " not found in configuration")
        }
        const variables = this.config.variable_lists[station_config.variable_list_key]
        if(!variables) {
            throw new Error("variable_list_key = " + station_config.variable_list_key + " not found in configuration")
        }
        
        const emas_table = await parseEmas(
            station_config.url,         // local file path OR http(s) URL
            station_id,
            variables,
            timestart,
            timeend
        )
        return emas_table
    }

    extractSerieFromTable(emas_table : EmasTable, var_id : number, series_id? : number) : Observacion[] {
        const variable = this.config.variable_map[var_id] 
        if(!variable) {
            throw new Error("var_id" + var_id + " not found in variable_map")
        }
        return emas_table.rows.map( row => {
            return { 
                timestart: row.date_time, 
                timeend: row.date_time, 
                valor: row[variable],
                series_id: series_id
            }
        })
    }

    async getSeries(filter : SeriesFilter={}) : Promise<Serie[]> {
        const station_ids = Object.keys(this.config.station_map)
        var series : Serie[] = await crud_serie.read({estacion_id: station_ids},{no_metadata: true})
        series = filterSeries(series, filter)
        const series_map : Record<number, SeriesMap> = {}
        for(const serie of series) {
            series_map[serie.id] = {
                estacion_id: serie.estacion.id,
                var_id: serie.var.id
            }
        }
        this.series_map = series_map
        return series
    }

    async get(filter : { series_id?: number | number[], timestart?: Date, timeend?: Date}) : Promise<Observacion[]> {
        if(!filter || !filter.series_id) {
            throw new Error("Missing series_id")
        }
        if(!this.series_map) {
            await this.getSeries({series_id: filter.series_id})
        }
        if(Array.isArray(filter.series_id)) {
            const observaciones : Observacion[] = []
            for(const s_id of filter.series_id) {
                const serie = this.series_map[s_id] 
                if(!serie) {
                    throw new Error("series_id " + filter.series_id + " not found")
                }
                const emas_table = await this.getData(serie.estacion_id, filter.timestart, filter.timeend)
                observaciones.push(...(this.extractSerieFromTable(emas_table, serie.var_id, s_id)))
            }
            return observaciones
        } else {
            const serie = this.series_map[filter.series_id] 
            if(!serie) {
                throw new Error("series_id " + filter.series_id + " not found")
            }
            const emas_table = await this.getData(serie.estacion_id, filter.timestart, filter.timeend)
            return this.extractSerieFromTable(emas_table, serie.var_id, filter.series_id)
        }
    }        
}