import { AbstractAccessorEngine, AccessorEngine, ObservacionesFilter, ObservacionesFilterWithArrays, SeriesFilter, SitesFilter, SitesFilterWithArrays, SeriesFilterWithArrays } from './abstract_accessor_engine'
import { filterSites, filterSeries, filterSeriesByIds } from '../accessor_utils'
import { Estacion, Observacion, Procedimiento, Serie, SerieOnlyIds, Unidades, Variable } from '../a5_types'
import { observacion as crud_observacion, estacion as crud_estacion, var as crud_var, procedimiento as crud_proc, unidades as crud_unidades, serie as crud_serie, accessor as crud_accessor } from '../CRUD'
import fetch from 'node-fetch';

type StationParams = {
  url: string,
  variable_list_key: string
}

type VariableParams = {
  name: string
  unit_id: number
}

type ClientConfig = {
  stations_file: string;
  url: string
  variable_lists: Record<string, string[]>,
  variable_map: Record<number, VariableParams>,
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
  "tempmedia", "tempmax", "tempmin", "humrel", "puntorocio", "velvientomedia", "dirviento",
  "recviento", "velvientomax", "dirvientomax", "sensterm", "indcalor", "indthw", "indthws",
  "presion", "precip", "intprecip", "radsolar", "enersolar", "maxradsolar", "graddcalor",
  "graddfrio", "tempint", "humint", "rocioint", "incalint", "et", "humsuelo1", "humsuelo2",
  "humsuelo3", "tempsuelo1", "tempsuelo2", "tempsuelo3", "humhoja", "muestviento",
  "txviento", "recepiss", "intarc"
];

const COMPASS_16 = {
  N:   0,
  NNE: 22.5,
  NE:  45,
  ENE: 67.5,
  E:   90,
  ESE: 112.5,
  SE:  135,
  SSE: 157.5,
  S:   180,
  SSW: 202.5,
  SW:  225,
  WSW: 247.5,
  W:   270,
  WNW: 292.5,
  NW:  315,
  NNW: 337.5
};


function dirToDegrees(direction : string) : number {
  if(!direction) {
    return
  }
  if(!(direction in COMPASS_16)) {
    throw new Error("Bad compass direction") 
  }
  return COMPASS_16[direction]
}

function parseDate(dateStr: string, timeStr: string, USADateFormat : boolean = false): Date | null {
  // dateStr: "m/d/yy"
  // timeStr: "hh:mm"
  if(USADateFormat) {
    var [m, d, yy] = dateStr.split("/").map(Number);
  } else {
    var [d, m, yy] = dateStr.split("/").map(Number);
  }
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
  variables: string[] = VARIABLES,
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
  tipo: "puntual" | "areal" | "raster",
  estacion_id: number,
  var_id: number,
  unit_id: number,
  proc_id: number
}


export class Client extends AbstractAccessorEngine implements AccessorEngine {

  config: ClientConfig

  series_map: Record<number, SeriesMap>

  static _get_is_multiseries = true

  constructor(config: ClientConfig) {
    super(config)
    this.setConfig(config)
  }

  default_config: ClientConfig = {
    "stations_file": "data/emas/stations.json",
    "url": "https://www.hidraulica.gob.ar/ema", // unused
    "variable_lists": {
      "basabil": [
        "tempmedia", "tempmax", "tempmin", "humrel", "puntorocio", "velvientomedia", "dirviento",
        "recviento", "velvientomax", "dirvientomax", "sensterm", "indcalor", "indthw", "indthws",
        "presion", "precip", "intprecip", "radsolar", "enersolar", "maxradsolar", "graddcalor",
        "graddfrio", "tempint", "humint", "rocioint", "incalint", "et", "humsuelo1", "humsuelo2",
        "humsuelo3", "tempsuelo1", "tempsuelo2", "tempsuelo3", "humhoja", "muestviento",
        "txviento", "recepiss", "intarc"
      ],
      "bovril": [
        "tempmedia", "tempmax", "tempmin", "humrel", "puntorocio", "velvientomedia", "dirviento",
        "recviento", "velvientomax", "dirvientomax", "sensterm", "indcalor", "indthw", "indthws",
        "presion", "precip", "intprecip", "radsolar", "enersolar", "maxradsolar", "indiceuv",
        "dosisuv", "uvmax", "graddcalor", "graddfrio", "tempint", "humint", "rocioint", "incalint",
        "et", "humsuelo1", "humsuelo2", "humsuelo3", "tempsuelo1", "tempsuelo2", "tempsuelo3",
        "humhoja", "muestviento", "txviento", "recepiss", "intarc"
      ],
      "colon": ["tempmedia", "tempmax", "tempmin", "humrel", "puntorocio", "velvientomedia", "dirviento", "recviento", "velvientomax", "dirvientomax", "sensterm", "indcalor", "indthw", "indthws", "presion", "precip", "intprecip", "radsolar", "enersolar", "maxradsolar", "graddcalor", "graddfrio", "tempint", "humint", "rocioint", "incalint", "et", "muestviento", "txviento", "recepiss", "intarc"],
      "galarza": ["tempmedia", "tempmax", "tempmin", "humrel", "puntorocio", "velvientomedia", "dirviento", "recviento", "velvientomax", "dirvientomax", "sensterm", "indcalor", "indthw", "indthws", "presion", "precip", "intprecip", "radsolar", "enersolar", "maxradsolar", "indiceuv", "dosisuv", "uvmax", "graddcalor", "graddfrio", "tempint", "humint", "rocioint", "incalint", "emcint", "densintaire", "et", "humsuelo1", "humsuelo2", "humsuelo3", "tempsuelo1", "tempsuelo2", "tempsuelo3", "humhoja", "muestviento", "txviento", "recepiss", "intarc"],
      "gualeguay": ["tempmedia", "tempmax", "tempmin", "humrel", "puntorocio", "velvientomedia", "dirviento", "recviento", "velvientomax", "dirvientomax", "sensterm", "indcalor", "indthw", "indthws", "presion", "precip", "intprecip", "radsolar", "enersolar", "maxradsolar", "indiceuv", "dosisuv", "uvmax", "graddcalor", "graddfrio", "tempint", "humint", "rocioint", "incalint", "emcint", "densintaire", "et", "humsuelo1", "humsuelo2", "humsuelo3", "tempsuelo1", "tempsuelo2", "tempsuelo3", "humhoja", "muestviento", "txviento", "recepiss", "intarc"],
      "gualeguaychu": ["tempmedia", "tempmax", "tempmin", "humrel", "puntorocio", "velvientomedia", "dirviento", "recviento", "velvientomax", "dirvientomax", "sensterm", "indcalor", "indthw", "presion", "precip", "intprecip", "graddcalor", "graddfrio", "tempint", "humint", "rocioint", "incalint", "muestviento", "txviento", "recepiss", "intarc"]
    },
    "variable_map": {
      53: { name: "tempmedia", unit_id: 12 },
      58: { name: "humrel", unit_id: 15 },
      43: { name: "puntorocio", unit_id: 12 },
      55: { name: "velvientomedia", unit_id: 13 },
      57: { name: "dirviento", unit_id: 16 },
      68: { name: "presion", unit_id: 17 },
      27: { name: "precip", unit_id: 9 },
      14: { name: "radsolar", unit_id: 144 }
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

  async getData(station_id: number, timestart?: Date, timeend?: Date): Promise<EmasTable> {
    const station_config = this.config.station_map[station_id]
    if (!station_config) {
      throw new Error("station_id = " + station_id + " not found in configuration")
    }
    const variables = this.config.variable_lists[station_config.variable_list_key]
    if (!variables) {
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

  extractSerieFromTable(emas_table: EmasTable, var_id: number, series_id?: number): Observacion[] {
    const variable = this.config.variable_map[var_id]
    if (!variable) {
      throw new Error("var_id" + var_id + " not found in variable_map")
    }
    return emas_table.rows.map(row => {
      const valor = (["dirviento", "dirvientomax"].indexOf(variable.name) >= 0) ? dirToDegrees(row.values[variable.name].toString()) : Number(row.values[variable.name])  
      return {
        timestart: row.date_time,
        timeend: row.date_time,
        valor: valor,
        series_id: series_id
      }
    })
  }

  async getSites(filter: SitesFilter = {}): Promise<Estacion[]> {
    if (!this.config.stations_file) {
      throw new Error("Missing stations_file in config")
    }
    if (!fs.existsSync(this.config.stations_file)) {
      throw new Error("Stations file not found")
    }
    const stations_ = fs.readFileSync(this.config.stations_file, { encoding: "utf-8" })
    const stations = JSON.parse(stations_)
    if (!Array.isArray(stations)) {
      throw new Error("stations file must be a json array")
    }
    const crud_stations = filterSites(stations, filter).map((s: any) => new crud_estacion(s))
    for (const s of crud_stations) {
      await s.getId()
    }
    return crud_stations
  }

  async getSeries(filter: SeriesFilter = {}): Promise<Serie[]> {
    const stations = await this.getSites({ estacion_id: filter.estacion_id })
    var series = []
    for (const station of stations) {
      const station_series = Object.keys(this.config.variable_map).map(var_id => {
        return {
          estacion_id: station.id,
          var_id: var_id,
          unit_id: this.config.variable_map[var_id].unit_id,
          proc_id: 1
        }
      })
      series.push(...station_series)
    }
    var crud_series = series.map(s => new crud_serie(s))
    for (const s of crud_series) {
      await s.getId(false)
    }

    crud_series = filterSeries(crud_series, filter)
    return crud_series
  }

  async getSavedSeries(filter: SeriesFilter = {}): Promise<Serie[]> {
    const station_ids = Object.keys(this.config.station_map)
    var series: Serie[] = await crud_serie.read({ estacion_id: station_ids })
    series = filterSeries(series, filter)
    const series_map: Record<number, SeriesMap> = {}
    for (const serie of series) {
      series_map[serie.id] = {
        tipo: serie.tipo,
        estacion_id: serie.estacion.id,
        var_id: serie.var.id,
        unit_id: serie.unidades.id,
        proc_id: serie.procedimiento.id
      }
    }
    this.series_map = series_map
    return series
  }

  async get(filter: { estacion_id?: number | number[], var_id?: number | number[], series_id?: number | number[], timestart?: Date, timeend?: Date }, options : {return_series : boolean}): Promise<Observacion[]> {
    if (!this.series_map) {
      await this.getSavedSeries({ series_id: filter.series_id })
    }
    const observaciones: Observacion[] = []
    const series = []
    if (filter.series_id) {
      if (Array.isArray(filter.series_id)) {    
        for (const s_id of filter.series_id) {
          const serie = this.series_map[s_id]
          if (!serie) {
            throw new Error("series_id " + filter.series_id + " not found")
          }
          const emas_table = await this.getData(serie.estacion_id, filter.timestart, filter.timeend)
          const obs = this.extractSerieFromTable(emas_table, serie.var_id, s_id)
          if(options  && options.return_series) {
            series.push({...serie, observaciones: obs})
          } else {
            observaciones.push(...obs)
          }
        }
      } else {
        const serie = this.series_map[filter.series_id]
        if (!serie) {
          throw new Error("series_id " + filter.series_id + " not found")
        }
        const emas_table = await this.getData(serie.estacion_id, filter.timestart, filter.timeend)
        const obs = this.extractSerieFromTable(emas_table, serie.var_id, filter.series_id)
        if(options  && options.return_series) {
          series.push({...serie, observaciones: obs})
        } else {
          observaciones.push(...obs)
        }
      }
    } else {
      // filter series and group by estacion_id
      const grouped = filterSeriesByIds(Object.entries(this.series_map).map(([key, obj]) => ({
        id: key,
        ...obj
      })), filter).reduce((acc: any, obj: any) => {
        const key = obj.estacion_id;
        (acc[key] ??= []).push(obj);
        return acc;
      }, {});
      const observaciones: Observacion[] = []
      // iter estaciones
      for (const estacion_id of Object.keys(grouped)) {
        const e_id = Number(estacion_id)
        const emas_table = await this.getData(e_id, filter.timestart, filter.timeend)
        for (const serie of grouped[estacion_id]) {
          const obs = this.extractSerieFromTable(emas_table, serie.var_id)
          if(options && options.return_series) {
            series.push({...serie, observaciones: obs})
          } else {
            observaciones.push(...obs)
          }
        }
      }
    }
    if(options  && options.return_series) {
      return series
    } else {  
      return observaciones
    }
  }

  async createAccessor() {
    const emas_accessor = new crud_accessor({
      name: "emas",
      class: "emas",
      url: this.config.url,
      config: this.config,
      series_tipo: "puntual",
      title: "Estaciones Meteorológicas Automáticas DPH Entre Ríos"
    })
    const created = await emas_accessor.create()
    return created
  }
}