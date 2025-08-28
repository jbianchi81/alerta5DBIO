import { JSDOM } from "jsdom";
import fetch from "node-fetch";
import { DateTime } from "luxon";
import { observacion as Observacion } from "../CRUD";
import { Observacion as ObservacionType } from "../a5_types"
import { TextDecoder } from "util";
import { AbstractAccessorEngine, AccessorEngine, Config } from "./abstract_accessor_engine";

interface UltimaAltura {
  valor: string;
  date: string;
  time: string;
  nombre_estacion: string;
}

interface FilterOptions {
  [key: string]: any;
}

interface SeriesIdMap {
  [key: string]: number;
}

async function fetchLatin1(url: string): Promise<string> {
  const res = await fetch(url);
  const buffer = await res.arrayBuffer();
  const decoder = new TextDecoder("iso-8859-1");
  return decoder.decode(buffer);
}

function parseDate(date_part: string, time_part: string): Date {
  const dmy = date_part.split("-").map(p => parseInt(p));
  const hm = time_part.split(" ")[0].split(":").map(p => parseInt(p));
  const z = time_part.split(" ")[2];
  const timezone = (z === "(RA)") ? "America/Argentina/Buenos_Aires" : "America/La_Paz";

  return new Date(
    DateTime.fromObject(
      { year: dmy[2], month: dmy[1], day: dmy[0], hour: hm[0], minute: hm[1] },
      { zone: timezone }
    ).toJSDate()
  );
}

function parseValue(value_string: string): number {
  return parseFloat(value_string.replace(/^\s+/, "").replace(/\s+/, ""));
}

const series_id_map: SeriesIdMap = {
  "Misión La Paz DE CTN": 42292,
  "Villa Montes": 42291,
  "Puente Aruma": 42294,
  "Viña Quemada": 42295,
  "Talula": 42296,
  "Tarapaya": 42297,
  "Palca Grande": 42298,
  "San Josecito": 42299,
};

function getSeriesId(nombre_estacion: string): number | undefined {
  const n = nombre_estacion.replace(/\-.*$/, "").replace(/\s+$/, "");
  return series_id_map[n];
}

function parseObs(o: UltimaAltura): ObservacionType {
  return new Observacion({
    tipo: "puntual",
    valor: parseValue(o.valor),
    timestart: parseDate(o.date, o.time),
    timeend: parseDate(o.date, o.time),
    series_id: getSeriesId(o.nombre_estacion),
  });
}

async function getUltimasAlturas(url : string): Promise<UltimaAltura[]> {
  const html = await fetchLatin1(url);

  const dom = new JSDOM(html);
  const container = dom.window.document.querySelector("div.pt-md-3:nth-child(3)");

  if (!container) {
    return [];
  }

  const items = container.querySelectorAll("div.col-3");
  const ultimas_alturas: UltimaAltura[] = [];

  for (const item of items) {
    const date = item.querySelector("div:nth-child(1) > small:nth-child(2)")?.textContent ?? "";
    if(date == "") {
        console.warn("Date string not found, skipping")
        continue
    }
    const nombre_estacion =
      item.querySelector("div:nth-child(1) > small:nth-child(4) > a:nth-child(1)")?.textContent ??
      "";
    if(nombre_estacion == "") {
        console.warn("Nombre estacion string not found, skipping")
        continue
    }
    const valor =
      item.querySelector("div:nth-child(1) > h3:nth-child(5) > span:nth-child(1)")?.textContent ??
      "";
    if(valor == "") {
        console.warn("Valor string not found, skipping")
        continue
    }
    const time = item.querySelector("div:nth-child(1) > small:nth-child(6)")?.textContent ?? "";
    if(time == "") {
        console.warn("Time string not found, skipping")
        continue
    }

    ultimas_alturas.push({
      valor,
      date,
      time,
      nombre_estacion,
    });
  }

  return ultimas_alturas;
}

// interface Config {
//     url: string
// }

export class Client extends AbstractAccessorEngine implements AccessorEngine {

    static _get_is_multiseries : boolean = true

    config : Config

    constructor(config : Config) {
        super(config)
        this.setConfig(config)
    }

    default_config : Config = {
        url : "https://www.pilcomayo.net"  
    }

    async get(filter: FilterOptions = {}, options: FilterOptions = {}): Promise<any[]> {
        const ultimas_alturas = await getUltimasAlturas(this.config.url);
        return ultimas_alturas.map(o => parseObs(o));
    }

    async update(filter: FilterOptions = {}, options: FilterOptions = {}): Promise<any[]> {
        const observaciones = await this.get(filter, options);
        return Promise.all(observaciones.map((o: any) => o.create()));
    }


}