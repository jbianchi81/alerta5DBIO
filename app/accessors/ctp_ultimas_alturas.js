const { JSDOM } = require("jsdom");
const fs = require('fs')
const fetch = require("node-fetch");
const { DateTime } = require('luxon')
const { observacion: Observacion, observaciones: Observaciones } = require('../CRUD')
const { TextDecoder } = require("util");

async function fetchLatin1(url) {
  const res = await fetch(url);
  const buffer = await res.arrayBuffer();
  const decoder = new TextDecoder("iso-8859-1");
  return decoder.decode(buffer);
}

function parseDate(date_part, time_part) {
    const dmy = date_part.split("-").map(p=>parseInt(p))
    const hm = time_part.split(" ")[0].split(":").map(p=>parseInt(p))
    const z = time_part.split(" ")[2]
    const timezone = (z == "(RA)") ? "America/Argentina/Buenos_Aires" : "America/La_Paz"
    return new Date(DateTime.fromObject({year: dmy[2], month: dmy[1], day: dmy[0], hour: hm[0], minute: hm[1]}, { zone: timezone }))
    // return new Date(dmy[2],dmy[1]-1, dmy[0], hm[0], hm[1])
}

function parseValue(value_string) {
    return parseFloat(value_string.replace(/^\s+/,"").replace(/\s+/,""))
}

const series_id_map = {
  'Misión La Paz DE CTN': 42292,
  'Villa Montes': 42291,
  'Puente Aruma': 42294,
  'Viña Quemada': 42295,
  'Talula': 42296,
  'Tarapaya': 42297,
  'Palca Grande': 42298,
  'San Josecito': 42299
}

function getSeriesId(nombre_estacion) {
    const n = nombre_estacion.replace(/\-.*$/,"").replace(/\s+$/,"")
    return series_id_map[n]
}

function parseObs(o) {
    return new Observacion({
        tipo: "puntual",
        valor: parseValue(o.valor), 
        timestart: parseDate(o.date, o.time), 
        timeend:  parseDate(o.date, o.time), 
        series_id: getSeriesId(o.nombre_estacion)
    })
}

async function getUltimasAlturas() {
    const url = "https://www.pilcomayo.net"
    const html = await fetchLatin1(url)
    // Load HTML (from file or string)
    // const html = fs.readFileSync("tmp/ctp_home.html", "latin1");
    // const res = await fetch(url, );
    // const html = await res.text();
    const dom = new JSDOM(html);

    const container = dom.window.document.querySelector("div.pt-md-3:nth-child(3)")

    const items = container.querySelectorAll("div.col-3")

    const ultimas_alturas = []
    for(const item of items) {
        const date = item.querySelector("div:nth-child(1) > small:nth-child(2)").textContent
        const nombre_estacion = item.querySelector("div:nth-child(1) > small:nth-child(4) > a:nth-child(1)").textContent
        const valor = item.querySelector("div:nth-child(1) > h3:nth-child(5) > span:nth-child(1)").textContent
        const time = item.querySelector("div:nth-child(1) > small:nth-child(6)").textContent
        ultimas_alturas.push({
            valor: valor,
            date: date, 
            time: time, 
            nombre_estacion: nombre_estacion
        })
    }
    return ultimas_alturas
}

async function get(filter={}, options={}) {
    const ultimas_alturas = await getUltimasAlturas()
    return ultimas_alturas.map(o => parseObs(o))
}

async function update(filter={}, options={}) {
    const observaciones = await get(filter,options)
    return Promise.all(observaciones.map(o => o.create()))
}

