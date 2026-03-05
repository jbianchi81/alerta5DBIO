"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Client = void 0;
const utils2_1 = require("../utils2");
const abstract_accessor_engine_1 = require("./abstract_accessor_engine");
const CRUD_js_1 = require("../CRUD.js");
class Client extends abstract_accessor_engine_1.AbstractAccessorEngine {
    constructor(config) {
        super(config);
        this.example_md_table = `| Estação Fluviométrica | Dia Atual  | Cota Atual (cm) | Dia +7 (cm) | Dia +14 (cm) | Dia +21 (cm) | Dia +28 (cm) |
        | --------------------- | ---------- | --------------- | ----------- | ------------ | ------------ | ------------ |
        | BARRA DO BUGRES       | 25/02/2026 | 301             | 250         | 225          | NA           | NA           |
        | CUIABÁ                | 25/02/2026 | 450             | 352         | 333          | NA           | NA           |
        | CÁCERES               | 25/02/2026 | 338             | 349         | 360          | 360          | 357          |
        | LADÁRIO               | 25/02/2026 | 112             | 119         | 130          | 135          | 140          |
        | FORTE COIMBRA         | 25/02/2026 | -1              | 11          | 19           | 27           | 34           |
        | PORTO MURTINHO        | 25/02/2026 | 223             | 229         | 238          | 252          | 267          |
    `;
        this.default_config = {
            url: "https://sgb.gov.br/sace/index_bacias_monitoradas.php?getbacia=bparaguaiboletim",
            series_id_map: {
                "BARRA DO BUGRES": 43910,
                "CUIABÁ": 43912,
                "CÁCERES": 43913,
                "LADÁRIO": 43914,
                "FORTE COIMBRA": 43916,
                "PORTO MURTINHO": 43917
            },
            station_name_column: "Estação Fluviométrica",
            file: "../public/planillas/sace_boletin_paraguai.md",
            null_value: "NA",
            forecast_column_map: {
                "Cota Atual (cm)": 0,
                "Dia +7 (cm)": 7,
                "Dia +14 (cm)": 14,
                "Dia +21 (cm)": 21,
                "Dia +28 (cm)": 28
            },
            date_column: "Dia Atual",
            cal_id: 712,
            scale: 0.01,
            precision: 2
        };
        this.config = this.default_config;
        Object.assign(this.config, config);
    }
    parseTableRow(row) {
        if (!(this.config.station_name_column in row)) {
            throw new Error(`Missing column ${this.config.station_name_column} in md table`);
        }
        const station_name = row[this.config.station_name_column];
        if (!(this.config.date_column in row)) {
            throw new Error(`Missing column ${this.config.date_column} in md table`);
        }
        const forecast_date = (0, utils2_1.parseDDMMYYYY)(row[this.config.date_column]);
        if (!(station_name in this.config.series_id_map)) {
            console.error(`Missing station name ${station_name} in config.series_id_map`);
            var series_id = 0;
        }
        else {
            var series_id = this.config.series_id_map[station_name];
        }
        const pronosticos = [];
        for (const col of Object.keys(this.config.forecast_column_map)) {
            const forecast_horizon_days = this.config.forecast_column_map[col];
            if (col in row) {
                if (row[col] == this.config.null_value) {
                    console.debug(`Skipping null value at station ${station_name}, forecast horizon ${forecast_horizon_days} days`);
                    continue;
                }
                const timestart = new Date(forecast_date);
                timestart.setDate(timestart.getDate() + forecast_horizon_days);
                const timeend = new Date(timestart);
                timeend.setDate(timeend.getDate() + 1);
                const valor = parseFloat((parseFloat(row[col]) * this.config.scale).toFixed(this.config.precision));
                pronosticos.push({
                    timestart: timestart,
                    timeend: timeend,
                    valor: valor
                });
            }
        }
        return {
            forecast_date: forecast_date,
            serie: {
                series_table: "series",
                series_id: series_id,
                pronosticos: pronosticos
            }
        };
    }
    readMdTableFile(path) {
        return __awaiter(this, void 0, void 0, function* () {
            const md_data = yield (0, utils2_1.readIfExists)(path);
            const data = (0, utils2_1.parseMarkdownTable)(md_data);
            const series = data.map(row => this.parseTableRow(row));
            if (!series.length) {
                throw new Error("No valid rows found in .md file");
            }
            return {
                forecast_date: series[0].forecast_date,
                series: series.map(s => s.serie),
                cal_id: this.config.cal_id
            };
        });
    }
    getPronostico() {
        return __awaiter(this, arguments, void 0, function* (filter = {}) {
            return this.readMdTableFile(filter.file || this.config.file);
        });
    }
    updatePronostico(filter) {
        return __awaiter(this, void 0, void 0, function* () {
            const corrida = yield this.getPronostico(filter);
            const c_cor = new CRUD_js_1.corrida(corrida);
            return c_cor.create();
        });
    }
}
exports.Client = Client;
