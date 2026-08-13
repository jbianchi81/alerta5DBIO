import { XMLParser } from "fast-xml-parser";
import { writeFile } from "node:fs/promises";
import { PathLike } from "node:fs";
import {ObservacionesFilter, SeriesFilter, filterSeries } from "./accessor_utils"
import {serie as A5Serie, estacion as A5Estacion, procedimiento as A5Procedimiento, unidades as A5Unidades, ObservacionDict, observaciones as A5Observaciones} from "../CRUD"
import {Variable as A5Variable} from "a5base/variable"
import { AbstractAccessorEngine } from "./abstract_accessor_engine";

function escapeRegExp(str : string) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); // $& means the whole matched string
}

function replaceAll(str : string, find : string, replace : string) {
  return str.replace(new RegExp(escapeRegExp(find), 'g'), replace);
}

interface Point {
	type: "Point";
	coordinates: number[]
}

interface Estacion {
	id?: number;
	id_externo: string;
    nombre: string;
    geom: Point;
    tabla: string;
}

interface Variable {
	id?: number;
	nombre: string;
}

interface Serie {
	estacion: Estacion;
	variable: {id: number};
	procedimiento: {id: number};
	unidades: {id: number};
}

interface GetDataFilter {
    idEstacion: string;
    variable: string;
    fechaDesde: Date;
    fechaHasta: Date;
}

interface GetDataOptions {
    save_raw_result?: PathLike | string;
}

interface UpdateOptions {
    save_raw_result?: PathLike | string;
	no_update?: boolean
}

export interface Observacion {
    idEstacion: string;
	variable: string;
    fecha: Date;
    valor: number;
}

interface Config {
	[key: string]: unknown
	
	url: string;
	tabla_id: string;
}

interface Site {
    id: string;
    name: string;
    lat: number;
    lon: number;
    date: Date;
    variables: string[];
}

interface SerieMap {
	idEstacion: string,
	variable: string,
	serie: A5Serie
}

export class Client extends AbstractAccessorEngine {

	url: string
	tabla_id: string

	var_map: Record<string, {var_id: number, proc_id: number, unit_id: number, "var"?: A5Variable, procedimiento?: A5Procedimiento, unidades?: A5Unidades}> = {
		"P": {
			var_id: 27,
			proc_id: 1,
			unit_id: 9
		},
		"H": {
			var_id: 2,
			proc_id: 1,
			unit_id: 11
		},
		"Q": {
			var_id: 4,
			proc_id: 1,
			unit_id: 10
		}
	}

	serie_map? : Record<number, SerieMap>

	private getVariableFromVarId(var_id : number) : string {
		if(!this.var_map) {
			throw new Error("var map not set")
		}
		for(const [key, varmap] of Object.entries(this.var_map)) {
			if(varmap.var_id == var_id) {
				return key
			}
		}
		throw new Error(`Var map for var_id=${var_id} not found`)
	}

    constructor(public readonly config: Config) {
		super(config)
		this.config = config
		this.url = config.url
		this.tabla_id = config.tabla_id
	}

    async getData(
        filter: GetDataFilter,
        options: GetDataOptions = {},
    ): Promise<Observacion[]> {
        const { idEstacion, variable, fechaDesde, fechaHasta } = filter;
		if(!idEstacion) {
			throw new Error("Missing idEstacion")
		}
		if(!variable) {
			throw new Error("Missing variable")
		}

        const xml = `<?xml version="1.0"?>
<soap:Envelope
    xmlns:soap="http://www.w3.org/2003/05/soap-envelope"
    xmlns:m="https://www.saltogrande.org/ws.php">
    <soap:Header/>
    <soap:Body>
        <m:HidroSerieHistorica>
            <m:idEstacion>${this.escapeXml(idEstacion)}</m:idEstacion>
            <m:variable>${this.escapeXml(variable.toUpperCase())}</m:variable>
            <m:fechaDesde>${this.formatDate(fechaDesde)}</m:fechaDesde>
            <m:fechaHasta>${this.formatDate(fechaHasta)}</m:fechaHasta>
        </m:HidroSerieHistorica>
    </soap:Body>
</soap:Envelope>`;

        const response = await fetch(this.url, {
            method: "POST",
            headers: {
                "Content-Type": "text/xml;charset=UTF-8",
                "SOAPAction": "HidroSerieHistorica",
            },
            body: xml,
        });

        const responseText = await response.text();

        if (options.save_raw_result) {
            await writeFile(options.save_raw_result, responseText);
        }

        if (!response.ok) {
            throw new Error(
                `Salto Grande SOAP request failed: ${response.status} ${response.statusText}`,
            );
        }

        const parser = new XMLParser({
            ignoreAttributes: false,
            removeNSPrefix: true,
        });

        const parsed = parser.parse(responseText);

        const itemValue =
            parsed?.Envelope?.Body?.HidroSerieHistoricaResponse?.return?.item;

		if (itemValue == null) {
            throw new Error("No se encontró tag <return> en la respuesta SOAP");
        }

        const items = Array.isArray(itemValue)
            ? itemValue
            : [itemValue];

        return items
            .map((item): Observacion | undefined => {
                const fecha = item?.Fecha["#text"];
                const valor = Number(item?.Valor["#text"]);

                if (!fecha || !Number.isFinite(valor)) {
                    return undefined;
                }

                // Same filtering as the Perl script.
                if (valor === -999 || valor === -999.9) {
                    return undefined;
                }

                const parsedDate = new Date(fecha);

                if (Number.isNaN(parsedDate.getTime())) {
                    return undefined;
                }

                return {
                    idEstacion,
					variable: variable.toUpperCase(),
                    fecha: parsedDate,
                    valor,
                };
            })
            .filter(
                (item): item is Observacion => item !== undefined,
            );
    }

	async getListaEstacionesTelemetricas(options : GetDataOptions={}): Promise<Site[]> {
		const xml = `<?xml version="1.0"?>
	<soap:Envelope
		xmlns:soap="http://www.w3.org/2003/05/soap-envelope"
		xmlns:m="https://www.saltogrande.org/ws.php">
		<soap:Header/>
		<soap:Body>
			<m:ListaEstacionesTelemetricas>
				<m:Activas>true</m:Activas>
			</m:ListaEstacionesTelemetricas>
		</soap:Body>
	</soap:Envelope>`;

		const response = await fetch(this.url, {
			method: "POST",
			headers: {
				"Content-Type": "text/xml;charset=UTF-8",
				"SOAPAction": "ListaEstacionesTelemetricas",
			},
			body: xml,
		});

		const responseText = await response.text();

		if (!response.ok) {
			throw new Error(
				`Salto Grande SOAP request failed: ${response.status} ${response.statusText}`,
			);
		}

		const parser = new XMLParser({
			ignoreAttributes: false,
			removeNSPrefix: true,
		});

		if (options.save_raw_result) {
            await writeFile(options.save_raw_result, responseText);
        }

		const parsed = parser.parse(responseText);

		const returnValue =
			parsed?.Envelope?.Body
				?.ListaEstacionesTelemetricasResponse
				?.return;

		if (returnValue == null) {
			throw new Error(
				"No se encontró <return> en la respuesta de ListaEstacionesTelemetricas",
			);
		}

		const items = Array.isArray(returnValue.item)
			? returnValue.item
			: returnValue.item
				? [returnValue.item]
				: [];

		return items.map((item : any): Site => {
			const variables = item.Variables?.item;

			return {
				id: String(item.Id["#text"]),
				name: String(item.Nombre["#text"]),
				lat: Number(item.Latitud["#text"]),
				lon: Number(item.Longitud["#text"]),
				date: new Date(item.Fecha["#text"]),
				variables: variables == null
					? []
					: Array.isArray(variables)
						? variables.map(v => String(v["#text"]))
						: [String(variables["#text"])],
			};
		});
	}

	parseSite(site : Site) : A5Serie[] {
		if(!site.variables.length) {
			return []
		}
		const estacion : A5Estacion = new A5Estacion({
			id_externo: site.id,
			nombre: site.name,
			geom: {
				type: "Point",
				coordinates: [site.lon, site.lat]
			},
			tabla: this.tabla_id,
			automatica: true,
			habilitar: true,
			propietario: "Salto Grande",
			real: true,
			tipo: "A"
		})
		return site.variables.map(v => {
			if(!(v in this.var_map)) {
				throw new Error(`Variable not found in mapping: ${v}`)
			}
			const varmap = this.var_map[v]
			return new A5Serie({
				tipo: "puntual",
				estacion: estacion,
				"var": (varmap.var) ? varmap.var : {id: varmap.var_id},
				procedimiento: (varmap.procedimiento) ? varmap.procedimiento : {id: varmap.proc_id},
				unidades: (varmap.unidades) ? varmap.unidades : {id: varmap.unit_id}
			})
		})
	}

    private formatDate(date: Date): string {
        const pad = (n: number) => String(n).padStart(2, "0");

        return [
            date.getFullYear(),
            pad(date.getMonth() + 1),
            pad(date.getDate()),
        ].join("-") + ` ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
    }

    private escapeXml(value: string): string {
		value = replaceAll(value,"&", "&amp;")
        value = replaceAll(value,"<", "&lt;")
        value = replaceAll(value,">", "&gt;")
        value = replaceAll(value,'"', "&quot;")
        value = replaceAll(value,"'", "&apos;");
		return value
    }

	async getMetadataOfVarMap() : Promise<void> {
		for(const key of Object.keys(this.var_map)) {
			const varmap = this.var_map[key]
			if(!varmap.var) {
				varmap.var = await A5Variable.read(varmap.var_id)
			}
			if(!varmap.procedimiento) {
				varmap.procedimiento = await A5Procedimiento.read({id: varmap.proc_id})
			}
			if(!varmap.unidades) {
				varmap.unidades = await A5Unidades.read(varmap.unit_id)
			}
		}
	}

	mapSeries(series : A5Serie[]) : void {
		this.serie_map = {}
		for (const s of series) {
			if(!s.var.id) {
				throw new Error(`Missing var id for series id=${s.id}`)
			}
			this.serie_map[s.id] = {
				idEstacion: s.estacion.id_externo,
				variable: this.getVariableFromVarId(s.var.id),
				serie: s
			}
		}
	}

	async getSeries(filter : SeriesFilter={}, options: GetDataOptions={}) : Promise<A5Serie[]> {
		await this.getMetadataOfVarMap()
		const sites = await this.getListaEstacionesTelemetricas(options)
		const series : A5Serie[] = []
		for(const s of sites) {
			const se : A5Serie[] = this.parseSite(s)
			for(const ser of se) {
				await ser.estacion.getEstacionId()
				await ser.getId(false)
			}
			series.push(...se)
		}
		this.mapSeries(series)
		return filterSeries(series, filter)
	}

	async updateSeries(filter : SeriesFilter={}, options : UpdateOptions={}) : Promise<A5Serie[]> {
		const series = await this.getSeries(filter, options)
		return A5Serie.create(
			series,
			{
				refresh_date_range: false,
				series_metadata: true,
				upsert_estacion: true,
				no_update: options.no_update
			})
	}

	private parseObservacion(
		o : Observacion, 
		series_id?: number) : ObservacionDict {
			return {
				series_id: series_id,
				timestart: o.fecha,
				timeend: o.fecha,
				valor: o.valor
			}
		}

	async get(
		filter : ObservacionesFilter,
		options : {
			save_raw_response?: boolean,
			return_series: true
		}
	) : Promise<A5Serie[]>	
	async get(
		filter : ObservacionesFilter,
		options? : {
			save_raw_response?: boolean,
			return_series?: false
		}
	) : Promise<A5Observaciones>
	async get(
		filter : ObservacionesFilter,
		options? : {
			save_raw_response?: boolean,
			return_series?: boolean
		}
	) : Promise<A5Observaciones|A5Serie[]> {
		const series_id = filter.series_id
		if(!series_id) {
			throw new Error("Missing series_id")
		}
		if(Array.isArray(series_id)) {
			throw new Error("Bad filter series_id. must be a number, not an array")
		}
		if(!(typeof series_id == "number")) {
			throw new Error("Bad filter series_id. must be a number")
		}
		if(!filter.timestart) {
			throw new Error("Missing timestart")
		}
		if(!filter.timeend) {
			throw new Error("Missing timeend")
		}
		if(!this.serie_map) {
			console.debug("serie map not set. Calling .getSeries()")
			await this.getSeries()
		}
		if(!this.serie_map) {
			throw new Error("Failed to get series")
		}
		if(!(series_id in this.serie_map)) {
			throw new Error(`series_id=${series_id} not found in series mapping`)
		}
		const seriemap = this.serie_map[series_id]
		const data = await this.getData(
			{
				idEstacion: seriemap.idEstacion,
				variable: seriemap.variable,
				fechaDesde: filter.timestart,
				fechaHasta: filter.timeend
			}
		)
		const observaciones = data.map(d => this.parseObservacion(d, series_id))
		if(options?.return_series) {
			const serie = seriemap.serie
			serie.setObservaciones(observaciones)
			return [serie]			
		} else {
			return new A5Observaciones(observaciones)
		}
	}
}