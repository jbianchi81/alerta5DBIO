import { XMLParser } from "fast-xml-parser";
import { writeFile } from "node:fs/promises";
import { PathLike } from "node:fs";
import {SeriesFilter, filterSeries } from "./accessor_utils"
import {serie as A5Serie, estacion as A5Estacion} from "../CRUD"

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

export interface Observaciones {
    idEstacion: string;
	variable: string;
    fecha: Date;
    valor: number;
}

interface Config {
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

export class Client {

	url: string
	tabla_id: string

	var_map: Record<string, {var_id: number, proc_id: number, unit_id: number}> = {
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

    constructor(public readonly config: Config) {
		this.config = config
		this.url = config.url
		this.tabla_id = config.tabla_id
	}

    async getData(
        filter: GetDataFilter,
        options: GetDataOptions = {},
    ): Promise<Observaciones[]> {
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
            .map((item): Observaciones | undefined => {
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
                (item): item is Observaciones => item !== undefined,
            );
    }

	async getSites(options : GetDataOptions={}): Promise<Site[]> {
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
				id: String(item.Id),
				name: String(item.Nombre),
				lat: Number(item.Latitud),
				lon: Number(item.Longitud),
				date: new Date(item.Fecha),
				variables: variables == null
					? []
					: Array.isArray(variables)
						? variables.map(String)
						: [String(variables)],
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
			tabla: this.tabla_id
		})
		return site.variables.map(v => {
			const varmap = this.var_map[v]
			return new A5Serie({
				estacion: estacion,
				variable: {id: varmap.var_id},
				procedimiento: {id: varmap.proc_id},
				unidades: {id: varmap.unit_id}
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
        return value
            .replaceAll("&", "&amp;")
            .replaceAll("<", "&lt;")
            .replaceAll(">", "&gt;")
            .replaceAll('"', "&quot;")
            .replaceAll("'", "&apos;");
    }

	async getSeries(filter : SeriesFilter={}, options={}) : Promise<Serie[]> {
		const sites = await this.getSites(options)
		const series : Serie[] = []
		for(const s of sites) {
			const se : A5Serie[] = this.parseSite(s)
			for(const ser of se) {
				await ser.estacion.getEstacionId()
				await ser.getId()
			}
			series.push(...se)
		}
		return filterSeries(series, filter)
	}
}