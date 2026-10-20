(function () {
	"use strict";

	const config = window.mapViewerConfig || {};
	const initialParams = config.initial_viewparams || {};
	const form = document.getElementById("map-viewer-form");
	const startInput = document.getElementById("map-viewer-timestart");
	const endInput = document.getElementById("map-viewer-timeend");
	const functionInput = document.getElementById("map-viewer-function");
	const sourceSearch = document.getElementById("map-viewer-source-search");
	const sourceIdInput = document.getElementById("map-viewer-source-id");
	const sourceResults = document.getElementById("source-results");
	const status = document.getElementById("map-viewer-status");
	const legendImage = document.getElementById("map-viewer-legend-image");
	const legendUnavailable = document.getElementById("map-viewer-legend-unavailable");
	const resultsPanel = document.getElementById("map-viewer-results");
	const featureResults = document.getElementById("map-viewer-feature-results");
	const wms = config.wms || {};
	const def_ts = new Date();
	def_ts.setDate(def_ts.getDate() - 7);
	const def_te = new Date();
	const params = {
		timestart: initialParams.timestart || def_ts.toISOString().substring(0,10),
		timeend: initialParams.timeend || def_te.toISOString().substring(0,10),
		function: initialParams.function || "sum",
		sourceId: initialParams.sourceId == null ? "" : String(initialParams.sourceId)
	};
	const validFunctions = ["sum", "avg", "min", "max", "count","stddev"];

	startInput.value = params.timestart;
	endInput.value = params.timeend;
	functionInput.value = validFunctions.includes(params.function) ? params.function : "sum";
	sourceIdInput.value = params.sourceId;

	function setStatus(message, isError) {
		status.textContent = message;
		status.className = isError ? "text-danger" : "text-muted";
	}

	function toViewparams(viewparams) {
		return Object.entries(viewparams)
			.map(([key, value]) => `${key}:${String(value).replace(/[;:,]/g, "")}`)
			.join(";");
	}

	function updateLegend() {
		if (!wms.endpoint || !wms.layers) {
			legendImage.hidden = true;
			legendUnavailable.hidden = false;
			return;
		}
		const query = new URLSearchParams({
			SERVICE: "WMS",
			VERSION: "1.1.1",
			REQUEST: "GetLegendGraphic",
			FORMAT: "image/png",
			LAYER: wms.layers,
			VIEWPARAMS: toViewparams(params)
		});
		legendImage.hidden = false;
		legendUnavailable.hidden = true;
		legendImage.src = `${wms.endpoint}?${query.toString()}`;
	}
	legendImage.addEventListener("error", function () {
		legendImage.hidden = true;
		legendUnavailable.hidden = false;
	});

	let wmsLayer;
	if (wms.endpoint && wms.layers) {
		wmsLayer = new ol.layer.Tile({
			source: new ol.source.TileWMS({
				url: wms.endpoint,
				params: {
					LAYERS: wms.layers,
					FORMAT: "image/png",
					TRANSPARENT: true,
					TILED: true,
					VIEWPARAMS: toViewparams(params)
				},
				serverType: "geoserver"
			})
		});
		wmsLayer.getSource().on("tileloaderror", function () {
			setStatus("No se pudo cargar una tesela WMS. Verifique el endpoint, la capa y los viewparams.", true);
		});
	} else {
		setStatus("Configure map_viewer.wms.endpoint y map_viewer.wms.layers para cargar la capa WMS.", true);
	}

	const map = new ol.Map({
		target: "map-viewer-map",
		layers: [
			new ol.layer.Tile({
				source: new ol.source.XYZ({
					url: "https://wms.ign.gob.ar/geoserver/gwc/service/tms/1.0.0/capabaseargenmap@EPSG%3A3857@png/{z}/{x}/{-y}.png"
				})
			}),
			...(wmsLayer ? [wmsLayer] : [])
		],
		view: new ol.View({
			center: config.view_center || [-55, -25],
			projection: "EPSG:4326",
			zoom: config.view_zoom || 5
		})
	});

	updateLegend();

	let featureInfoRequest = 0;
	let chartIndex = 0;

	function formatProperty(value) {
		if (value == null) {
			return "";
		}
		if (typeof value === "object") {
			return JSON.stringify(value);
		}
		return String(value);
	}

	function parseSeries(value) {
		const series = (typeof value === "string") ? JSON.parse(value) : value;
		if (!series || typeof series !== "object" || Array.isArray(series)) {
			throw new Error("La propiedad valor no contiene una serie temporal.");
		}
		const points = Object.entries(series).map(([timestamp, rawValue]) => {
			const match = timestamp.match(/^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2}))?)?/);
			if (!match) {
				return null;
			}
			const date = new Date(
				Number(match[1]),
				Number(match[2]) - 1,
				Number(match[3]),
				Number(match[4] || 0),
				Number(match[5] || 0),
				Number(match[6] || 0)
			);
			const number = Number(rawValue);
			return Number.isFinite(number) ? [date.getTime(), number] : null;
		}).filter(Boolean);
		points.sort((left, right) => left[0] - right[0]);
		if (points.length === 0) {
			throw new Error("No hay valores numéricos con fechas válidas en valor.");
		}
		return points;
	}

	function renderFeature(feature, index) {
		const properties = feature && feature.properties;
		if (!properties || typeof properties !== "object" || Array.isArray(properties)) {
			return null;
		}
		const card = document.createElement("article");
		card.className = "map-viewer-feature";
		const heading = document.createElement("h4");
		heading.textContent = properties.nombre ? String(properties.nombre) : `Resultado ${index + 1}`;
		card.appendChild(heading);

		const table = document.createElement("table");
		table.className = "map-viewer-property-table";
		const body = document.createElement("tbody");
		for (const [key, value] of Object.entries(properties)) {
			if (key === "valor") {
				continue;
			}
			const row = document.createElement("tr");
			const name = document.createElement("th");
			name.scope = "row";
			name.textContent = key;
			const propertyValue = document.createElement("td");
			propertyValue.textContent = formatProperty(value);
			row.append(name, propertyValue);
			body.appendChild(row);
		}
		table.appendChild(body);
		card.appendChild(table);

		if (Object.prototype.hasOwnProperty.call(properties, "valor")) {
			try {
				const points = parseSeries(properties.valor);
				const seriesContainer = document.createElement("div");
				seriesContainer.className = "map-viewer-series";

				const toggleButton = document.createElement("button");
				toggleButton.type = "button";
				toggleButton.className = "btn btn-outline-secondary btn-sm map-viewer-series-controls";
				toggleButton.textContent = "Ver tabla";
				toggleButton.setAttribute("aria-pressed", "false");
				seriesContainer.appendChild(toggleButton);

				const chartContainer = document.createElement("div");
				chartContainer.className = "map-viewer-chart";
				chartContainer.id = `map-viewer-chart-${++chartIndex}`;
				seriesContainer.appendChild(chartContainer);

				const seriesTable = document.createElement("table");
				seriesTable.className = "map-viewer-series-table";
				seriesTable.hidden = true;
				const seriesHead = document.createElement("thead");
				const headerRow = document.createElement("tr");
				for (const label of ["Fecha", "Valor"]) {
					const header = document.createElement("th");
					header.scope = "col";
					header.textContent = label;
					headerRow.appendChild(header);
				}
				seriesHead.appendChild(headerRow);
				seriesTable.appendChild(seriesHead);
				const seriesBody = document.createElement("tbody");
				for (const [timestamp, value] of points) {
					const row = document.createElement("tr");
					const dateCell = document.createElement("td");
					dateCell.textContent = new Date(timestamp).toLocaleString();
					const valueCell = document.createElement("td");
					valueCell.textContent = String(value);
					row.append(dateCell, valueCell);
					seriesBody.appendChild(row);
				}
				seriesTable.appendChild(seriesBody);
				seriesContainer.appendChild(seriesTable);
				card.appendChild(seriesContainer);

				toggleButton.addEventListener("click", function () {
					const showTable = seriesTable.hidden;
					seriesTable.hidden = !showTable;
					chartContainer.hidden = showTable;
					toggleButton.textContent = showTable ? "Ver gráfico" : "Ver tabla";
					toggleButton.setAttribute("aria-pressed", String(showTable));
					if (!showTable && chartContainer.mapViewerChart) {
						chartContainer.mapViewerChart.reflow();
					}
				});
				chartContainer.mapViewerPoints = points;
				chartContainer.mapViewerTitle = heading.textContent;
				chartContainer.mapViewerVarId = Number(properties.var_id);
			} catch (error) {
				const message = document.createElement("p");
				message.className = "map-viewer-no-series";
				message.textContent = `No se pudo graficar valor: ${error.message}`;
				card.appendChild(message);
			}
		}
		return card;
	}

	map.on("singleclick", function (event) {
		if (!wmsLayer) {
			return;
		}
		const source = wmsLayer.getSource();
		const url = source.getFeatureInfoUrl(
			event.coordinate,
			map.getView().getResolution(),
			map.getView().getProjection(),
			{
				INFO_FORMAT: "application/json",
				QUERY_LAYERS: wms.layers,
				FEATURE_COUNT: 10,
				VIEWPARAMS: toViewparams(params)
			}
		);
		if (!url) {
			setStatus("No se pudo crear la consulta de información para este punto.", true);
			return;
		}

		const requestId = ++featureInfoRequest;
		featureResults.replaceChildren();
		resultsPanel.hidden = false;
		setStatus("Consultando la capa en el punto seleccionado...", false);
		fetch(url)
			.then((response) => {
				if (!response.ok) {
					throw new Error(`HTTP ${response.status}`);
				}
				return response.json();
			})
			.then((data) => {
				if (requestId !== featureInfoRequest) {
					return;
				}
				if (!data || !Array.isArray(data.features)) {
					throw new Error("La respuesta WMS no contiene una lista de entidades.");
				}
				if (data.features.length === 0) {
					resultsPanel.hidden = true;
					setStatus("No se encontraron resultados en ese punto.", false);
					return;
				}
				const cards = data.features
					.map((feature, index) => renderFeature(feature, index))
					.filter(Boolean);
				if (cards.length === 0) {
					throw new Error("La respuesta WMS no contiene propiedades para mostrar.");
				}
				featureResults.replaceChildren(...cards);
				for (const chartContainer of featureResults.querySelectorAll(".map-viewer-chart")) {
					const useBars = [1, 31, 34, 91].includes(chartContainer.mapViewerVarId);
					chartContainer.mapViewerChart = Highcharts.chart(chartContainer.id, {
						chart: { type: useBars ? "column" : "spline", zoomType: "x" },
						title: { text: `Serie temporal: ${chartContainer.mapViewerTitle}` },
						xAxis: { type: "datetime", title: { text: "Fecha" } },
						yAxis: { title: { text: "Valor" } },
						plotOptions: useBars ? {
							column: {
								pointPadding: 0,
								groupPadding: 0,
								borderWidth: 0,
								minPointLength: 3
							}
						} : {},
						tooltip: {
							xDateFormat: "%Y-%m-%d %H:%M:%S",
							pointFormat: "<b>{point.y}</b>"
						},
						series: [{
							name: "valor",
							data: chartContainer.mapViewerPoints
						}],
						credits: { enabled: false }
					});
				}
				setStatus(`Se encontraron ${cards.length} resultado(s).`, false);
				resultsPanel.scrollIntoView({ behavior: "smooth", block: "start" });
			})
			.catch((error) => {
				if (requestId !== featureInfoRequest) {
					return;
				}
				resultsPanel.hidden = true;
				setStatus(`No se pudo consultar la capa: ${error.message}`, true);
			});
	});

	form.addEventListener("submit", function (event) {
		event.preventDefault();
		params.timestart = startInput.value;
		params.timeend = endInput.value;
		params.function = functionInput.value;
		params.sourceId = sourceIdInput.value;
		if (params.timestart && params.timeend && params.timestart > params.timeend) {
			setStatus("La fecha de inicio debe ser anterior o igual a la fecha de fin.", true);
			return;
		}
		if (wmsLayer) {
			wmsLayer.getSource().updateParams({ VIEWPARAMS: toViewparams(params) });
			updateLegend();
			setStatus("", false);
		}
	});

	let sources = [];
	let selectedSource = null;

	function hideSourceResults() {
		sourceResults.replaceChildren();
		sourceResults.style.display = "none";
	}

	function showSourceResults(query) {
		sourceResults.replaceChildren();
		const normalizedQuery = query.trim().toLocaleLowerCase();
		const matches = sources.filter((source) =>
			source.nombre.toLocaleLowerCase().includes(normalizedQuery) ||
			String(source.id).includes(normalizedQuery)
		).slice(0, 30);
		for (const source of matches) {
			const option = document.createElement("button");
			option.type = "button";
			option.setAttribute("role", "option");
			option.textContent = `${source.nombre} (${source.id})`;
			option.addEventListener("click", function () {
				selectedSource = source;
				sourceIdInput.value = String(source.id);
				sourceSearch.value = source.nombre;
				hideSourceResults();
			});
			sourceResults.appendChild(option);
		}
		if (matches.length === 0) {
			const emptyMessage = document.createElement("div");
			emptyMessage.className = "p-2 text-muted";
			emptyMessage.textContent = "No se encontraron fuentes.";
			sourceResults.appendChild(emptyMessage);
		}
		sourceResults.style.display = "block";
	}

	sourceSearch.addEventListener("input", function () {
		if (selectedSource && sourceSearch.value !== selectedSource.nombre) {
			selectedSource = null;
			sourceIdInput.value = "";
		}
		showSourceResults(sourceSearch.value);
	});
	sourceSearch.addEventListener("focus", function () {
		showSourceResults(sourceSearch.value);
	});
	document.addEventListener("click", function (event) {
		if (!sourceSearch.contains(event.target) && !sourceResults.contains(event.target)) {
			hideSourceResults();
		}
	});

	fetch("obs/fuentes")
		.then((response) => {
			if (!response.ok) {
				throw new Error(`HTTP ${response.status}`);
			}
			return response.json();
		})
		.then((data) => {
			if (!Array.isArray(data) || data.some((source) =>
				!source || source.id == null || typeof source.nombre !== "string"
			)) {
				throw new Error("La respuesta de obs/fuentes no tiene el formato esperado.");
			}
			sources = data;
			if (params.sourceId) {
				selectedSource = sources.find((source) => String(source.id) === params.sourceId) || null;
				if (selectedSource) {
					sourceSearch.value = selectedSource.nombre;
				}
			}
		})
		.catch((error) => {
			setStatus(`No se pudieron cargar las fuentes: ${error.message}`, true);
		});
}());
