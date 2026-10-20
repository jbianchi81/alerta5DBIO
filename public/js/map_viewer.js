(function () {
	"use strict";

	const config = window.mapViewerConfig || {};
	const form = document.getElementById("map-viewer-form");
	const startInput = document.getElementById("map-viewer-timestart");
	const endInput = document.getElementById("map-viewer-timeend");
	const functionInput = document.getElementById("map-viewer-function");
	const layerInput = document.getElementById("map-viewer-layer");
	const status = document.getElementById("map-viewer-status");
	const legendImage = document.getElementById("map-viewer-legend-image");
	const legendUnavailable = document.getElementById("map-viewer-legend-unavailable");
	const resultsPanel = document.getElementById("map-viewer-results");
	const featureResults = document.getElementById("map-viewer-feature-results");
	const mapElement = document.getElementById("map-viewer-map");
	const wms = config.wms || {};
	const layers = Array.isArray(config.layers) ? config.layers : [];
	const rastextractEndpoint = (config.rastextract && config.rastextract.endpoint) || "res/rastextract/rast";
	const validFunctions = ["sum", "avg", "min", "max", "count","stddev"];
	const params = {};
	let activeLayer = null;
	let pendingMapLoads = 0;
	let wmsLayer = null;
	let activeOverlayLayer = null;
	let rasterRequestId = 0;
	let legendRequestId = 0;
	let featureInfoRequest = 0;

	function beginMapLoad() {
		pendingMapLoads += 1;
		mapElement.classList.add("map-viewer-loading");
	}

	function finishMapLoad() {
		pendingMapLoads = Math.max(0, pendingMapLoads - 1);
		if (pendingMapLoads === 0) {
			mapElement.classList.remove("map-viewer-loading");
		}
	}

	function dateDefault(value) {
		if (typeof value === "string") {
			return value;
		}
		if (value && typeof value.days === "number" && Number.isFinite(value.days)) {
			const date = new Date();
			date.setDate(date.getDate() + value.days);
			const year = date.getFullYear();
			const month = String(date.getMonth() + 1).padStart(2, "0");
			const day = String(date.getDate()).padStart(2, "0");
			return `${year}-${month}-${day}`;
		}
		return "";
	}

	for (const layer of layers) {
		if (!layer || typeof layer.name !== "string" || typeof layer.layer_name !== "string" ||
			!layer.view_params || typeof layer.view_params !== "object" || Array.isArray(layer.view_params) ||
			!layer.defaults || typeof layer.defaults !== "object" ||
			(layer.type != null && !["wms", "rastextract"].includes(layer.type))) {
			setStatus("La configuración de map_viewer.layers no es válida.", true);
			return;
		}
		const option = document.createElement("option");
		option.value = layer.name;
		option.textContent = layer.name;
		layerInput.appendChild(option);
	}

	function setStatus(message, isError) {
		status.textContent = message;
		status.className = isError ? "text-danger" : "text-muted";
	}

	function toViewparams(viewparams) {
		return Object.entries(viewparams)
			.map(([key, value]) => `${key}:${String(value).replace(/[;:,]/g, "")}`)
			.join(";");
	}

	function setLayerDefaults(layer) {
		activeLayer = layer;
		layerInput.value = layer.name;
		const defaults = layer.defaults;
		startInput.value = dateDefault(defaults.timestart);
		endInput.value = dateDefault(defaults.timeend);
		functionInput.value = validFunctions.includes(defaults.function) ? defaults.function : "sum";
		for (const key of Object.keys(params)) {
			delete params[key];
		}
		Object.assign(params, layer.view_params, {
			timestart: startInput.value,
			timeend: endInput.value,
			function: functionInput.value
		});
	}

	function rastextractUrl(extraParams) {
		const url = new URL(rastextractEndpoint, document.baseURI);
		for (const [key, value] of Object.entries(activeLayer.view_params || {})) {
			url.searchParams.set(key, String(value));
		}
		url.searchParams.set("timeStart", params.timestart);
		url.searchParams.set("timeEnd", params.timeend);
		url.searchParams.set("function", String(params.function).toLowerCase());
		for (const [key, value] of Object.entries(extraParams)) {
			url.searchParams.set(key, String(value));
		}
		return url;
	}

	function resolveRasterHref(href) {
		if (typeof href !== "string" || href.length === 0) {
			throw new Error("La respuesta de rastextract no contiene un hlink válido.");
		}
		return new URL(href, rastextractUrl({}).href).href;
	}

	async function updateLegend() {
		const requestId = ++legendRequestId;
		if (!activeLayer) {
			legendImage.hidden = true;
			legendUnavailable.hidden = false;
			return;
		}
		if (activeLayer.type === "rastextract") {
			try {
				const response = await fetch(rastextractUrl({
					make_legend: "true",
					try_cached: "true"
				}));
				if (!response.ok) {
					throw new Error(`HTTP ${response.status}`);
				}
				const result = await response.json();
				if (requestId !== legendRequestId) {
					return;
				}
				legendImage.src = resolveRasterHref(result.hlink);
				legendImage.hidden = false;
				legendUnavailable.hidden = true;
				document.getElementById("map-viewer-legend-title").textContent = String(activeLayer.title || activeLayer.name);
			} catch (error) {
				if (requestId !== legendRequestId) {
					return;
				}
				legendImage.hidden = true;
				legendUnavailable.hidden = false;
				setStatus(`No se pudo cargar la leyenda ráster: ${error.message}`, true);
			}
			return;
		}
		if (!wms.endpoint) {
			legendImage.hidden = true;
			legendUnavailable.hidden = false;
			return;
		}
		const query = new URLSearchParams({
			SERVICE: "WMS",
			VERSION: "1.1.1",
			REQUEST: "GetLegendGraphic",
			FORMAT: "image/png",
			LAYER: activeLayer.layer_name,
			STYLES: activeLayer.styles,
			VIEWPARAMS: toViewparams(params)
		});
		legendImage.hidden = false;
		legendUnavailable.hidden = true;
		legendImage.src = `${wms.endpoint}?${query.toString()}`;
		document.getElementById("map-viewer-legend-title").textContent = String(activeLayer.title || activeLayer.name);
	}
	legendImage.addEventListener("error", function () {
		legendImage.hidden = true;
		legendUnavailable.hidden = false;
	});

	const map = new ol.Map({
		target: "map-viewer-map",
		interactions: ol.interaction.defaults({
			// dragPan: false,
			mouseWheelZoom: false
		}).extend([
			// new ol.interaction.DragPan({
			// 	condition: (event) => event.originalEvent.ctrlKey
			// }),
			new ol.interaction.MouseWheelZoom({
				condition: (event) => event.originalEvent.ctrlKey
			})
		]),
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

	function removeActiveOverlay() {
		if (activeOverlayLayer) {
			map.removeLayer(activeOverlayLayer);
			activeOverlayLayer = null;
		}
		wmsLayer = null;
	}

	function createWmsLayer() {
		if (!wms.endpoint) {
			throw new Error("Configure map_viewer.wms.endpoint para las capas WMS.");
		}
		wmsLayer = new ol.layer.Tile({
			source: new ol.source.TileWMS({
				url: wms.endpoint,
				params: {
					LAYERS: activeLayer.layer_name,
					FORMAT: "image/png",
					TRANSPARENT: true,
					TILED: true,
					STYLES: activeLayer.styles,
					VIEWPARAMS: toViewparams(params)
				},
				serverType: "geoserver"
			})
		});
		wmsLayer.getSource().on("tileloadstart", beginMapLoad);
		wmsLayer.getSource().on("tileloadend", finishMapLoad);
		wmsLayer.getSource().on("tileloaderror", function () {
			finishMapLoad();
			setStatus("No se pudo cargar una tesela WMS. Verifique el endpoint, la capa y los viewparams.", true);
		});
		activeOverlayLayer = wmsLayer;
		map.addLayer(wmsLayer);
	}

	async function loadRasterLayer() {
		const requestId = ++rasterRequestId;
		beginMapLoad();
		try {
			const response = await fetch(rastextractUrl({
				only_hlink: "true",
				format: "PNG"
			}));
			if (!response.ok) {
				throw new Error(`HTTP ${response.status}`);
			}
			const result = await response.json();
			if (requestId !== rasterRequestId) {
				return;
			}
			const properties = result && result.propiedades;
			const lowerLeft = properties && properties.ll;
			const upperRight = properties && properties.ur;
			const width = Number(properties && properties.width);
			const height = Number(properties && properties.height);
			const extent = Array.isArray(lowerLeft) && Array.isArray(upperRight)
				? [...lowerLeft, ...upperRight].map(Number)
				: [];
			if (extent.length !== 4 || extent.some((coordinate) => !Number.isFinite(coordinate)) ||
				!Number.isFinite(width) || width <= 0 || !Number.isFinite(height) || height <= 0) {
				throw new Error("La respuesta de rastextract no contiene extent o dimensiones válidas.");
			}
			const imageProjection = `EPSG:${properties.srs || "4326"}`;
			const imageSource = new ol.source.ImageStatic({
				url: resolveRasterHref(result.hlink),
				imageExtent: extent,
				imageSize: [width, height],
				projection: imageProjection
			});
			imageSource.on("imageloadstart", beginMapLoad);
			imageSource.on("imageloadend", finishMapLoad);
			imageSource.on("imageloaderror", function () {
				finishMapLoad();
				setStatus("No se pudo cargar la imagen ráster devuelta por rastextract.", true);
			});
			activeOverlayLayer = new ol.layer.Image({ source: imageSource });
			map.addLayer(activeOverlayLayer);
			setStatus("", false);
		} catch (error) {
			if (requestId === rasterRequestId) {
				setStatus(`No se pudo obtener la capa ráster: ${error.message}`, true);
			}
		} finally {
			finishMapLoad();
		}
	}

	function applyActiveLayer() {
		rasterRequestId += 1;
		featureInfoRequest += 1;
		removeActiveOverlay();
		if (activeLayer.type === "rastextract") {
			wmsLayer = null;
			loadRasterLayer();
		} else {
			try {
				createWmsLayer();
				setStatus("", false);
			} catch (error) {
				setStatus(error.message, true);
			}
		}
		updateLegend();
	}

	if (layers.length > 0) {
		setLayerDefaults(layers[0]);
		applyActiveLayer();
	} else {
		setStatus("Configure al menos una entrada en map_viewer.layers.", true);
	}

	let chartIndex = 0;
	let propertyTableIndex = 0;

	function formatProperty(value) {
		if (value == null) {
			return "";
		}
		if (typeof value === "object") {
			return JSON.stringify(value);
		}
		return String(value);
	}

	function parseSeries(value, timeseries_property) {
		const series = (typeof value === "string") ? JSON.parse(value) : value;
		if (!series || typeof series !== "object") {
			throw new Error("La propiedad " + timeseries_property + " no contiene una serie temporal.");
		}
		if(Array.isArray(series)) {
			var series_obj = {};
			for (var i = 0; i < series.length; i++) {
				if (Array.isArray(series[i])) {
					series_obj[series[i][0]] = series[i][1];
				} else if (series[i] && typeof series[i] === "object") {
					series_obj[series[i].ts] = series[i].f;
				}
			}
		} else {
			var series_obj = series;
		}
		const points = Object.entries(series_obj).map(([timestamp, rawValue]) => {
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
		heading.textContent = properties.estacion_nombre ? String(properties.estacion_nombre) : properties.nombre ? String(properties.nombre) : `Resultado ${index + 1}`;
		heading.textContent = properties.var_nombre ? `${heading.textContent} - ${String(properties.var_nombre)}` : heading.textContent
		card.appendChild(heading);

		const table = document.createElement("table");
		table.className = "map-viewer-property-table";
		const body = document.createElement("tbody");
		for (const [key, value] of Object.entries(properties)) {
			if (key === "valor" && !Object.hasOwnProperty.call(properties, "timeseries")) {
				continue;
			}
			if (key === "timeseries") {
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
		table.hidden = true;
		table.id = `map-viewer-properties-${++propertyTableIndex}`;
		const propertiesToggle = document.createElement("button");
		propertiesToggle.type = "button";
		propertiesToggle.className = "btn btn-outline-secondary btn-sm map-viewer-series-controls";
		propertiesToggle.textContent = "Mostrar atributos";
		propertiesToggle.setAttribute("aria-expanded", "false");
		propertiesToggle.setAttribute("aria-controls", table.id);
		propertiesToggle.addEventListener("click", function () {
			table.hidden = !table.hidden;
			propertiesToggle.textContent = table.hidden ? "Mostrar atributos" : "Ocultar atributos";
			propertiesToggle.setAttribute("aria-expanded", String(!table.hidden));
		});
		const timeseries_property = (Object.prototype.hasOwnProperty.call(properties, "timeseries")) ? "timeseries" : (Object.prototype.hasOwnProperty.call(properties, "valor")) ? "valor" : undefined
		if (timeseries_property) {
			try {
				const points = parseSeries(properties[timeseries_property], timeseries_property);
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
				chartContainer.mapViewerPlotLines = [
					["nivel_de_alerta", "nivel de alerta", "#ffff00"],
					["nivel_de_evacuacion", "nivel de evacuacion", "#ff0000"],
					["nivel_de_aguas_bajas", "nivel de aguas bajas", "#ff8c00"]
				].flatMap(([property, label, color]) => {
					const rawValue = properties[property];
					if (rawValue == null || (typeof rawValue === "string" && rawValue.trim() === "")) {
						return [];
					}
					const value = Number(rawValue);
					if (!Number.isFinite(value)) {
						return [];
					}
					return [{
						value,
						color,
						dashStyle: "DashDot",
						width: 2,
						zIndex: 5,
						label: { text: label, style: { color } }
					}];
				});
			} catch (error) {
				const message = document.createElement("p");
				message.className = "map-viewer-no-series";
				message.textContent = `No se pudo graficar valor: ${error.message}`;
				card.appendChild(message);
			}
		}
		card.appendChild(propertiesToggle)
		card.appendChild(table);
		return card;
	}

	function showFeatureResults(features) {
		const cards = features
			.map((feature, index) => renderFeature(feature, index))
			.filter(Boolean);
		if (cards.length === 0) {
			throw new Error("La respuesta no contiene propiedades para mostrar.");
		}
		featureResults.replaceChildren(...cards);
		for (const chartContainer of featureResults.querySelectorAll(".map-viewer-chart")) {
			const useBars = [1, 31, 34, 91].includes(chartContainer.mapViewerVarId);
			const ymin = Math.min(
				chartContainer.mapViewerPoints.reduce((a, b) => (a < b[1]) ? a : b[1], Infinity),
				chartContainer.mapViewerPlotLines.reduce((a, b) => (a < b.value) ? a : b.value, Infinity)
			);
			const ymax = Math.max(
				chartContainer.mapViewerPoints.reduce((a, b) => (a > b[1]) ? a : b[1], -Infinity),
				chartContainer.mapViewerPlotLines.reduce((a, b) => (a > b.value) ? a : b.value, -Infinity)
			);
			chartContainer.mapViewerChart = Highcharts.chart(chartContainer.id, {
				chart: { type: useBars ? "column" : "spline", zoomType: "x" },
				title: { text: `Serie temporal: ${chartContainer.mapViewerTitle}` },
				xAxis: { type: "datetime", title: { text: "Fecha" } },
				yAxis: {
					title: { text: "Valor" },
					plotLines: chartContainer.mapViewerPlotLines,
					min: ymin,
					max: ymax
				},
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
		resultsPanel.hidden = false;
		setStatus(`Se encontraron ${cards.length} resultado(s).`, false);
		resultsPanel.scrollIntoView({ behavior: "smooth", block: "start" });
	}

	function rastextractTimeseriesUrl(coordinate) {
		const url = rastextractUrl({ geom: `${coordinate[0]},${coordinate[1]}` });
		url.pathname = url.pathname.replace(/\/rast\/?$/, "/timeseries");
		return url;
	}

	async function queryRasterTimeseries(coordinate) {
		const requestId = ++featureInfoRequest;
		featureResults.replaceChildren();
		resultsPanel.hidden = false;
		setStatus("Consultando la serie temporal en el punto seleccionado...", false);
		try {
			const response = await fetch(rastextractTimeseriesUrl(coordinate));
			if (!response.ok) {
				throw new Error(`HTTP ${response.status}`);
			}
			const result = await response.json();
			if (requestId !== featureInfoRequest) {
				return;
			}
			if (!result || !result.header || !Array.isArray(result.data)) {
				throw new Error("La respuesta de rastextract/timeseries no tiene header y data válidos.");
			}
			const column = result.header.columns && Object.values(result.header.columns)[0] || {};
			const properties = {
				docType: result.docType,
				version: result.version,
				...Object.fromEntries(Object.entries(result.header).map(([key, value]) => [`header.${key}`, value])),
				coordinates: coordinate,
				var_id: column.var_id,
				var_nombre: column.property || column.nombre,
				timeseries: result.data
			};
			showFeatureResults([{ properties }]);
		} catch (error) {
			if (requestId !== featureInfoRequest) {
				return;
			}
			resultsPanel.hidden = true;
			setStatus(`No se pudo consultar la serie temporal ráster: ${error.message}`, true);
		}
	}

	map.on("singleclick", function (event) {
		if (activeLayer && activeLayer.type === "rastextract") {
			queryRasterTimeseries(event.coordinate);
			return;
		}
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
				QUERY_LAYERS: activeLayer.layer_name,
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
				showFeatureResults(data.features);
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
		if (params.timestart && params.timeend && params.timestart > params.timeend) {
			setStatus("La fecha de inicio debe ser anterior o igual a la fecha de fin.", true);
			return;
		}
		if (activeLayer.type === "rastextract") {
			applyActiveLayer();
		} else if (wmsLayer) {
			wmsLayer.getSource().updateParams({
				LAYERS: activeLayer.layer_name,
				STYLES: activeLayer.styles,
				VIEWPARAMS: toViewparams(params)
			});
			updateLegend();
			setStatus("", false);
		}
	});

	layerInput.addEventListener("change", function () {
		const selected = layers.find((layer) => layer.name === layerInput.value);
		if (!selected) {
			setStatus("La capa seleccionada no existe en la configuración.", true);
			return;
		}
		setLayerDefaults(selected);
		applyActiveLayer();
	});
}());
