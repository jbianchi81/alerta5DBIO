/**
 * @typedef {"puntual" | "areal" | "raster" | "rast"} TipoSerie
 */

/**
 * @typedef {Object} SerieFilter
 * 
 * @property {number} [id]
 * @property {number} [series_id]
 * @property {number} [var_id]
 * @property {number} [proc_id]
 * @property {number} [unit_id]
 * @property {true}  [public]  // boolean_only_true
 *
 * @property {string|Date} [timestart]
 * @property {string|Date} [timeend]
 * 
 * @property {number} [count]
 * @property {"H"|"C"|"N"|"R"|"h"|"c"|"n"|"r"} [data_availability]
 * @property {string} [GeneralCategory]
 *
 * @property {TipoSerie} [tipo]             // puntual, areal, raster
 *
 * // ---- PUNTUAL extra ----
 * @property {number}   [estacion_id]
 * @property {string}   [id_externo]
 * @property {string}   [tabla_id]
 * @property {number}   [red_id]
 * @property {string}   [pais]
 * @property {string}   [search]
 * 
 * // ---- AREAL extra ----
 * @property {number} [exutorio_id]
 * 
 * // ---- RASTER extra ----
 * @property {number} [fuentes_id]
 * @property {string} [geom]  // usually WKT / geojson
 *
 * // ---- General ----
 * @property {boolean} [has_obs]
 * @property {boolean} [has_prono]
 * @property {number}  [cal_id]
 * @property {number}  [cal_grupo_id]
 * 
 * // ---- Pagination ----
 * @property {number} [limit]
 * @property {number} [offset]
 */

/**
 * @typedef {Object} SerieOptions
 * @property {boolean} [no_metadata=false]
 * @property {boolean} [include_geom=false]
 * @property {boolean} [no_geom=false]
 * @property {string|string[]} [sort]    // field or list of fields
 * @property {"ASC"|"DESC"} [order]      // override order
 */
