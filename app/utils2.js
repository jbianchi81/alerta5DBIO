"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.pasteIntoSQLQuery = exports.interval_key_map = exports.intervalFromString = exports.createInterval = exports.control_filter2 = exports.assertValidDateTruncField = exports.not_null = exports.listFilesSync = exports.runCommandAndParseJSON = void 0;
const child_process_1 = require("child_process");
const util_1 = require("util");
const fs = __importStar(require("fs"));
const path = __importStar(require("path"));
const postgres_interval_1 = __importDefault(require("postgres-interval"));
const execAsync = (0, util_1.promisify)(child_process_1.exec);
function runCommandAndParseJSON(cmd) {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            const { stdout } = yield execAsync(cmd);
            const data = JSON.parse(stdout);
            // console.log('Parsed JSON:', data);
            return data;
        }
        catch (err) {
            console.error('Command or parse error:', err.message);
            throw err;
        }
    });
}
exports.runCommandAndParseJSON = runCommandAndParseJSON;
function listFilesSync(dir) {
    const entries = fs.readdirSync(dir, { withFileTypes: true });
    return entries
        .filter(entry => entry.isFile())
        .map(entry => path.join(dir, entry.name));
}
exports.listFilesSync = listFilesSync;
class not_null extends Object {
}
exports.not_null = not_null;
function assertValidDateTruncField(field) {
    if ([
        "microseconds",
        "milliseconds",
        "second",
        "minute",
        "hour",
        "day",
        "week",
        "month",
        "quarter",
        "year",
        "decade",
        "century",
        "millennium"
    ].indexOf(field) < 0) {
        throw (new Error("Invalid date_trunc field: " + field));
    }
}
exports.assertValidDateTruncField = assertValidDateTruncField;
function control_filter2(valid_filters, filter, default_table, crud, throw_on_error = false) {
    // valid_filters = { column1: { table: "table_name", type: "data_type", required: bool, column: "column_name"}, ... }  
    // filter = { column1: "value1", column2: "value2", ....}
    // default_table = "table"
    var filter_string = " ";
    var errors = [];
    Object.keys(valid_filters).forEach(key => {
        var table_prefix = (valid_filters[key].table) ? '"' + valid_filters[key].table + '".' : (default_table) ? '"' + default_table + '".' : "";
        var column_name = (valid_filters[key].column) ? '"' + valid_filters[key].column + '"' : '"' + key + '"';
        var fullkey = table_prefix + column_name;
        if (filter[key] instanceof not_null) {
            filter_string += ` AND ` + fullkey + ` IS NOT NULL `;
        }
        else if (typeof filter[key] != "undefined" && filter[key] !== null) {
            if (/[';]/.test(filter[key])) {
                errors.push("Invalid filter value");
                console.error(errors[errors.length - 1]);
            }
            if (valid_filters[key].type == "regex_string") {
                var regex = filter[key].replace('\\', '\\\\');
                filter_string += " AND " + fullkey + " ~* '" + filter[key] + "'";
            }
            else if (valid_filters[key].type == "string") {
                if (Array.isArray(filter[key])) {
                    var values = filter[key].filter(v => v != null).map(v => v.toString()).filter(v => v != "");
                    if (!values.length) {
                        errors.push("Empty or invalid string array");
                        console.error(errors[errors.length - 1]);
                    }
                    else {
                        if (valid_filters[key].case_insensitive) {
                            filter_string += ` AND lower(${fullkey}) IN ( ${values.map(v => `lower('${v}')`).join(",")})`;
                        }
                        else {
                            filter_string += ` AND ${fullkey} IN ( ${values.map(v => `'${v}'`).join(",")})`;
                        }
                    }
                }
                else {
                    if (valid_filters[key].case_insensitive) {
                        filter_string += ` AND lower(${fullkey})=lower('${filter[key]}')`;
                    }
                    else {
                        filter_string += " AND " + fullkey + "='" + filter[key] + "'";
                    }
                }
            }
            else if (valid_filters[key].type == "boolean") {
                var boolean = (/^[yYtTvVsS1]/.test(filter[key])) ? "true" : "false";
                filter_string += " AND " + fullkey + "=" + boolean + "";
            }
            else if (valid_filters[key].type == "boolean_only_true") {
                if (/^[yYtTvVsS1]/.test(filter[key])) {
                    filter_string += " AND " + fullkey + "=true";
                }
            }
            else if (valid_filters[key].type == "boolean_only_false") {
                if (!/^[yYtTvVsS1]/.test(filter[key])) {
                    filter_string += " AND " + fullkey + "=false";
                }
            }
            else if (valid_filters[key].type == "geometry") {
                if (!("archive" in filter[key] && typeof filter[key].toSQL === "function")) {
                    errors.push("Invalid geometry object");
                    console.error(errors[errors.length - 1]);
                }
                else {
                    filter_string += "  AND ST_Distance(st_transform(" + fullkey + ",4326),st_transform(" + filter[key].toSQL() + ",4326)) < 0.001";
                }
            }
            else if (valid_filters[key].type == "date" || valid_filters[key].type == "timestamp") {
                var d;
                if (filter[key] instanceof Date) {
                    d = filter[key];
                }
                else {
                    d = new Date(filter[key]);
                }
                if (valid_filters[key].trunc != undefined) {
                    assertValidDateTruncField(valid_filters[key].trunc);
                    filter_string += ` AND date_trunc('${valid_filters[key].trunc}',${fullkey}) = date_trunc('${valid_filters[key].trunc}', '${d.toISOString()}'::timestamptz)`;
                }
                else {
                    filter_string += " AND " + fullkey + "='" + d.toISOString() + "'::timestamptz";
                }
            }
            else if (valid_filters[key].type == "timestart") {
                var offset = (new Date().getTimezoneOffset() * 60 * 1000) * -1;
                if (filter[key] instanceof Date) {
                    var ldate = new Date(filter[key].getTime() + offset).toISOString();
                    filter_string += " AND " + fullkey + ">='" + ldate + "'";
                }
                else {
                    var d = new Date(filter[key]);
                    var ldate = new Date(d.getTime() + offset).toISOString();
                    filter_string += " AND " + fullkey + ">='" + ldate + "'";
                }
            }
            else if (valid_filters[key].type == "timeend") {
                var offset = (new Date().getTimezoneOffset() * 60 * 1000) * -1;
                if (filter[key] instanceof Date) {
                    var ldate = new Date(filter[key].getTime() + offset).toISOString();
                    filter_string += " AND " + fullkey + "<='" + ldate + "'";
                }
                else {
                    var d = new Date(filter[key]);
                    var ldate = new Date(d.getTime() + offset).toISOString();
                    filter_string += " AND " + fullkey + "<='" + ldate + "'";
                }
            }
            else if (valid_filters[key].type == "greater_or_equal_date") {
                var ldate = new Date(filter[key]).toISOString();
                filter_string += ` AND ${fullkey} >= '${ldate}'::timestamptz`;
            }
            else if (valid_filters[key].type == "smaller_or_equal_date") {
                var ldate = new Date(filter[key]).toISOString();
                filter_string += ` AND ${fullkey} <= '${ldate}'::timestamptz`;
            }
            else if (valid_filters[key].type == "numeric_interval") {
                if (Array.isArray(filter[key])) {
                    if (filter[key].length < 2) {
                        errors.push("numeric_interval debe ser de al menos 2 valores");
                        console.error(errors[errors.length - 1]);
                    }
                    else {
                        filter_string += " AND " + fullkey + ">=" + parseFloat(filter[key][0]) + " AND " + key + "<=" + parseFloat(filter[key][1]);
                    }
                }
                else {
                    filter_string += " AND " + fullkey + "=" + parseFloat(filter[key]);
                }
            }
            else if (valid_filters[key].type == "numeric_min") {
                filter_string += " AND " + fullkey + ">=" + parseFloat(filter[key]);
            }
            else if (valid_filters[key].type == "numeric_max") {
                filter_string += " AND " + fullkey + "<=" + parseFloat(filter[key]);
            }
            else if (valid_filters[key].type == "integer") {
                if (Array.isArray(filter[key])) {
                    var values_int = filter[key].map(v => parseInt(v)).filter(v => v.toString() != "NaN");
                    if (!values_int.length) {
                        errors.push(`Invalid integer array : ${filter[key].toString()}`);
                        console.error(errors[errors.length - 1]);
                    }
                    else {
                        filter_string += " AND " + fullkey + " IN (" + values_int.join(",") + ")";
                    }
                }
                else {
                    var value = parseInt(filter[key]);
                    if (value.toString() == "NaN") {
                        errors.push(`Invalid integer: ${filter[key]}`);
                        console.error(errors[errors.length - 1]);
                    }
                    else {
                        filter_string += " AND " + fullkey + "=" + value + "";
                    }
                }
            }
            else if (valid_filters[key].type == "number" || valid_filters[key].type == "float") {
                if (Array.isArray(filter[key])) {
                    var values_ = filter[key].map(v => parseFloat(v)).filter(v => v.toString() != "NaN");
                    if (!values_.length) {
                        errors.push(`Invalid float array: ${filter[key].toString()}`);
                        console.error(errors[errors.length - 1]);
                    }
                    else {
                        filter_string += " AND " + fullkey + " IN (" + values_.join(",") + ")";
                    }
                }
                else {
                    var value = parseFloat(filter[key]);
                    if (value.toString() == "NaN") {
                        errors.push(`Invalid float: ${filter[key]}`);
                        console.error(errors[errors.length - 1]);
                    }
                    else {
                        filter_string += " AND " + fullkey + "=" + value + "";
                    }
                }
            }
            else if (valid_filters[key].type == "interval") {
                var value_interval = createInterval(filter[key]);
                if (!value_interval) {
                    throw ("invalid interval filter: " + filter[key]);
                }
                filter_string += ` AND ${fullkey}='${value_interval.toPostgres()}'::interval`;
            }
            else if (valid_filters[key].type == "jsonpath") {
                if (!valid_filters[key].expression) {
                    throw new Error("Missing expression for valid_filter " + key);
                }
                const jsonpath_expression = valid_filters[key].expression.replace("$0", filter[key]);
                filter_string += ` AND jsonb_path_exists(${fullkey}, '${jsonpath_expression}')`;
            }
            else {
                if (Array.isArray(filter[key])) {
                    filter_string += " AND " + fullkey + " IN (" + filter[key].join(",") + ")";
                }
                else {
                    filter_string += " AND " + fullkey + "=" + filter[key] + "";
                }
            }
        }
        else if (valid_filters[key].required) {
            errors.push("Falta valor para filtro obligatorio " + key);
            console.error(errors[errors.length - 1]);
        }
    });
    if (errors.length > 0) {
        if (throw_on_error) {
            throw ("Invalid filter:\n" + errors.join("\n"));
        }
        else {
            return null;
        }
    }
    else {
        return filter_string;
    }
}
exports.control_filter2 = control_filter2;
function createInterval(value) {
    if (!value) {
        return; //  parsePGinterval()
    }
    if (value.constructor && value.constructor.name == 'PostgresInterval') {
        var interval = (0, postgres_interval_1.default)("0 seconds");
        Object.assign(interval, value);
        return interval;
    }
    if (value instanceof Object) {
        var interval = (0, postgres_interval_1.default)("0 seconds");
        Object.keys(value).map(k => {
            switch (k) {
                case "milliseconds":
                case "millisecond":
                    interval.milliseconds = value[k];
                    break;
                case "seconds":
                case "second":
                    interval.seconds = value[k];
                    break;
                case "minutes":
                case "minute":
                    interval.minutes = value[k];
                    break;
                case "hours":
                case "hour":
                    interval.hours = value[k];
                    break;
                case "days":
                case "day":
                    interval.days = value[k];
                    break;
                case "months":
                case "month":
                case "mon":
                    interval.months = value[k];
                    break;
                case "years":
                case "year":
                    interval.years = value[k];
                    break;
                default:
                    break;
            }
        });
        return interval;
    }
    if (typeof value == 'string') {
        if (isJson(value)) {
            var interval = (0, postgres_interval_1.default)("0 seconds");
            Object.assign(interval, JSON.parse(value));
            return interval;
        }
        else {
            return intervalFromString(value);
            // return parsePGinterval(value)
        }
    }
    else {
        console.error("timeSteps.createInterval: Invalid value");
        return;
    }
}
exports.createInterval = createInterval;
function isJson(str) {
    try {
        JSON.parse(str);
    }
    catch (e) {
        return false;
    }
    return true;
}
function intervalFromString(interval_string) {
    const kvp = interval_string.split(/\s+/);
    if (kvp.length > 1) {
        var interval = (0, postgres_interval_1.default)("0 seconds");
        for (var i = 0; i < kvp.length - 1; i = i + 2) {
            var key = exports.interval_key_map[kvp[i + 1].toLowerCase()];
            if (!key) {
                throw ("Invalid interval key " + kvp[i + 1].toLowerCase());
            }
            interval[key] = parseInt(kvp[i]);
        }
    }
    else {
        var interval = (0, postgres_interval_1.default)(interval_string);
    }
    // Object.assign(interval,JSON.parse(value))
    return interval;
}
exports.intervalFromString = intervalFromString;
exports.interval_key_map = {
    milliseconds: "milliseconds",
    millisecond: "milliseconds",
    seconds: "seconds",
    second: "seconds",
    minutes: "minutes",
    minute: "minutes",
    hours: "hours",
    hour: "hours",
    days: "days",
    day: "days",
    months: "months",
    month: "months",
    mon: "months",
    years: "years",
    year: "years"
};
function pasteIntoSQLQuery(query, params) {
    for (var i = params.length - 1; i >= 0; i--) {
        var value;
        switch (typeof params[i]) {
            case "string":
                value = escapeLiteral(params[i]);
                break;
            case "number":
                value = parseFloat(params[i]);
                if (value.toString() == "NaN") {
                    throw (new Error("Invalid number"));
                }
                break;
            case "object":
                if (params[i] instanceof Date) {
                    value = "'" + params[i].toISOString() + "'::timestamptz::timestamp";
                }
                else if (params[i] instanceof Array) {
                    // if(/';/.test(params[i].join(","))) {
                    // 	throw("Invalid value: contains invalid characters")
                    // }
                    value = escapeLiteral(`{${params[i].join(",")}}`); // .map(v=> (typeof v == "number") ? v : "'" + v.toString() + "'")
                }
                else if (params[i] === null) {
                    value = "NULL";
                }
                else if (params[i].constructor && params[i].constructor.name == 'PostgresInterval') {
                    value = `${escapeLiteral(params[i].toPostgres())}::interval`;
                }
                else {
                    value = escapeLiteral(params[i].toString());
                }
                break;
            case "undefined":
                value = "NULL";
                break;
            default:
                value = escapeLiteral(params[i].toString());
        }
        var I = parseInt(i) + 1;
        var placeholder = "\\$" + I.toString();
        // console.log({placeholder:placeholder,value:value})
        query = query.replace(new RegExp(placeholder, "g"), value);
    }
    return query;
}
exports.pasteIntoSQLQuery = pasteIntoSQLQuery;
