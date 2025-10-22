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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.downloadFile = exports.getDayOfYear = exports.generateDailyDates = void 0;
function generateDailyDates(from, to) {
    const result = [];
    // Normalize start date to 09:00
    let current = new Date(from);
    current.setHours(9, 0, 0, 0);
    // Also normalize end date to 09:00
    const end = new Date(to);
    end.setHours(9, 0, 0, 0);
    while (current <= end) {
        result.push(new Date(current)); // clone to avoid reference issues
        current.setDate(current.getDate() + 1); // move to next day
    }
    return result;
}
exports.generateDailyDates = generateDailyDates;
function getDayOfYear(date) {
    const startOfYear = new Date(date.getFullYear(), 0, 1); // Jan 1, midnight
    const diff = date.getTime() - startOfYear.getTime();
    const oneDayMs = 1000 * 60 * 60 * 24;
    return Math.floor(diff / oneDayMs) + 1; // +1 because Jan 1 is day 1
}
exports.getDayOfYear = getDayOfYear;
const axios_1 = __importDefault(require("axios"));
const fs_1 = require("fs");
const path_1 = require("path");
function downloadFile(url, outputPath, params) {
    return __awaiter(this, void 0, void 0, function* () {
        const filename = outputPath !== null && outputPath !== void 0 ? outputPath : (0, path_1.basename)(url);
        console.debug(`Downloading ${url} -> ${filename}`);
        const response = yield axios_1.default.get(url, { responseType: "stream", params: params });
        const writer = (0, fs_1.createWriteStream)(filename);
        return new Promise((resolve, reject) => {
            response.data.pipe(writer);
            writer.on("finish", resolve);
            writer.on("error", reject);
        });
    });
}
exports.downloadFile = downloadFile;
