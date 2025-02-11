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
Object.defineProperty(exports, "__esModule", { value: true });
const fs_1 = require("fs");
const child_process_1 = require("child_process");
const path = __importStar(require("path"));
const os = __importStar(require("os"));
const timeSteps_1 = require("./timeSteps");
const internal = {
    fromRaster(input_file) {
        if (!(0, fs_1.existsSync)(input_file)) {
            throw (`Input raster file '${input_file}' not found`);
        }
        var file_info = (0, child_process_1.execSync)(`gdalinfo -json ${input_file}`, { encoding: 'utf-8' });
        file_info = JSON.parse(file_info);
        const buffer = (0, fs_1.readFileSync)(input_file);
        var timeupdate = new Date(file_info["metadata"][""]["timeupdate"]) 
        timeupdate = (timeupdate.toString() != "Invalid Date") ? timeupdate : new Date()
        return {
            tipo: "raster",
            valor: buffer,
            id: file_info["metadata"][""]["id"],
            series_id: file_info["metadata"][""]["series_id"],
            timestart: new Date(file_info["metadata"][""]["timestart"]),
            timeend: new Date(file_info["metadata"][""]["timeend"]),
            timeupdate: timeupdate
        };
    },
    /**
     * Import GDAL files as array of ObservacionesRaster
     * @param filename - either a single file name, a list of file names or a tar.gz file name.
     * @param timestart - optional. If not passed, reads from file metadata
     * @param dt - time step between observations. default: 1 day
     * @param time_support - optional. Temporal footprint of the observations. If not passed, observations are considered instantaneous
     * @param series_id  - optional. If not passed, reads from file metadata
     * @returns Array of ObservacionesRaster
     */
    importRaster: (filename, timestart, dt = { days: 1 }, time_support = {}, series_id
    // create : boolean = false
    ) => __awaiter(void 0, void 0, void 0, function* () {
        var tmp_dir;
        if (!Array.isArray(filename)) {
            // check if file exists
            if (!(0, fs_1.existsSync)(filename)) {
                throw new Error(`File ${filename} not found`);
            }
            // check if file is gzip
            const is_gzip = yield isGzipFile(filename);
            if (is_gzip) {
                tmp_dir = yield extractTarGz(filename);
                var filename_array = listFilesSync(tmp_dir);
            }
            else {
                var filename_array = [filename];
            }
        }
        else {
            var filename_array = filename;
        }
        var observaciones = filename_array.map(f => {
            const filepath = path.resolve(tmp_dir, f);
            return internal.fromRaster(filepath);
        });
        if (timestart) {
            var timestep = new Date(timestart);
            observaciones.forEach((o, i) => {
                o.timestart = timestep;
                if (time_support) {
                    o.timeend = (0, timeSteps_1.advanceTimeStep)(o.timestart, time_support);
                }
                else {
                    o.timeend = o.timestart;
                }
                timestep = (0, timeSteps_1.advanceTimeStep)(timestep, dt);
            });
        }
        if (series_id) {
            observaciones.forEach((o) => {
                o.series_id = series_id;
            });
        }
        if (tmp_dir) {
            yield removeDirectory(tmp_dir);
        }
        return observaciones;
    })
};
function isGzipFile(filePath) {
    return new Promise((resolve, reject) => {
        const stream = (0, fs_1.createReadStream)(filePath, { start: 0, end: 1 });
        stream.on('data', (chunk) => {
            resolve(chunk[0] === 0x1F && chunk[1] === 0x8B);
        });
        stream.on('error', reject);
    });
}
const zlib = __importStar(require("zlib"));
const tar = __importStar(require("tar"));
/**
 * Extracts a .tar.gz file to a temporary directory.
 * @param filePath - Path to the .tar.gz file.
 * @returns Promise<string> - Resolves to the extraction directory path.
 */
function extractTarGz(filePath) {
    return __awaiter(this, void 0, void 0, function* () {
        return new Promise((resolve, reject) => {
            const tempDir = (0, fs_1.mkdtempSync)(path.join(os.tmpdir(), 'extract-')); // Create a temp directory
            (0, fs_1.createReadStream)(filePath)
                .pipe(zlib.createGunzip()) // Decompress gzip
                .pipe(tar.extract({ cwd: tempDir })) // Extract tar contents into tempDir
                .on('finish', () => {
                console.debug(`Extracted to: ${tempDir}`);
                resolve(tempDir);
            })
                .on('error', reject);
        });
    });
}
/**
 * Removes a directory and its contents.
 * @param dirPath - Path to the directory to be deleted.
 * @returns Promise<void>
 */
function removeDirectory(dirPath) {
    return __awaiter(this, void 0, void 0, function* () {
        return new Promise((resolve, reject) => {
            (0, fs_1.rm)(dirPath, { recursive: true, force: true }, (err) => {
                if (err)
                    return reject(err);
                console.debug(`Deleted: ${dirPath}`);
                resolve();
            });
        });
    });
}
function listFilesSync(dirPath) {
    try {
        return (0, fs_1.readdirSync)(dirPath, { withFileTypes: true })
            .filter(file => file.isFile())
            .map(file => file.name);
    }
    catch (err) {
        throw err;
    }
}
module.exports = internal;
