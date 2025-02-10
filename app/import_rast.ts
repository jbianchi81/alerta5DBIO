import {ObservacionRaster, Interval} from './a5_types'
import {readFileSync, existsSync, createReadStream, createWriteStream, mkdtempSync, rm, readdirSync} from 'fs'
import {execSync} from 'child_process'
import createGunzip from 'zlib'
import * as path from 'path'
import * as os from 'os'
import {advanceTimeStep} from './timeSteps'

const internal = {

    fromRaster(input_file : string) {
		if(!existsSync(input_file)) {
			throw(`Input raster file '${input_file}' not found`)
		}
		var file_info = execSync(`gdalinfo -json ${input_file}`,{encoding:'utf-8'})
		file_info = JSON.parse(file_info)
		const buffer = readFileSync(input_file)
		return {
			tipo: "raster",
			valor: buffer,
			id: file_info["metadata"][""]["id"],
			series_id: file_info["metadata"][""]["series_id"],
			timestart: new Date(file_info["metadata"][""]["timestart"]),
			timeend: new Date(file_info["metadata"][""]["timeend"]),
			timeupdate: new Date(file_info["metadata"][""]["timeupdate"])
		} as ObservacionRaster
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
    importRaster: async(
        filename : string|string[],
        timestart : Date,
        dt : Interval = {days: 1},
        time_support : Interval = {},
        series_id : number
        // create : boolean = false
    ) : Promise<ObservacionRaster[]> => {
        var tmp_dir : string
        if(!Array.isArray(filename)) {
            // check if file exists
            if(!existsSync(filename)) {
                throw new Error(`File ${filename} not found`)
            }
            // check if file is gzip
            const is_gzip : boolean = await isGzipFile(filename)
            if(is_gzip) {
                tmp_dir = await extractTarGz(filename)
                var filename_array : Array<string> = listFilesSync(tmp_dir)
            } else {
                var filename_array : Array<string> = [ filename ]
            } 
        } else {
            var filename_array : Array<string> = filename
        }
        
        var observaciones = filename_array.map(f=>{
            const filepath = path.resolve(tmp_dir, f)
            return internal.fromRaster(filepath) 
        })
        if(timestart) {
            var timestep = new Date(timestart)
            observaciones.forEach( (o, i) => {
                o.timestart = timestep
                if(time_support) {
                    o.timeend = advanceTimeStep(o.timestart, time_support)
                } else {
                    o.timeend = o.timestart
                }
                timestep = advanceTimeStep(timestep, dt)
            })
        }
        if(series_id) {
            observaciones.forEach( (o) => {
                o.series_id = series_id
            })
        }
        
        if(tmp_dir) {
            await removeDirectory(tmp_dir)
        }        

        return observaciones
    }
}

function isGzipFile(filePath : string) : Promise<boolean>{
    return new Promise((resolve, reject) => {
        const stream = createReadStream(filePath, { start: 0, end: 1 });

        stream.on('data', (chunk) => {
            resolve(chunk[0] === 0x1F && chunk[1] === 0x8B);
        });

        stream.on('error', reject);
    });
}

import * as zlib from 'zlib';
import * as tar from 'tar';

/**
 * Extracts a .tar.gz file to a temporary directory.
 * @param filePath - Path to the .tar.gz file.
 * @returns Promise<string> - Resolves to the extraction directory path.
 */
async function extractTarGz(filePath: string): Promise<string> {
    return new Promise((resolve, reject) => {
        const tempDir = mkdtempSync(path.join(os.tmpdir(), 'extract-')); // Create a temp directory

        createReadStream(filePath)
            .pipe(zlib.createGunzip()) // Decompress gzip
            .pipe(tar.extract({ cwd: tempDir })) // Extract tar contents into tempDir
            .on('finish', () => {
                console.debug(`Extracted to: ${tempDir}`);
                resolve(tempDir);
            })
            .on('error', reject);
    });
}

/**
 * Removes a directory and its contents.
 * @param dirPath - Path to the directory to be deleted.
 * @returns Promise<void>
 */
async function removeDirectory(dirPath: string): Promise<void> {
    return new Promise((resolve, reject) => {
        rm(dirPath, { recursive: true, force: true }, (err) => {
            if (err) return reject(err);
            console.debug(`Deleted: ${dirPath}`);
            resolve();
        });
    });
}

function listFilesSync(dirPath: string): string[] {
    try {
        return readdirSync(dirPath, { withFileTypes: true })
            .filter(file => file.isFile())
            .map(file => file.name);
    } catch (err) {
        throw err
    }
}

module.exports = internal