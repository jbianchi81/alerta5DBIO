const { execSync } = require('child_process')

function NC2GTiff (input, output) {
    // get lon bounds
    var gdalinfo_lon = JSON.parse(execSync(`gdalinfo -json -sd 2 -stats ${input}`))
    // get lat bounds    
    var gdalinfo_lat = JSON.parse(execSync(`gdalinfo -json -sd 3 -stats ${input}`))
    // convert to GTiff
    execSync(`gdal_translate -of GTiff -a_ullr ${gdalinfo_lon["bands"][0]["metadata"][""]["STATISTICS_MINIMUM"]} ${gdalinfo_lat["bands"][0]["metadata"][""]["STATISTICS_MAXIMUM"]} ${gdalinfo_lon["bands"][0]["metadata"][""]["STATISTICS_MAXIMUM"]} ${gdalinfo_lat["bands"][0]["metadata"][""]["STATISTICS_MINIMUM"]} ${input} ${output}`)
}

const program = require('commander')
program
  .command('convert <input> <output>')
  .alias('c')
  .description('convert rqpe netcdf to gtiff')
  .action((input,output) => {
    NC2GTiff(input,output)
  })

program.parse(process.argv)