require('./setGlobal')
const config = global.config // require('config');
const program = require('commander');
const procedures = require('./procedures')
const logger = require('./logger');
const CRUD = require('./CRUD')
const path = require('path');
const { DateFromDateOrInterval } = require('./timeSteps');

const parseKVPArray = function(filter) {
    if(!filter) {
        return {}
    }
    const filter_obj = {}
    if(filter.length) {
        for(var f of filter) {
            var kvp = f.split("=")
            if (kvp.length < 2) {
                throw("Invalid key-value pair. use format 'key1=value1 key2=value2 ...'")
            }
            var key  = kvp[0].replace(/^["']/,"").replace(/["']$/,"")
            var value  = kvp[1].replace(/^["']/,"").replace(/["']$/,"")
            // if key already exists, push into array
            if(Object.keys(filter_obj).indexOf(key) >= 0) {
                if(Array.isArray(filter_obj[key])) {
                    filter_obj[key].push(value)
                } else {
                    filter_obj[key] = [filter_obj[key], value]
                }
            } else {
                filter_obj[key] = value
            }
        }
    }
    return filter_obj
}

const getOutputOptions = function(options={}) {
    const output_options = {}
    if(options.header) {
        output_options.header = true
    }
    if(options.output_individual_files_pattern) {
        output_options.output_individual_files = {
            pattern: options.output_individual_files_pattern,
            base_path: (options.base_path) ? path.resolve(options.base_path) : path.resolve(),
            iter_field: options.iter_field
        }
    } 
    if(options.columns) {
        output_options.columns = options.columns
    }
    if(options.pretty) {
        output_options.pretty = options.pretty
    }
    return output_options
}

const writeResult = async function(procedure,result,options={}) {
    if(!result) {
        logger.error("Null result")
        process.exit(1)
    }
    if(!options.output && !options.output_individual_files_pattern) {
        const output_format = (options.format) ? options.format : "json"
        try {
            if(output_format == "csv") {
                var csv_result = (typeof result.toCSV === 'function') ? result.toCSV({header:options.header,columns:options.columns}) : (Array.isArray(result)) ? procedure.class.toCSV(result,{header:options.header,columns:options.columns}) : CSV.stringify(result) // result.map(r=>r.toCSV()).join("\n") 
                process.stdout.write(csv_result)
            } else if(output_format.toLowerCase() == "geojson") {
                // check if instance method exists
                if(typeof result.toGeoJSON === 'function') {
                    var geojson_result = result.toGeoJSON()
                } else if (Array.isArray(result)) {
                    // check if static method exists
                    if (typeof procedure.class.toGeoJSON === 'function') {
                        var geojson_result = procedure.class.toGeoJSON(result)
                    } else {
                        var geojson_result = {
                            "type": "FeatureCollection",
                            "features": []
                        }
                        result.forEach((item,i)=>{
                            // for each item, check if instance method exists
                            if(typeof item.toGeoJSON === 'function') {
                                geojson_result.features.push(item.toGeoJSON())
                            } else {
                                throw("toGeoJSON method not found in item " + i)
                            }
                        })
                    }
                } else {
                    throw("toGeoJSON method not found in class " + procedure.class_name)
                }
                process.stdout.write(JSON.stringify(geojson_result,null,4))
            } else {
                // if(result.toJSON) {
                //     process.stdout.write(result.toJSON())
                // } else {
                if(options.pretty) {
                    process.stdout.write(JSON.stringify(result,null,4))
                } else {
                    process.stdout.write(JSON.stringify(result))
                }
                // }
            }
        } catch(e) {
            logger.error(e)
            process.exit(1)
        }
    } else {
        try {
            await procedure.writeResult()
        } catch(e) {
            logger.error(e)
            process.exit(1)
        }
        if(Array.isArray(result)) {
            logger.info(`read ${result.length} ${procedure.class_name}s`)
        } else {
            logger.info(`read ${procedure.class_name} id: ${result.id}`)
        }
        logger.info("End.")
    }
}

if(1==1) {
    program
    .version('0.0.1')
    .description('observations database CRUD procedures');

    program
    .command('run <files...>')
    .alias('r')
    .description('run one or more sequence of procedures as defined in provided json o yaml files')
    .option("-v, --validate",'validate only (don\'t run')
    .option("-t, --test",'run in test mode',false)
    //   .option("-p, --parameter <parameter...>", "send positional or key-value parameters")
    .action(async (files,options) => {
        var test_result = true
        var error_log = []
        const client = await global.pool.connect()
        for(var i in files) {
            var filename = files[i]
            logger.info("~~~ Sequence file: " + filename + " ~~~")
            if(options.validate) {
                try {
                    procedures.validateSequence(filename)
                } catch(e) {
                    if(options.test) {
                        test_result = false
                    }
                    logger.error(e)
                }
                continue
            }
            try {
                var result = await procedures.runSequence(filename,options.test,client)
            } catch (e) {
                logger.error(e)
                // error_log.push({filename:filename,errors: [e.toString()]})
                test_result = false
                error_log.push({filename: filename, errors: [e.toString()]})
                continue
            }
            if(options.test && result.success===false) {
                test_result = false
                error_log.push({filename: filename, errors: result.reasons})
            }
        }
        logger.info("End.")
        client.release()
        if(test_result===false) {
            if(files.length > 1) {
                printErrorSummmary(error_log,files.length)
            }
            process.exit(1)
        } else {
            logger.info("All procedure sequences successful!")
            process.exit(0)
        }
    })

    program
    .command('create crud_class <files...>')
    .alias('C')
    .description('Run create procedure for given class and json or csv data')
    .option("-v, --validate",'validate only (don\'t run)')
    .option("-t, --test",'run in test mode',false)
    .option("-o, --output <value>",'save output to file')
    .option("-f, --format <value>",'input files format: json, csv or geojson (default: json)')
    .option("-H, --header",'use this option if csv input file has a header')
    .option("-a, --all", 'in serie creation, create parent objects (estacion, fuente, var, procedimiento, unidades')
    .option("-s, --station", "create parent station (estacion)")
    .action(async (crud_class,files,options) => {
        var test_result = true
        if(!CRUD.hasOwnProperty(crud_class)) {
            logger.error("Invalid crud class")
            process.exit(1)
        }
        const class_name = crud_class
        crud_class = CRUD.CRUD[class_name]
        const all_results = []
        for(var i in files) {
            var filename = files[i]
            logger.info("Data file: " + filename)
            try {
                const params = {class_name: class_name, options: {}}
                if(options.format && options.format == "csv") {
                    params.csvfile = path.resolve(filename)
                    params.options.header = options.header
                } else if(options.format && options.format == "geojson") {
                    params.geojsonfile = path.resolve(filename)
                } else {
                    params.jsonfile = path.resolve(filename)
                }
                if(options.all) {
                    params.options.all = options.all
                }
                if(options.station) {
                    params.options.upsert_estacion = true
                }
                var procedure = new procedures.CreateProcedure(params)
            } catch(e) {
                if(options.test) {
                    test_result = false
                }
                logger.error(e)
                continue
            }
            if(options.validate) {
                continue
            }
            try {
                var result = await procedure.run()
            } catch(e) {
                logger.error(e)
                test_result = false
                continue
            }
            console.log(`created ${result.length} ${class_name}s`)
            all_results.push(...result)
        }   
        if(options.output) {
            try {
                fs.writeFileSync(path.resolve(options.output),JSON.stringify(all_results))
            } catch(e) {
                logger.error(e.toString())
                process.exit(1)
            }
        }
        logger.info("End.")
        if(test_result===false) {
            process.exit(1)
        } else {
            process.exit(0)
        }     
    })

    program
    .command('read crud_class [filter...]')
    .alias('R')
    .description('Run read procedure for given class and output in selected format. Accepts zero to many filters as "key1=value1 key2=value2 ..."')
    .option("-v, --validate",'validate only (don\'t run)')
    .option("-o, --output <value>",'save output to file. If -o nor -i are set, output is printed to STDOUT')
    .option("-f, --format <value>",'output format (json (default), geojson or csv)')
    .option("-p, --pretty",'pretty print output')
    .option("-i, --output_individual_files_pattern <value>",'output one file for each retrieved element using this printf pattern to use with element id and, additional fields (with -F option)')
    .option("-b, --base_path <value>",'to use together with -i. Prepends this base path to the constructed file paths')
    .option("-F, --iter_field <value>",'Field of elements to iterate over to generate separate output files (combined with -i)')
    .option("-h, --header",'add csv header to output (with --format csv)')
    .option("-c, --columns <value...>","read only this columns")
    .option("-O, --more_options <value...>","additional kvp options")
    .action(async (crud_class,filter,options) => {
        try {
            filter = parseKVPArray(filter)
        } catch (e) {
            logger.error(e)
            process.exit(1)
        }
        // console.debug(filter)
        // var test_result = true
        if(!CRUD.hasOwnProperty(crud_class)) {
            logger.error("Invalid crud class")
            process.exit(1)
        }
        const class_name = crud_class
        crud_class = CRUD.CRUD[class_name]
        const read_options = getOutputOptions(options)
        if(options && options.more_options) {
            try {
                options.more_options = parseKVPArray(options.more_options)
            } catch (e) {
                logger.error(e)
                process.exit(1)
            }
            Object.assign(read_options,options.more_options)
        }
        try {
            var procedure = new procedures.ReadProcedure({class_name: class_name, filter:filter, output: (options.output) ? path.resolve(options.output) : undefined, output_format: options.format, options: read_options})
        } catch(e) {
            logger.error(e)
            process.exit(1)
        }
        if(options.validate) {
            process.exit(0)
        }
        try {
            var result = await procedure.run()
        } catch(e) {
            logger.error(e)
            process.exit(1)
        }
        await writeResult(procedure,result,options)
        process.exit(0)
    })

    program
    .command('update crud_class [filter...]')
    .alias('U')
    .description('Run update procedure for given class and output in selected format. Accepts zero to many filters as "key1=value1 key2=value2 ..." and one to many fields to update with values as "-u key1=new_value1 key2=new_value2 ..."')
    .option("-v, --validate",'validate only (don\'t run)')
    .option("-o, --output <value>",'save output to file. If -o nor -i are set, output is printed to STDOUT')
    .option("-f, --format <value>",'output format (json (default) or csv)')
    .option("-i, --output_individual_files_pattern <value>",'output one file for each retrieved element using this printf pattern to use with element id and, additional fields (with -F option)')
    .option("-b, --base_path <value>",'to use together with -i. Prepends this base path to the constructed file paths')
    .option("-F, --iter_field <value>",'Field (array type property) of elements to iterate over to generate separate output files (combined with -i)')
    .option("-u, --update <value...>",'equal separated key-value pairs to set on matched elements, i.e: "-u key1=new_value1 key2=new_value2')
    .option("-U, --update_from_file <value>",'update table from a csv or json file. Ignores [filter...], --update and --validate')
    .action(async (crud_class,filter,options) => {
        filter = parseKVPArray(filter)
        var test_result = true
        if(!CRUD.hasOwnProperty(crud_class)) {
            logger.error("Invalid crud class")
            process.exit(1)
        }
        const class_name = crud_class
        crud_class = CRUD.CRUD[class_name]
        const update_options = getOutputOptions(options)
        if(options.update_from_file) {
            const args = {class_name: class_name, output: (options.output) ? path.resolve(options.output) : undefined, output_format: options.format, options: update_options}
            if(options.format && options.format == "csv") {
                args.csvfile = path.resolve(options.update_from_file)
            } else {
                args.jsonfile = path.resolve(options.update_from_file)
            }
            try {
                var procedure = new procedures.UpdateFromFileProcedure(args)
                var result = await procedure.run()
            } catch (e) {
                logger.error(e)
                process.exit(1)
            }
        } else {
            if(!options.update) {
                logger.error("Missing --update option")
                process.exit(1)
            }
            const update = parseKVPArray(options.update)
            try {
                var procedure = new procedures.UpdateProcedure({class_name: class_name, filter:filter, output: (options.output) ? path.resolve(options.output) : undefined, output_format: options.format, options: update_options, update:update})
            } catch(e) {
                logger.error(e)
                process.exit(1)
            }
            if(options.validate) {
                process.exit(0)
            }
            try {
                var result = await procedure.run()
            } catch(e) {
                logger.error(e)
                process.exit(1)
            }
        }
        await writeResult(procedure,result,options)   
        process.exit(0)
    })

    program
    .command('delete crud_class [filter...]')
    .alias('D')
    .description('Run delete procedure for given class and output in selected format. Accepts zero to many filters as "key1=value1 key2=value2 ..."')
    .option("-v, --validate",'validate only (don\'t run)')
    .option("-o, --output <value>",'save output to file. If -o nor -i are set, output is printed to STDOUT')
    .option("-f, --format <value>",'output format (json (default) or csv)')
    .option("-i, --output_individual_files_pattern <value>",'output one file for each retrieved element using this printf pattern to use with element id and, additional fields (with -F option)')
    .option("-b, --base_path <value>",'to use together with -i. Prepends this base path to the constructed file paths')
    .option("-F, --iter_field <value>",'Field of elements to iterate over to generate separate output files (combined with -i)')
    .action(async (crud_class,filter,options) => {
        filter = parseKVPArray(filter)
        var test_result = true
        if(!CRUD.hasOwnProperty(crud_class)) {
            logger.error("Invalid crud class")
            process.exit(1)
        }
        const class_name = crud_class
        crud_class = CRUD.CRUD[class_name]
        const delete_options = getOutputOptions(options)
        try {
            var procedure = new procedures.DeleteProcedure({class_name: class_name, filter:filter, output: (options.output) ? path.resolve(options.output): undefined, output_format: options.format, options: delete_options})
        } catch(e) {
            logger.error(e)
            process.exit(1)
        }
        if(options.validate) {
            process.exit(0)
        }
        try {
            var result = await procedure.run()
        } catch(e) {
            logger.error(e)
            process.exit(1)
        }
        await writeResult(procedure,result,options)
        process.exit(0)
    })

    program
    .command('get accessor_class [filter...]')
    .alias('g')
    .description('Run data download procedure for given accessor class and output in selected format. Accepts zero to many filters as "key1=value1 key2=value2 ..."')
    .option("-v, --validate",'validate only (don\'t run)')
    .option("-o, --output <value>",'save output to file. If -o nor -i are set, output is printed to STDOUT')
    .option("-f, --format <value>",'output format (json (default) or csv)')
    .option("-i, --output_individual_files_pattern <value>",'output one file for each retrieved element using this printf pattern to use with element id and, additional fields (with -F option)')
    .option("-b, --base_path <value>",'to use together with -i. Prepends this base path to the constructed file paths')
    .option("-F, --iter_field <value>",'Field of elements to iterate over to generate separate output files (combined with -i)')
    .option("-u, --update","Update series in database from downloaded records")
    .action(async (accessor_class,filter,options) => {
        try {
            filter = parseKVPArray(filter)
        } catch (e) {
            logger.error(e)
            process.exit(1)
        }
        // console.debug({filter: filter, options: options})
        // var test_result = true
        // if(!Accessors.hasOwnProperty(accessor_class)) {
        //     logger.error("Invalid accessor class")
        //     process.exit(1)
        // }
        const class_name = accessor_class
        try {
            accessor = await Accessors.new(class_name)
        } catch(e) {
            console.error(e)
            process.exit(1)
        }
        const get_options = getOutputOptions(options)
        const output = (options.output) ? path.resolve(options.output) : undefined
        try {
            if(options.update) {
                // console.debug("Update from accessor")
                var procedure = new procedures.UpdateFromAccessorProcedure({accessor_id: class_name, filter:filter, output: output, output_format: options.format, options: get_options})
            } else {
                var procedure = new procedures.DownloadFromAccessorProcedure({accessor_id: class_name, filter:filter, output: output, output_format: options.format, options: get_options})
            }
        } catch(e) {
            logger.error(e)
            process.exit(1)
        }
        if(options.validate) {
            process.exit(0)
        }
        try {
            var result = await procedure.run()
        } catch(e) {
            logger.error(e)
            process.exit(1)
        }
        await writeResult(procedure,result,options)
        process.exit(0)
    })
    
    program
    .command("run-asoc <timestart> <timeend> [filter...]")
    .alias('a')
    .alias('asoc')
    .description('Run asociacion. Requires timestart and timeend and filters as "key1=value1 key2=value2 ..."')
    .option("-a, --agg_func", "aggregation function (mean,sum,min,max)")
    .option("-d, --dt", "time interval")
    .option("-t, --t_offset", "time offset")
    .option("-o, --output <value>",'save output to file')
    .option("-f, --format <value>",'output format (json (default) or csv)')
    .action(async (timestart,timeend,filter,options) => {
        timestart = DateFromDateOrInterval(timestart)
        timeend = DateFromDateOrInterval(timeend)
        try {
            filter = parseKVPArray(filter)
        } catch (e) {
            logger.error(e)
            process.exit(1)
        }
        filter.timestart = timestart
        filter.timeend = timeend
        const asoc_options = {
            agg_func: options.agg_func,
            dt: options.dt,
            t_offset: options.t_offset
        }
        if(filter.id) {
            try {
                var procedure = new procedures.RunAsociacionProcedure({
                    id: filter.id,
                    filter: filter,
                    options: asoc_options,
                    output: options.output,
                    output_format: options.output_format
                })
            } catch (e) {
                logger.error(e)
                process.exit(1)
            }
        } else {
            try {
                var procedure = new procedures.RunAsociacionesProcedure({
                    filter: filter,
                    options: asoc_options,
                    output: options.output,
                    output_format: options.output_format
                })
            } catch (e) {
                logger.error(e)
                process.exit(1)
            }
        }
        try {
            await procedure.run()
        } catch(e) {
            logger.error(e)
            process.exit(1)
        }
        if(options.output) {
            try {
                await procedure.writeResult()
            } catch(e) {
                logger.error(e)
                process.exit(1)
            }
        }
        process.exit(0)
    })


    program.parse(process.argv);
}

function printErrorSummmary(error_log,n) {
    logger.error("Run " + n + " sequence files of which " + error_log.length + " failed")
    for(var i in error_log) {
        var f = error_log[i]
        logger.error("<" + i + "> Sequence file " + f.filename)
        for(var j in f.errors) {
            var e = f.errors[j]
            if(e) {
                logger.error("  *" + j + "* " + e)
            } else {
                logger.info("  *" + j + "* OK")
            }
        }
    }
}

