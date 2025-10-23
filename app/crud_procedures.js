require('./setGlobal')
const config = global.config // require('config');
const CRUD = require('./CRUD')
const crud = CRUD.CRUD
const Accessors = require('./accessors')
const accessor_mapping = require('./accessor_mapping')
CRUD.accessor_feature_of_interest = accessor_mapping.accessor_feature_of_interest
CRUD.accessor_observed_property = accessor_mapping.accessor_observed_property
CRUD.accessor_unit_of_measurement = accessor_mapping.accessor_unit_of_measurement
CRUD.accessor_timeseries_observation = accessor_mapping.accessor_timeseries_observation
CRUD.accessor_time_value_pair = accessor_mapping.accessor_time_value_pair
const {program} = require('commander');
const fs = require('fs')
const {writeFile} = require('fs/promises')
const timeSteps = require('./timeSteps');
var sprintf = require('sprintf-js').sprintf, vsprintf = require('sprintf-js').vsprintf
var pexec = require('child-process-promise').exec;
const YAML = require('yaml');
const logger = require('./logger');
const { DateFromDateOrInterval } = require('./timeSteps');
const path = require('path');
const { exit } = require('process');
// const userAdmin = require('../../appController/app/userAdmin')
CRUD.user = require('a5base/userAdmin').User
const CSV = require('csv-string')
const {getDeepValue, delay} = require('./utils')
const { accessor_feature_of_interest } = require('./accessor_mapping')
const { updateFlowcatSeries } = require('./update_flowcat_series')
const { Client: ThreddsClient, tifDirToObservacionesRaster} = require('./accessors/thredds')
const internal = {}

/**
 * Class that represents a CRUD procedure. Abstract class to extend into specific procedure classes.
 */
internal.CrudProcedure = class  {
    /**
     * Instantiates a CRUD procedure
     * @param {Object} args
     * @param {string} args.procedureName
     * @param {Object} args.options
     * @param {Object} args.options.output_individual_files
     * @param {string} args.options.output_individual_files.pattern - string pattern with {{placeholders}} to substitute with values of properties of the result elements (deep properties may be accessed using point separators, i.e: "{{estacion.id}}")
     * @param {string} args.options.output_individual_files.base_path
     * @param {string[]} args.options.output_individual_files.fields - not used
     * @param {string} args.output
     * @param {string} [args.output_format=json]
     * @param {Object[]} tests
     * @param {string} tests[].testName
     * @param {Object} tests[].arguments
     * @param {string} sequence_file_location - path of the sequence yml file. If defined, set all input and output filenames relative to this.
     * @param {Client} client - connected database client
     */
    constructor() {
        // console.log(arguments)
        this.procedureClass = "CrudProcedure"
        this.files_base_dir = (arguments[2]) ? arguments[2] : "."
        this.client = (arguments[3]) ? arguments[3] : undefined
        // logger.info("files_base_dir: " + this.files_base_dir)
        if(arguments[1]) {
            if(!Array.isArray(arguments[1])) {
                throw("Bad property tests, must be an array")
            }
            this.tests = []
            for(var i in arguments[1]) {
                if(!arguments[1][i].testName) {
                    throw("Missing test.testName")
                }
                if(!internal.availableTests.hasOwnProperty(arguments[1][i].testName)) {
                    throw("Bad argument test.class: class not found")
                }
                this.tests.push(new internal.availableTests[arguments[1][i].testName]({...arguments[1][i].arguments,files_base_dir: this.files_base_dir}))
            }
        }
        this.output = (arguments[0] && arguments[0].output) ? path.resolve(this.files_base_dir,arguments[0].output) : (arguments[0] && arguments[0].options && arguments[0].options.output) ? path.resolve(this.files_base_dir,arguments[0].options.output) : undefined
        this.output_format = (arguments[0] && arguments[0].output_format) ? arguments[0].output_format : (arguments[0].options && arguments[0].options.output_format) ? arguments[0].options.output_format : "json"
        this.options = (arguments[0].options) ? arguments[0].options : {}
        if(this.output || (this.options && this.options.output_individual_files)) {
            this.write_result = true
        }
        if(this.options && this.options.output_individual_files) {
            this.options.output_individual_files.base_path = (this.options.output_individual_files.base_path) ? path.resolve(this.files_base_dir,this.options.output_individual_files.base_path) : path.resolve(this.files_base_dir)
        }
        if(this.options && this.options.raw_output_file) {
            this.options.raw_output_file = path.resolve(this.files_base_dir,this.options.raw_output_file)
        }
        // logger.info("options: " + JSON.stringify(this.options))
    }
    run() {
        this.result = [] // new internal.CrudProcedureResult(undefined,test)
        return this.result
    }
    async runTests() {
        try {
            if(this.prepare) {
                this.prepare()
            }
            this.result = await this.run()
            if(this.write_result) {
                await this.writeResult()
            }
        } catch(e) {
            this.test_result = false
            this.test_fail_reasons = [e.toString()]
            logger.error(`Test failed: procedure "${this.procedureClass}" execution returned error: ${e.toString()}`)
            return {
                success: this.test_result,
                reasons: this.test_fail_reasons
            }
        }
        var test_flag = true
        var fail_reasons = []
        for(var i in this.tests) {
            logger.info(`test ${i}: ${this.tests[i].testName}`)
            var test_result = this.tests[i].run(this.result)
            if(!test_result.value) {
                test_flag = false
                logger.error(`Test failed: procedure "${this.procedureClass}" result failed ${this.tests[i].testName} test. reason: ${test_result.reason}`)
                fail_reasons.push(test_result.reason)
            }
        }
        this.test_result = test_flag
        if(fail_reasons.length) {
            this.test_fail_reasons = fail_reasons
        }
        return {
            success: test_flag,
            reasons: this.test_fail_reasons
        }
    }
    async writeToOutput(data,format) {
        if(!this.output) {
            throw("output path not specified")
        }
        await this.writeFile(this.output,data,format,this.options.pretty)
    }
    async writeResult(output_format) {
        if(!this.result) {
            throw("result not found")
        }
        output_format = (output_format) ? output_format : this.output_format
        if(this.options && this.options.output_individual_files) {
            var base_path = (this.options.output_individual_files.base_path) ? this.options.output_individual_files.base_path : ""
            var pattern = (this.options.output_individual_files.pattern) ? this.options.output_individual_files.pattern : "{{class_name}}_{{id}}"
            pattern = (output_format && ["csv","csv_cat"].indexOf(output_format) >= 0) ? `${pattern}.csv` : (output_format && output_format == "raster") ? `${pattern}.tif` : `${pattern}.json`
            const results = (Array.isArray(this.result)) ? this.result : [this.result]
            for(var i in results) {
                var filename = pattern.toString()
                filename = filename.replace(new RegExp('{{class_name}}',"g"),this.class_name)
                filename = filename.replace(new RegExp('{{id}}',"g"),results[i].id)
                for(var key of Object.keys(results[i])) {
                    const string_value = (results[i][key]) ? (results[i][key] instanceof Date) ? results[i][key].toISOString() : results[i][key].toString() : "undefined"
                    filename = filename.replace(new RegExp('{{' + key + '}}',"g"),string_value)                    
                }
                filename = replacePlaceholders(filename,results[i])
                if(this.options.output_individual_files.iter_field) {
                    for(var j in results[i][this.options.output_individual_files.iter_field]) {
                        const item = results[i][this.options.output_individual_files.iter_field][j] 
                        if(j.toString() == "metadata") {
                            var filename_ = filename.toString()
                            filename_ = filename_.replace(/\{\{.+?\}\}/,"")
                            filename_ = `${filename_}.metadata`
                            var output = path.resolve(base_path,filename_)
                            await this.writeFile(output,item,"json",this.options.pretty) // output_format)
                            continue
                        }
                        // console.log(item instanceof CRUD.observacion)
                        var filename_ = filename.toString()
                        filename_ = replacePlaceholders(filename_,item)
                        var output = path.resolve(base_path,filename_)
                        await this.writeFile(output,item,output_format,this.options.pretty)
                    }
                } else {
                    var output = path.resolve(base_path,filename)
                    await this.writeFile(output,results[i],output_format,this.options.pretty)
                }
            }
        } else if(this.output) {
            await this.writeToOutput(this.result,output_format)
        } else {
            throw("missing output or options.output_individual_files")
        }
    }
    async writeFile(output,data,output_format,pretty=false) {
        output_format = (output_format) ? output_format : this.output_format
        output = (output) ? output : this.output
        data = (data) ? data : this.result
        if(output_format=="json") {
            if(pretty) {
                await writeFile(output,JSON.stringify(data,undefined,4))
            } else {
                await writeFile(output,JSON.stringify(data))
            }
        } else if(output_format.toLowerCase()=="jsonless") {
            if(typeof data.toJSONless === 'function') {
                var jsonless_result = data.toJSONless()
            } else if(Array.isArray(data)) {
                if (this.class && typeof this.class.toJSONless === 'function') {
                    var jsonless_result = this.class.toJSONless(data)
                } else {
                    var jsonless_result = []
                    data.forEach((item,i)=>{
                        // for each item, check if instance method exists
                        if(typeof item.toJSONless === 'function') {
                            jsonless_result.features.push(item.toJSONless())
                        } else {
                            throw("toJSONless method not found in item " + i)
                        }
                    })
                }
            } else {
                throw("toJSONless method not found in class " + this.class_name)
            }
            if(pretty) {
                await writeFile(output,JSON.stringify(jsonless_result,null,4))
            } else {
                await writeFile(output,JSON.stringify(jsonless_result))
            }        
        } else if(output_format.toLowerCase() == "geojson") {
            // check if instance method exists
            if(typeof data.toGeoJSON === 'function') {
                var geojson_result = data.toGeoJSON()
            } else if (Array.isArray(data)) {
                // check if static method exists
                if (this.class && typeof this.class.toGeoJSON === 'function') {
                    var geojson_result = this.class.toGeoJSON(data)
                } else {
                    var geojson_result = {
                        "type": "FeatureCollection",
                        "features": []
                    }
                    data.forEach((item,i)=>{
                        // for each item, check if instance method exists
                        if(typeof item.toGeoJSON === 'function') {
                            geojson_result.features.push(item.toGeoJSON())
                        } else {
                            throw("toGeoJSON method not found in item " + i)
                        }
                    })
                }
            } else {
                throw("toGeoJSON method not found in class " + this.class_name)
            }
            if(pretty) {
                await writeFile(output,JSON.stringify(geojson_result,null,4))
            } else {
                await writeFile(output,JSON.stringify(geojson_result))
            }
        } else if(output_format=="csv") {
            if(!data.toCSV) {
                if(Array.isArray(data)) {
                    if(!data.length) {
                        await writeFile(output,data)
                    } else if (data[0].constructor.toCSV) {
                        csv_string = data[0].constructor.toCSV(data,this.options)
                        await writeFile(output,csv_string)
                    } else if (data[0].toCSV) {
                        var csv_string = ""
                        if(data[0].constructor.getCSVHeader) {
                            csv_string = data[0].constructor.getCSVHeader(this.options.columns) + "\n"
                        } 
                        csv_string = csv_string + data.map(i=>i.toCSV({columns:this.options.columns})).join("\n") 
                        await writeFile(output,csv_string)
                    } else {
                        throw("toCSV() not defined for this class")
                    }
                } else {
                    throw("toCSV() not defined for this class")
                }
            } else {
                const toCSV_options = (this.class_name == "serie" && data.observaciones && data.observaciones.length) ? {print_observaciones:true} : {}
                if(this.options.header) {
                    toCSV_options.header = true
                }
                await writeFile(output,data.toCSV(toCSV_options))
            }
        } else if(output_format=="csv_cat") {
            if(!data.toCSVcat) {
                throw("toCSVcat method not present in object")    
            }
            await writeFile(output,data.toCSVcat())
            
        } else if(output_format=="raster") {
            if(Array.isArray(data)) {
                if(!data.length) {
                    throw("Data send to write is empty array")
                }
                data = data[0]
            }
            if(!data.toRaster) {
                throw("toRaster method not found for an instance of this class")
            }
            await data.toRaster(output)
        } else if(output_format=="gmd") {
            if(!Array.isArray(data)) {
                if(data.rows) {
                    data = data.rows
                } else {
                    data = [ data ]
                }
            }
            if(!data[0] instanceof CRUD.serie) {
                throw("toGmd method not present in object")    
            }
            logger.warn("Writing only first match")
            await writeFile(output,data[0].toGmd())
            
        } else {
            throw("Invalid format")
        }
        logger.info(`wrote ${output}`)
        await delay(500) 
    }
}

// internal.CrudProcedureResult = class {
//     constructor(content,test) {
//         this.content = content
//         this.test = test
//     }
//     runTest() {
//         return this.test()
//     }
// }

internal.CrudProcedureTest = class {
    constructor() {
        // logger.info("CrudProcedureTest arguments: " + JSON.stringify(arguments))
        this.testName = "CrudProcedureTest"
        this.files_base_dir = arguments[0].files_base_dir // (arguments[1]) ? arguments[1] : "."
        this.client = (arguments[1]) ? arguments[1] : undefined
    }
}

internal.TruthyTest = class extends internal.CrudProcedureTest {
    constructor(args) {
        super(args)
        this.testName = "TruthyTest"
    }
    run(result) {
        return {
            value: (result) ? true : false,
            reason: (result) ? undefined : "result is Falsy"
        }
    }
}

internal.NonEmptyArrayTest = class extends internal.CrudProcedureTest {
    // tests if array length is >=1 
    constructor(args) {
        super(args)
        this.testName = "NonEmptyArrayTest"
    }
    run(result) {
        var value = true
        var reason
        if(!result) {
            value = false
            reason = "Result is undefined"
        } else if(!Array.isArray(result)) {
            value = false
            reason = "Result must be an Array"
        } else if(!result.length) {
            value = false
            reason = "Result array length must be >=1"
        }
        return {
            value: value,
            reason: reason
        }
    }
}

internal.ArrayOfArraysTest = class extends internal.CrudProcedureTest {
    // tests if array length is >=1 
    constructor(args) {
        super(args)
        this.testName = "NonEmptyArrayOfArraysTest"
        this.property_name = args.property_name
        this.outside_array_length = args.outside_array_length
        this.inside_array_length = args.inside_array_length
    }
    run(result) {
        var value = true
        var reason
        if(!result) {
            value = false
            reason = "Result is undefined"
        } else if(!Array.isArray(result)) {
            if(this.property_name) {
                if(!result.hasOwnProperty(this.property_name)) {
                    value = false
                    reason = "property name " + this.property_name + " not found"
                } else if(!Array.isArray(result[this.property_name])) {
                    value = false
                    reason = "property " + this.property_name + " is not an Array"
                } else if (this.outside_array_length && result[this.property_name].length != this.outside_array_length) {
                    value = false
                    reason = "outside array length " + result[this.property_name].length + " is not equal to " + this.outside_array_length
                } else {
                    var index = -1
                    for(var item of result[this.property_name]) {
                        index = index + 1
                        if(!Array.isArray(item)) {
                            value = false
                            reason = "property " + this.property_name + " item " + index + " is not an Array"
                            break
                        } else if(this.inside_array_length && item.length != this.inside_array_length) {
                            value = false
                            reason = "inside array length " + item.length + " of item " + index + " is not equal to " + this.inside_array_length
                            break
                        }
                    }
                }
            } else {
                value = false
                reason = "Result must be an Array"
            }
        } else if(this.outside_array_length && result.length != this.outside_array_length) {
            value = false
            reason = "outside array length " + result.length + " is not equal to " + this.outside_array_length
        } else {
            var index = -1
            for(var item of result) {
                index = index + 1
                if(!Array.isArray(item)) {
                    value = false
                    reason = "item " + index + " is not an Array"
                    break
                } else if(this.inside_array_length && item.length != this.inside_array_length) {
                    value = false
                    reason = "inside array length " + item.length + " of item " + index + " is not equal to " + this.inside_array_length
                    break
                }
            }
        }
        return {
            value: value,
            reason: reason
        }
    }
}

internal.EmptyArrayTest = class extends internal.CrudProcedureTest {
    // tests if array length is ==0 
    constructor(args) {
        super(args)
        this.testName = "EmptyArrayTest"
        this.property_name = (args.property_name) ? args.property_name : undefined
        this.all = args.all ? args.all : false
    }
    run(result) {
        var value = true
        var reason
        if(!result) {
            value = false
            reason = "Result is undefined"
        } else if(!Array.isArray(result)) {
            if (this.property_name) {
                if(!result.hasOwnProperty(this.property_name)) {
                    value = false
                    reason = "Result must have property " + this.property_name
                } else if(!Array.isArray(result[this.property_name])) {
                    value = false
                    reason = "Result property " + this.property_name + " must be an array"
                } else if(result[this.property_name].length) {
                    value = false
                    reason = "Result array length must be == 0"
                }
            } else {
                value = false
                reason = "Result must be an Array"
            }
        } else if(this.property_name) {
            if(this.all) {
                for(var i in result) {
                    const item = result[i]
                    if(!item.hasOwnProperty(this.property_name)) {
                        value = false
                        reason = "Result item " + i + " property " + this.property_name + " missing"
                        break
                    }
                    if(!Array.isArray(item[this.property_name])) {
                        value = false
                        reason = "Result item " + i + " property " + this.property_name + " is not an array"
                        break
                    }
                    if(item[this.property_name].length) {
                        value = false
                        reason = "Result item " + i + " property " + this.property_name + " must be of length 0"
                        break
                    }
                }
            } else {
                if(!result[0]) {
                    value = false
                    reason = "Result item 0 undefined"
                } else if(!result[0].hasOwnProperty(this.property_name)) {
                    value = false
                    reason = "Result item 0 must have property " + this.property
                } else if(!Array.isArray(result[0][this.property_name])) {
                    value = false
                    reason = "Result item 0 property " + this.property_name + " must be an array"
                } else if(result[0][this.property_name].length) {
                    value = false
                    reason = "Result array length must be == 0"
                }
            }
        } else if(result.length > 0) {
            value = false
            reason = "Result array length must be == 0"
        }
        return {
            value: value,
            reason: reason
        }
    }
}

internal.ArrayLengthTest = class extends internal.CrudProcedureTest {
    /**
     *  tests for minimum length and/or maximum length
     * @param {Object} [args={min_length:1}]
     *  @param {Integer} args.min_length
     *  @param {Integer} args.max_length
     *  @param {string} args.property_name - read array in this property of first result element
    */
    constructor(args) {
        super(args)
        this.testName = "ArrayLengthTest"
        this.min_length = (args.min_length) ? args.min_length : 1
        this.max_length = args.max_length
        this.property_name = (args.property_name) ? args.property_name : undefined
        this.all = args.all ? args.all : false
    }
    run(result) {
        var value = true
        var reason
        if (this.property_name) {
            if (Array.isArray(result)) {
                if(!result.length) {
                    return {
                        value: false,
                        reason: "Result must be of length>0"
                    }
                } else {
                    if(this.all) {
                        for(var i in result) {
                            var property_value = getDeepValue(result[i],this.property_name)
                            var fail_reason = this.checkArrayLength(property_value,`Result index ${i} property ${this.property_name}`)
                            if(fail_reason) {
                                return {
                                    value: false,
                                    reason: fail_reason
                                }
                            }
                        }
                        return {
                            value: true
                        }
                    } else {
                        var property_value = getDeepValue(result[0],this.property_name)
                        if(property_value === undefined) {
                            return {
                                value: false,
                                reason: `Result index 0 property ${this.property_name} is undefined`
                            }  
                        }
                        var array = property_value 
                    }
                }
            } else {
               var array = getDeepValue(result,this.property_name)
            }
        } else {
            var array = result
        }
        var fail_reason = this.checkArrayLength(array)
        if(fail_reason) {
            return {
                value: false,
                reason: fail_reason
            }
        }
        return {
            value: true
        }
    }

    checkArrayLength(array,name="Result") {
        if(array === undefined) {
            return `${name} is undefined`
        }
        if(!Array.isArray(array)) {
            return `${name} must be an Array`
        }
        if(this.min_length!=null && this.max_length!=null && this.min_length==this.max_length) {
            if(array.length!=this.min_length) {
                return `${name} array length (${array.length}) must be equal to ${this.min_length}`
            }
        }
        if(array.length<this.min_length) {
            return `${name} array length (${array.length}) must be equal or greater than ${this.min_length}`
        }
        if(this.max_length && array.length > this.max_length) {
            return `${name} array length (${array.length}) must be equal or lower than ${this.max_length}`
        }
        return
    }
}

internal.ArrayIsOrderedTest = class extends internal.CrudProcedureTest {
    /**
     *  Tests if array is ordered by a property.
     * @param {Object} args
     * @param {string} args.property_name 
     * @param {string} args.order // asc (default) or desc 
     * @param {string} args.parent_property
     * @param {string} args.all
     *  */
    constructor(args) {
        super(args)
        this.testName = "ArrayIsOrderedTest"
        if(!args.property_name) {
            throw("Missing required parameter test.params.property_name")
        }
        this.property_name = args.property_name
        this.desc = (args.order && args.order.toLowerCase() == "desc") ? true : false
        this.parent_property = args.parent_property
        this.all = args.all
    }
    run(result) {
        var value = true
        var reason
        if(result instanceof Array) {
            if(this.parent_property) {
                for(var i in result) {
                    if(result[i] instanceof Object === false) {
                        value = false
                        reason = `Result item ${i} must be an Object"`
                        break
                    }
                    var parent_value = getDeepValue(result[i],this.parent_property)
                    var fail_reason  = this.checkArrayOrder(parent_value,`Result item ${i} property ${this.parent_property}`)
                    if(fail_reason) {
                        value = false
                        reason = fail_reason
                        break
                    }
                }
            } else {
                var fail_reason = this.checkArrayOrder(result,`Result`)
                if(fail_reason) {
                    value = false
                    reason = fail_reason
                }
            }
        } else if (result instanceof Object === false) {
            value = false
            reason = "Result must be an Object or array of Objects"
        } else if(this.parent_property) {
            var parent_value = getDeepValue(result,this.parent_property)
            var fail_reason = this.checkArrayOrder(result,`Result property ${this.parent_property}`)
            if(fail_reason) {
                value = false
                reason = fail_reason
            }
        } else {
            value = false
            reason = `Result must be an array or parent property must be indicated`
        }
        return {
            value: value,
            reason: reason
        }
    }

    checkArrayOrder(node,name="Result") {
        if(node === undefined) {
            return `${name} undefined`
        }
        if(!Array.isArray(node)) {
            return `${name} must be an array"`
        }
        for(var j=1;j<node.length;j++) {
            var property_value_precedent = getDeepValue(node[j-1],this.property_name)
            var property_value = getDeepValue(node[j],this.property_name)
            if(typeof property_value_precedent == "string" && typeof property_value == "string") {
                var compared = property_value_precedent.localeCompare(property_value)
            } else if (property_value_precedent instanceof Date && property_value instanceof Date) {
                var compared = (property_value_precedent.getTime() < property_value.getTime()) ? -1 : (property_value_precedent.getTime() == property_value.getTime()) ? 0 : 1
            } else {
                var compared = (property_value_precedent < property_value) ? -1 : (property_value_precedent == property_value) ? 0 : 1
            }
            if(this.desc) {
                if(compared == -1) {
                    return `${name} item ${j}: property ${this.property_name} is greater than precedent`
                }
            } else {
                if(compared == 1) {
                    return `${name} item ${j}: property ${this.property_name} is smaller than precedent`
                }
            }
        }
        return
    }    
}

internal.PropertyExistsTest = class extends internal.CrudProcedureTest {
    /**
     *  Tests if result is an Object or Array of Objects and has certain property. If result is an Array, all items are evaluated.
     * @param {Object} args
     * @param {string} args.property_name 
     *  */
    constructor(args) {
        super(args)
        this.testName = "PropertyExistsTest"
        if(!args.property_name) {
            throw("Missing required parameter test.params.property_name")
        }
        this.property_name = args.property_name
    }
    run(result) {
        var value = true
        var reason
        if(result instanceof Array) {
            for(var i in result) {
                if(result[i] instanceof Object === false) {
                    value = false
                    reason = `Result item ${i} must be an Object"`
                    break
                }
                var property_value = getDeepValue(result[i],this.property_name)
                if(property_value === undefined) {
                    value = false
                    reason = `Result item ${i} must have own property ${this.property_name}`
                    break
                }
            }
        } else if (result instanceof Object === false) {
            value = false
            reason = "Result must be an Object or array of Objects"
        } else {
            var property_value = getDeepValue(result,this.property_name)
            if(property_value === undefined) {
                value = false
                reason = `Result must have own property ${this.property_name}`
            }
        }
        return {
            value: value,
            reason: reason
        }
    }
}

internal.PropertyIsUndefinedTest = class extends internal.CrudProcedureTest {
    /**
     *  Tests if result is an Object or Array of Objects if a certain property is undefined. If result is an Array, all items are evaluated.
     * @param {Object} args
     * @param {string} args.property_name 
     *  */
    constructor(args) {
        super(args)
        this.testName = "PropertyIsUndefinedTest"
        if(!args.property_name) {
            throw("Missing required parameter test.params.property_name")
        }
        this.property_name = args.property_name
    }
    run(result) {
        var value = true
        var reason
        if(result instanceof Array) {
            for(var i in result) {
                if(result[i] instanceof Object === false) {
                    value = false
                    reason = `Result item ${i} must be an Object"`
                    break
                } else if(result[i].hasOwnProperty(this.property_name) && result[i][this.property_name] !== undefined) {
                    value = false
                    reason = `Result item ${i} property ${this.property_name} must be undefined`
                    break
                }
            }
        } else if (result instanceof Object === false) {
            value = false
            reason = "Result must be an Object or array of Objects"
        } else if(result.hasOwnProperty(this.property_name) && result[this.property_name] !== undefined) {
            value = false
            reason = `Result must property ${this.property_name} must be undefined`
        }
        return {
            value: value,
            reason: reason
        }
    }
}

internal.PropertyEqualsTest = class extends internal.CrudProcedureTest {
    /**
     *  tests if result is an Object or Array of Object and has a certain property with a defined value. If it is an Array, the item at the selected index is evaluated (defaults to 0) unless all=true.
     * @param {Object} args
     * @param {string} args.property_name
     * @param {any} args.property_value
     * @param {Integer} [args.index=0]
     * @param {Boolean} [args.all=false]
     * @param {Boolean} [args.or_greater]
     * @param {Boolean} [args.or_smaller]
     * @param {Boolean} [args.one_of]
     * @param {string} args.parent_property
    */
    constructor(args) {
        super(args)
        this.testName = "PropertyEqualsTest"
        if(!args.property_name) {
            throw("Missing required parameter test.params.property_name")
        }
        if(args.property_value === undefined) {
            throw("Missing required parameter test.params.property_value")
        }
        this.index = (args.index) ? args.index : 0
        this.property_name = args.property_name
        this.property_value = args.property_value
        this.parent_property = args.parent_property
        this.all = (args.all) ? args.all : false
        this.or_greater = (args.or_greater) ? args.or_greater : false
        this.or_smaller = (args.or_smaller) ? args.or_smaller : false
        if((this.or_greater || this.or_smaller) && parseFloat(this.property_value).toString() == "NaN") {
            // try parse as Date
            try {
                this.property_value = DateFromDateOrInterval(this.property_value)
            } catch (e) {
                throw("Bad argument property_value: can't be parsed as float or Date")
            }
        }
        if(!this.or_greater && !this.or_smaller && args.one_of) {
            this.one_of = true
            if(!Array.isArray(this.property_value)) {
                throw("Bad argument property_value: must be an array")
            }
        } else {
            this.one_of = false
        }
    }
    run(result) {
        var value = true
        var reason
        if(result instanceof Array) {
            if(!result.length) {
                value = false
                reason = `Result array must be of length >= 1`
            } else if(this.all) {
                loop1:for(var i in result) {
                    if(i.toString() == "metadata") {
                        continue
                    }
                    if(result[i] instanceof Object === false) {
                        value = false
                        reason = `Result item ${i} must be an Object`
                        break
                    } else {
                        if(this.parent_property) {
                            var fail_reason = this.checkParentProperty(result[i],`Result index ${i}`)
                            if(fail_reason) {
                                value = false
                                reason = fail_reason
                                break
                            }
                        } else {
                            var property_value = getDeepValue(result[i],this.property_name)
                            var fail_reason = this.isEqual(property_value,`Result item ${i}`)
                            if(fail_reason) {
                                value = false
                                reason = fail_reason
                                break
                            }
                        }
                    }
                }
            } else if(this.index + 1 > result.length) {
                value = false
                reason = `Result must be of length >= ${this.index + 1}`
            } else if(result[this.index] instanceof Object === false) {
                value = false
                reason = "Result must be an Object or array of Objects"
            } else {
                if(this.parent_property) {
                    var fail_reason = this.checkParentProperty(result[this.index],`Result index ${this.index}`)
                    if(fail_reason) {
                        value = false
                        reason = fail_reason
                    }
                } else {
                    var property_value = getDeepValue(result[this.index],this.property_name)
                    var fail_reason = this.isEqual(property_value,`Result item ${this.index}`)
                    if(fail_reason) {
                        value = false
                        reason = fail_reason
                    }
                }
            }                      
        } else if (result instanceof Object === false) {
            value = false
            reason = "Result must be an Object or array of Objects"
        // } else if(!result.hasOwnProperty(this.property_name)) {
        //     value = false
        //     reason = "Result object must have own property " + this.property_name
        } else {
            if(this.parent_property) {
                var fail_reason = this.checkParentProperty(result,`Result`,this.all,this.index)
                if(fail_reason) {
                    value = false
                    reason = fail_reason
                }
            } else {
                var property_value = getDeepValue(result,this.property_name)
                var fail_reason = this.isEqual(property_value,"Result object")
                if(fail_reason) {
                    value = false
                    reason = fail_reason
                }
            }
        }
        return {
            value: value,
            reason: reason
        }
    }

    isEqual(value,name="Result object") {
        var compare_value = this.property_value
        if(value === undefined) {
            return `${name} must have own property ${this.property_name}`
        }
        if(compare_value instanceof Date) {
            try {
                value = DateFromDateOrInterval(value)
            } catch (e) {
                return `${name} property ${this.property_name} must be parseable into Date type`
            }
            // converts date to epoch to compare
            compare_value = compare_value.getTime()
            value = value.getTime()
        }
        if(JSON.stringify(value) != JSON.stringify(compare_value)) {
            if(this.or_greater) {
                if(value < compare_value) {
                    return `${name} property ${this.property_name} must be equal or greater than ${compare_value.toString()}`
                }
            } else if(this.or_smaller) {
                if(value > compare_value) {
                    return `${name} property ${this.property_name} must be equal or smaller than ${compare_value.toString()}`
                }
            } else if (this.one_of) {
                // console.log("is " + value + " one of " + JSON.stringify(compare_value))
                if(compare_value.map(v=>JSON.stringify(v)).indexOf(JSON.stringify(value)) < 0) {
                    return `${name} ${this.property_name} must be one of ${compare_value.map(v=>JSON.stringify(v)).join(", ")}. Instead, ${value} was found`                      
                }
            } else {
                return `${name} property ${this.property_name} must equal ${compare_value.toString()}`
            }
        }
        return
    }

    checkParentProperty(node,name="Result object",all=true,index=0) {
        var parent_property_value = getDeepValue(node,this.parent_property)
        if(parent_property_value === undefined) {
            return `${name} property ${this.parent_property} not found`
        }
        if(parent_property_value instanceof Object === false) {
            return `${name} property ${this.parent_property} must be an Object or Array`
        }
        if(Array.isArray(parent_property_value)) {
            if(all) {
                for(var j in parent_property_value) {
                    var property_value = getDeepValue(parent_property_value[j],this.property_name)
                    var fail_reason = this.isEqual(property_value,`${name} property ${this.parent_property} item ${j}`)
                    if(fail_reason) {
                        return fail_reason
                    }            
                }
            } else if (parent_property_value.length < index + 1) {
                return `${name} property ${this.parent_property} item ${index} out of range`
            } else {
                var property_value = getDeepValue(parent_property_value[index],this.property_name)
                var fail_reason = this.isEqual(property_value,`${name} property ${this.parent_property} item ${index}`)
                if(fail_reason) {
                    return fail_reason
                }
            }
        } else {
            var property_value = getDeepValue(parent_property_value,this.property_name)
            var fail_reason = this.isEqual(property_value,`${name} property ${this.parent_property}`)
            if(fail_reason) {
                return fail_reason
            }  
        }
    }
}

internal.PropertyIsEqualOrGreaterThanTest = class extends internal.PropertyEqualsTest {
    /**
      *  tests if result is an Object or Array of Object and has a certain property with a defined value. If it is an Array, the item at the selected index is evaluated (defaults to 0) unless all=true.
      * @param {Object} args
      * @param {string} args.property_name
      * @param {any} args.property_value
      * @param {Integer} [args.index=0]
      * @param {Boolean} [args.all=false]
     */
     constructor(args) {
        args.or_greater = true
        args.or_smaller = false
        args.one_of = false
        console.log(JSON.stringify(args))
         super(args)
         this.testName = "PropertyIsEqualOrGreaterThanTest"
     }
 }
 

internal.PropertyIsEqualOrSmallerThanTest = class extends internal.PropertyEqualsTest {
     /**
       *  tests if result is an Object or Array of Object and has a certain property with a defined value. If it is an Array, the item at the selected index is evaluated (defaults to 0) unless all=true.
       * @param {Object} args
       * @param {string} args.property_name
       * @param {any} args.property_value
       * @param {Integer} [args.index=0]
       * @param {Boolean} [args.all=false]
      */
          constructor(args) {
            args.or_smaller = true
            args.or_greater = false
            args.one_of = false
            super(args)
            this.testName = "PropertyIsEqualOrSmallerThanTest"
        }
  }

internal.PropertyEqualsOneOfTest = class extends internal.PropertyEqualsTest {
    /**
      *  tests if result is an Object or Array of Object and has a certain property with a defined value. If it is an Array, the item at the selected index is evaluated (defaults to 0) unless all=true.
      * @param {Object} args
      * @param {string} args.property_name
      * @param {any} args.property_value
      * @param {Integer} [args.index=0]
      * @param {Boolean} [args.all=false]
     */
    constructor(args) {
        args.or_smaller = false
        args.or_greater = false
        args.one_of = true
        super(args)
        this.testName = "PropertyEqualsOneOfTest"
    }
}


internal.PropertyAggEqualsTest = class extends internal.CrudProcedureTest {
    /**
     *  tests aggregate value of a property of result elements against value or range
     * @param {Object} args
     * @param {string} args.property_name
     * @param {number} args.property_value
     * @param {number[]} args.property_range
     * @param {string} agg_function - mean (default), sum, min, max, first, last
    */
    constructor(args) {
        super(args)
        this.testName = "PropertyAggEqualsTest"
        if(!args.property_name) {
            throw("Missing required parameter property_name")
        }
        this.property_name = args.property_name
        this.agg_function = (args.agg_function) ? args.agg_function : "mean"
        const aggFunctions = {
            "mean": (values) => {
                var sum = values.reduce((agg,value) => agg + value, 0) 
                return sum / values.length
            },
            "sum": (values) => {
                return  values.reduce((agg,value) => agg + value, 0)
            },
            "min": (values) => {
                return values.reduce((agg,value) => (agg < value) ? agg  : value, values[0])
            },
            "max": (values) => {
                return values.reduce((agg,value) => (agg > value) ? agg  : value, values[0])
            },
            "first": (values) => values[0],
            "last": (values) => values[values.length-1]
        }
        if(aggFunctions.hasOwnProperty(this.agg_function) === false) {
            throw("invalid agg_function. Valid values: mean (default), sum, min, max, first, last")
        }
        this.aggFunction = aggFunctions[this.agg_function]
        if(args.property_value) {
            this.property_value = args.property_value
        } else {
            if(!args.property_range) {
                throw("Missing required parameter property_value or property_range")
            }
            if(args.property_range.length < 2) {
                throw("Invalid property_range: must be of length 2")
            }
            this.property_range = args.property_range
        }
    }
    run(result) {
        var value = true
        var reason
        if(result instanceof Array) {
            if(!result.length) {
                value = false
                reason = `Result array must be of length >= 1`
            } else {
                var values = []
                for(var i in result) {
                    if(i.toString() == "metadata") {
                        continue
                    }
                    if (result[i] instanceof Object === false) {
                        value = false
                        reason = `Result item ${i} must be an Object`
                        break
                    } else if(!result[i].hasOwnProperty(this.property_name)) {
                        value = false
                        reason = `Result item ${i} must have own property ${this.property_name}`
                        break
                    } else {
                        values.push(result[i][this.property_name])
                    }
                }
                var agg_value = this.aggFunction(values)
                if(this.property_value) {
                    if(agg_value != this.property_value) {
                        value = false
                        reason = `Result property ${this.property_name} aggregate ${this.agg_function} must equal ${this.property_value}. Instead, ${agg_value} was found`
                    }
                } else if(this.property_range) {
                    if(agg_value < this.property_range[0] || agg_value > this.property_range[1]) {
                        value = false
                        reason = `Result property ${this.property_name} aggregate ${this.agg_function} must fall within range ${this.property_range[0]} - ${this.property_range[1]}. Instead, ${agg_value} was found`
                    }
                }
            }
        } else {
            value = false
            reason = "Result must be an array of Objects"
        }
        return {
            value: value,
            reason: reason
        }
    }
}


internal.PropertyIsValidDateTest = class extends internal.CrudProcedureTest {
    /**
     *  tests if result is an Object or Array of Object and has a certain property with a defined value. If it is an Array, the item at the selected index is evaluated (defaults to 0) unless all=true.
     * @param {Object} args
     * @param {string} args.property_name
     * @param {Integer} [args.index=0]
     * @param {Boolean} [args.all=false]
    */
    constructor(args) {
        super(args)
        this.testName = "PropertyIsValidDateTest"
        if(!args.property_name) {
            throw("Missing required parameter test.params.property_name")
        }
        this.index = (args.index) ? args.index : 0
        this.property_name = args.property_name
        this.all = (args.all) ? args.all : false
    }
    run(result) {
        var value = true
        var reason
        if(result instanceof Array) {
            if(!result.length) {
                value = false
                reason = `Result array must be of length >= 1`
            } else if(this.all) {
                for(var i in result) {
                    if(i.toString() == "metadata") {
                        continue
                    }
                    if (result[i] instanceof Object === false) {
                        value = false
                        reason = `Result item ${i} must be an Object`
                        break
                    } else if(!result[i].hasOwnProperty(this.property_name)) {
                        value = false
                        reason = `Result item ${i} must have own property ${this.property_name}`
                        break
                    } else if(result[i][this.property_name] instanceof Date === false) {
                        value = false
                        reason = `Result item ${i} property ${this.property_name} must be a valid Date`
                        break
                    } else if(result[i][this.property_name].toString() == 'Invalid Date') {
                        value = false
                        reason = `Result item ${i} property ${this.property_name} must be a valid Date`
                        break
                    }
                }
            } else {
                if(this.index + 1 > result.length) {
                    value = false
                    reason = `Result must be of length >= ${this.index + 1}`
                } else if(result[this.index] instanceof Object === false) {
                    value = false
                    reason = "Result must be an Object or array of Objects"
                } else if(result[this.index][this.property_name] instanceof Date === false) {
                    value = false
                    reason = `Result item ${this.index} property ${this.property_name} must be a valid Date`
                }  else if(result[this.index][this.property_name].toString() == 'Invalid Date') {
                    value = false
                    reason = `Result item ${i} property ${this.property_name} must be a valid Date`
                }
            }
        } else if (result instanceof Object === false) {
            value = false
            reason = "Result must be an Object or array of Objects"
        } else if(!result.hasOwnProperty(this.property_name)) {
            value = false
            reason = "Result object must have own property " + this.property_name
        } else if(new Date(result[this.property_name]).toString() == 'Invalid Date') {
            value = false
            reason = `Result object property ${this.property_name} must be a valid Date`
        }
        return {
            value: value,
            reason: reason
        }
    }
}


internal.ResultIsInstanceOfTest  = class extends internal.CrudProcedureTest {
    /**
     *  Tests if result object is instance of named class (within CRUD). If result is an Array, it tests all its elements unless index is specified 
     * @param {Object} args
     * @param {string} args.class_name
     * @param {Integer} [args.index] 
    */
    constructor(args) {
        super(args)
        this.testName = "ResultIsInstanceOfTest"
        if(!args.class_name) {
            throw("Missing required parameter test.params.class_name")
        }
        if(!CRUD.hasOwnProperty(args.class_name)) {
            throw("Bad parameter test.params.class_name: " + args.class_name + " not found in CRUD")
        }
        this.class_name = args.class_name
        this.class = CRUD[args.class_name]
        this.index = args.index
    }
    run(result) {
        var value = true
        var reason
        if(this.class.prototype instanceof Array) {
            if(result instanceof this.class === false) {
                value = false
                reason = `Result must be an instance of class ${this.class_name}`
            }
        } else if(result instanceof Array) {
            if(typeof this.index !== 'undefined') {
                if(result[this.index] instanceof Object === false) {
                    value = false
                    reason = `Result element ${this.index} must be an Object`
                } else if(result[this.index] instanceof this.class === false) {
                    value = false
                    reason = `Result element ${this.index} must be an instance of class ${this.class_name}`
                }
            } else {
                // evaluate all elements in array
                for(var i in result) {
                    if(result[i] instanceof Object === false) {
                        value = false
                        reason = `Result element ${i} must be an Object`
                        break
                    } else if(result[i] instanceof this.class === false) {
                        value = false
                        reason = `Result element ${i} must be an instance of class ${this.class_name}`
                        break
                    } 
                }
            }
        } else if (result instanceof Object === false) {
            value = false
            reason = "Result must be an Object or array of Objects"
        } else if(result instanceof this.class === false) {
            value = false
            reason = `Result must be an instance of class ${this.class_name}`
        }
        return {
            value: value,
            reason: reason
        }
    }
}
internal.PropertyIsInstanceOfTest = class extends internal.CrudProcedureTest {
    /**
     * Tests if object property exists and is an instance of named class (within CRUD). If result is an Array, element at selected index is evaluated (defaults to 0) unless all=true.
     * @param {Object} args
     * @param {string} args.property_name
     * @param {string} args.class_name
     * @param {Boolean} [args.all=false]
     *  */ 
    constructor(args) {
        super(args)
        this.testName = "PropertyIsInstanceOfTest"
        if(!args.property_name) {
            throw("Missing required parameter test.params.property_name")
        }
        if(!args.class_name) {
            throw("Missing required parameter test.params.class_name")
        }
        if(!CRUD.hasOwnProperty(args.class_name)) {
            throw("Bad parameter test.params.class_name: " + args.class_name + " not found in CRUD")
        }
        this.index = (args.index) ? args.index : 0
        // if(!crud[args.class_name] instanceof Class) {
        //     throw("Bad parameter test.params.class_name: " + args.class_name + " is not a Class")
        // }
        this.property_name = args.property_name
        this.class = CRUD[args.class_name]
        this.class_name = args.class_name
        this.all = (args.all) ? args.all : false
    }
    testElement(result, value, reason) {
        if(result instanceof Object === false) {
            value = false
            reason = "Result must be an Object or array of Objects"
        } else {
            var property_value = getDeepValue(result,this.property_name)
            if(property_value == undefined) {
                value = false
                reason = "Result object must have own property " + this.property_name
            } else if(property_value instanceof this.class === false) {
                value = false
                reason = "Result object property " + this.property_name + " must be an instance of class " + this.class_name
            }
        }
        return [value, reason]
    }
    run(result) {
        let value = true, reason
        if(result instanceof Array) {
            if(this.all) {
                for(var i in result) {
                    [value, reason] = this.testElement(result[i] ,value, reason)
                    if(!value) {
                        break
                    }
                }
            } else if (result.length < this.index + 1) {
                value = false
                reason = `Result musy be of length >= ${this.index + 1}`
            } else {
                [value, reason] = this.testElement(result[this.index], value, reason)
            } 
        } else {
            [value, reason] = this.testElement(result, value, reason)
        }
        return {
            value: value,
            reason: reason
        }
    }
}

internal.OutputFileTest = class extends internal.CrudProcedureTest {
 /**
     * Tests if json/csv output file is valid and the content matches the procedure result
     * @param {Object} args
     * @param {string} args.class_name - the crud class of the output file contents
     * @param {string} args.result_property_name - validate the file contents against this property of the procedure result (instead of the whole)
     * @param {string} args.output - output file to test
     * @param {string} args.output_format - format of the file to test (json (default) or csv)
     * @param {string} args.property_name - validate this property from the file (instead of the whole)
     * @param {index} args.index - validate the file contents against the index'th element of result
     * @para {boolean} args.header - take first csv line as column names
     */ 
    constructor(args) {
        super(args)
        this.testName = "OutputFileTest"
        if(!args.class_name) {
            throw("Missing required parameter test.params.class_name")
        }
        if(!CRUD.hasOwnProperty(args.class_name)) {
            throw("Invalid class_name: not a crud class")
        }
        if(!args.output) {
            throw("Missing required parameter test.params.output")
        }
        this.output_format = (args.output_format) ? args.output_format : "json"
        this.property_name = args.property_name
        this.result_property_name = args.result_property_name
        this.result_deep_property = args.result_deep_property
        this.class_name = args.class_name
        this.class = CRUD[args.class_name]
        this.output = path.resolve(this.files_base_dir,args.output)
        // logger.info("files_base_dir: " + this.files_base_dir + ", output: " + this.output)
        this.header = args.header
        this.index = args.index
    }
    run(result) {
        let value = true, reason
        try {
            var parsed_content = internal.validateDataFile(this.class,this.output,this.property_name,this.output_format,this.header)
        } catch(e) {
            logger.error(e)
            value = false
            reason = `File is invalid: ${e.toString()}`
        }
        if(parsed_content) {
            if(this.result_deep_property) {
                var result_deep_value = getDeepValue(result,this.result_deep_property)
                const result_deep_value_string = JSON.stringify(result_deep_value)
                const parsed_content_string = JSON.stringify(parsed_content)
                if(result_deep_value_string !== parsed_content_string) {
                    value = false
                    if(!result_deep_value_string) {
                        reason = `Deep value of result property ${this.result_deep_property} stringification resulted in undefined`
                    } else {
                        const diff_pos = findFirstDiffPos(result_deep_value_string,parsed_content_string)
                        const context_string =  result_deep_value_string.slice(Math.max(0,diff_pos-20),Math.min(result_deep_value_string.length,diff_pos+20))
                        const context_string_parsed =  parsed_content_string.slice(Math.max(0,diff_pos-20),Math.min(parsed_content_string.length,diff_pos+20))
                        reason = `Parsed content differs from result property ${this.result_deep_property} at position ${diff_pos}:\n    ${context_string}\n    ${context_string_parsed}`
                    }
                }
            } else if(this.index != null) {
                if(!result[this.index]) {
                    value = false
                    reason = `Index ${this.index} of result not found`
                } else if(!Array.isArray(parsed_content)) {
                    // if parsed content is not an array, it tests the whole against result[index]
                    const result_string = JSON.stringify(result[this.index])
                    const parsed_content_string = JSON.stringify(parsed_content)
                    if(result_string !== parsed_content_string) {
                        value = false
                        const diff_pos = findFirstDiffPos(result_string,parsed_content_string)
                        const context_string =  result_string.slice(Math.max(0,diff_pos-20),Math.min(result_string.length,diff_pos+20))
                        const context_string_parsed =  parsed_content_string.slice(Math.max(0,diff_pos-20),Math.min(parsed_content_string.length,diff_pos+20))
                        reason = `Parsed content differs from result at index ${this.index}, position ${diff_pos}:\n    ${context_string}\n    ${context_string_parsed}`
                    }
                } else if (!parsed_content[this.index]) {
                    value = false
                    reason = `Index ${this.index} of parsed_content not found`
                } else {
                    if(this.result_property_name) {
                        if(!result.hasOwnProperty(this.result_property_name)) {
                            value = false
                            reason = `Result is missing property ${this.result_property_name}`
                        } else {
                            var [is_equal, pos_string] = isDiffString(JSON.stringify(result[this.result_property_name][this.index]),JSON.stringify(parsed_content[this.index]))
                            if(!is_equal) {
                                value = false
                                reason = `Parsed content at index ${this.index} differs from result at index ${this.index}:\n${pos_string}`
                            }
                        }    
                    } else {
                        var [is_equal, pos_string] = isDiffString(JSON.stringify(result[this.index]),JSON.stringify(parsed_content[this.index]))
                        if(!is_equal) {
                            value = false
                            reason = `Parsed content at index ${this.index} differs from result at index ${this.index}:\n${pos_string}`
                        }
                    }
                }
            } else if(this.result_property_name) {
                if(!result[0]) {
                    value = false
                    reason = `Result is missing element of index 0`
                }
                if(!result[0].hasOwnProperty(this.result_property_name)) {
                    value = false
                    reason = `Result[0] is missing property ${this.result_property_name}`
                } else if(result[0][this.result_property_name].toJSON && parsed_content.toJSON) {
                    var [is_equal, pos_string] = isDiffString(JSON.stringify(result[0][this.result_property_name].toJSON()),JSON.stringify(parsed_content.toJSON()))
                    if(!is_equal) {
                        value = false
                        reason = `File content differs from result[0][${this.result_property_name}]:\n${pos_string}`
                    }
                } else if(result[0][this.result_property_name].length != parsed_content.length) {
                    value = false
                    reason = `File content length differs from result[0][${this.result_property_name}]`
                } else {
                    for(var i in result[0][this.result_property_name]) {
                        var [is_equal, pos_string] = isDiffString(JSON.stringify(result[0][this.result_property_name][i]),JSON.stringify(parsed_content[i]))
                        if(!is_equal) {
                            value = false
                            reason = `File element ${i} differs from result[0][${this.result_property_name}][${i}]:\n${pos_string}`
                            break
                        }
                    }
                }
            } else if(result.toJSON && parsed_content.toJSON) {
                var [is_equal, pos_string] = isDiffString(JSON.stringify(result.toJSON()),JSON.stringify(parsed_content.toJSON()))
                if(!is_equal) {
                    value = false
                    reason = `File content differs from result:\n${pos_string}`
                }
            } else if(!Array.isArray(result)) {
                if(!Array.isArray(parsed_content)) {
                    var [is_equal, pos_string] = isDiffString(JSON.stringify(result), JSON.stringify(parsed_content))
                    if(!is_equal) {
                        value = false
                        reason = `File content differs from result:\n${pos_string}`
                    }
                } else {
                    var [is_equal, pos_string] = isDiffString(JSON.stringify(result), JSON.stringify(parsed_content[0]))
                    if(!is_equal) {
                        value = false
                        reason = `File element 0 differs from result`
                    }

                }
            } else if(result.length != parsed_content.length) {
                value = false
                reason = `File content length differs from output`
            } else {
                for(var i in result) {
                    var [is_equal, pos_string] = isDiffString(JSON.stringify(result[i]), JSON.stringify(parsed_content[i]))
                    if(!is_equal) {
                        value = false
                        reason = `File element ${i} differs from result element ${i}:\n${pos_string}`
                        break
                    }
                }
            }
        }
        return {
            value: value,
            reason: reason
        }
    }   
}

// PROCEDURES

internal.UpdateCubeFromSeriesProcedure = class extends internal.CrudProcedure {
    constructor () {
        super(...arguments)
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        this.procedureClass = "UpdateCubeFromSeriesProcedure"
        if(!arguments[0].series_id) {
            throw("Missing argument series_id")
        }
        if(!arguments[0].timestart) {
            throw("Missing argument timestart")
        }
        if(!arguments[0].timeend) {
            throw("Missing argument timeend")
        }
        this.series_id = parseInt(arguments[0].series_id)
        this.timestart = arguments[0].timestart
        this.timeend = arguments[0].timeend
        this.forecast_date = (arguments[0].forecast_date) ? new Date(forecast_date) : undefined
        this.is_public = arguments[0].public
        this.fuentes_id = (arguments[0].fuentes_id) ? parseInt(arguments[0].fuentes_id) : undefined
    }
    async run() {
        this.result = await crud.updateCubeFromSeries(this.series_id,this.timestart,this.timeend,this.forecast_date,this.is_public,this.fuentes_id,this.client)
        return this.result
    }
}

internal.UpdateSerieRastFromCubeProcedure = class extends internal.CrudProcedure {
    constructor () {
        super(...arguments)
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        this.procedureClass = "UpdateSerieRastFromCubeProcedure"
        if(!arguments[0].filter) {
            throw("missing argument filter")
        }
        if(!arguments[0].filter.fuentes_id) {
            throw("Missing argument fuentes_id")
        }
        if(!arguments[0].filter.series_id) {
            throw("Missing argument series_id")
        }
        if(!arguments[0].filter.timestart) {
            throw("Missing argument timestart")
        }
        if(!arguments[0].filter.timeend) {
            throw("Missing argument timeend")
        }
        this.series_id = parseInt(arguments[0].filter.series_id)
        this.timestart = DateFromDateOrInterval(arguments[0].filter.timestart)
        this.timeend = DateFromDateOrInterval(arguments[0].filter.timeend)
        this.forecast_date = (arguments[0].filter.forecast_date) ? DateFromDateOrInterval(arguments[0].filter.forecast_date) : undefined
        this.is_public = arguments[0].filter.public
        this.fuentes_id = parseInt(arguments[0].filter.fuentes_id)
        this.t_offset = (this.options.offset) ? timeSteps.createInterval(this.options.t_offset) : undefined
        this.hour = this.options.hour
    }
    async run() {
        this.result = await crud.upsertRastFromCube(this.fuentes_id,this.timestart,this.timeend,this.forecast_date,this.is_public,this.series_id,this.t_offset,this.hour)
        return this.result
    }
}

internal.GetPpCdpBatchProcedure = class extends internal.CrudProcedure {
    constructor () {
        super(...arguments)
        this.procedureClass = "GetPpCdpBatchProcedure"
        if(!arguments[0]) {
            arguments[0] = {}
        }
        this.timestart = arguments[0].timestart
        this.timeend = arguments[0].timeend
        this.filter = arguments[0].filter
        // this.options = arguments[0].options
        this.upsert =  arguments[0].upsert
    }
    async run() {
        this.result = await crud.get_pp_cdp_batch(this.timestart,this.timeend,this.filter,this.options,this.upsert,this.client)
        return this.result
    }
}

internal.GetPpCdpDiario = class extends internal.CrudProcedure {
    constructor(){
        super(...arguments)
        this.procedureClass = "GetPpCdpDiario"
        if(!arguments[0]) {
            arguments[0] = {}
        }
        if(!arguments[0].date) {
            throw("Missing date")
        }
        this.date = timeSteps.DateFromDateOrInterval(arguments[0].date)
        this.filter = arguments[0].filter
        this.options = arguments[0].options
        this.upsert =  arguments[0].upsert
    }
    async run() {
        this.result = await crud.get_pp_cdp_diario(this.date,this.filter,this.options,this.upsert)
        return this.result
    }
   
}

internal.Campo2RastProcedure = class extends internal.CrudProcedure {
    procedureClass = "Campo2RastProcedure"
    timestart
    timeend
    var_id
    filter
    options
    series_id
    area_id
    dt
    t_offset
    constructor() {
        super(...arguments)
        this.procedureClass = "Campo2RastProcedure"
        if(!arguments[0].timestart) {
            throw("Missing timestart")
        }
        this.timestart = timeSteps.DateFromDateOrInterval(arguments[0].timestart)
        if(!arguments[0].timeend) {
            throw("Missing timeend")
        }
        this.timeend = timeSteps.DateFromDateOrInterval(arguments[0].timeend)
        if(!arguments[0].var_id) {
            throw("Missing var_id")
        }
        this.var_id = parseInt(arguments[0].var_id)
        if(!arguments[0].series_id) {
            throw("Missing series_id")
        }
        this.filter = arguments[0].filter
        this.options = arguments[0].options
        this.series_id = (arguments[0].series_id) ? parseInt(arguments[0].series_id) : undefined
        this.area_id = (arguments[0].area_id) ? arguments[0].area_id : undefined
        this.dt = (arguments[0].dt) ? new timeSteps.Interval(arguments[0].dt) : undefined
        this.t_offset = (arguments[0].t_offset) ? new timeSteps.Interval(arguments[0].t_offset) : undefined
    }


    async run () {
        this.result = await crud.seriescampo2rast(
            this.timestart,
            this.timeend,
            this.var_id,
            this.filter,
            this.options,
            this.series_id,
            this.area_id,
            this.dt,
            this.t_offset
        )
        return this.result
    }
}

internal.TestAccessorProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "TestAccessorProcedure"
        this.accessor_id = arguments[0].accessor_id
        if(!this.accesor_id) {
            throw("Missing accessor_id")
        }
    }
    async run() {
        var accessor = await Accessors.new(this.accessor_id)
        this.result = await accessor.engine.test()
        console.log("Accessor test result: " + this.result)
        return this.result
    }
}

internal.UpdateFromAccessorProcedure = class extends internal.CrudProcedure {
    /**
     * Update timeseries procedure from an accessor source
     * @param {Object} arguments - arguments
     * @param {string} arguments.accessor_id - identifier of the accessor (name field)
     * @param {Object} arguments.filter - series and observation filter
     * @param {Date|string} arguments.filter.timestart
     * @param {Date|string} arguments.filter.timeend
     * @param {Date|string} arguments.filter.forecast_date
     * @param {Object} arguments.options
     * @param {boolean} arguments.options.no_update_date_range - Don't refresh series date range materialized view
     * @returns {internal.UpdateFromAccessorProcedure} UpdateFromAccessorProcedure
     */
    constructor() {
        super(...arguments)
        this.procedureClass = "UpdateFromAccessorProcedure"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        if(!arguments[0].accessor_id) {
            throw("Missing accessor_id")
        }
        this.accessor_id = arguments[0].accessor_id
        this.filter = arguments[0].filter
        if(this.filter && this.filter.timestart) {
            this.filter.timestart = timeSteps.DateFromDateOrInterval(this.filter.timestart)
        }
        if(this.filter && this.filter.timeend) {
            this.filter.timeend = timeSteps.DateFromDateOrInterval(this.filter.timeend)
        }
        if(this.filter && this.filter.forecast_date) {
            this.filter.forecast_date = timeSteps.DateFromDateOrInterval(this.filter.forecast_date)
        }
        // this.options = arguments[0].options
        // this.output = arguments[0].output
    }
    async run() {
        var accessor = await Accessors.new(this.accessor_id)
        this.result = await accessor.updateSeries(this.filter,this.options)
        return this.result
    }
}

internal.DeleteFromAccessorProcedure = class extends internal.CrudProcedure {
    /**
     * Update timeseries procedure from an accessor source
     * @param {Object} arguments - arguments
     * @param {string} arguments.accessor_id - identifier of the accessor (name field)
     * @param {Object} arguments.filter - series and observation filter
     * @param {Date|string} arguments.filter.timestart
     * @param {Date|string} arguments.filter.timeend
     * @param {Date|string} arguments.filter.forecast_date
     * @param {Object} arguments.options
     * @returns {internal.DeleteFromAccessorProcedure} DeleteFromAccessorProcedure
     */
    constructor() {
        super(...arguments)
        this.procedureClass = "DeleteFromAccessorProcedure"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        if(!arguments[0].accessor_id) {
            throw("Missing accessor_id")
        }
        this.accessor_id = arguments[0].accessor_id
        this.filter = arguments[0].filter
        if(this.filter && this.filter.timestart) {
            this.filter.timestart = timeSteps.DateFromDateOrInterval(this.filter.timestart)
        }
        if(this.filter && this.filter.timeend) {
            this.filter.timeend = timeSteps.DateFromDateOrInterval(this.filter.timeend)
        }
        if(this.filter && this.filter.forecast_date) {
            this.filter.forecast_date = timeSteps.DateFromDateOrInterval(this.filter.forecast_date)
        }
        // this.options = arguments[0].options
        // this.output = arguments[0].output
    }
    async run() {
        var accessor = await Accessors.new(this.accessor_id)
        this.result = await accessor.deleteSeries(this.filter,this.options)
        return this.result
    }
}


internal.DeleteMetadataFromAccessorProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "DeleteMetadataFromAccessorProcedure"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        if(!arguments[0].accessor_id) {
            throw("Missing accessor_id")
        }
        this.accessor_id = arguments[0].accessor_id
        this.filter = arguments[0].filter
    }
    async run() {
        var accessor = await Accessors.new(this.accessor_id)
        this.result = await accessor.deleteMetadata(this.filter,this.options)
                // if(this.output) {
                //     fs.writeFileSync(this.output,JSON.stringify(series,undefined,2))
                // }
        return this.result
    }
}

internal.DeleteSitesFromAccessorProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "DeleteSitesFromAccessorProcedure"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        if(!arguments[0].accessor_id) {
            throw("Missing accessor_id")
        }
        this.accessor_id = arguments[0].accessor_id
        this.filter = arguments[0].filter
    }
    async run() {
        var accessor = await Accessors.new(this.accessor_id)
        this.result = await accessor.deleteSites(this.filter,this.options)
                // if(this.output) {
                //     fs.writeFileSync(this.output,JSON.stringify(series,undefined,2))
                // }
        return this.result
    }
}


internal.UpdateMetadataFromAccessorProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "UpdateMetadataFromAccessorProcedure"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        if(!arguments[0].accessor_id) {
            throw("Missing accessor_id")
        }
        this.accessor_id = arguments[0].accessor_id
        this.filter = arguments[0].filter
    }
    async run() {
        var accessor = await Accessors.new(this.accessor_id)
        this.result = await accessor.updateMetadata(this.filter,this.options)
        return this.result
    }
}

internal.GetMetadataFromAccessorProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "GetMetadataFromAccessorProcedure"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        if(!arguments[0].accessor_id) {
            throw("Missing accessor_id")
        }
        this.accessor_id = arguments[0].accessor_id
        this.filter = arguments[0].filter
        if(this.filter && this.filter.timestart) {
            this.filter.timestart = timeSteps.DateFromDateOrInterval(this.filter.timestart)
        }
        if(this.filter && this.filter.timeend) {
            this.filter.timeend = timeSteps.DateFromDateOrInterval(this.filter.timeend)
        }
        if(this.filter && this.filter.forecast_date) {
            this.filter.forecast_date = timeSteps.DateFromDateOrInterval(this.filter.forecast_date)
        }
        // this.options = arguments[0].options
        // this.output = (arguments[0].output) ? arguments[0].output : undefined
    }
    async run() {
        var accessor = await Accessors.new(this.accessor_id)
        this.result = await accessor.getMetadata(this.filter,this.options)
        return this.result
    }
}

internal.GetSitesFromAccessorProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "GetSitesFromAccessorProcedure"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        if(!arguments[0].accessor_id) {
            throw("Missing accessor_id")
        }
        this.accessor_id = arguments[0].accessor_id
        this.filter = arguments[0].filter
        this.class = CRUD.estacion
        this.class_name = "estacion"
    }
    async run() {
        var accessor = await Accessors.new(this.accessor_id)
        if(this.options.update) {
            this.result = await accessor.updateSites(this.filter, this.options)
        } else {
            this.result = await accessor.getSites(this.filter,this.options)
        }
        return this.result
    }
}

internal.MapSitesFromAccessorProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "MapSitesFromAccessorProcedure"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        if(!arguments[0].accessor_id) {
            throw("Missing accessor_id")
        }
        this.accessor_id = arguments[0].accessor_id
        this.filter = arguments[0].filter ?? {} 
    }
    async run() {
        const read_filter = {...this.filter}
        read_filter.accessor_id = this.filter.accessor_id
        const foi = await accessor_feature_of_interest.read(read_filter)
        const estaciones = []
        for(var feature of foi) {
            estaciones.push(await feature.mapToEstacion(feature.estacion_id))
        }
        this.result = estaciones
        return this.result
    }
}

internal.ReadVariablesFromAccessorProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "ReadVariablesFromAccessorProcedure"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        if(!arguments[0].accessor_id) {
            throw("Missing accessor_id")
        }
        this.accessor_id = arguments[0].accessor_id
        this.filter = arguments[0].filter ?? {} 
        if(arguments[0].options && arguments[0].options.map) {
            this.options.map = true
        }
    }
    async run() {
        const read_filter = {...this.filter}
        read_filter.accessor_id = this.filter.accessor_id
        const variables = await accessor_mapping.accessor_timeseries_observation.getDistinctVariables(read_filter,this.options)
        this.result = variables
        return this.result
    }
}


internal.TestAccessorProcedure  = class  extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "TestAccessorProcedure"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        if(!arguments[0].accessor_id) {
            throw("Missing accessor_id")
        }
        this.accessor_id = arguments[0].accessor_id
    }
    async run() {
        const accessor = await Accessors.new(this.accessor_id)
        this.result = await accessor.test()
        console.log("Accesor test result:" + this.result.toString())
        return this.result
    }
}

internal.DownloadFromAccessorProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "DownloadFromAccessorProcedure"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        if(!arguments[0].accessor_id) {
            throw("Missing accessor_id")
        }
        if(!this.output) {
            if(!this.options || !this.options.output_individual_files) {
                throw("Missing output or options.output_individual_files")
            }
        }
        this.accessor_id = arguments[0].accessor_id
        this.filter = arguments[0].filter
        // this.options = arguments[0].options
        // this.output = arguments[0].output
        // this.file_prefix = (args.file_prefix) ? args.file_prefix : ""
    }
    async run() {
        const accessor = await Accessors.new(this.accessor_id)
        const series = await accessor.getSeries(this.filter,this.options)
        if(!series.length) {
            logger.warn("no series found")
            this.result = []
            return []
        }
        this.result = series
            // if(this.filter.tipo=="raster") {
            //     this.writeResult("raster")
            // } else {
        return this.result
            // }
            // for(var i in series) {
            //     if(!series[i].observaciones.length) {
            //         console.warn("No observaciones found for series " + series.id)
            //         continue
            //     }
            //     if(series[i].tipo == "raster") {
            //         for(var j in series[i].observaciones) {
            //             const obs = series[i].observaciones[j]
            //             const filename = (obs.forecast_date) ? `${this.output}.${series[i].id}.${obs.forecast_date}.${obs.timestart}` : `${this.output}.${series[i].id}.${obs.timestart}`
            //             fs.writeFileSync(filename,new Buffer.from(obs.valor))
            //         }
            //     } else {
            //         this.writeResult()
            //         // const filename = `${this.output}.${series[i].id}`
            //         // fs.writeFileSync(filename,JSON.stringify(series[i]))
            //     }
            // }
    }
}

internal.ReadFromAccessorProcedure = internal.DownloadFromAccessorProcedure

internal.GetPronosticoFromAccessorProcedure =  class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "GetPronosticoFromAccessorProcedure"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        if(!arguments[0].accessor_id) {
            throw("Missing accessor_id")
        }
        this.accessor_id = arguments[0].accessor_id
        this.filter = arguments[0].filter
        // this.options = arguments[0].options
    }
    async run() {
        var accessor = await Accessors.new(this.accessor_id)
        this.result = await accessor.getPronostico(this.filter,this.options)
        return this.result
    }
}

internal.UpdatePronosticoFromAccessorProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "UpdatePronosticoFromAccessorProcedure"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        if(!arguments[0].accessor_id) {
            throw("Missing accessor_id")
        }
        this.accessor_id = arguments[0].accessor_id
        this.filter = arguments[0].filter
        // this.options = arguments[0].options
    }
    async run() {
        var accessor = await Accessors.new(this.accessor_id)
        this.result = await accessor.updatePronostico(this.filter,this.options)
        return this.result
    }
}

internal.MapAccessorTableFromCSVProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "MapAccessorTableFromCSVProcedure"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        if(!arguments[0].accessor_id) {
            throw("Missing accessor_id")
        }
        if(!arguments[0].class_name) {
            throw("Missing class_name")
        }
        if(!arguments[0].csv_file) {
            throw("Missing csv_file")
        }
        this.csv_file = path.resolve(this.files_base_dir, arguments[0].csv_file)
        if(!CRUD.hasOwnProperty(arguments[0].class_name)) {
            throw("Invalid class_name: " + arguments[0].class_name + " not present in CRUD")
        }
        this.class_name = arguments[0].class_name
        this.class = CRUD[this.class_name]
        if(!this.class.hasOwnProperty("updateFromCSV")) {
            throw("Invalid class: " + this.class_name + ". updateFromCSV not present in class")
        }
        this.accessor_id = arguments[0].accessor_id
        // this.filter = arguments[0].filter
        // this.options = arguments[0].options
    }
    async run() {
        // var accessor = await Accessors.new(this.accessor_id)
        const result = await this.class.updateFromCSV(this.accessor_id,this.csv_file)
        this.result = result
        return this.result
    }
}

internal.ComputeQuantilesProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "ComputeQuantilesProcedure"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        this.filter = arguments[0].filter // to select from corridas (cor_id, cal_id, forecast_date), series (tipo, series_id, var_id, estacion_id), pronosticos (timestart, timeend, qualifier)
        if(!this.filter) {
            throw("Missing filter")
        }
        this.quantiles = arguments[0].quantiles
        this.labels = arguments[0].labels
        this.create = arguments[0].create
    }
    async run() {
        const corridas = await CRUD.corrida.read(this.filter,{includeProno:false})
        for(var corrida of corridas) {
            await corrida.getQuantileSeries({
                timestart: this.filter.timestart,
                timeend: this.filter.timeend
            },
            this.quantiles,
            this.labels,
            this.create,
            this.client)
        }
        this.result = corridas
        return this.result
    }
}

internal.UpdateSeriesPronoDateRangeProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "UpdateSeriesPronoDateRangeProcedure"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        this.filter = arguments[0].filter ?? {}
    }
    async run() {
        if(this.options.group_by_qualifier) {
            await crud.updateSeriesPronoDateRangeByQualifier(this.filter,this.options)
        } else {
            await crud.updateSeriesPronoDateRange(this.filter,this.options)
        }
        return
    }
}

internal.GetAggregatePronosticosProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "GetAggregatePronosticosProcedure"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        this.filter = arguments[0].filter
        if(!this.filter) {
            throw new Error("Missing filter")
        }
        this.time_step = arguments[0].time_step // timeSteps.createInterval(arguments[0].time_step)
        if(!this.time_step) {
            throw new Error("missing time_step")
        }
        this.dest_tipo = arguments[0].dest_tipo
        if(!this.dest_tipo)  {
            throw new Error("missing dest_tipo")
        }
        this.tipo = arguments[0].tipo ?? this.filter.tipo
        this.source_series_id = arguments[0].source_series_id
        this.dest_series_id = arguments[0].dest_series_id
        this.estacion_id = arguments[0].estacion_id ?? this.filter.estacion_id
        this.dest_fuentes_id = arguments[0].dest_fuentes_id
        this.source_var_id = arguments[0].source_var_id ?? this.filter.var_id
        this.dest_var_id = arguments[0].dest_var_id
        this.agg_function = arguments[0].agg_function
        this.precision = arguments[0].precision
        this.time_offset = arguments[0].time_offset
        this.date_offset = arguments[0].date_offset
        this.utc = arguments[0].utc
        this.create = arguments[0].create
        this.group_by_qualifier = arguments[0].group_by_qualifier
        console.log({filter:this.filter,tipo:this.tipo})
    }
    async run() {
        const corridas = await CRUD.corrida.read(this.filter,{includeProno:false,group_by_qualifier:this.group_by_qualifier})
        const results = []
        for(var corrida of corridas) {
            console.log("got corrida with " + corrida.series.length + " series")
            if(this.time_step == "month") { //if(timeSteps.advanceTimeStep(new Date(2000,0,1),this.time_step).toISOString() == timeSteps.advanceTimeStep(new Date(2000,0,1),timeSteps.createInterval({"month":1})).toISOString()) {
                console.log("monthly")
                var series_mensuales = await corrida.getMonthlyMean(
                    {
                        estacion_id: this.estacion_id,
                        source_tipo: this.source_tipo,
                        source_series_id: this.source_series_id,
                        dest_series_id: this.dest_series_id,
                        source_var_id: this.source_var_id,
                        dest_var_id: this.dest_var_id,
                        qualifier: this.filter.qualifier,
                        timestart: this.filter.timestart,
                        timeend: this.filter.timeend,
                        dest_fuentes_id: this.dest_fuentes_id
                    },
                    this.date_offset,
                    this.create,
                    this.client
                )
            } else {
                console.log("other interval")
                var series_mensuales = await corrida.getAggregateSeries(this.tipo,this.source_series_id,this.dest_series_id,this.estacion_id,this.dest_fuentes_id,this.source_var_id,this.dest_var_id,this.dest_tipo,this.filter.timestart,this.filter.timeend,this.time_step,this.agg_function,this.precision,this.time_offset,this.utc,this.filter.proc_id,this.create,this.client)

            }
            results.push({
                id: corrida.id,
                forecast_date: corrida.forecast_date,
                series: series_mensuales
            })         
        }
        this.result = results
        return this.result
    }
}

internal.DeleteCorridas = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "DeleteCorridas"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        if(!arguments[0].filter) {
            throw("Missing filter")
        }
        this.filter = arguments[0].filter
        for(let v of ["date","forecast_date","forecast_timestart","forecast_timeend","timestart","timeend"]) {
            if(this.filter[v]) {
                this.filter[v] = timeSteps.DateFromDateOrInterval(this.filter[v])
            }
        }
    }
    async run() {
        this.result = await crud.deleteCorridas(this.filter)
        return this.result
    }
}

internal.DeleteObservacionesProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "DeleteObservacionesProcedure"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        if(!arguments[0].tipo) {
            throw("Missing tipo")
        }
        if(!arguments[0].filter) {
            throw("Missing filter")
        }
        this.tipo = arguments[0].tipo
        this.filter = arguments[0].filter
        this.options = arguments[0].options
        if(this.filter.timestart) {
            this.filter.timestart = timeSteps.DateFromDateOrInterval(this.filter.timestart)
        }
        if(this.filter.timeend) {
            this.filter.timeend = timeSteps.DateFromDateOrInterval(this.filter.timeend)
        }
        if(this.filter.timeupdate) {
            this.filter.timeupdate = timeSteps.DateFromDateOrInterval(this.filter.timeupdate)
        }
    }
    async run() {
        const series_filter = {...this.filter}
        delete series_filter.timestart
        delete series_filter.timeend
        delete series_filter.id
        var series = await crud.getSeries(this.tipo,series_filter,{"no_metadata":true},this.client)
        this.result = []
        for(var i in series) {
            logger.info("serie id:" + series[i].id)
            const filter = {
                series_id: series[i].id,
                timestart: this.filter.timestart,
                timeend: this.filter.timeend,
                timeupdate: this.filter.timeupdate,
                id: this.filter.id,
                valor: this.filter.valor,
                unit_id: this.filter.unit_id
            }
            if(this.options && this.options.no_send_data) {
                var count = await crud.deleteObservaciones(this.tipo,filter,{"no_send_data":true},this.client)
                logger.info("deleted " + count + " observaciones")
                this.result.push(count)
            } else {
                var deleted = await crud.deleteObservaciones(this.tipo,filter,this.client)
                logger.info("deleted " + deleted.length + " observaciones")
                this.result.push(...deleted)
            }
        }
        return this.result
    }
}

internal.DeleteObservacionesCuboProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "DeleteObservacionesCuboProcedure"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        // if(!args.id) {
        //     throw("Missing id (fuentes_id)")
        // }
        if(!arguments[0].filter) {
            throw("Missing filter")
        }
        // this.id = args.id
        this.filter = arguments[0].filter
        // this.options = arguments[0].options
        if(this.filter.timestart) {
            this.filter.timestart = timeSteps.DateFromDateOrInterval(this.filter.timestart)
        }
        if(this.filter.timeend) {
            this.filter.timeend = timeSteps.DateFromDateOrInterval(this.filter.timeend)
        }
        if(this.filter.forecast_date) {
            this.filter.forecast_date = timeSteps.DateFromDateOrInterval(this.filter.forecast_date)
        }
    }
    async run() {
        var series = await crud.getCubeSeries(this.filter.fuentes_id,this.filter.tipo,this.filter.proc_id,this.filter.unit_id,this.filter.var_id,this.filter.data_table,undefined,true)
        if(!series.length) {
            throw("No series found")
        }
        const filter = {
            timestart: this.filter.timestart,
            timeend: this.filter.timeend,
            forecast_date: this.filter.forecast_date
        }
        this.result = []
        for(var i in series) {
            const serie = series[i]
            const deleted = await crud.deleteObservacionesCubo(serie.fuente.id,filter,{no_send_data:this.options.no_send_data})
            this.result.push(deleted)
            logger.info("Deleted " + deleted + " rows from " + serie.fuente.data_table)
        }
        return this.result
    }
}

internal.RunAsociacionesProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "RunAsociacionesProcedure"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        if(!arguments[0].filter) {
            throw("Missing filter")
        }
        if(!arguments[0].filter.timestart || !arguments[0].filter.timeend) {
            throw("Missing timestart and/or timeend")
        }
        arguments[0].filter.timestart = timeSteps.DateFromDateOrInterval(arguments[0].filter.timestart,undefined,arguments[0].filter.round_to)
        arguments[0].filter.timeend = timeSteps.DateFromDateOrInterval(arguments[0].filter.timeend,undefined,arguments[0].filter.round_to)
        if(arguments[0].filter.forecast_date) {
            arguments[0].filter.forecast_date = timeSteps.DateFromDateOrInterval(arguments[0].filter.forecast_date,undefined,arguments[0].filter.round_to)
        }
        this.filter = arguments[0].filter
        // this.options = arguments[0].options
    }
    async run() {
        this.result = await crud.runAsociaciones(this.filter,this.options)
        return this.result
    }
}

internal.RunAsociacionProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "RunAsociacionProcedure"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        if(!arguments[0].id && !arguments[0].asociacion_id) {
            throw("Missing asociacion_id")
        }
        if(!arguments[0].filter) {
            throw("Missing filter")
        }
        if(!arguments[0].filter.timestart || !arguments[0].filter.timeend) {
            throw("Missing timestart and/or timeend")
        }
        arguments[0].filter.timestart = timeSteps.DateFromDateOrInterval(arguments[0].filter.timestart)
        arguments[0].filter.timeend = timeSteps.DateFromDateOrInterval(arguments[0].filter.timeend)
        this.asociacion_id = (arguments[0].id) ? arguments[0].id : arguments[0].asociacion_id
        arguments[0].filter.forecast_date = timeSteps.DateFromDateOrInterval(arguments[0].filter.forecast_date)
        this.filter = arguments[0].filter
        // this.options = arguments[0].options
        console.debug({
            timestart: arguments[0].filter.timestart,
            timeend: arguments[0].filter.timeend,
            forecast_date: arguments[0].filter.forecast_date
        })
    }
    async run() {
        this.result = await crud.runAsociacion(this.asociacion_id,this.filter,this.options)
        return this.result
    }
}

internal.PruneObservacionesProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "PruneObservacionesProcedure"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        if(!arguments[0].filter) {
            throw("Missing filter")
        }
        this.filter = {
            "series_id": arguments[0].filter.series_id,
            "timestart": timeSteps.DateFromDateOrInterval(arguments[0].filter.timestart),
            "timeend": timeSteps.DateFromDateOrInterval(arguments[0].filter.timeend),
            "proc_id": arguments[0].filter.proc_id,
            "var_id": arguments[0].filter.var_id,
            "unit_id": arguments[0].filter.unit_id
        }
        this.tipo = (arguments[0].tipo) ? arguments[0].tipo : "puntual"
        if(arguments[0].filter.fuentes_id) {
            if(this.tipo == "puntual") {
                this.filter.red_id = arguments[0].filter.fuentes_id
            } else {
                this.filter.fuentes_id = arguments[0].filter.fuentes_id
            }
        }
        if(arguments[0].options) {
            this.options = {
                "no_send_data": arguments[0].options.no_send_data
            }
        } else {
            this.options = {}
        }
        // this.output = (arguments[0].output) ? arguments[0].output : undefined
    }
    
    async run() {
        this.result = await crud.pruneObs(this.tipo,this.filter,this.options)
        return this.result
    }
}

internal.ThinObservacionesProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "ThinObservacionesProcedure"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        if(!arguments[0].filter) {
            throw("Missing filter")
        }
        this.filter = {
            "series_id": arguments[0].filter.series_id,
            "timestart": timeSteps.DateFromDateOrInterval(arguments[0].filter.timestart),
            "timeend": timeSteps.DateFromDateOrInterval(arguments[0].filter.timeend)
        }
        if(arguments[0].filter.proc_id) {
            this.filter.proc_id = arguments[0].filter.proc_id
        }
        if(arguments[0].filter.var_id) {
            this.filter.var_id = arguments[0].filter.var_id
        }
        if(arguments[0].filter.unit_id) {
            this.filter.unit_id = arguments[0].filter.unit_id
        }
        this.tipo = (arguments[0].tipo) ? arguments[0].tipo : "puntual"
        if(arguments[0].filter.fuentes_id) {
            if(this.tipo == "puntual") {
                this.filter.red_id = arguments[0].filter.fuentes_id
            } else {
                this.filter.fuentes_id = arguments[0].filter.fuentes_id
            }
        }
        this.options = {
            "interval": (arguments[0].options) ? arguments[0].options.interval : undefined,
            "deleteSkipped": (arguments[0].options) ? arguments[0].options.delete_skipped : undefined,
            "returnSkipped": (arguments[0].options) ? arguments[0].options.return_skipped : undefined
        }
        // this.output = (arguments[0].output) ? arguments[0].output : undefined
        this.no_send_data = (arguments[0].options) ? arguments[0].options.no_send_data : undefined
    }
    async run() {
        this.result = await crud.thinObs(this.tipo,this.filter,this.options)
        return this.result
    }
}

internal.SaveObservacionesProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "SaveObservacionesProcedure"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        if(!arguments[0].filter) {
            throw("Missing filter")
        }
        if(!arguments[0].filter.timestart || !arguments[0].filter.timeend) {
            throw("missing timestart and/or timeend")
        }
        if(!arguments[0].filter.series_id && !arguments[0].filter.fuentes_id && !arguments[0].filter.var_id && !arguments[0].filter.estacion_id) {
            throw("missing series_id or fuentes_id or var_id or estacion_id")
        }
        this.filter = arguments[0].filter
        this.filter.timestart = timeSteps.DateFromDateOrInterval(this.filter.timestart)
        this.filter.timeend = timeSteps.DateFromDateOrInterval(this.filter.timeend)
        this.tipo = (arguments[0].tipo) ? arguments[0].tipo : "puntual"
        // this.options = (arguments[0].options) ? arguments[0].options : {}
    }
    async run() {
        this.result = await crud.guardarObservaciones(this.tipo,this.filter,this.options)
        return this.result
    }
}

internal.UpdateSerieFromPronoProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "UpdateSerieFromPronoProcedure"
        if(!arguments[0]) {
            throw("Missing arguments")
        }
        if(!arguments[0].cal_id) {
            throw("Missing cal_id")
        }
        if(!arguments[0].source_series_id) {
            throw("Missing source_series_id")
        }
        if(!arguments[0].dest_series_id) {
            throw("Missing dest_series_id")
        }
        if(arguments[0].filter && arguments[0].filter.timestart) {
            arguments[0].filter.timestart = timeSteps.DateFromDateOrInterval(arguments[0].filter.timestart)
        }
        if(arguments[0].filter && arguments[0].filter.timeend) {
            arguments[0].filter.timeend = timeSteps.DateFromDateOrInterval(arguments[0].filter.timeend)
        }
        this.cal_id = arguments[0].cal_id
        this.source_series_id = arguments[0].source_series_id
        this.source_tipo = arguments[0].source_tipo ?? "puntual"
        this.dest_series_id = arguments[0].dest_series_id
        this.dest_tipo = arguments[0].dest_tipo ?? "puntual"
        if(arguments[0].filter && arguments[0].filter.forecast_date) {
            arguments[0].filter.forecast_date = timeSteps.DateFromDateOrInterval(arguments[0].filter.forecast_date)
        }
        this.filter = arguments[0].filter
    }
    async run() {
        this.result = await crud.updateSerieFromProno(this.cal_id, this.source_series_id, this.source_tipo, this.dest_series_id, this.dest_tipo, this.filter,this.options)
        return this.result
    }
}


function eval_template(s, params) {
    return Function(...Object.keys(params), "return " + s)(...Object.values(params));
}

internal.GrassBatchJobProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        if(!arguments[0].batchjob) {
            throw("Missing batchjob")
        }
        this.batchjob = sprintf("%s",arguments[0].batchjob) // batchjob path relative to base path
        this.mapset = sprintf("%04d",Math.floor(Math.random()*10000))
        var location = (arguments[0].location) ? arguments[0].location : "~/GISDATABASE/WGS84"
        this.location = sprintf("%s/%s",location,this.mapset) // sprintf("%s/GISDATABASE/WGS84/%s",process.env.HOME,mapset)
        this.command = sprintf("grass %s -c --exec %s", this.location, this.batchjob)
        if(arguments[0].date) {
            this.date = timeSteps.DateFromDateOrInterval(arguments[0].date)
            this.y = sprintf("%04d",this.date.getUTCFullYear())
            this.m = sprintf("%02d",this.date.getUTCMonth()+1)
            this.d = sprintf("%02d",this.date.getUTCDate())
            this.h = sprintf("%02d",this.date.getUTCHours())
        }
        if(arguments[0].filepath) {
            this.filepath = arguments[0].filepath
        } else if(arguments[0].filepath_pattern) {
            this.filepath_pattern = arguments[0].filepath_pattern
            this.filepath = eval_template(sprintf("`%s`",this.filepath_pattern),this)
        }
        this.env = (arguments[0].env) ? arguments[0].env : {}
        if(!this.env.filepath && this.filepath) {
            this.env.filepath = this.filepath
        }
    }
    // async callPrintWindMap(path,skip_print) {
    //     if(path) {
    //         console.log("callPrintWindMap: path: " + path)
    //         process.env.gefs_run_path = path
    //     }
    //     if(skip_print) {
    //         process.env.skip_print = "True"
    //     }
    //     return pexec(command)
    //     .then(result=>{
    //         console.log("batch job called")
    //         var stdout = result.stdout
    //         var stderr = result.stderr
    //         if(stdout) {
    //             console.log(stdout)
    //         }
    //         if(stderr) {
    //             console.error(stderr)
    //         }
    //         process.env.gefs_run_path = undefined
    //         return
    //     })
    // }
    async run() {
        this.set_env()
        logger.info("command: " + this.command)
        logger.info("filepath: " + this.env.filepath)
        this.result = await pexec(this.command)
        logger.info("batch job called")
        var stdout = this.result.stdout
        var stderr = this.result.stderr
        if(stdout) {
            logger.debug(stdout)
        }
        if(stderr) {
            logger.error(stderr)
        }
        this.unset_env()            
        return this.result
    }
    set_env() {
        for (const [key,value] of Object.entries(this.env)) {
            process.env[key] = value
        }
    }
    unset_env() {
        for (const key of Object.keys(this.env)) {
            process.env[key] = undefined
        }
    }    
}

internal.UpdateFlowcatSeriesProcedure = class extends internal.CrudProcedure {
    /**
     * Updates hydrological status series (categorical) from observed series
     * @param filter {}
     * @param filter.timestart - defaults to 1990-01-01
     * @param filter.timeend - defaults to now
     */
    constructor() {
        super(...arguments)
        this.filter = (arguments[0].filter) ? arguments[0].filter : {}
        if(this.filter.timestart) {
            this.filter.timestart = DateFromDateOrInterval(this.filter.timestart)
        } else {
            this.filter.timestart = new Date(1990,0,1)
        }
        if(this.filter.timeend) {
            this.filter.timeend = DateFromDateOrInterval(this.filter.timeend)
        } else {
            this.filter.timeend = new Date()
        }
    }
    async run() {
        const result = await updateFlowcatSeries(this.filter.timestart, this.filter.timeend)
        this.result = result
        return this.result
    }
}

internal.ImportNetcdfProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "ImportNetcdfProcedure"
        if(!arguments[0].series_id) {
            throw "missing series_id"
        }
        if(!arguments[0].dir_path) {
            throw "missing dir_path"
        }
        this.series_id = arguments[0].series_id
        this.dir_path = arguments[0].dir_path
        this.conversion_factor = arguments[0].conversion_factor
    }

    async run() {
        const client = new ThreddsClient({})
        const observaciones = await client.importFromDir(
            this.series_id,
            this.dir_path,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            this.conversion_factor
        )
        this.result = observaciones
        return this.result
    }
}

internal.ImportTifProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "ImportTifProcedure"
        if(!arguments[0].series_id) {
            throw "missing series_id"
        }
        if(!arguments[0].dir_path) {
            throw "missing dir_path"
        }
        this.series_id = arguments[0].series_id
        this.dir_path = arguments[0].dir_path
        this.interval = arguments[0].interval
        this.return_dates = arguments[0].return_dates
        this.create = arguments[0].create
    }

    async run() {
        const observaciones = await tifDirToObservacionesRaster(
            this.dir_path,
            this.series_id,
            this.interval,
            this.create,
            this.return_dates 
        )
        this.result = observaciones
        return this.result
    }
}


internal.ValidateProcedure = class extends internal.CrudProcedure {
    /**
     * Instantiates procedure to validate data
     * @param {*} args
     * @param {string} args.class_name - crud class against which to validate
     * @param {Object[]} args.elements - data to validate. May be an object or an array of objects
     * @param {string} jsonfile - json data file to validate
     */
    constructor() {
        super(...arguments)
        this.procedureClass = "ValidateProcedure"
        if(!arguments[0].class_name) {
            throw("Missing required parameter test.params.class_name")
        }
        if(!CRUD.hasOwnProperty(arguments[0].class_name)) {
            throw("Bad parameter test.params.class_name: " + arguments[0].class_name + " not found in CRUD")
        }
        this.class_name = arguments[0].class_name
        this.class = CRUD[arguments[0].class_name]
        if(!arguments[0].elements) {
            if(!arguments[0].jsonfile) {
                throw("Missing argument 'elements' or 'jsonfile'")
            }
            this.elements = internal.validateDataFile(this.class,path.resolve(this.files_base_dir,arguments[0].jsonfile),undefined,undefined,this.options.header)
            // this.elements = fs.readFileSync(arguments[0].jsonfile,"utf-8")
            // this.elements = JSON.parse(this.elements)
        } else {
            this.elements = arguments[0].elements
            if(Array.isArray(this.elements)) {
                for(var element of this.elements) {
                    if(element.filename) {
                        element.filename = path.resolve(this.files_base_dir,element.filename)
                    }
                    element = new this.class(element)
                }
                // this.elements = this.elements.map(e=> new this.class(e))
            } else {
                if(this.elements.filename) {
                    this.elements.filename = path.resolve(this.files_base_dir,this.elements.filename)
                }
                this.elements = [new this.class(this.elements)]
            }
        }
    }
    async run() {
        logger.info("Input is valid: counted " + this.elements.length + " elements")
        this.result = this.elements
        return this.result
    }
}

/** tuple creation procedure */
internal.CreateProcedure = class extends internal.CrudProcedure {
    /**
     * Create a CreateProcedure
     * @param {Object} arguments
     * @param {string} arguments.class_name
     * @param {Array<Object>|Object} arguments.elements - data to create. May be an object or an array of objects
     * @param {string} arguments.jsonfile - read data from this json file. It may contain an object or an array of objects
     * @param {string} arguments.csvfile - read data from this csv file.
     * @param {string} arguments.rasterfile - read data from this raster file.
     * @param {string} arguments.geojsonfile - read data from this geojson file. Each feature will be converted to an object of class class_name
     * @param {string} arguments.property_name - optionally read this property of the parsed content of jsonfile (instead of the whole content)
     * @param {string} arguments.output - write result to this file
     * @param {object} options
     * @param {boolean} options.header - use this option if csv input file has a header
     */
    constructor() {
        super(...arguments)
        this.procedureClass = "CreateProcedure"
        if(!arguments[0].class_name) {
            throw("Missing required parameter test.arguments.class_name")
        }
        if(!CRUD.hasOwnProperty(arguments[0].class_name)) {
            throw("Bad parameter test.arguments.class_name: " + arguments[0].class_name + " not found in CRUD")
        }
        this.class_name = arguments[0].class_name
        this.class = CRUD[arguments[0].class_name]
        this.property_name = arguments[0].property_name
        if(!arguments[0].elements) {
            if(!arguments[0].jsonfile) {
                if(!arguments[0].csvfile) {
                    if(!arguments[0].rasterfile) {
                        if(!arguments[0].geojsonfile) {
                           throw("Missing argument 'elements' or 'jsonfile' or 'csvfile' or 'rasterfile' or geojsonfile")
                        }
                        this.geojsonfile = path.resolve(this.files_base_dir,arguments[0].geojsonfile)
                    } else {
                        this.rasterfile = path.resolve(this.files_base_dir,arguments[0].rasterfile)
                    }
                } else {
                    // this.elements = internal.validateDataFile(this.class,arguments[0].csvfile,this.property_name,"csv")
                    this.csvfile = path.resolve(this.files_base_dir,arguments[0].csvfile)
                }
            } else {
                // this.elements = internal.validateDataFile(this.class,arguments[0].jsonfile,this.property_name)
                this.jsonfile = path.resolve(this.files_base_dir,arguments[0].jsonfile)
            }
            // this.elements = fs.readFileSync(arguments[0].jsonfile,"utf-8")
            // this.elements = JSON.parse(this.elements)
        } else {
            this.elements = arguments[0].elements
            if(Array.isArray(this.elements)) {
                for(var element of this.elements) {
                    if(element.filename) {
                        element.filename = path.resolve(this.files_base_dir,element.filename)
                    }
                    // element = new this.class(element)
                }
            } else {
                if(this.elements.filename) {
                    this.elements.filename = path.resolve(this.files_base_dir, this.elements.filename)
                }
                this.elements = [this.elements]
                // this.elements = [new this.class(this.elements)]
            }
        }
    }
    prepare() {
        if(this.elements) {
            this.elements = this.elements.map(e=>new this.class(e))
            for(var i in this.elements) {
                console.debug({element: i,is_of_class: this.elements[i] instanceof this.class, class_name: this.class_name})
            }
        }
    }
    async run() {
        const data = (this.elements) ? this.elements : (this.jsonfile) ? this.class.readFile(this.jsonfile,"json",{property_name:this.property_name}) : (this.csvfile) ? this.class.readFile(this.csvfile,"csv",{property_name:this.property_name,header:this.options.header}) : (this.rasterfile) ? this.class.readFile(this.rasterfile,"raster",{property_name:this.property_name}) : this.class.readFile(this.geojsonfile,"geojson",{nombre_property:this.nombre_property, id_property: this.id_property})
        if(this.class.hasOwnProperty("create")) {
            var options = (this.options) ? {
                all: this.options.all,
                upsert_estacion: this.options.upsert_estacion,
                generate_id: this.options.generate_id,
                refresh_date_range: this.options.refresh_date_range,
                create_cube_table: this.options.create_cube_table
            } : {}
            this.result = await this.class.create((Array.isArray(data)) ? data : [data],options)
        } else {
            this.result = []
            if(Array.isArray(data)) {
                // console.debug("CreateProcedure, data.length: " + data.length)
                for(var i in data) {
                    var options = (this.options) ? {
                        series_metadata: this.options.all,
                        refresh_date_range: this.options.refresh_date_range,
                        create_cube_table: this.options.create_cube_table,
                        no_update: this.options.no_update
                    } : {}
                    // console.log({create_options:options})
                    this.result.push(await data[i].create(options)) //this.class.create(this.elements,this.class)
                }
            } else {
                var options = (this.options) ? {
                    series_metadata: this.options.all,
                    refresh_date_range: this.options.refresh_date_range,
                    create_cube_table: this.options.create_cube_table
                } : {}
                this.result.push(await data.create(options))
            }
        }
        return this.result.filter(r=>r)
    }
}

internal.AggregateProcedure = class extends internal.CrudProcedure {
    /**
     * Aggregates timeseries to specified time units using specified aggregation function
     * @param filter
     * @param {string} filter.tipo
     * @param {integer} filter.series_id
     * @param {Date} filter.timestart
     * @param {Date} filter.timeend
     * @param options
     * @param {string} options.agg_function - mean, sum, nearest, min, max, first, last, math
     * @param {string} options.time_step - day, month
     * @param {integer} options.precision - number of decimals to round resulting values
     * @param {string|Object} options.time_support - time support of source series (to override series metadata)
     * @param {string} options.expression - mathematical expression to use with agg_function=math
     * @param {integer} options.min_obs - lower threshold of observations count per timestep
     * @param {Boolean} options.inst - if the source series is instantaneous (zero time support) (overrides series metadata)
     * @param {integer} options.dest_series_id - set this series_id to the resulting timeseries
     * @param {Boolean} options.update - save results into destination series
     * @returns {internal.AggregateProcedure}
     */
    constructor() {
        super(...arguments)
        this.procedureClass = "AggregateProcedure"
        this.filter = (arguments[0].filter) ? arguments[0].filter : {}
        if(!this.filter.series_id) {
            throw("Missing series_id")
        }
        this.filter.id = this.filter.series_id
        this.filter.tipo = (this.filter.tipo) ? this.filter.tipo : "puntual"
        if(this.filter.timestart) {
            this.filter.timestart = DateFromDateOrInterval(this.filter.timestart)
        }
        if(this.filter.timeend) {
            this.filter.timeend = DateFromDateOrInterval(this.filter.timeend)
        }
        if(this.filter.forecast_date) {
            this.filter.forecast_date = DateFromDateOrInterval(this.filter.forecast_date)
        }
        if(this.filter.timeupdate) {
            this.filter.timeupdate = DateFromDateOrInterval(this.filter.timeupdate)
        }
        this.options = (arguments[0].options) ? arguments[0].options : {}
        if(!this.options.time_step) {
            throw("missing options.time_step (day, month)")
        }
    }
    /**
     * Run series temporal aggregatation procedure and return array of observaciones
     * @returns {Promise<CRUD.observaciones>}
     */
    async run() {
        this.serie = await CRUD.serie.read(this.filter)
        this.result = this.serie.aggregateTimeStep(this.filter.timestart,this.filter.timeend,this.options.time_step,this.options.agg_function,this.options.precision,this.options.time_support,this.options.expression, this.options.min_obs, this.options.inst,this.options.dest_series_id)
        if(this.options.update) {
            if(!this.options.dest_series_id) {
                throw("Missing options.dest_series_id: it is required when options.update is set to true")  
            } 
            this.result = await CRUD.observaciones.create(this.result)
        }
        return this.result
    }
}

internal.ReadProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "ReadProcedure"
        if(!arguments[0].class_name) {
            throw("Missing required parameter test.params.class_name")
        }
        if(!CRUD.hasOwnProperty(arguments[0].class_name)) {
            throw("Bad parameter test.params.class_name: " + arguments[0].class_name + " not found in CRUD")
        }
        this.class_name = arguments[0].class_name
        this.class = CRUD[arguments[0].class_name]
        if(!this.class) {
            throw("Bad parameter test.params.class_name: " + arguments[0].class_name + " is undefined")
        }
        this.filter = this.parseFilter(arguments[0].filter)
    }

    parseFilter(filter={}) {
        if(filter.timestart) {
            filter.timestart = DateFromDateOrInterval(filter.timestart)
        }
        if(filter.timeend) {
            filter.timeend = DateFromDateOrInterval(filter.timeend)
        }
        if(filter.begin_position) {
            filter.begin_position = DateFromDateOrInterval(filter.begin_position)
        }
        if(filter.end_position) {
            filter.end_position = DateFromDateOrInterval(filter.end_position)
        }
        if(filter.forecast_date) {
            filter.forecast_date = DateFromDateOrInterval(filter.forecast_date)
        }
        if(filter.timeupdate) {
            filter.timeupdate = DateFromDateOrInterval(filter.timeupdate)
        }
        if(filter.date_range_before) {
            filter.date_range_before = DateFromDateOrInterval(filter.date_range_before)
        }
        if(filter.date_range_after) {
            filter.date_range_after = DateFromDateOrInterval(filter.date_range_after)
        }
        if(filter.geom) {
            if(typeof filter.geom == 'string') {
                filter.geom = new CRUD.geometry('BOX',filter.geom)
            } else {
                filter.geom = new CRUD.geometry(filter.geom)
            }
        }
        return filter
    }

    async run() {
        this.result = await this.class.read(this.filter,this.options)
        if(this.options.columns) {
            if(Array.isArray(this.result)) {
                return this.result.map(row=>row.partial(this.options.columns))
            } else {
                return this.result.partial(this.options.columns)
            }

        }
        return this.result
    }
} 

internal.UpdateFromFileProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "UpdateFromFileProcedure"
        if(!arguments[0].class_name) {
            throw("Missing required parameter test.params.class_name")
        }
        if(!CRUD.hasOwnProperty(arguments[0].class_name)) {
            throw("Bad parameter test.params.class_name: " + arguments[0].class_name + " not found in CRUD")
        }
        if(arguments[0].jsonfile) {
            this.input_file = path.resolve(this.files_base_dir,arguments[0].jsonfile)
            this.input_format = "json"
        } else {
            if(!arguments[0].csvfile) {
                throw("Missing required argument 'jsonfile' or 'csvfile'")
            }
            this.input_file = path.resolve(this.files_base_dir,arguments[0].csvfile)
            this.input_format = "csv"
        } 
        if(!fs.existsSync(this.input_file)) {
            throw("Input file " + this.input_file + " not found")
        }
        this.class_name = arguments[0].class_name
        this.class = CRUD[arguments[0].class_name]
    }
    async run() {
        this.result = await this.class.updateFromFile(this.input_file,this.input_format)
        return this.result
    }
}


internal.UpdateProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "UpdateProcedure"
        if(!arguments[0].class_name) {
            throw("Missing required parameter test.params.class_name")
        }
        if(!CRUD.hasOwnProperty(arguments[0].class_name)) {
            throw("Bad parameter test.params.class_name: " + arguments[0].class_name + " not found in CRUD")
        }
        if(!arguments[0].update) {
            throw("Missing required argument 'update'")
        }
        if(!arguments[0].update instanceof Object || !Object.keys(arguments[0].update).length) {
            throw("Bad argument 'update': must be a nonempty Object")
        }
        this.update = arguments[0].update 
        this.class_name = arguments[0].class_name
        this.class = CRUD[arguments[0].class_name]
        this.filter = arguments[0].filter
    }
    async run() {
        if(this.class.hasOwnProperty("update")) {
            this.result = await this.class.update(this.filter,this.update)
            return this.result
        }
        var elements = await this.class.read(this.filter)
        if(elements != null && !Array.isArray(elements)) {
            elements = [elements]
        }
        if(!elements || !elements.length) {
            console.log("No elements matched filter criteria")
            return []
        } 
        this.result = []
        for(var i in elements) {
            this.result.push(await elements[i].update(this.update))
        }
        return this.result
    }
}

internal.DeleteProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "DeleteProcedure"
        if(!arguments[0].class_name) {
            throw("Missing required parameter test.params.class_name")
        }
        if(!CRUD.hasOwnProperty(arguments[0].class_name)) {
            throw("Bad parameter test.params.class_name: " + arguments[0].class_name + " not found in CRUD")
        }
        this.class_name = arguments[0].class_name
        this.class = CRUD[arguments[0].class_name]
        this.filter = internal.parseFilter(arguments[0].filter)
    }
    async run() {
        this.result = await this.class.delete(this.filter,this.options)
        return this.result
    }
}

internal.ComputeProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "ComputeProcedure"
        if(!arguments[0].class_name) {
            throw("Missing required parameter test.params.class_name")
        }
        if(!CRUD.hasOwnProperty(arguments[0].class_name)) {
            throw("Bad parameter test.params.class_name: " + arguments[0].class_name + " not found in CRUD")
        }
        this.class_name = arguments[0].class_name
        this.class = CRUD[arguments[0].class_name]
        this.filter = arguments[0].filter
    }
    async run() {
        this.result = await this.class.compute(this.filter,this.options)
        return this.result
    }
}

internal.ArchiveProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "ArchiveProcedure"
        if(!arguments[0].class_name) {
            throw("Missing required parameter test.params.class_name")
        }
        if(!CRUD.hasOwnProperty(arguments[0].class_name)) {
            throw("Bad parameter test.params.class_name: " + arguments[0].class_name + " not found in CRUD")
        }
        this.class_name = arguments[0].class_name
        this.class = CRUD[arguments[0].class_name]
        this.filter = arguments[0].filter
    }
    async run() {
        this.result = await this.class.archive(this.filter,this.options)
        return this.result
    }
}

internal.BackupProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "BackupProcedure"
        if(!arguments[0].class_name) {
            throw("Missing required parameter test.params.class_name")
        }
        if(!CRUD.hasOwnProperty(arguments[0].class_name)) {
            throw("Bad parameter test.params.class_name: " + arguments[0].class_name + " not found in CRUD")
        }
        this.class_name = arguments[0].class_name
        this.class = CRUD[arguments[0].class_name]
        this.filter = arguments[0].filter
    }
    async run() {
        this.result = await this.class.backup(this.filter,this.options)
        return this.result
    }
}

internal.RestoreProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "RestoreProcedure"
        if(!arguments[0].class_name) {
            throw("Missing required parameter test.params.class_name")
        }
        if(!CRUD.hasOwnProperty(arguments[0].class_name)) {
            throw("Bad parameter test.params.class_name: " + arguments[0].class_name + " not found in CRUD")
        }
        this.class_name = arguments[0].class_name
        this.class = CRUD[arguments[0].class_name]
        this.filter = arguments[0].filter
    }
    async run() {
        this.result = await this.class.restore(this.filter,this.options)
        return this.result
    }
}

internal.GetSeriesBySiteAndVarProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "GetSerieBySiteAndVarProcedure"
        if(!arguments[0].filter) {
            throw("Missing filter")
        }
        this.estacion_id = arguments[0].filter.estacion_id
        this.var_id = arguments[0].filter.var_id
        this.startdate = (arguments[0].filter.timestart) ? DateFromDateOrInterval(arguments[0].filter.timestart) : undefined
        this.enddate = (arguments[0].filter.timeend) ? DateFromDateOrInterval(arguments[0].filter.timeend) : undefined
        this.includeProno = (arguments[0].options) ? arguments[0].options.includeProno : true
        this.regular = (arguments.options) ? arguments[0].options.regular : false
        this.dt = (arguments.options) ? arguments[0].options.dt : "1 days"
        this.proc_id = arguments[0].filter.proc_id
        this.isPublic = (arguments[0].options) ? arguments[0].options.isPublic : undefined
        this.forecast_date = (arguments[0].filter.forecast_date) ? DateFromDateOrInterval(arguments[0].filter.forecast_date) : undefined
        this.series_id = (arguments[0].filter.series_id) ? arguments[0].filter.series_id : undefined
        this.tipo = (arguments[0].filter.tipo) ? arguments[0].filter.tipo : undefined
        this.from_view = (arguments.options) ? arguments[0].options.from_view : undefined
        this.get_cal_stats  = (arguments.options) ? arguments[0].options.get_cal_stats : undefined
    }
    async run() {
        this.result = await crud.getSeriesBySiteAndVar(
            this.estacion_id,
            this.var_id,
            this.startdate,
            this.enddate,
            this.includeProno,
            this.regular,
            this.dt,
            this.proc_id,
            this.isPublic,
            this.forecast_date,
            this.series_id,
            this.tipo,
            this.from_view,
            this.get_cal_stats
        )
        return this.result
    }
}

internal.RastToArealProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "RastToArealProcedure"
        if(!arguments[0].filter) {
            throw("Missing filter")
        }
        if(!arguments[0].filter.series_id) {
            throw("missing filter.series_id [of series_rast]")
        }
        if(!arguments[0].filter.timestart) {
            throw("missing filter.timestart")
        }
        if(!arguments[0].filter.timeend) {
            throw("missing filter.timeend")
        }
        this.area_id = arguments[0].filter.area_id ?? "all"
        this.series_id = arguments[0].filter.series_id
        this.timestart = DateFromDateOrInterval(arguments[0].filter.timestart)
        this.timeend = DateFromDateOrInterval(arguments[0].filter.timeend)
        this.cor_id = arguments[0].filter.cor_id
        this.cal_id = arguments[0].filter.cal_id
        this.forecast_date = (arguments[0].filter.forecast_date) ? DateFromDateOrInterval(arguments[0].filter.forecast_date) : undefined
        // options:
        // - no_insert
        // - funcion
        // - only_obs
    }
    async run() {
        if(Array.isArray(this.area_id)) {
            this.result = []
            for(var a of this.area_id) {
                this.result.push(await crud.rast2areal(this.series_id,this.timestart,this.timeend,a,this.options, undefined, this.cor_id, this.cal_id, this.forecast_date))
            }
        } else {
            this.result = await crud.rast2areal(this.series_id,this.timestart,this.timeend,this.area_id,this.options, undefined, this.cor_id, this.cal_id, this.forecast_date)
        }
        return this.result
    }
}

internal.RastExtractProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "RastExtractProcedure"
        if(!arguments[0].filter) {
            throw("Missing filter")
        }
        this.filter = arguments[0].filter
        if(!this.filter.series_id) {
            throw("missing filter.series_id [of series_rast]")
        }
        if(!this.filter.timestart) {
            throw("missing filter.timestart")
        }
        if(!this.filter.timeend) {
            throw("missing filter.timeend")
        }
        this.series_id = this.filter.series_id
        this.timestart = DateFromDateOrInterval(this.filter.timestart)
        this.timeend = DateFromDateOrInterval(this.filter.timeend)
        this.forecast_date = DateFromDateOrInterval(this.filter.forecast_date)
        // more filters:
        // - cal_id
        // - cor_id
        // - forecast_date
        // - qualifier
        // options:
        // - bbox
        // - pixel_height
        // - pixel_width
        // - srid
        // - funcion
    }
    async run() {
        const result_serie = await crud.rastExtract(this.series_id,this.timestart,this.timeend,this.options,undefined, undefined, this.filter.cal_id, this.filter.cor_id, this.forecast_date, this.filter.qualifier)
        this.result = result_serie.observaciones
        return this.result
    }
}

internal.RastExtractByAreaProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "RastExtractByAreaProcedure"
        if(!arguments[0].filter) {
            throw("Missing filter")
        }
        if(!arguments[0].filter.series_id) {
            throw("missing filter.series_id [of series_rast]")
        }
        if(!arguments[0].filter.timestart) {
            throw("missing filter.timestart")
        }
        if(!arguments[0].filter.timeend) {
            throw("missing filter.timeend")
        }
        this.series_id = arguments[0].filter.series_id
        this.timestart = DateFromDateOrInterval(arguments[0].filter.timestart)
        this.timeend = DateFromDateOrInterval(arguments[0].filter.timeend)
        if(!arguments[0].filter.area_id) {
            if(!arguments[0].filter.area_geom) {
                throw("Missing either filter.area_id or filter.area_geom")
            }            
            this.area = arguments[0].filter.area_geom
        } else {
            this.area = arguments[0].filter.area_id
        }
        // options:
        // - agg_func
        // - no_insert
        // - no_send_data
    }
    async run() {
        const result_serie = await crud.rastExtractByArea(this.series_id,this.timestart,this.timeend,this.area, this.options)
        this.result = result_serie.observaciones
        return this.result
    }
}

internal.RastExtractByPointProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "RastExtractByPointProcedure"
        if(!arguments[0].filter) {
            throw("Missing filter")
        }
        if(!arguments[0].filter.series_id) {
            throw("missing filter.series_id [of series_rast]")
        }
        if(!arguments[0].filter.timestart) {
            throw("missing filter.timestart")
        }
        if(!arguments[0].filter.timeend) {
            throw("missing filter.timeend")
        }
        this.series_id = arguments[0].filter.series_id
        this.timestart = DateFromDateOrInterval(arguments[0].filter.timestart)
        this.timeend = DateFromDateOrInterval(arguments[0].filter.timeend)
        if(!arguments[0].filter.estacion_id) {
            if(!arguments[0].filter.estacion_geom) {
                throw("Missing either filter.estacion_id or filter.estacion_geom")
            }            
            this.point = arguments[0].filter.estacion_geom
        } else {
            this.point = arguments[0].filter.estacion_id
        }
        // options:
        // - funcion
        // - max_distance
        // - buffer
        // - output_series_id
    }
    async run() {
        const result_serie = await crud.rastExtractByPoint(this.series_id,this.timestart,this.timeend,this.point,this.options)
        this.result = result_serie.observaciones
        return this.result
    }
}

internal.GetDerivedSerieProcedure = class extends internal.CrudProcedure {
    constructor() {
        super(...arguments)
        this.procedureClass = "GetDerivedSerieProcedure"
        if(!arguments[0].filter) {
            throw("Missing filter")
        }
        if(!arguments[0].filter.series_id) {
            throw("missing filter.series_id")
        }
        this.series_id = arguments[0].filter.series_id
        if(!Array.isArray(this.series_id)) {
            this.series_id = [this.series_id]
        }
        if(!arguments[0].filter.timestart) {
            throw("missing filter.timestart")
        }
        if(!arguments[0].filter.timeend) {
            throw("missing filter.timeend")
        }
        if(!this.options.expression) {
            throw("missing options.expression")
        }
        if(this.options.create_observaciones && !this.options.output_series_id) {
            throw("Missing options.output_series_id (create_observaciones is true")
        }
        this.tipo = arguments[0].filter.tipo ?? "puntual"
        this.timestart = DateFromDateOrInterval(arguments[0].filter.timestart)
        this.timeend = DateFromDateOrInterval(arguments[0].filter.timeend)
        // options:
        // - expression (mandatory, string)
        // - output_series_id (optional (mandatory if create_observaciones=true), int)
        // - create_observaciones (optional, bool, default false)
        // - use_source_unit_id (optiona, bool, default true)
        // - unit_id (optional, int)
    }
    async run() {
        // console.debug("tipo: " + this.tipo + ", series_id: " + this.series_id)
        const result_serie = await CRUD.serie.getDerivedSerie(
            this.tipo, 
            this.series_id, 
            this.timestart,
            this.timeend,
            "expression",
            this.options.expression,
            undefined,
            this.options.output_series_id,
            this.options.create_observaciones,
            this.options.unit_id
        )        
        this.result = result_serie.observaciones
        return this.result
    }
}

////////////////////////////////////////

const availableCrudProcedures = {
    "UpdateCubeFromSeriesProcedure": internal.UpdateCubeFromSeriesProcedure,
    "UpdateSerieRastFromCubeProcedure": internal.UpdateSerieRastFromCubeProcedure,
    "GetPpCdpBatchProcedure": internal.GetPpCdpBatchProcedure,
    "TestAccessorProcedure": internal.TestAccessorProcedure,
    "UpdateFromAccessorProcedure":internal.UpdateFromAccessorProcedure,
    "DeleteFromAccessorProcedure": internal.DeleteFromAccessorProcedure,
    "GetMetadataFromAccessorProcedure":internal.GetMetadataFromAccessorProcedure,
    "UpdateMetadataFromAccessorProcedure":internal.UpdateMetadataFromAccessorProcedure,
    "DeleteMetadataFromAccessorProcedure": internal.DeleteMetadataFromAccessorProcedure,
    "DeleteSitesFromAccessorProcedure": internal.DeleteSitesFromAccessorProcedure,
    "GetPronosticoFromAccessorProcedure":internal.GetPronosticoFromAccessorProcedure,
    "UpdatePronosticoFromAccessorProcedure": internal.UpdatePronosticoFromAccessorProcedure,
    "MapAccessorTableFromCSVProcedure": internal.MapAccessorTableFromCSVProcedure,
    "ComputeQuantilesProcedure": internal.ComputeQuantilesProcedure,
    "GetAggregatePronosticosProcedure": internal.GetAggregatePronosticosProcedure,
    "TestAccessorProcedure": internal.TestAccessorProcedure,
    "ReadFromAccessorProcedure": internal.ReadFromAccessorProcedure,
    "DownloadFromAccessorProcedure": internal.DownloadFromAccessorProcedure,
    "DeleteObservacionesProcedure": internal.DeleteObservacionesProcedure,
    "DeleteObservacionesCuboProcedure": internal.DeleteObservacionesCuboProcedure,
    "DeleteCorridas": internal.DeleteCorridas,
    "RunAsociacionesProcedure": internal.RunAsociacionesProcedure,
    "RunAsociacionProcedure": internal.RunAsociacionProcedure,
    "GetPpCdpDiario": internal.GetPpCdpDiario,
    "Campo2RastProcedure": internal.Campo2RastProcedure,
    "PruneObservacionesProcedure": internal.PruneObservacionesProcedure,
    "ThinObservacionesProcedure": internal.ThinObservacionesProcedure,
    "SaveObservacionesProcedure": internal.SaveObservacionesProcedure,
    "GrassBatchJobProcedure": internal.GrassBatchJobProcedure,
    "UpdateFlowcatSeriesProcedure": internal.UpdateFlowcatSeriesProcedure,
    "CreateProcedure": internal.CreateProcedure,
    "ReadProcedure": internal.ReadProcedure,
    "UpdateProcedure": internal.UpdateProcedure,
    "DeleteProcedure": internal.DeleteProcedure,
    "ComputeProcedure": internal.ComputeProcedure,
    "ValidateProcedure": internal.ValidateProcedure,
    "AggregateProcedure": internal.AggregateProcedure,
    "ArchiveProcedure": internal.ArchiveProcedure,
    "BackupProcedure": internal.BackupProcedure,
    "RestoreProcedure": internal.RestoreProcedure,
    "GetSitesFromAccessorProcedure": internal.GetSitesFromAccessorProcedure,
    "UpdateFromFileProcedure": internal.UpdateFromFileProcedure,
    "MapSitesFromAccessorProcedure": internal.MapSitesFromAccessorProcedure,
    "ReadVariablesFromAccessorProcedure": internal.ReadVariablesFromAccessorProcedure,
    "GetSeriesBySiteAndVarProcedure": internal.GetSeriesBySiteAndVarProcedure,
    "UpdateSeriesPronoDateRangeProcedure": internal.UpdateSeriesPronoDateRangeProcedure,
    "RastToArealProcedure": internal.RastToArealProcedure,
    "RastExtractProcedure": internal.RastExtractProcedure,
    "RastExtractByAreaProcedure": internal.RastExtractByAreaProcedure,
    "RastExtractByPointProcedure": internal.RastExtractByPointProcedure,
    "GetDerivedSerieProcedure": internal.GetDerivedSerieProcedure,
    "UpdateSerieFromPronoProcedure": internal.UpdateSerieFromPronoProcedure,
    "ImportNetcdfProcedure": internal.ImportNetcdfProcedure,
    "ImportTifProcedure": internal.ImportTifProcedure
}

internal.availableTests = {
    // "CrudProcedureResultTest" : internal.CrudProcedureResultTest,
    "TruthyTest": internal.TruthyTest,
    "NonEmptyArrayTest": internal.NonEmptyArrayTest,
    "EmptyArrayTest": internal.EmptyArrayTest,
    "ArrayOfArraysTest": internal.ArrayOfArraysTest,
    "ArrayLengthTest": internal.ArrayLengthTest,
    "PropertyExistsTest": internal.PropertyExistsTest,
    "PropertyIsUndefinedTest": internal.PropertyIsUndefinedTest,
    "PropertyEqualsTest": internal.PropertyEqualsTest,
    "PropertyIsInstanceOfTest": internal.PropertyIsInstanceOfTest,
    "ResultIsInstanceOfTest": internal.ResultIsInstanceOfTest,
    "OutputFileTest": internal.OutputFileTest,
    "PropertyIsValidDateTest": internal.PropertyIsValidDateTest,
    "PropertyAggEqualsTest": internal.PropertyAggEqualsTest,
    "PropertyIsEqualOrSmallerThanTest": internal.PropertyIsEqualOrSmallerThanTest,
    "PropertyIsEqualOrGreaterThanTest": internal.PropertyIsEqualOrGreaterThanTest,
    "PropertyEqualsOneOfTest": internal.PropertyEqualsOneOfTest,
    "ArrayIsOrderedTest": internal.ArrayIsOrderedTest
}

internal.CrudProcedureSequenceRunner = class {
    constructor(args,client) {
        if(!args) {
            throw("Missing arguments")
        }
        if(!args.sequence) {
            throw("Invalid procedureSequenceObject: missing sequence")
        }
        if(!Array.isArray(args.sequence)) {
            throw("Invalid procedureSequenceObject: sequence must be an array")
        }
        if(!args.sequence.length) {
            throw("Invalid procedureSequenceObject: sequence is empty")
        }
        this.sequence = []
        for (var i in args.sequence) {
            // console.log(`procedure ${i}: ${JSON.stringify(args.sequence[i])}`)
            var procedure = args.sequence[i]
            if(!procedure.procedureName) {
                throw("Invalid procedure: missing procedureName")
            }
            if(!availableCrudProcedures[procedure.procedureName]) {
                throw(`Invalid procedure: procedureName "${procedure.procedureName}" is not available`)
            }
            this.sequence.push(new availableCrudProcedures[procedure.procedureName](procedure.arguments,procedure.tests,args.sequence_file_location,client))
        }
        if(client) {
            this.client = client
        }
    }
    async run() {
        for (var i in this.sequence) {
            await this.runProcedure(i)
            // console.log(`Running procedure ${i}, class ${this.sequence[i].procedureClass}`)
            // await this.sequence[i].run()
        }
    }
    async runProcedure(i) {
        logger.info(`Running procedure ${i}, class ${this.sequence[i].procedureClass}`)
        if(this.sequence[i].prepare) {
            this.sequence[i].prepare()
        }
        var result = await this.sequence[i].run()
        // result = new internal.ProcedureResult(result)
        if(this.sequence[i].write_result) {
            await this.sequence[i].writeResult()
        }
        return result
    }
    async runProcedureTests(i) {
        var result = await this.sequence[i].runTests()
        return result
    }
    async runTests() {
        var test_flag = true
        var reasons = []
        for (var i in this.sequence) {
            logger.info(`<<< Procedure ${i}: ${this.sequence[i].procedureClass} >>>`)
            var result = await this.sequence[i].runTests()
            logger.info(`test result: ${JSON.stringify(result)}`)
            test_flag = (!result.success) ? false : test_flag
            reasons.push(result.reasons)
        }
        if(test_flag) {
            logger.info("All tests passed!")
        } else {
            logger.error(`At least one test failed.`)
            for(var reason of reasons.filter(r=>r)) {
                logger.error(`Reason: ${reason}`)
            }
        }
        return {
            success: test_flag,
            reasons: reasons
        }
    }
}

const exit_codes = {
    101: "File not found",
    102: "File invalid",
    103: "Procedure failed"
}

internal.validateSequence = function(filename,client) {
    var parsed_content = internal.parseYmlFile(filename) 
    parsed_content.sequence_file_location = path.dirname(filename)
    try {
        var procedureSequence = new internal.CrudProcedureSequenceRunner(parsed_content,client)
    } catch(e) {
        logger.error(e)
        // console.error(e)
        throw(`Failed to instantiate procedureSequence. ${e.toString()}`)
    }
    return procedureSequence
}

internal.runSequence = async function(filename,test_mode=false,client) {
    var procedureSequence = internal.validateSequence(filename,client)
    // console.log(parsed_content)
    if(test_mode) {
        return procedureSequence.runTests()
    }
    return procedureSequence.run()
}

internal.parseCsvFile = function(filename,crud_class,options={}) {
    const separator = options.separator ?? ","
    if(!crud_class.fromCSV) {
        throw("fromCSV method undefined for this CRUD class")
    }
    if(!fs.existsSync(filename)) {
        throw(`file ${filename} not found.`)
    }
    try {
        var content = fs.readFileSync(filename,"utf-8")
    } catch(e) {
        throw(`Couldn't read file ${filename}. ${e.toString()}`)
    }
    if(crud_class.prototype instanceof Array) {
        try {
            var parsed_content = crud_class.fromCSV(content,separator)
        } catch(e) {
            throw(`Couldn't parse file ${filename}. ${e.toString()}`)
        }    
    } else {
        try {
            var rows = content.split("\n")
            if(options.header) {
                var columns = rows.shift()
                columns = columns.split(separator) 
            } else {
                var columns = undefined
            }
            var parsed_content = rows.map(r=>crud_class.fromCSV(r,separator,columns))
        } catch(e) {
            throw(`Couldn't parse file ${filename}. ${e.toString()}`)
        }
    }
    console.debug(JSON.stringify({"parsed_content": parsed_content}))
    return parsed_content    
}

internal.parseYmlFile = function(filename) {
    if(!fs.existsSync(filename)) {
        throw(`file ${filename} not found.`)
    }
    try {
        var content = fs.readFileSync(filename,"utf-8")
    } catch(e) {
        throw(`Couldn't read file ${filename}. ${e.toString()}`)
    }
    try {
        var parsed_content = YAML.parse(content)
    } catch(e) {
        throw(`Couldn't parse file ${filename}. ${e.toString()}`)
    }
    return parsed_content    
}

internal.validateDataFile = function(crud_class,filename,property_name,format="yml",header=false) {
    if(format == "csv") {
        return internal.parseCsvFile(filename,crud_class,{header:header})
    } else if(format == "raster") {
        return internal.readRasterFile(filename,crud_class)
    } else if(format == "buffer") {
        return fs.readFileSync(filename)
    } else {
        return internal.validateYmlDataFile(crud_class,filename,property_name)
    }
}

internal.readRasterFile = function(filename,crud_class) {
    if(!crud_class.fromRaster) {
        throw("fromRaster method not found for this class")
    }
    return crud_class.fromRaster(filename)
}

internal.validateYmlDataFile = function(crud_class,filename,property_name) {
    var parsed_content = internal.parseYmlFile(filename)
    if(property_name) {
        if(!parsed_content.hasOwnProperty(property_name)) {
            throw("Invalid data file: property" + property_name + " not found")
        }
        parsed_content = parsed_content[property_name]
    }
    if(!Array.isArray(parsed_content)) {
        if(!parsed_content instanceof Object) {
            throw("Invalid data file: must be an Array of Objects or an Object")
        }
        try {
            return new crud_class(parsed_content)
        }
        catch(e) {
            logger.error(e)
            throw(`Failed to instantiate content of file ${filename}. ${e.toString()}`)
        }
    }
    if(crud_class.prototype instanceof Array) {
        return new crud_class(parsed_content)
    }
    const result = []
    for(var i in parsed_content) {
        try {
            result.push(new crud_class(parsed_content[i]))
        } catch(e) {
            logger.error(e)
            throw(`Failed to instantiate element ${i} of file ${filename}. ${e.toString()}`)
        }
    }
    return result
}

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
            } else if (output_format.toLowerCase() == "gmd") {
                if(!Array.isArray(result)) {
                    if(result.rows) {
                        result = result.rows
                    } else {
                        result = [ result ]
                    }
                }
                if(!result.length) {
                    throw("No records found")
                }
                if(!result[0] instanceof CRUD.serie) {
                    throw("toGmd method not present in object")    
                }
                logger.warn("Only writing first match")
                process.stdout.write((result[0].toGmd()))
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

internal.parseFilter = function(filter) {
    if(!filter) {
        return filter
    }
    for(var key of Object.keys(filter)) {
        if(["timestart","timeend","forecast_date"].indexOf(key) >= 0) {
            filter[key] = DateFromDateOrInterval(filter[key])
        }
    }
    return filter
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
                    internal.validateSequence(filename)
                } catch(e) {
                    if(options.test) {
                        test_result = false
                    }
                    logger.error(e)
                }
                continue
            }
            try {
                var result = await internal.runSequence(filename,options.test,client)
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
    .option("-p, --property_name <value>", "read list of elements to create from this property of the root element, or of each item if the root element is a list")
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
                if(options.property_name) {
                    params.property_name = options.property_name
                }
                var procedure = new internal.CreateProcedure(params)
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
    .option("-f, --format <value>",'output format (json (default), jsonless, geojson, csv o gmd)')
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
        if(options.format && options.format.toLowerCase() == "gmd") {
            read_options.include_geom = true
        }
        try {
            var procedure = new internal.ReadProcedure({class_name: class_name, filter:filter, output: (options.output) ? path.resolve(options.output) : undefined, output_format: options.format, options: read_options})
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
                var procedure = new internal.UpdateFromFileProcedure(args)
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
                var procedure = new internal.UpdateProcedure({class_name: class_name, filter:filter, output: (options.output) ? path.resolve(options.output) : undefined, output_format: options.format, options: update_options, update:update})
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
    .option("--save", "For forecast runs (class corrida), save into corridas_guardadas before deleting")
    .option("--save-prono", "For forecast runs (class corrida), save only forecasted tuples (discard warmup period) into corridas_guardadas before deleting")
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
        if(options.save) {
            delete_options.save = true
        }
        if(options.saveProno) {
            delete_options.save_prono = true
        }
        try {
            var procedure = new internal.DeleteProcedure({class_name: class_name, filter:filter, output: (options.output) ? path.resolve(options.output): undefined, output_format: options.format, options: delete_options})
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
    .option("-p, --pretty",'pretty print output')
    .option("-P, --forecast", "Get (and update if -u is set) forecasts instead of observations (using GetPronosticoFromAccessorProcedure/UpdatePronosticoFromAccessorProcedure)")
    .option("-n, --no_update_date_range", "no_update_date_range")
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
                if(options.forecast) {
                    var procedure_class = internal.UpdatePronosticoFromAccessorProcedure
                } else {
                    var procedure_class = internal.UpdateFromAccessorProcedure
                }
            } else {
                if(options.forecast) {
                    var procedure_class = internal.GetPronosticoFromAccessorProcedure
                } else {
                    var procedure_class = internal.DownloadFromAccessorProcedure
                }
            }       
            var procedure = new procedure_class(
                {
                    accessor_id: class_name,
                    filter: filter,
                    output: output,
                    output_format: options.output_format,
                    options: (options.update) ? {...get_options, no_update_date_range: options.no_update_date_range} : get_options
                }
            )
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
    .command('get-sites accessor_class [filter...]')
    .alias('s')
    .description('Run site metadata download procedure for given accessor class and output in selected format. Accepts zero to many filters as "key1=value1 key2=value2 ..."')
    .option("-v, --validate",'validate only (don\'t run)')
    .option("-o, --output <value>",'save output to file. If -o nor -i are set, output is printed to STDOUT')
    .option("-f, --format <value>",'output format (json (default) or csv)')
    .option("-i, --output_individual_files_pattern <value>",'output one file for each retrieved element using this printf pattern to use with element id and, additional fields (with -F option)')
    .option("-b, --base_path <value>",'to use together with -i. Prepends this base path to the constructed file paths')
    .option("-F, --iter_field <value>",'Field of elements to iterate over to generate separate output files (combined with -i)')
    .option("-u, --update","Update sites in database from downloaded records")
    .option("-p, --pretty",'pretty print output')
    .action(async (accessor_class,filter,options) => {
        try {
            filter = parseKVPArray(filter)
        } catch (e) {
            logger.error(e)
            process.exit(1)
        }
        console.debug({filter: filter, options: options})
        const class_name = accessor_class
        try {
            accessor = await Accessors.new(class_name)
        } catch(e) {
            console.error(e)
            process.exit(1)
        }
        const get_options = getOutputOptions(options)
        get_options.update = options.update
        const output = (options.output) ? path.resolve(options.output) : undefined
        try {
            // console.debug("Update from accessor")
            var procedure = new internal.GetSitesFromAccessorProcedure({accessor_id: class_name, filter:filter, output: output, output_format: options.format, options: get_options})
        } catch(e) {
            logger.error(e)
            process.exit(1)
        }
        if(options.validate) {
            process.exit(0)
        }
        try {
            var result = await procedure.run()
            // if(options.update) {
            //     result = await CRUD.estacion.create(result)
            // }
        } catch(e) {
            logger.error(e)
            process.exit(1)
        }
        await writeResult(procedure,result,options)
        process.exit(0)
    })

    program
    .command('get-series accessor_class [filter...]')
    .alias('S')
    .description('Run series metadata download procedure for given accessor class and output in selected format. Accepts zero to many filters as "key1=value1 key2=value2 ..."')
    .option("-v, --validate",'validate only (don\'t run)')
    .option("-o, --output <value>",'save output to file. If -o nor -i are set, output is printed to STDOUT')
    .option("-f, --format <value>",'output format (json (default) or csv)')
    .option("-i, --output_individual_files_pattern <value>",'output one file for each retrieved element using this printf pattern to use with element id and, additional fields (with -F option)')
    .option("-b, --base_path <value>",'to use together with -i. Prepends this base path to the constructed file paths')
    .option("-F, --iter_field <value>",'Field of elements to iterate over to generate separate output files (combined with -i)')
    .option("-u, --update","Update series in database from downloaded records")
    .option("-s, --update-stations","update stations (sites) in database from downloaded records")
    .option("-p, --pretty",'pretty print output')
    .action(async (accessor_class,filter,options) => {
        try {
            filter = parseKVPArray(filter)
        } catch (e) {
            logger.error(e)
            process.exit(1)
        }
        console.debug({filter: filter, options: options})
        const class_name = accessor_class
        try {
            accessor = await Accessors.new(class_name)
        } catch(e) {
            console.error(e)
            process.exit(1)
        }
        const get_options = getOutputOptions(options)
        if(options.updateStations) {
            get_options.upsert_estacion = true
        } else {
            get_options.upsert_estacion = false
        }
        const output = (options.output) ? path.resolve(options.output) : undefined
        try {
            if(options.update) {
                var procedure = new internal.UpdateMetadataFromAccessorProcedure({accessor_id: class_name, filter:filter, output: output, output_format: options.format, options: get_options})
            } else {
                var procedure = new internal.GetMetadataFromAccessorProcedure({accessor_id: class_name, filter:filter, output: output, output_format: options.format, options: get_options})
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
            t_offset: options.t_offset,
            no_insert: options.no_insert,
            no_update: options.no_update,
            no_insert_as_obs: options.no_insert_as_obs,
            inst: options.inst
        }
        if(filter.id) {
            try {
                var procedure = new internal.RunAsociacionProcedure({
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
                var procedure = new internal.RunAsociacionesProcedure({
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


const replacePlaceholders = function(string,object) {
    var placeholders = string.match(/\{\{.+?\}\}/g)
    if (placeholders && placeholders.length) {
        placeholders.forEach(placeholder=>{
            var deep_key = placeholder.replace("{{","").replace("}}","")
            var value = getDeepValue(object,deep_key)
            if(value != null) {
                if(value instanceof Date) {
                    string = string.replace(placeholder,value.toISOString())
                } else {
                    string = string.replace(placeholder,value)
                }
            }
        })
    }
    return string
}

function isDiffString(a, b) {
    if(a === b) {
        return [true, undefined]
    } else {
        var diff_pos = findFirstDiffPos(a, b)
        var pos_strings = `${a.slice(Math.max(0,diff_pos-35),Math.min(diff_pos+35,a.length))}\n${b.slice(Math.max(0,diff_pos-35),Math.min(diff_pos+35,b.length))}`
        return [false, pos_strings]
    }
}

function findFirstDiffPos(a, b) {
    var i = 0;
    if (a === b) return -1;
    while (a[i] === b[i]) i++;
    return i;
}
module.exports = internal