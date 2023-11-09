const Ajv = require('ajv')
const a5_schema_dir = (global.config && global.config.a5_schema_dir) ? global.config.a5_schema_dir : "../public/schemas/a5"
const fs = require('fs')
const yaml = require('yaml')
const path = require('path')

const schemas = []
const schema_files = fs.readdirSync(path.resolve(__dirname,a5_schema_dir))
schema_files.forEach(file=>{
    const schema = yaml.parse(fs.readFileSync(path.resolve(__dirname,a5_schema_dir,file),'utf-8'))
    // console.log({file: file, type: typeof schema})
    schemas.push(schema)
})
const ajv = new Ajv({schemas: schemas})

module.exports = ajv