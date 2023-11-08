const yaml = require('yaml')
const fs = require('fs')
const fsPromises = fs.promises
const Ajv = require('ajv')
const {corrida: Corrida} = require('./CRUD')
const path = require('path')

const internal = {}

internal.prono_yml = class {
	constructor(config) {
		this.config = {}
		Object.assign(this.config,this.default_config)
		Object.assign(this.config,config)
	}
	default_config = {
		file: "../public/planillas/prono.yml",
		schema: "../public/schemas/a5/corrida.yml"
	}
	async test() {
		return fsPromises.access(__dirname + "/" + this.config.file)
		.then(()=>{
			return true
		})
		.catch(e=>{
			return false
		})
	}
	async parsePronoYml(file,validate=false) {
		if(!file) {
			file = this.config.file
		}
		var prono = yaml.parse(fs.readFileSync(path.resolve(__dirname,file),'utf-8'))
        if(validate) {
            const ajv = new Ajv()
            const schema = yaml.parse(fs.readFileSync(path.resolve(__dirname,this.config.schema),'utf-8'))
            const valid = ajv.validate(schema,prono)
            if(!valid) {
                throw(ajv.errors)
            }
        }
		return new Corrida(prono)
	}
    async get(filter={}) {
        return this.parsePronoYml(filter.file)
    }
    async update(filter={}) {
        const corrida = await this.get(filter)
        await corrida.create()
        return corrida
    }
}

module.exports = internal