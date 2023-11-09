const yaml = require('yaml')
const fs = require('fs')
const fsPromises = fs.promises
const ajv = require('./a5_validator') // new Ajv()
const validator = ajv.getSchema('corrida.yml')
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
			console.error(e)
			return false
		})
	}
	async parsePronoYml(file,validate=true) {
		if(!file) {
			file = this.config.file
		}
		var prono = yaml.parse(fs.readFileSync(path.resolve(__dirname,file),'utf-8'))
        if(validate) {
            // const schema = yaml.parse(fs.readFileSync(path.resolve(__dirname,this.config.schema),'utf-8'))
            const valid = validator(prono)
            if(!valid) {
				throw(new Error(JSON.stringify(validator.errors)))
            }
			console.log("input corrida is valid")
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
	async updatePronostico(filter={}) {
		return this.update(filter)
	}
}

module.exports = internal