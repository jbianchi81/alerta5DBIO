import {baseModel} from 'a5base/baseModel' 
import {control_filter2} from './utils'

interface AreaGroup {
	id : number
	name : string
}

const area_group = class extends baseModel {
	id : number
	name : string
	constructor(args : AreaGroup) {
		super()
		this.id = args.id
		this.name = args.name
	}

	static async read(filter  :{id?: number, name?: string} ={},options : {get_areas?: boolean}={}) {
		const filter_string = control_filter2({
				id: {type: "integer"},
				name: {type: "string"}
			},
			filter,
			"area_groups"
		) 
		if(options.get_areas) {
			const result = await global.pool.query(``)
		} else {
			const result = await global.pool.query(`SELECT id, name FROM area_groups WHERE 1=1 ${filter_string}`)
			return result.rows.map((r : AreaGroup) => new this(r))
		}
	}
}

export default area_group
