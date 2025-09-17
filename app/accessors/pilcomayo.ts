import axios, { AxiosInstance } from "axios"
import { AbstractAccessorEngine, AccessorEngine, Config } from "./abstract_accessor_engine"

interface LoginResponse {
    id_client : string
	name : string
	tokenAuth : string
	expires : string
}

interface ListEstacionesResponse {
	id_estacion : string
	nombre : string
	codigo : string
	latitud : string
	longitud : string
	nivel_mar : string
	direccion : string
	en_actividad : string
	estado : string
	descripcion : string
}

export class Client extends AbstractAccessorEngine implements AccessorEngine {

    static _get_is_multiseries : boolean = false

    config : Config

    connection: AxiosInstance

    constructor(config : Config) {
        super(config)
        this.setConfig(config)
        this.connection = axios.create()
    }

    default_config : Config = {
        url: "https://api.pilcomayo.net",
        username: "my_username",
        password: "my_password"
    }

    async login() : Promise<LoginResponse> {
        const response = await this.connection.post(
            `${this.config.url}/login`, 
            {
                username: this.config.username,
                password: this.config.password
            })
        if(response.status != 200) {
            throw new Error(`Login failed: ${response.statusText}`)
        }
        if(!response.data || !response.data.tokenAuth) {
            throw new Error(`Login failed: tokenAuth missing in response`)
        }
        return response.data as LoginResponse
    }

    async listEstaciones(id_estacion? : string) : Promise<ListEstacionesResponse[]> {
        const response = await this.connection.post(
            `${this.config.url}/list_estaciones`, 
            {
                id_estacion
            })
        if(response.status != 200) {
            throw new Error(`Request failed: ${response.statusText}`)
        }
        if(!response.data) {
            throw new Error(`listEstaciones failed: no data`)
        }
        if(Array.isArray(response.data)) {
            return response.data as ListEstacionesResponse[]            
        } else {
            return [response.data] as ListEstacionesResponse[]
        }
    }
}
