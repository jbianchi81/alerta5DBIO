import {fetchData, parseUtcDateTime, filterSeries} from './accessor_utils.js'
import { AbstractAccessorEngine } from './abstract_accessor_engine.js'
import {estacion as Estacion, serie as Serie, var as Variable, procedimiento as Procedimiento, unidades as Unidades, observacion as Observacion} from '../CRUD.js'
import {Interval, advanceTimeStep} from '../timeSteps.js'

type Sensor = {
    uid : number
    name : string
    visible : boolean
}

type Device = {
    id : number
    name : string
    site : string
    lastContact : string
    sensors : Sensor[]
}

type Config = {
    url : string
    key : string
    devices : number[]
    parameters : number[]
    tabla_id? : string
    coordinates? : Record<number, number[]> // mapping of device ids --> [lon, lat]
    var_map? : Record<number, VarMap>
    dt?: {hours?: number, minutes?: number}
}

type GetDevicesResponse = {
    devices : Device[]
}

type Parameter = {
    id : number
    name : string
    visible : boolean
    unit : string
    precision : number
    sensorName : string
    sensorUID : number
    isCalculated : boolean
}

type GetParametersResponse = {
    parameters: Parameter[]
}

/**
 * Values of -100000 or lower represent error codes.
 * Value is null if there is no data at this timestamp.
 */
type DataValue = {
    parameterId : number
    value : string    
}

type DataPoint = {
    timestamp : string
    values : DataValue[]
}

type GetDataResponse = {
    data : DataPoint[]
    info : {
        total : number,
        count : number,
        more : boolean,
        lastDataPointTimestamp : string
    }
}

type ParameterDataPoint = {
    timestamp : string
    value : string
}


type GetParameterDataResponse = {
    data : ParameterDataPoint[]
    info : {
        total : number,
        count : number,
        more : boolean,
        lastDataPointTimestamp : string
    }
}

type RecentDataPoint = {
    id : number // parameter id
    name : string // parameter name
    unit : string
    timestamp : string
    value : string
}

type GetLatestDataResponse = {
    data : RecentDataPoint[]
}

type VarMap = {
    var_id: number
    proc_id: number
    unit_id: number
}

export class Client extends AbstractAccessorEngine {

    url : string
    key : string
    devices : number[]
    parameters : number[]
    tabla_id? : string
    coordinates?: Record<number, number[]>
    var_map?: Record<number, VarMap>
    dt?: Interval

    getParameterId(var_id : number, proc_id : number, unit_id: number) : number|void {
        if(!this.var_map) {
            return
        }
        for(const [par_id, vmap] of Object.entries(this.var_map)) {
            if (vmap.var_id == var_id && vmap.proc_id == proc_id && vmap.unit_id == unit_id) {
                return parseInt(par_id)
            }
        }
        return        
    }

    /**
     * https://www.nexsens.com/knowledge-base-v2/software/wqdatalive/user-guide/wqdata-live-data-api-doc#operation/devicesInfo
     * no tiene las coordenadas
     * @param baseUrl 
     * @param apiKey 
     * @returns 
     */
    static async getDevices(
        baseUrl : string="https://www.wqdatalive.com/api/v1",
        apiKey : string="") : Promise<GetDevicesResponse> {
        return fetchData(
            `${baseUrl}/devices`, 
            {"params": {"apiKey": apiKey}}
        )
    }

    /**
     * https://www.nexsens.com/knowledge-base-v2/software/wqdatalive/user-guide/wqdata-live-data-api-doc#operation/deviceInfo
     * @param baseUrl 
     * @param apiKey 
     * @param deviceId 
     * @returns 
     */
    static async getDevice(
        baseUrl : string="https://www.wqdatalive.com/api/v1",
        apiKey : string="",
        deviceId : number
    ) : Promise<Device> {
        return fetchData(
            `${baseUrl}/devices/${deviceId}`, 
            {"params": {"apiKey": apiKey}}
        )
    }

    /**
     * https://www.nexsens.com/knowledge-base-v2/software/wqdatalive/user-guide/wqdata-live-data-api-doc#tag/Parameters
     * @param baseUrl 
     * @param apiKey 
     * @param deviceId 
     * @returns 
     */
    static async getParameters(
        baseUrl : string="https://www.wqdatalive.com/api/v1",
        apiKey : string="",
        deviceId : number
    ) : Promise<GetParametersResponse> {
        return fetchData(
            `${baseUrl}/devices/${deviceId}/parameters`, 
            {"params": {"apiKey": apiKey}}
        )
    }

    /**
     * https://www.nexsens.com/knowledge-base-v2/software/wqdatalive/user-guide/wqdata-live-data-api-doc#operation/ParameterInfo
     * @param baseUrl 
     * @param apiKey 
     * @param deviceId 
     * @param parameterId 
     * @returns 
     */
    static async getParameter(
        baseUrl : string="https://www.wqdatalive.com/api/v1",
        apiKey : string="",
        deviceId : number,
        parameterId : number
    ) : Promise<Parameter> {
        return fetchData(
            `${baseUrl}/devices/${deviceId}/parameters/${parameterId}`, 
            {"params": {"apiKey": apiKey}}
        )
    }

    /**
     * https://www.nexsens.com/knowledge-base-v2/software/wqdatalive/user-guide/wqdata-live-data-api-doc#operation/MultipleParametersData
     * @param baseUrl 
     * @param apiKey 
     * @param deviceId 
     * @param from - start time in UTC YYYY-MM-DD HH:mm:ss
     * @param to - end time in UTC YYYY-MM-DD HH:mm:ss
     * @returns 
     */
    static async getData(
        baseUrl : string="https://www.wqdatalive.com/api/v1",
        apiKey : string="",
        deviceId : number,
        from : Date|string,
        to : Date|string,
        parameterIds? : number[]
    ) : Promise<GetDataResponse> {
        from = (from instanceof Date) ? from.toISOString().replace("T"," ").substring(0,19) : from
        to = (to instanceof Date) ? to.toISOString().replace("T"," ").substring(0,19) : to
        const parameterIds_str = (parameterIds) ? parameterIds.join(",") : undefined
        return fetchData(
            `${baseUrl}/devices/${deviceId}/parameters/data`, 
            {"params": {"apiKey": apiKey, "from": from, "to": to, "parameterIds": parameterIds_str}}
        )
    }

    /**
     * https://www.nexsens.com/knowledge-base-v2/software/wqdatalive/user-guide/wqdata-live-data-api-doc#operation/SingleParameterData
     * @param baseUrl 
     * @param apiKey 
     * @param deviceId 
     * @param parameterId 
     * @param from 
     * @param to 
     * @returns 
     */
    static async getParameterData(
        baseUrl : string="https://www.wqdatalive.com/api/v1",
        apiKey : string="",
        deviceId : number,
        parameterId : number,
        from : Date|string,
        to : Date|string
    ) : Promise<GetParameterDataResponse> {
        from = (from instanceof Date) ? from.toISOString().replace("T"," ").substring(0,19) : from
        to = (to instanceof Date) ? to.toISOString().replace("T"," ").substring(0,19) : to
        return fetchData(
            `${baseUrl}/devices/${deviceId}/parameters/${parameterId}/data`, 
            {"params": {"apiKey": apiKey, "from": from, "to": to}}
        )
    }

    static async getDataLatest(
        baseUrl : string="https://www.wqdatalive.com/api/v1",
        apiKey : string="",
        deviceId : number,
        parameterIds? : number[]
    ) : Promise<GetLatestDataResponse> {
        const parameterIds_str = (parameterIds) ? parameterIds.join(",") : undefined
        return fetchData(
            `${baseUrl}/devices/${deviceId}/parameters/data/latest`, 
            {"params": {"apiKey": apiKey, "parameterIds": parameterIds_str}}
        )
    }

    static async getParameterDataLatest(
        baseUrl : string="https://www.wqdatalive.com/api/v1",
        apiKey : string="",
        deviceId : number,
        parameterId : number
    ) : Promise<RecentDataPoint> {
        return fetchData(
            `${baseUrl}/devices/${deviceId}/parameters/${parameterId}/data/latest`, 
            {"params": {"apiKey": apiKey}}
        )
    }

    constructor(config : Config) {
        super(config)
        this.url = config.url
        this.key = config.key
        this.devices = config.devices
        this.parameters = config.parameters
        this.tabla_id = config.tabla_id
        this.coordinates = config.coordinates
        this.var_map = config.var_map
        this.dt = (config.dt) ? new Interval(config.dt) : undefined
    }

    async getDevices() : Promise<GetDevicesResponse> {
        return Client.getDevices(
            this.url,
            this.key
        )
    }

    async getDevice(deviceId : number) : Promise<Device> {
        return Client.getDevice(
            this.url,
            this.key,
            deviceId
        )
    }

    async getParameters(
        deviceId : number
    ) : Promise<GetParametersResponse> {
        return Client.getParameters(
            this.url, 
            this.key,
            deviceId
        )
    }

    async getParameter(
        deviceId : number,
        parameterId : number
    ) : Promise<Parameter> {
        return Client.getParameter(
            this.url, 
            this.key,
            deviceId,
            parameterId
        )
    }

    async getData(
        deviceId : number,
        from : Date|string,
        to : Date|string,
        parameterIds? : number[]
    ) : Promise<GetDataResponse> {
        return Client.getData(
            this.url,
            this.key,
            deviceId,
            from,
            to,
            parameterIds
        )
    }

    async getParameterData(
        deviceId : number,
        parameterId : number,
        from : Date|string,
        to : Date|string
    ) : Promise<GetParameterDataResponse> {
        return Client.getParameterData(
            this.url,
            this.key,
            deviceId,
            parameterId,
            from,
            to
        )
    }

    async getDataLatest(
        deviceId : number,
        parameterIds? : number[]
    ) : Promise<GetLatestDataResponse> {
        return Client.getDataLatest(
            this.url, 
            this.key,
            deviceId,
            parameterIds
        )
    }

    async getParameterDataLatest(
        deviceId : number,
        parameterId : number
    ) : Promise<RecentDataPoint> {
        return Client.getParameterDataLatest(
            this.url,
            this.key,
            deviceId,
            parameterId
        )
    }

    // a5 interface //////////////////////////////

    parseDevice(device : Device) : Estacion{
        const geom = (this.coordinates && device.id in this.coordinates) ? { type: "Point", coordinates: this.coordinates[device.id]} : undefined
        return new Estacion({
            id_externo: device.id,
            tabla: this.tabla_id,
            geom: geom,
            nombre: `${device.site} - ${device.name}`
        })
    }

    async getSites(filter : {estacion_id? : number|number[], id_externo? : string|string[]}={}) : Promise<Estacion[]> {
        if(!this.tabla_id) {
            throw new Error("missing tabla_id from config")
        }
        if(filter.id_externo && typeof filter.id_externo == "string" ) {
            const device = await this.getDevice(parseInt(filter.id_externo)) 
            var devices = [ device ]
        } else {
            var devices = (await this.getDevices()).devices
        }
        const estaciones : Estacion = []
        for(const device of devices) {
            const estacion = this.parseDevice(device)
            await estacion.getEstacionId()
            estaciones.push(estacion)
        }
        return estaciones
    }

    async parseParameters(
        parameters : Parameter[],
        estacion : Estacion,
        skip_unmatched : boolean=true
    ) : Promise<Serie[]> {

        if(!this.var_map) {
            throw new Error("Var map missing")
        }
        const series : Serie[] = []
        for(const parameter of parameters) {
            if(parameter.id in this.var_map) {
                const serie = new Serie({
                    tipo: "puntual",
                    estacion: estacion,
                    var: await Variable.read(this.var_map[parameter.id].var_id),
                    procedimiento: await Procedimiento.read({id: this.var_map[parameter.id].proc_id || 1}),
                    unidades: await Unidades.read({id: this.var_map[parameter.id].unit_id})
                })
                await serie.getId(false)
                series.push(serie)
            } else {
                if(!skip_unmatched) {
                    throw new Error(`Parameter id ${parameter.id} not found in var_map`)
                }
                console.warn(`Parameter id ${parameter.id} not found in var_map`)
            }
        }
        return series
    }

    async getSeries(
        filter : {
            id_externo? : string|string[], 
            estacion_id?: number|number[], 
            var_id? : number|number[], 
            proc_id?: number|number[],
            unit_id?: number|number[]
        }) : Promise<Serie[]> {
        
        const estaciones = await this.getSites(filter)
        const series : Serie[] = []
        for(const estacion of estaciones) {
            const parameters = (await this.getParameters(parseInt(estacion.id_externo))).parameters
            const series_ : Serie[] = await this.parseParameters(parameters, estacion)
            series.push(...series_)
        }
        return filterSeries(series, filter)
    }

    parseData(
        data : ParameterDataPoint[],
        series_id : number
    ) : Observacion[] {
        const obs : Observacion[] = []
        for(const d of data) {
            if(d.value == null) {
                // skips null
                continue
            }

            const valor = parseFloat(d.value)
            if(valor.toString() == "NaN") {
                // skip invalid
                continue
            }
            if(valor <= -100000) {
                // skip error code
                continue
            }

            const ts = parseUtcDateTime(d.timestamp)
            var te = new Date(ts)
            if(this.dt) {
                te = advanceTimeStep(te, this.dt)
            }
            obs.push({
                tipo: "puntual", 
                series_id: series_id, 
                timestart: ts, 
                timeend: te, 
                valor: valor
            })
        }
        return obs
    }

    /**
     * No multiseries, retrieves only first series match
     * @param filter 
     * @param options 
     * @returns 
     */
    async get(
        filter : {
            series_id?: number|number[],
            estacion_id?: number|number[],
            var_id?: number|number[],
            unit_id?:number|number[]
            timestart: Date,
            timeend: Date
        },
        options : {
            return_series ? : boolean
        } = {}
    ) : Promise<Observacion[]|Serie[]> {
        if(!filter) {
            throw new Error("Missing filter")
        }
        if(!filter.timestart || !filter.timeend) {
            throw new Error("Missing timestart+timeend")
        }

        // find matching serie
        if(filter.series_id) {
            const series = await Serie.read({id: filter.series_id})
            if(!series) {
                throw new Error("serie not found with id=" + filter.series_id)
            }
            if(Array.isArray(series)) {
                var serie = series[0]
            } else {
                var serie = series
            }
        } else if(filter.estacion_id && filter.var_id) {
            const series = await Serie.read(filter)
            if(!series.length) {
                throw new Error("series not found with filter: estacion_id" + filter.estacion_id + " var_id=" + filter.var_id)
            }
            var serie = series[0]
        } else {
            throw new Error("missing filter.series_id or filter.var_id+filter.estacion_id")
        }

        // find matching parameter id
        const par_id = this.getParameterId(serie.var.id, serie.procedimiento.id, serie.unidades.id)
        if(!par_id) {
            throw new Error("Parameter id not found for series_id " + serie.id)
        }

        // retrieve data
        const data = await this.getParameterData(
            parseInt(serie.estacion.id_externo),
            par_id,
            filter.timestart,
            filter.timeend
        )
        const observaciones = this.parseData(data.data, serie.id)
        
        // return
        if(options.return_series) {
            serie.observaciones = observaciones
            return [serie]
        } else {
            return observaciones
        }
    }
    
}