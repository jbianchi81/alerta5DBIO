import {Corrida as Corrida, SerieProno} from './a5_types'

export class corrida implements Corrida {
    constructor(params : Corrida)
    create() : Promise<corrida>
    cal_id: number
    forecast_date: Date
    series: SerieProno[]
}