const om_ogc_timeseries_client = require("./om_ogc_timeseries_client").client

internal.client = class extends om_ogc_timeseries_client {

    static _get_is_multiseries = true

    async get(filter={},options={}) {
        if(!filter.timestart) {
            throw("om_ogc_timeseries_client: client.get: missing filter.timestart")
        }
        if(!filter.timeend) {
            throw("om_ogc_timeseries_client: client.get: missing filter.timeend")
        }
        options.raw = false
        options.tvp = true
        var time_value_pairs = await this.getObservations(filter,options,true)
        const observaciones = time_value_pairs.map(tvp=>tvp.toObservacion(tso))
        return observaciones
    }
}
