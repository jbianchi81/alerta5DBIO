const internal = {}

internal.AbstractAccessorEngine = class {

    default_config = {}

    config = {}

    constructor(config={}) {
        this.config = {}
        Object.assign(this.config,this.default_config)
        Object.assign(this.config,config)
    }
}

module.exports = internal