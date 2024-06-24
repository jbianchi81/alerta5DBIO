const internal = {}

internal.AbstractAccessorEngine = class {

    default_config = {}

    config = {}

    setConfig(config) {
        this.config = {}
        Object.assign(this.config,this.default_config)
        Object.assign(this.config,config)
    }

    constructor(config={}) {
        this.setConfig(config)
    }
}

module.exports = internal