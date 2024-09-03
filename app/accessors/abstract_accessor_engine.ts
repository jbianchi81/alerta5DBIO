export class AbstractAccessorEngine {

    default_config : Object = {}

    config : Object = {}

    setConfig(config : Object) : void {
        this.config = {}
        Object.assign(this.config,this.default_config)
        Object.assign(this.config,config)
    }

    constructor(config : Object = {}) {
        this.setConfig(config)
    }
}
