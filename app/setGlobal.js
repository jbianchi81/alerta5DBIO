if(!global.pool) {
    // console.log("setting global.pool")
    const path = require('path')
    process.env["NODE_CONFIG_DIR"] = path.resolve(__dirname,"../config/")
    process.env["A5_BASE_DIR"] = path.resolve(__dirname,"..")
    const config = require('config');
    global.config = config
    if(! config.database) {
        throw("Missing config.database")
    }
    const { Pool } = require('pg')
    global.pool = new Pool(config.database)
    if(!global.dbConnectionString) {
        global.dbConnectionString = "host=" + config.database.host + " user=" + config.database.user + " dbname=" + config.database.database + " password=" + config.database.password + " port=" + config.database.port
    }
    if (!Promise.allSettled) {
        Promise.allSettled = promises =>
          Promise.all(
            promises.map((promise, i) =>
              promise
                .then(value => ({
                  status: "fulfilled",
                  value,
                }))
                .catch(reason => ({
                  status: "rejected",
                  reason,
                }))
            )
        );
    }   
}
