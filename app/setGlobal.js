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
    if(! config.database.idleTimeoutMillis) {
      config.database.idleTimeoutMillis = 1000
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
    
    global.logPoolUsage = function () {
      global.getSessionCount()
      .then(sessions=>{
        console.log(new Date().toISOString() + " POOL TOTAL: " + global.pool.totalCount + ", WAITING: " + global.pool.waitingCount + ", IDLE: " + global.pool.idleCount + ". Sessions: active: " + sessions.active + ", idle: " + sessions.idle + ", iit: " + sessions["idle in transaction"])
      })
    }
    global.getSessionCount = async () => {
      try {
        var result =  await global.pool.query("select state,count(*) from pg_stat_activity group by state")
      } catch (e) {
        throw(e)
      }
      const o = {
        "idle": 0,
        "active": 0,
        "idle in transaction": 0
      }
      result.rows.forEach(r=>{
        o[r.state]  = r.count
      })
      return o
    }
    if(config.log_pool_usage && config.log_pool_usage.activate) {
      setInterval(global.logPoolUsage,config.log_pool_usage.interval)
    }
}
