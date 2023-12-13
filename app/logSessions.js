require('./setGlobal')
const program = require('commander');

logSessions = function () {
    global.getSessionCount()
    .then(sessions=>{
      console.log(new Date().toISOString() + " Sessions: active: " + sessions.active + ", idle: " + sessions.idle + ", iit: " + sessions["idle in transaction"])
    })
  }

const default_interval = (global.config.log_pool_usage && global.config.log_pool_usage.interval) ? global.config.log_pool_usage.interval : 5000

program
.version('0.0.1')
.description('log database usage (number of sessions) at interval')

program.command('run')
  .argument('[interval]','time interval in milliseconds')
  .action((interval) => {
      interval = interval ?? default_interval
      setInterval(logSessions,interval)  
  })

program.parse()