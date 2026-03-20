import app, { auth } from "./rest.mjs";
import fs  from "promise-fs"

// MEMORY USAGE LOG
function logMemUsage(logfile="logs/memUsage.log") {
	var mu = process.memoryUsage()
	var now = new Date()
	var line = [now.toISOString(),mu.rss,mu.heapTotal,mu.heapUsed,mu.external].join(",") + "\n"
	// console.log(line)
	fs.appendFileSync(logfile,line)
}

fs.writeFileSync("logs/memUsage.log","#timestamp,rss,heapTotal,heapUsed,external\n")
setInterval(logMemUsage,10000)

const config = global.config

const port = process.env.PORT || config.rest.port || 3000

const auth_levels = {
	"public": auth.isPublic,
	"authenticated": auth.isAuthenticated,
	"writer": auth.isWriter,
	"admin": auth.isAdmin,
	"public_view": auth.isPublicView,
	"authenticated_view": auth.isAuthenticatedView,
	"writer_view": auth.isWriterView,
	"admin_view": auth.isAdminView
};

(async ()=> {

	if(config.rest.child_apps) {
		for(const child_app of config.rest.child_apps) {
			console.debug(`loading child app source ${child_app.source}`)
			const child_app_source = (await import (child_app.source)).default;
			const auth_middleware = (child_app.auth && auth_levels.hasOwnProperty(child_app.auth)) ? auth_levels[child_app.auth] : auth.isPublic
			app.use(child_app.path, auth_middleware, child_app_source);
		}
	}

	app.listen(port, (err) => {
		if (err) {
			return console.log('Err',err)
		}
		console.log(`server listening on port ${port}`)
	})

})()

