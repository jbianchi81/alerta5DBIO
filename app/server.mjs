import app, { auth } from "./rest.mjs";

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

