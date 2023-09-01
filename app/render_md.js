'use strict'

var marked = require("marked")
var fs = require("fs")

var md = function(filename) {
    var filecontent = fs.readFileSync(filename,"utf-8")
    var html = marked(filecontent)
    return html 
}

var render_md = function(req,res) {
    var filename  = __dirname + "../public/md/" + req.path.split("/").pop().replace(/\..*$/,"") + ".md"
    if (!fs.existsSync(filename)) {
        res.status(400).send("File not found")
        return
    }
    var html = md(filename)
    res.render(html)
    return
}

module.exports = {"md":md,"render_md":render_md}