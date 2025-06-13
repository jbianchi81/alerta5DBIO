require('./setGlobal')
const { exec } = require('child_process')

exec(`fuser -k ${global.config.rest.port}/tcp`, (err, stdout, stderr) => {
  if (err) {
    console.error(`Error stopping process: ${stderr}`);
  } else {
    console.log(`Stopped process using port ${global.config.rest.port}:\n${stdout}`);
  }
});
