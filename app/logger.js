var colors = require('colors');

module.exports = {
  error: message => console.error(colors.red(message)),
  warn: message => console.log(message.yellow),
  info: message => console.log(message.green),
  debug: message => console.log(message.blue),
};