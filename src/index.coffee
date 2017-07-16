Connection = require "./connection"

module.exports = (uri, options) -> 
  new Connection(uri, options)