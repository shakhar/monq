Async = require "async"
Mongo = require "mongojs"

uri = process.env.MONGODB_URI ? "mongodb://localhost:27017/monq_tests"
exports.db = Mongo(uri, [], { safe: true })

exports.each = (fixture, fn, callback) ->
  Async.each fixture, (args, next) ->
    fn.call null, args..., next
  , callback

exports.flushWorker = (worker, callback) ->
  worker.start()
  worker.once "empty", -> worker.stop(callback)
