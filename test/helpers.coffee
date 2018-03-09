Async = require "async"

exports.each = (fixture, fn, callback) ->
  Async.each fixture, (args, next) ->
    fn.call null, args..., next
  , callback

exports.flushWorker = (worker, callback) ->
  worker.start()
  worker.once "empty", -> worker.stop(callback)
