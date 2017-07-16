_ = require "lodash"
Mongo = require "mongojs"

Queue = require "./queue"
Worker = require "./worker"

class Connection
  constructor: (uri, options) ->
    @db = Mongo(uri, [], options)

  worker: (queues, options = {}) ->
    collection = options.collection ? "jobs"

    if queues == "*"
      options.universal = true
      queue = @queue "*", { universal: true, collection }
      return new Worker([queue], options)

    queues = [queues] unless _.isArray(queues)

    queues = queues.map (queue) ->
      if _.isString(queue)
        queue = @queue queue, { collection }
        
      queue

    return new Worker(queues, options)

  queue: (name, options) ->
    new Queue this, name, options

  close: -> 
    @db.close()

module.exports = Connection