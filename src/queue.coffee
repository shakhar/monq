_ = require "lodash"
Async = require "async"
Mewtwo = require "mewtwo"
Mongo = require "mongojs"

Db = require "./db"
Job = require "./job"

class Queue
  constructor: (connection, name, options) ->
    if _.isObject(name) && !options?
      options = name
      name = undefined

    options ?= {}
    options.collection ?= "jobs"
    options.universal ?= false

    @connection = connection
    @name = name ? "default"
    @options = options
    @collection = @connection.db.collection(@options.collection)

    @mewtwo = new Mewtwo
      log: false,
      timeout: options.lockTimeout ? 10000

    if options.index != false
      Db.index @collection

  job: (data) ->
    new Job @collection, data

  get: (id, callback) ->
    id = new Mongo.ObjectID(id) if _.isString(id)

    query = { _id: id }
    query.queue = @name unless @options.universal

    @collection.findOne query, (err, data) =>
      return callback(err) if err?

      job = new Job(@collection, data)
      callback(null, job)

  enqueue: ([name, params, options = {}]..., callback) ->
    job = @job
      name: name
      params: params
      queue: @name
      query: parseQuery(options.query)
      attempts: parseAttempts(options.attempts)
      timeout: parseTimeout(options.timeout)
      delay: options.delay
      priority: options.priority

    job.enqueue callback

  dequeue: ([options = {}]..., callback) ->
    dequeueLock = undefined

    Async.waterfall [
      (next) => 
        @mewtwo.acquire "dequeue", (err, lock) ->
          dequeueLock = lock
          next err

      (next) =>
        @_getDequeuedJobsQueries next

      (queries, next) =>
        query = 
          status: "queued"
          delay: { $lte: new Date() }

        query.queue = @name unless @options.universal
        query.priority = { $gte: options.minPriority } if options.minPriority?
        query.name = { $in: Object.keys(options.callbacks) } if options.callbacks?
        query["$and"] = queries unless _.isEmpty(queries)

        sort = { priority: -1, _id: 1 }
        update = { $set: { status: "dequeued", dequeued: new Date() } }

        @collection.findAndModify { query, sort, update, new: true }, next

    ], (error, doc) =>
      # Release lock even when an error occurs
      @mewtwo.release dequeueLock, (err = error) =>
        return callback(err) if err?
        return callback() unless doc?
        callback err, @job(doc)

  _getDequeuedJobsQueries: (callback) ->
    query = { status: "dequeued" }
    projection = { query: 1 }
    sort = { priority: -1 }
    options = { sort }

    @collection.find query, projection, options, (err, jobs) ->
      return callback(err) if err?
      return callback(null, []) if _.isEmpty(jobs)

      queries = _.chain(jobs)
        .map (job) -> unescapeObject(job.query)
        .compact()
        .uniqWith _.isEqual
        .value()

      callback(null, queries)      

module.exports = Queue

# Helpers

parseTimeout = (timeout) ->
  return unless timeout?
  parseInt timeout, 10

parseAttempts = (attempts) ->
  return unless attempts?
  throw new Error("attempts must be an object") unless _.isObject(attempts)

  result =
    count: parseInt(attempts.count, 10)

  if attempts.delay?
    result.delay = parseInt(attempts.delay, 10)
    result.strategy = attempts.strategy

  result

parseQuery = (query) ->
  return unless query?
  throw new Error("query must be an object") unless _.isObject(query)

  escapeObject query

escapeObject = (obj) ->
  if _.isString(obj)
    return obj.replace("$", "&dollar").replace ".", "&dot"

  if _.isArray(obj)
    return _.map(obj, escapeObject)

  if _.isObject(obj)
    return _.chain(obj)
      .mapKeys (v, k) -> escapeObject(k)
      .mapValues(escapeObject)
      .value()
  
  obj

unescapeObject = (obj) ->
  if _.isString(obj)
    return obj.replace("&dollar", "$").replace "&dot", "."

  if _.isArray(obj)
    return _.map(obj, unescapeObject)

  if _.isObject(obj)
    return _.chain(obj)
      .mapKeys (v, k) -> unescapeObject(k)
      .mapValues(unescapeObject)
      .value()

  obj
