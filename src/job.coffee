Events = require "events"

class Job extends Events.EventEmitter
  constructor: (collection, data) ->
    @collection = collection

    return @data = new JobData() unless data?
    
    # Convert plain object to JobData type
    data.__proto__ = JobData.prototype
    @data = data

  save: (callback) ->
    @collection.save @data, (err, doc) =>
      @data._id ?= doc._id if doc?
      callback err, this

  cancel: (callback) ->
    if @data.status != "queued"
      return callback(new Error("Only queued jobs may be cancelled"))

    @data.status = "cancelled"
    @data.ended = new Date()

    @save callback

  complete: (result, callback) ->
    @data.status = "complete"
    @data.ended = new Date()
    @data.result = result

    @save callback

  fail: (err, callback) ->
    @data.status = "failed"
    @data.ended = new Date()
    @data.error = err.message
    @data.stack = err.stack

    @save callback

  enqueue: (callback) ->
    @data.delay ?= new Date()
    @data.priority ?= 0
    @data.status = "queued"
    @data.enqueued = new Date()

    @save callback

  delay: (delay, callback) ->
    @data.delay = new Date(new Date().getTime() + delay)

    @enqueue callback

module.exports = Job

Function::property = (prop, desc) ->
  Object.defineProperty @prototype, prop, desc

class JobData
  @property "id", 
    get: -> @_id && @_id.toString && @_id.toString()
