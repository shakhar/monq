_ = require "lodash"
Async = require "async"
Events = require "events"

Queue = require "./queue"

class Worker extends Events.EventEmitter
  constructor: (queues = [], options = {}) ->
    @empty = 0
    @queues = queues

    @interval = options.interval ? 5000
    @callbacks = options.callbacks ? {}
    @lockCallbacks = options.lockCallbacks ? {}
    @strategies = options.strategies ? {}
    @universal = options.universal ? false

    # Default retry strategies
    @strategies.linear ?= @_linear
    @strategies.exponential ?= @_exponential

    # This worker will only process jobs of this priority or higher
    @minPriority = options.minPriority

  register: (callbacks) ->
    _.forEach callbacks, (callback, name) =>
      @callbacks[name] = callback

  registerLock: (lockCallbacks) ->
    _.forEach lockCallbacks, (lockCallback, name) =>
      @lockCallbacks[name] = lockCallback

  strategies: (strategies) ->
    _.forEach strategies, (strategy, name) =>
      @strategies[name] = strategy

  start: (callback) ->
    if _.isEmpty(@queues)
      return setTimeout @start.bind(this), @interval

    # returns to queue stucked dequeued jobs from unexpected restart
    @_enqueueDequeuedJobs @queues, (err) =>
      @working = true
      @poll()
      callback?(err)

  stop: (callback = ->) ->
    callback() unless @working

    @working = false

    if @pollTimeout
      clearTimeout @pollTimeout
      @pollTimeout = null
      return callback()

    @once "stopped", callback

  addQueue: (queue) ->
    @queues.push(queue) unless @universal

  poll: ->
    return @emit("stopped") unless @working

    @dequeue (err, job) =>
      return @emit("error", err) if err?

      if job? 
        @empty = 0
        @emit "dequeued", job.data
        return @work(job)

      @emit("empty")

      @empty++ if @empty < @queues.length

      return @poll() unless @empty == @queues.length
        
      # All queues are empty, wait a bit
      @pollTimeout = setTimeout =>
        @pollTimeout = null
        @poll()
      , @interval

  dequeue: (callback) ->
    queue = @queues.shift()
    @queues.push queue

    queue.dequeue
      minPriority: @minPriority
      callbacks: @callbacks
      lockCallbacks: @lockCallbacks
    , callback

  work: (job) ->
    finished = false

    done = (err, result) =>
      # It's possible that this could be called twice in the case that a job times out,
      # but the handler ends up finishing later on
      return if finished

      finished = true

      clearTimeout timer
      @emit "done", job.data

      if err?
        return @error job, err, (err) =>
          return @emit("error", err) if err?
          @emit "failed", job.data
          @poll()

      job.complete result, (err) =>
        return @emit("error", err) if err?
        @emit "complete", job.data
        @poll()

    if job.data.timeout?
      timer = setTimeout =>
        done.call this, new Error("timeout")
      , job.data.timeout

    @process job.data, done

  process: (data, callback) ->
    func = @callbacks[data.name]

    unless func?
      return callback(new Error("No callback registered for '#{data.name}'"))
        
    func data.params, callback

  error: (job, err, callback) ->
    attempts = job.data.attempts
    remaining = 0

    if attempts?
      remaining = attempts.remaining = (attempts.remaining ? attempts.count) - 1

    return job.fail(err, callback) if remaining <= 0

    if remaining > 0
      strategy = @strategies[attempts.strategy ? "linear"]

      unless strategy
        strategy = @_linear
        console.error "No such retry strategy: '#{attempts.strategy}'"
        console.error "Using linear strategy"

      wait = if attempts.delay? then strategy(attempts) else 0

      job.delay wait, callback

  _enqueueDequeuedJobs: (queues, callback) ->
    update = { $set: { status: "queued" } }
    options = { multi: true }

    Async.each queues, (queue, next) ->
      query = { queue: queue.name, status: "dequeued" }
      queue.collection.update query, update, options, next
    , callback

  # Strategies
  # ---------------
  
  _linear: (attempts) -> 
    attempts.delay

  _exponential: (attempts) ->
    attempts.delay * (attempts.count - attempts.remaining)

module.exports = Worker

