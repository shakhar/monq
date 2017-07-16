Async = require "async"
Sinon = require "sinon"

Helpers = require "./helpers"
Queue = require "../src/queue"
Worker = require "../src/worker"

redisClient = require("redis").createClient()

{ expect } = require "chai"

describe "Timeout", ->
  queue = handler = worker = failed = undefined

  beforeEach ->
    queue = new Queue { db: Helpers.db }
    handler = Sinon.spy (params, callback) -> # Dont call the callback, let it timeout
    failed = Sinon.spy()

    worker = new Worker [queue], { interval: 10 }
    worker.register { timeout: handler }
    worker.on "failed", failed

  afterEach (done) ->
    Async.parallel [
      (next) -> redisClient.flushdb next
      (next) -> queue.collection.remove {}, next
    ], done

  after (done) ->
    redisClient.quit done

  describe "worker processing job with a timeout", ->
    beforeEach (done) ->
      queue.enqueue "timeout", {}, { timeout: 10 }, done

    beforeEach (done) ->
      Helpers.flushWorker worker, done

    it "calls the handler once", ->
      expect(handler.callCount).to.equal 1

    it "emits 'failed' event once", ->
      expect(failed.callCount).to.equal 1

    it "updates the job status", ->
      job = failed.lastCall.args[0]
      expect(job.status).to.equal "failed"
      expect(job.error).to.equal "timeout"

  describe "worker processing job with a timeout and retries", ->
    beforeEach (done) ->
      queue.enqueue "timeout", {}, { timeout: 10, attempts: { count: 3 } }, done

    beforeEach (done) ->
      Helpers.flushWorker worker, done

    it "calls the handler three times", ->
      expect(handler.callCount).to.equal 3

    it "updates the job status", ->
      job = failed.lastCall.args[0]
      expect(job.status).to.equal "failed"
      expect(job.error).to.equal "timeout"