Async = require "async"
Sinon = require "sinon"

Helpers = require "./helpers"
Job = require "../src/job"
Queue = require "../src/queue"
Worker = require "../src/worker"

redisClient = require("redis").createClient()

{ expect } = require "chai"

describe "Retries", ->
  queue = handler = worker = failed = undefined

  beforeEach ->
    queue = new Queue { db: Helpers.db }

    handler = Sinon.spy (params, callback) -> callback(new Error())
    failed = Sinon.spy()

    worker = new Worker [queue], { interval: 10 }
    worker.register { retry: handler }
    worker.on "failed", failed

  afterEach (done) ->
    Async.parallel [
      (next) -> redisClient.flushdb next
      (next) -> queue.collection.remove {}, next
    ], done

  after (done) ->
    redisClient.quit done

  describe "worker retrying job", ->
    beforeEach (done) ->
      queue.enqueue "retry", {}, { attempts: { count: 3 } }, done

    beforeEach (done) ->
      Helpers.flushWorker worker, done

    it "calls the handler once for each retry", ->
      expect(handler.callCount).to.equal 3

    it "emits failed once for each failed attempt", ->
      expect(failed.callCount).to.equal 3

    it "updates the job status", ->
      job = failed.lastCall.args[0]
      expect(job.attempts.remaining).to.equal 0
      expect(job.attempts.count).to.equal 3
      expect(job.status).to.equal "failed"

  describe "worker retrying job with delay", ->
    start = undefined

    beforeEach (done) ->
      queue.enqueue "retry", {}, { attempts: { count: 3, delay: 100 } }, done

    describe "after first attempt", ->
      beforeEach (done) ->
        start = new Date()
        Helpers.flushWorker worker, done

      it "calls handler once", ->
        expect(handler.callCount).to.equal 1

      it "emits 'failed' once", ->
        expect(failed.callCount).to.equal 1

      it "re-enqueues job with delay", ->
        job = failed.lastCall.args[0]
        expect(job.status).to.equal "queued"
        expect(new Date(job.delay).getTime()).to.be.at.least start.getTime() + 100

      it "does not immediately dequeue job", (done) ->
        Helpers.flushWorker worker, ->
          expect(handler.callCount).to.equal 1
          done()

    describe "after all attempts", ->
      delay = undefined

      beforeEach ->
        delay = Sinon.stub Job.prototype, "delay", (delay, callback) ->
          expect(delay).to.equal 100

          @data.delay = new Date()
          @enqueue callback

      beforeEach (done) ->
        Helpers.flushWorker worker, done

      afterEach ->
        delay.restore()

      it "calls the handler once for each retry", ->
        expect(handler.callCount).to.equal 3

      it "emits failed once for each failed attempt", ->
        expect(failed.callCount).to.equal 3

      it "updates the job status", ->
        job = failed.lastCall.args[0]
        expect(job.attempts.remaining).to.equal 0
        expect(job.attempts.count).to.equal 3
        expect(job.status).to.equal "failed"

  describe "worker retrying job with no retries", ->
    beforeEach (done) ->
      queue.enqueue "retry", {}, { attempts: { count: 0 } }, done

    beforeEach (done) ->
      Helpers.flushWorker worker, done

    it "calls the handler once", ->
      expect(handler.callCount).to.equal 1

    it "emits failed once", ->
      expect(failed.callCount).to.equal 1

    it "updates the job status", ->
      job = failed.lastCall.args[0]
      expect(job.status).to.equal "failed"