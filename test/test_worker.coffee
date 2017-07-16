Async = require "async"
Sinon = require "sinon"

Helpers = require "./helpers"
Queue = require "../src/queue"
Worker = require "../src/worker"

redisClient = require("redis").createClient()

{ expect } = require "chai"

describe "Worker", ->
  job = queues = worker = undefined

  beforeEach ->
    job = 
      data: {}
      complete: ->
      fail: ->

    queues = ["foo", "bar", "baz"].map (name) ->
      new Queue { db: Helpers.db }, name

    worker = new Worker queues

  afterEach (done) ->
    Async.parallel [
      (next) -> redisClient.flushdb next
      (next) -> queues[0].collection.remove {}, next
    ], done

  after (done) ->
    redisClient.quit done

  it "has default polling interval", ->
    expect(worker.interval).to.equal 5000

  it "is an event emitter", (done) ->
    worker.on "foo", (bar) ->
      expect(bar).to.equal "bar"
      done()

    worker.emit "foo", "bar"

  describe "when dequeuing", ->
    foo = bar = baz = undefined

    beforeEach ->
      foo = Sinon.stub(worker.queues[0], "dequeue").yields()
      bar = Sinon.stub(worker.queues[1], "dequeue").yields()
      baz = Sinon.stub(worker.queues[2], "dequeue").yields()

      worker.dequeue ->
      worker.dequeue ->
      worker.dequeue ->
      worker.dequeue ->

    it "cycles queues", ->
      expect(foo.calledTwice).to.be.true
      expect(foo.calledBefore(bar)).to.be.true
      expect(bar.calledOnce).to.be.true
      expect(bar.calledBefore(baz)).to.be.true
      expect(baz.calledOnce).to.be.true
      expect(baz.calledBefore(foo)).to.be.true

  describe "when starting", ->
    beforeEach ->
      worker.poll = ->

    describe "when job already dequeued", ->
      beforeEach (done) ->
        Async.series [
          (next) -> worker.start next
          (next) -> worker.queues[0].enqueue "foo", {}, next
          (next) -> worker.queues[0].dequeue next
          (next) ->
            worker.queues[0].collection.find { status: "dequeued" }, (err, docs) ->
              expect(err).to.not.exist
              expect(docs[0].name).to.equal "foo"
              worker.stop()
              next()
          (next) -> worker.start next
        ], done

      it "returns dequeued job to queue", (done) ->
        worker.queues[0].collection.find { status: "queued" }, (err, docs) ->
          expect(err).to.not.exist
          expect(docs[0].name).to.equal "foo"
          done()

    describe "when job from foreign queue already dequeued", ->
      foreignQueue  = undefined

      beforeEach (done) ->
        foreignQueue = new Queue { db: Helpers.db }, "foreign"

        Async.series [
          (next) -> worker.start next
          (next) -> foreignQueue.enqueue "foo", {}, next
          (next) -> foreignQueue.dequeue next
          (next) ->
            foreignQueue.collection.find { status: "dequeued" }, (err, docs) ->
              expect(err).to.not.exist
              expect(docs[0].name).to.equal "foo"
              worker.stop()
              next()
          (next) -> worker.start next
        ], done

      it "does not return dequeued job to queue", (done) ->
        foreignQueue.collection.find { status: "queued" }, (err, docs) ->
          expect(err).to.not.exist
          expect(docs[0]).to.not.exist
          done()

  describe "when polling", ->
    describe "when error", ->
      it "emits an 'error' event", (done) ->
        error = new Error()
        
        Sinon.stub(worker, "dequeue").yields error

        worker.on "error", (err) ->
          expect(err).to.equal error
          done()

        worker.start()

    describe "when job is available", ->
        work = undefined

        beforeEach ->
          work = Sinon.stub worker, "work"
          Sinon.stub(worker.queues[0], "dequeue").yields null, job

        it "works on the job", (done) ->
          worker.start ->
            expect(work.calledOnce).to.be.true
            expect(work.getCall(0).args[0]).to.equal job
            done()

        it "emits 'dequeued' event", (done) ->
          worker.on "dequeued", (j) ->
            expect(j).to.equal job.data
            done()

          worker.start()

    describe "when no job is available", ->
      clock = undefined

      beforeEach ->                
        clock = Sinon.useFakeTimers()

        Sinon.stub(worker.queues[0], "dequeue").yields()
        Sinon.stub(worker.queues[1], "dequeue").yields()
        Sinon.stub(worker.queues[2], "dequeue").yields()

      afterEach ->
        clock.restore()

      it "waits an interval before polling again", (done) ->
        worker.start ->
          poll = Sinon.spy worker, "poll"
          clock.tick worker.interval
          worker.stop()

          expect(poll.calledOnce).to.be.true
          done()

    describe "when stopping with a job in progress", ->
        dequeueStubs = undefined

        beforeEach (done) ->
          dequeueStubs = worker.queues.map (queue) ->
            Sinon.stub(queue, "dequeue").yieldsAsync null, job

          Sinon.stub(worker, "process").yields null, "foobar"
          Sinon.stub(job, "complete").yields()

          worker.start ->
            worker.work job
            done()

        it "waits for the job to finish", (done) ->
          expect(worker.working).to.be.true

          worker.stop ->
            expect(worker.working).to.be.false
            expect(dequeueStubs[0].calledOnce).to.be.true

            # It doesn't get the stop signal until after the next dequeue is in motion
            expect(dequeueStubs[1].calledOnce).to.be.true

            # Make sure it didn't continue polling after we told it to stop
            expect(dequeueStubs[2].calledOnce).to.be.false

            expect(worker.listeners("done")).to.be.empty
            done()

    describe "when stopping during an empty dequeue", ->
      dequeueStubs = undefined

      beforeEach (done) ->
        dequeueStubs = worker.queues.map (queue) ->
          Sinon.stub(queue, "dequeue").yieldsAsync()

        worker.start done

      it "stops cleanly", (done) ->
        expect(worker.working).to.be.true

        worker.stop ->
          expect(worker.working).to.be.false
          expect(dequeueStubs[0].called).to.be.true

          # Make sure it didn't continue polling after we told it to stop
          expect(dequeueStubs[1].called).to.be.false

          expect(dequeueStubs[2].called).to.be.false
          expect(worker.listeners("done")).to.be.empty
          done()

    describe "when stopping between polls", ->
      dequeueStubs = undefined

      beforeEach (done) ->
        dequeueStubs = worker.queues.map (queue) ->
          Sinon.stub(queue, "dequeue").yieldsAsync()

        worker.start done

      it "stops cleanly", (done) ->
        expect(worker.working).to.be.true

        worker.once "empty", ->
          worker.stop ->
            expect(worker.working).to.be.false
            expect(dequeueStubs[0].called).to.be.true

            # Make sure it didn't continue polling after we told it to stop
            expect(dequeueStubs[1].called).to.be.false

            expect(dequeueStubs[2].called).to.be.false
            expect(worker.listeners("done")).to.be.empty
            done()

    describe "when stopping twice", ->
      dequeueStubs = undefined

      beforeEach (done) ->
        dequeueStubs = worker.queues.map (queue) ->
          Sinon.stub(queue, "dequeue").yieldsAsync()

        worker.start done

      it "does not error", (done) ->
        worker.stop ->
          worker.stop()
          done()

  describe "when working", ->
    describe "when processing fails", ->
      error = fail = poll = undefined

      beforeEach ->
        error = new Error()
        fail = Sinon.stub(job, "fail").yields()
        poll = Sinon.spy worker, "poll"
        Sinon.stub(worker, "process").yields error

      it "fails the job", ->
        worker.work job
        expect(fail.calledOnce).to.be.true
        expect(fail.getCall(0).args[0]).to.equal error

      it "emits 'done' event", (done) ->
        worker.on "done", (data) ->
          expect(data).to.equal job.data
          done()

        worker.work job

      it "emits 'failed' event", (done) ->
        worker.on "failed", (data) ->
          expect(data).to.equal job.data
          done()

        worker.work(job)

      it "polls for a new job", ->
        worker.work job
        expect(poll.calledOnce).to.be.true

    describe "when processing succeeds", ->
      complete = poll = undefined

      beforeEach ->
        complete = Sinon.stub(job, "complete").yields()
        poll = Sinon.spy worker, "poll"
        Sinon.stub(worker, "process").yields null, "foobar"

      it "completes the job", ->
        worker.work job

        expect(complete.calledOnce).to.be.true
        expect(complete.getCall(0).args[0]).to.equal "foobar"

      it "emits 'done' event", (done) ->
        worker.on "done", (data) ->
          expect(data).to.equal job.data
          done()

        worker.work job

      it "emits 'complete' event", (done) ->
        worker.on "complete", (data) ->
          expect(data).to.equal job.data
          done()

        worker.work job

      it "polls for a new job", ->
        worker.work job
        expect(poll.calledOnce).to.be.true

  describe "when processing", ->
    beforeEach ->
      worker.register
        example: (params, callback) -> callback(null, params)

    it "passes job to registered callback", (done) ->
      worker.process { name: "example", params: { foo: "bar" } }, (err, result) ->
        expect(result).to.deep.equal { foo: "bar" }
        done()

    it "returns error if there is no registered callback", (done) ->
      worker.process { name: "asdf" }, (err) ->
        expect(err).to.exist
        done()