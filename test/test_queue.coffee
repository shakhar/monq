Async = require "async"

Queue = require "../src/queue"

MongoClient = require("mongodb").MongoClient
RedisClient = require("redis").createClient()

{ expect } = require "chai"

uri = "mongodb://localhost:27017/monq_tests"

describe "Queue", ->
  job = queue = undefined

  before (done) ->
    MongoClient.connect uri, (err, @db) => done(err)

  after (done) ->
    @db.close done

  beforeEach ->
    queue = new Queue { db: @db }

  afterEach (done) ->
    Async.parallel [
      (next) -> RedisClient.flushdb next
      (next) -> queue.collection.remove {}, next
    ], done

  after (done) ->
    RedisClient.quit done

  describe "enqueue", ->
    beforeEach (done) ->
      queue.enqueue "foo", { bar: "baz" }, (err, j) ->
        job = j
        done err

    it "has a name", ->
      expect(job.data.name).to.equal "foo"

    it "has a queue", ->
      expect(job.data.queue).to.equal "default"

    it "has params", ->
      expect(job.data.params).to.deep.equal { bar: "baz" }

    it "has an enqueued date", ->
      expect(job.data.enqueued).to.exist
      expect(job.data.enqueued).to.be.at.most new Date()

    it "has a delay timestamp", ->
      expect(job.data.delay).to.exist
      expect(job.data.delay).to.be.at.most new Date()
      expect(job.data.delay.getTime()).to.exist

    it "has 'queued' status", ->
      expect(job.data.status).to.equal "queued"

    it "can be retrieved", (done) ->
      queue.get job.data._id.toString(), (err, doc) ->
        expect(err).to.not.exist
        expect(doc).to.exist
        expect(doc.data._id).to.deep.equal job.data._id
        expect(doc.data.foo).to.equal job.data.foo
        done()

  describe "dequeue", ->
    beforeEach (done) ->
      Async.series [
        (next) -> 
          queue.enqueue "foo1", { bar: "baz" }, next  

        (next) ->
          delay = new Date()
          delay.setFullYear delay.getFullYear() + 10
          queue.enqueue "foo2", { bar: "baz" }, { delay }, next

        (next) ->
          queue.dequeue (err, j) ->
            job = j
            next err
      ], done
      

    it "finds first job for given queue", ->
      expect(job).to.exist
      expect(job.data.name).to.equal "foo1"
      expect(job.data.queue).to.equal "default"
      expect(job.data.params).to.deep.equal { bar: "baz" }

    it "has a dequeued date", ->
      expect(job.data.dequeued).to.exist
      expect(job.data.dequeued).to.be.at.most new Date()

    it "has 'dequeued' status", ->
      expect(job.data.status).to.equal "dequeued"

    it "does not dequeue delayed job", (done) ->
      queue.dequeue (err, j) ->
        expect(err).to.not.exist
        expect(j).to.not.exist
        done()

  describe "dequeue with query", ->
    beforeEach (done) ->
      query = 
        $or: [
          { "params.bar": { $ne: "baz" } }
          { "params.bar": "baz", priority: { $gte: 2 } }
        ]

      Async.series [
        (next) -> queue.enqueue "foo1", { bar: "baz" }, { priority: 2, query }, next
        (next) -> queue.dequeue next
      ], done

    describe "when job unmatches dequeued job's query", ->
      beforeEach (done) ->
        Async.series [
          (next) -> 
            queue.enqueue "foo2", { bar: "baz" }, { priority: 1 }, next
          
          (next) -> 
            queue.dequeue (err, j) ->
              job = j
              next err
        ], done

      it "does not dequeue job", ->
        expect(job).to.not.exist

    describe "when job matches first part of dequeued job's query", ->
      beforeEach (done) ->
        Async.series [
          (next) -> 
            queue.enqueue "foo2", { bar: "baz2" }, { priority: 1 }, next

          (next) -> 
            queue.dequeue (err, j) ->
              job = j
              done err
        ], done

      it "does dequeue job", ->
        expect(job).to.exist
        expect(job.data.name).to.equal "foo2"

    describe "when job matches second part of dequeued job's query", ->
      beforeEach (done) ->
        Async.series [
          (next) -> 
            queue.enqueue "foo2", { bar: "baz" }, { priority: 2 }, next

          (next) ->
            queue.dequeue (err, j) ->
              job = j
              done err
        ], done

        it "does dequeue job", ->
          expect(job).to.exist
          expect(job.data.name).to.equal "foo2"