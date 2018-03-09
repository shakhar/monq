Async = require "async"
Sinon = require "sinon"

Helpers = require "./helpers"
Queue = require "../src/queue"
Worker = require "../src/worker"

MongoClient = require("mongodb").MongoClient
RedisClient = require("redis").createClient()

jobs = require("./fixtures/priority_jobs")

{ expect } = require "chai"

uri = "mongodb://localhost:27017/monq_tests"

describe "Priority", ->
  handler = queue = worker = undefined

  before (done) ->
    MongoClient.connect uri, (err, @db) => done(err)

  after (done) ->
    @db.close done

  beforeEach ->
    queue = new Queue { db: @db }
    handler = Sinon.spy (params, callback) -> callback()

  afterEach (done) ->
    Async.parallel [
      (next) -> RedisClient.flushdb next
      (next) -> queue.collection.remove {}, next
    ], done

  after (done) ->
    RedisClient.quit done

  describe "worker with no minimum priority", ->
    beforeEach (done) ->
      worker = new Worker [queue], { interval: 1 }
      worker.register { priority: handler }
      Helpers.each jobs, queue.enqueue.bind(queue), done

    beforeEach (done) ->
      Helpers.flushWorker worker, done

    it "calls handler once for each job", ->
      expect(handler.callCount).to.equal 9

    it "processes jobs with higher priority first", ->
      labels = handler.args.map (args) -> args[0].label
      expect(labels).to.deep.equal ["i", "h", "d", "e", "f", "g", "b", "c", "a"]

  describe "worker with minimum priority", ->
    beforeEach (done) ->
      worker = new Worker [queue], { interval: 1, minPriority: 1 }
      worker.register { priority: handler }
      Helpers.each jobs, queue.enqueue.bind(queue), done

    beforeEach (done) ->
      Helpers.flushWorker worker, done

    it "calls handler once for each job with sufficient priority", ->
      expect(handler.callCount).to.equal(2)

    it "processes jobs with higher priority first", ->
      labels = handler.args.map (args) -> args[0].label
      expect(labels).to.deep.equal ["i", "h"]
