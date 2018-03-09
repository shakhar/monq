Job = require "../src/job"
MongoClient = require("mongodb").MongoClient

{ expect } = require "chai"

uri = "mongodb://localhost:27017/monq_tests"

describe "Job", ->
  job = undefined

  before (done) ->
    MongoClient.connect uri, (err, @db) => done(err)

  after (done) ->
    @db.close done

  beforeEach ->
    @collection = @db.collection("jobs")

  afterEach (done) ->
    @collection.remove {}, {}, done

  it "has data object", ->
    job = new Job(@collection, { foo: "bar" })
    expect(job.data).to.deep.equal { foo: "bar" }

  describe "when saving", ->
    beforeEach (done) ->
      job = new Job(@collection, { foo: "bar" })
      job.save done

    it "has an '_id'", ->
      expect(job.data._id).to.exist
      expect(job.data.foo).to.equal "bar"

    it "is inserted into collection", (done) ->
      @collection.findOne { _id : job.data._id }, (err, doc) ->
        expect(err).to.not.exist
        expect(doc).to.exist
        expect(doc._id).to.deep.equal job.data._id
        expect(doc.foo).to.equal job.data.foo
        done()

    it "contains a string id", (done) ->
      @collection.findOne { _id : job.data._id }, (err, doc) ->
        expect(err).to.not.exist
        expect(doc._id.toString()).to.equal job.data.id
        done()

  describe "when updating", ->
    beforeEach (done) ->
      job = new Job(@collection, { foo: "bar" })

      job.save (err) ->
        expect(err).to.not.exist
        expect(job.data.foo).to.equal "bar"
        job.data.foo = "baz"
        job.save done

    it "has udpated data", ->
      expect(job.data.foo).to.equal "baz"

  describe "when completing", ->
    beforeEach (done) ->
      job = new Job(@collection, { foo: "bar" })
      job.complete { bar: "baz" }, done

    it "has a complete status", ->
      expect(job.data.status).to.equal "complete"

    it "has an end time", ->
      expect(job.data.ended).to.be.at.most new Date()

    it "has a result", ->
      expect(job.data.result).to.deep.equal { bar: "baz" }

  describe "when failing", ->
    beforeEach (done) ->
      job = new Job(@collection, { foo: "bar" })
      job.fail new Error("baz"), done

    it "has a failed status", ->
      expect(job.data.status).to.equal "failed"

    it "has an end time", ->
      expect(job.data.ended).to.exist
      expect(job.data.ended).to.be.at.most new Date()

    it "has an error", ->
      expect(job.data.error).to.exist
      expect(job.data.error).to.equal "baz"

    it "has a stack", ->
      expect(job.data.stack).to.exist

  describe "when cancelling a queued job", ->
    beforeEach (done) ->
      job = new Job(@collection, { foo: "bar", status: "queued" })
      job.cancel done

    it "is cancelled", ->
      expect(job.data.status).to.equal "cancelled"

  describe "when cancelling a complete job", ->
    error = undefined

    beforeEach (done) ->
      job = new Job(@collection, { foo: "bar", status: "complete" })
      job.cancel (err) ->
        error = err
        done()

    it "is returns error", ->
      expect(job.data.status).to.equal "complete"
      expect(error.message).to.equal "Only queued jobs may be cancelled"