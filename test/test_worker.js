var assert = require('assert');
var async = require('async');
var sinon = require('sinon');
var helpers = require('./helpers');
var Queue = require('../lib/queue');
var Worker = require('../lib/worker');

var redisClient = require("redis").createClient()

describe('Worker', function () {
    var job, queues, worker;

    beforeEach(function () {
        job = {
            data: {},
            complete: function () {},
            fail: function () {}
        };

        queues = ['foo', 'bar', 'baz'].map(function (name) {
            return new Queue({ db: helpers.db }, name);
        });

        worker = new Worker(queues);
    });

    afterEach(function(done) {
        async.parallel([
            function(next) { redisClient.flushdb(next); },
            function(next) { queues[0].collection.remove({}, next); },
        ], done);
    });

    after(function(done) {
        redisClient.quit(done);
    });

    it('has default polling interval', function () {
        assert.equal(worker.interval, 5000);
    });

    it('is an event emitter', function (done) {
        worker.on('foo', function (bar) {
            assert.equal(bar, 'bar');
            done();
        });

        worker.emit('foo', 'bar');
    });

    describe('when dequeuing', function () {
        it('cycles queues', function () {
            var foo = sinon.stub(worker.queues[0], 'dequeue').yields();
            var bar = sinon.stub(worker.queues[1], 'dequeue').yields();
            var baz = sinon.stub(worker.queues[2], 'dequeue').yields();

            worker.dequeue(function () {});
            worker.dequeue(function () {});
            worker.dequeue(function () {});
            worker.dequeue(function () {});

            assert.ok(foo.calledTwice);
            assert.ok(foo.calledBefore(bar));
            assert.ok(bar.calledOnce);
            assert.ok(bar.calledBefore(baz));
            assert.ok(baz.calledOnce);
            assert.ok(baz.calledBefore(foo));
        });
    });

    describe('when starting', function() {
        beforeEach(function() {
            worker.poll = function() {}
        });

        describe('when job already dequeued', function() {
            beforeEach(function (done) {
                async.series([
                    function(next) { worker.start(next); },
                    function(next) { worker.queues[0].enqueue('foo', {}, next); },
                    function(next) { worker.queues[0].dequeue(next); },
                    function(next) {
                        worker.queues[0].collection.find({ status: "dequeued" }, function(err, docs){        
                            assert.equal(docs[0].name, 'foo');
                            worker.stop();
                            next();
                        });
                    },
                    function(next) { worker.start(next); }
                ], done);
            });

            it('returns dequeued job to queue', function (done) {
                worker.queues[0].collection.find({ status: "queued" }, function(err, docs){
                    assert.equal(docs[0].name, 'foo');
                    done()
                });
            });
        });

        describe('when job from foreign queue already dequeued', function() {
            beforeEach(function (done) {
                foreignQueue = new Queue({ db: helpers.db }, 'foreign');

                async.series([
                    function(next) { worker.start(next); },
                    function(next) { foreignQueue.enqueue('foo', {}, next); },
                    function(next) { foreignQueue.dequeue(next); },
                    function(next) {
                        foreignQueue.collection.find({ status: "dequeued" }, function(err, docs){
                            assert.equal(docs[0].name, 'foo');
                            worker.stop();
                            next();
                        });
                    },
                    function(next) { worker.start(next); }
                ], done);
            });

            it('does not return dequeued job to queue', function (done) {
                foreignQueue.collection.find({ status: "queued" }, function(err, docs){
                    assert.equal(docs[0], undefined);
                    done()
                });
            });
        });
    });

    describe('when polling', function () {
        describe('when error', function () {
            it('emits an `error` event', function (done) {
                var error = new Error();
                
                sinon.stub(worker, 'dequeue').yields(error);

                worker.on('error', function (err) {
                    assert.equal(err, error);
                    done();
                });

                worker.start();
            });
        });

        describe('when job is available', function () {
            var work;

            beforeEach(function () {
                work = sinon.stub(worker, 'work');

                sinon.stub(worker.queues[0], 'dequeue').yields(null, job);
            });

            it('works on the job', function (done) {
                worker.start(function(){
                    assert.ok(work.calledOnce);
                    assert.equal(work.getCall(0).args[0], job);
                    done()
                });
            });

            it('emits `dequeued` event', function (done) {
                worker.on('dequeued', function (j) {
                    assert.equal(j, job.data);
                    done();
                });

                worker.start();
            });
        });

        describe('when no job is available', function () {
            var clock;

            beforeEach(function () {
                clock = sinon.useFakeTimers();

                sinon.stub(worker.queues[0], 'dequeue').yields(null, null);
                sinon.stub(worker.queues[1], 'dequeue').yields(null, null);
                sinon.stub(worker.queues[2], 'dequeue').yields(null, null);
            });

            afterEach(function () {
                clock.restore();
            });

            it('waits an interval before polling again', function (done) {
                worker.start(function(){
                    var poll = sinon.spy(worker, 'poll');
                    clock.tick(worker.interval);
                    worker.stop();

                    assert.ok(poll.calledOnce);
                    done()
                });
            });
        });

        describe('when stopping with a job in progress', function () {
            var dequeueStubs;

            beforeEach(function (done) {
                dequeueStubs = worker.queues.map(function (queue) {
                    return sinon.stub(queue, 'dequeue').yieldsAsync(null, job);
                });

                sinon.stub(worker, 'process').yields(null, 'foobar');
                sinon.stub(job, 'complete').yields();

                worker.start(function(){
                    worker.work(job);
                    done()
                });
            });

            it('waits for the job to finish', function (done) {
                assert.ok(worker.working);

                worker.stop(function () {
                    assert.ok(!worker.working);
                    assert.ok(dequeueStubs[0].calledOnce);

                    // It doesn't get the stop signal until after the next dequeue is in motion
                    assert.ok(dequeueStubs[1].calledOnce);

                    // Make sure it didn't continue polling after we told it to stop
                    assert.ok(!dequeueStubs[2].called);

                    assert.equal(worker.listeners('done').length, 0);
                    done();
                });
            });
        });

        describe('when stopping during an empty dequeue', function () {
            var dequeueStubs;

            beforeEach(function (done) {
                dequeueStubs = worker.queues.map(function (queue) {
                    return sinon.stub(queue, 'dequeue').yieldsAsync(null, null);
                });

                worker.start(done);
            });

            it('stops cleanly', function (done) {
                assert.ok(worker.working);

                worker.stop(function () {
                    assert.ok(!worker.working);
                    assert.ok(dequeueStubs[0].called);

                    // Make sure it didn't continue polling after we told it to stop
                    assert.ok(!dequeueStubs[1].called);

                    assert.ok(!dequeueStubs[2].called);
                    assert.equal(worker.listeners('done').length, 0);
                    done();
                });
            });
        });

        describe('when stopping between polls', function () {
            var dequeueStubs;

            beforeEach(function (done) {
                dequeueStubs = worker.queues.map(function (queue) {
                    return sinon.stub(queue, 'dequeue').yieldsAsync(null, null);
                });

                worker.start(done);
            });

            it('stops cleanly', function (done) {
                assert.ok(worker.working);

                worker.once('empty', function () {
                    worker.stop(function () {
                        assert.ok(!worker.working);
                        assert.ok(dequeueStubs[0].called);

                        // Make sure it didn't continue polling after we told it to stop
                        assert.ok(!dequeueStubs[1].called);

                        assert.ok(!dequeueStubs[2].called);
                        assert.equal(worker.listeners('done').length, 0);
                        done();
                    });
                });
            });
        });

        describe('when stopping twice', function () {
            var dequeueStubs;

            beforeEach(function (done) {
                dequeueStubs = worker.queues.map(function (queue) {
                    return sinon.stub(queue, 'dequeue').yieldsAsync(null, null);
                });

                worker.start(done);
            });

            it('does not error', function (done) {
                worker.stop(function () {
                    worker.stop();
                    done();
                });
            });
        });
    });

    describe('when working', function () {
        describe('when processing fails', function () {
            var error, fail, poll;

            beforeEach(function () {
                error = new Error();
                
                fail = sinon.stub(job, 'fail').yields();
                poll = sinon.spy(worker, 'poll');

                sinon.stub(worker, 'process').yields(error);
            });

            it('fails the job', function () {
                worker.work(job);

                assert.ok(fail.calledOnce);
                assert.equal(fail.getCall(0).args[0], error)
            });

            it('emits `done` event', function (done) {
                worker.on('done', function (data) {
                    assert.equal(data, job.data);
                    done();
                });

                worker.work(job);
            });

            it('emits `failed` event', function (done) {
                worker.on('failed', function (data) {
                    assert.equal(data, job.data);
                    done();
                });

                worker.work(job);
            });

            it('polls for a new job', function () {
                worker.work(job);

                assert.ok(poll.calledOnce);
            });
        });

        describe('when processing succeeds', function () {
            var complete, poll;

            beforeEach(function () {
                complete = sinon.stub(job, 'complete').yields();
                poll = sinon.spy(worker, 'poll');

                sinon.stub(worker, 'process').yields(null, 'foobar');
            });

            it('completes the job', function () {
                worker.work(job);

                assert.ok(complete.calledOnce);
                assert.equal(complete.getCall(0).args[0], 'foobar')
            });

            it('emits `done` event', function (done) {
                worker.on('done', function (data) {
                    assert.equal(data, job.data);
                    done();
                });

                worker.work(job);
            });

            it('emits `complete` event', function (done) {
                worker.on('complete', function (data) {
                    assert.equal(data, job.data);
                    done();
                });

                worker.work(job);
            });

            it('polls for a new job', function () {
                worker.work(job);

                assert.ok(poll.calledOnce);
            });
        });
    });

    describe('when processing', function () {
        beforeEach(function () {
            worker.register({
                example: function (params, callback) {
                    callback(null, params);
                }
            });
        });

        it('passes job to registered callback', function (done) {
            worker.process({ name: 'example', params: { foo: 'bar' }}, function (err, result) {
                assert.deepEqual(result, { foo: 'bar' });
                done();
            });
        });

        it('returns error if there is no registered callback', function (done) {
            worker.process({ name: 'asdf' }, function (err, result) {
                assert.ok(err);
                done();
            });
        });
    });
});