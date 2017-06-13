var _ = require('lodash');
var mongo = require('mongojs');
var db = require('./db');
var Job = require('./job');

module.exports = Queue;

function Queue(connection, name, options) {
    if (typeof name === 'object' && options === undefined) {
        options = name;
        name = undefined;
    }

    options || (options = {});
    options.collection || (options.collection = 'jobs');
    options.universal || (options.universal = false);

    this.connection = connection;
    this.name = name || 'default';
    this.options = options;

    this.collection = this.connection.db.collection(this.options.collection);

    if (options.index !== false) {
        db.index(this.collection);
    }
}

Queue.prototype.job = function (data) {
    return new Job(this.collection, data);
};

Queue.prototype.get = function (id, callback) {
    var self = this;

    if (typeof id === 'string') {
        id = new mongo.ObjectID(id);
    }

    var query = { _id: id };
    if (!this.options.universal) {
        query.queue = this.name;
    }

    this.collection.findOne(query, function (err, data) {
        if (err) return callback(err);

        var job = new Job(self.collection, data);
        callback(null, job);
    });
};

Queue.prototype.enqueue = function (name, params, options, callback) {
    if (!callback && typeof options === 'function') {
      callback = options;
      options = {};
    }

    var job = this.job({
        name: name,
        params: params,
        queue: this.name,
        query: parseQuery(options.query),
        attempts: parseAttempts(options.attempts),
        timeout: parseTimeout(options.timeout),
        delay: options.delay,
        priority: options.priority
    });

    job.enqueue(callback);
};

Queue.prototype.dequeue = function (options, callback) {
    var self = this;

    if (callback === undefined) {
        callback = options;
        options = {};
    }

    this.getDequeuedJobsQueries( function (err, queries) {
        if (err) return callback(err);

        var query = {
            status: Job.QUEUED,
            delay: { $lte: new Date() }
        };

        if (!self.options.universal) {
            query.queue = self.name;
        }

        if (options.minPriority !== undefined) {
            query.priority = { $gte: options.minPriority };
        }

        if (options.callbacks !== undefined) {
            var callback_names = Object.keys(options.callbacks);
            query.name = { $in: callback_names };
        }

        if(!_.isEmpty(queries)) {
            query["$and"] = queries;
        }

        var sort = { priority: -1, _id: 1 };
        var update = { $set: { status: Job.DEQUEUED, dequeued: new Date() }};

        self.collection.findAndModify({
            query: query,
            sort: sort,
            update: update,
            new: true
        }, function (err, doc) {
            if (err) return callback(err);
            if (!doc) return callback();

            callback(null, self.job(doc));
        });
    });
};

Queue.prototype.getDequeuedJobsQueries = function (callback) {
    var query = { status: Job.DEQUEUED };
    var projection = { query: 1 }
    var sort = { priority: -1 };
    var options = { sort: sort };

    this.collection.find(query, projection, options, function (err, jobs) {
        if (err) return callback(err);
        
        if (jobs.length > 0) {
            var queries =  _.chain(jobs)
                            .map(function(job) { return unescapeObject(job.query); })
                            .compact()
                            .uniqWith(_.isEqual)
                            .value();

            return callback(null, queries);
        }
        
        return callback(null, []);
    });
};

// Helpers

function parseTimeout(timeout) {
    if (timeout === undefined) return undefined;
    return parseInt(timeout, 10);
}

function parseAttempts(attempts) {
    if (attempts === undefined) return undefined;

    if (!_.isObject(attempts)) {
        throw new Error('attempts must be an object');
    }

    var result = {
        count: parseInt(attempts.count, 10)
    };

    if (attempts.delay !== undefined) {
        result.delay = parseInt(attempts.delay, 10);
        result.strategy = attempts.strategy;
    }

    return result;
}

function parseQuery(query) {
    if (query === undefined) return undefined;

    if (!_.isObject(query)) {
        throw new Error('query must be an object');
    }

    return escapeObject(query);
}

function escapeObject(obj) {
    if (_.isString(obj)) {
        return obj.replace("$", "&dollar").replace(".", "&dot");
    }

    if (_.isArray(obj)) {
        return _.map(obj, function(n) { return escapeObject(n); });
    }

    if (_.isObject(obj)) {
        return _.chain(obj)
                .mapKeys(function(v, k) { return escapeObject(k); })
                .mapValues(function(v) { return escapeObject(v); })
                .value();
    }

    return obj
}

function unescapeObject(obj) {
    if (_.isString(obj)) {
        return obj.replace("&dollar", "$").replace("&dot", ".");
    }

    if (_.isArray(obj)) {
        return _.map(obj, function(n) { return unescapeObject(n); });
    }

    if (_.isObject(obj)) {
        return _.chain(obj)
                .mapKeys(function(v, k) { return unescapeObject(k); })
                .mapValues(function(v) { return unescapeObject(v); })
                .value();
    }

    return obj
}