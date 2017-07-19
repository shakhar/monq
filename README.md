monq
====

Monq is a MongoDB-backed job queue for Node.js.

Usage
-----

Connect to MongoDB by specifying a URI or providing `host`, `port` and `database` options:

```javascript
Monq = require "monq"
Client = Monq "mongodb://localhost:27017/monq_example"
```

Enqueue jobs by supplying a job name and a set of parameters.  Below, the job `reverse` is being placed into the `example` queue:

```javascript
queue = Client.queue "example"

queue.enqueue "reverse", { text: "foobar" }, (err, job) ->
  console.log "enqueued:", job.data
```

Create workers to process the jobs from one or more queues.  The functions responsible for performing a job must be registered with each worker:

```javascript
worker = Client.worker ["example"]

worker.register
  reverse: (params, callback) ->
    try
      reversed = params.text.split("").reverse().join("")
      callback null, reversed
    catch (err) 
      callback err

worker.start()
```

Create workers' lock functions to dequeue only unlocked jobs.  The functions responsible for performing a job must be registered with each worker: (The `registerLock` callbacks should return an error when lock exists)

```javascript
worker = Client.worker ["example"]

worker.register
  reverse: (params, callback) -> ...

worker.registerLock
  reverse: (job, callback) -> ...

worker.start()
```


Events
------

Workers will emit various events while processing jobs:

```javascript
worker.on "dequeued", (data) -> …
worker.on "failed", (data) -> … 
worker.on "complete", (data) -> … 
worker.on "error", (err) -> … 
```

Prequisites
-----------

Written in coffeescript, so a coffee compiler is needed to build

    npm install -g coffee-script


Install
-------

    npm install monq

Tests
-----

    npm test

You can optionally specify the MongoDB URI to be used for tests:

    MONGODB_URI=mongodb://localhost:27017/monq_tests npm test
