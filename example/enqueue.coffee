Monq = require "../lib/index"

client = Monq process.env.MONGODB_URI ? "mongodb://localhost:27017/monq_example", { safe: true }
queue = client.queue "foo"

queue.enqueue "uppercase", { text: "bar" }, (err, job) ->
  throw err if err?
  console.log "Enqueued:", job.data
  process.exit()
