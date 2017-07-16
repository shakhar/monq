exports.index = (collection) ->
  collection.getIndexes (err, indexes) ->
    if err?
      return if err.code == 26 # MongoError: no collection
      return console.error(err)

    dropIndex = (name) ->
      if indexes.some((index) -> index.name == name)
        collection.dropIndex name, (err) ->
          console.error(err) if err?
          
    dropIndex "status_1_queue_1_enqueued_1"
    dropIndex "status_1_queue_1_enqueued_1_delay_1"

  # Ensures there's a reasonable index for the poling dequeue
  # Status is first b/c querying by status = queued should be very selective
  collection.ensureIndex { status: 1, queue: 1, priority: -1, _id: 1, delay: 1 }, (err) ->
    console.error(err) if err?
