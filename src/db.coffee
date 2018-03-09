exports.index = (collection) ->
  collection.ensureIndex { status: 1, queue: 1, priority: -1, _id: 1, delay: 1 }, (err) ->
    console.error(err) if err?
