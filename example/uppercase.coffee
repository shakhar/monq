module.exports = (params, callback) ->
  # Pretend this is a long running job
  # by setting a 3 second timeout
  setTimeout ->
    uppercase = params.text.toUpperCase()
    callback null, uppercase
  , 3000