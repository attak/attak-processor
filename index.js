"use strict"

var fs = require('fs')
var AWS = require('aws-sdk')
var uuid = require('uuid')
var async = require('async')
var domain = require('domain')
var extend = require('extend')
var TopologyUtils = require('./topology_utils')

var getNext = function(topology, topic, current) {
  var next = []
  for (var streamName in topology.streams) {
    var stream = topology.streams[streamName]
    if (stream.from === current && (stream.topic || topic) === topic) {
      next.push(stream.to)
    }
  }

  return next
}

function stringify(obj, replacer, spaces, cycleReplacer) {
  return JSON.stringify(obj, serializer(replacer, cycleReplacer), spaces)
}

function serializer(replacer, cycleReplacer) {
  var stack = [], keys = []

  if (cycleReplacer == null) cycleReplacer = function(key, value) {
    if (stack[0] === value) return "[Circular ~]"
    return "[Circular ~." + keys.slice(0, stack.indexOf(value)).join(".") + "]"
  }

  return function(key, value) {
    if (stack.length > 0) {
      var thisPos = stack.indexOf(this)
      ~thisPos ? stack.splice(thisPos + 1) : stack.push(this)
      ~thisPos ? keys.splice(thisPos, Infinity, key) : keys.push(key)
      if (~stack.indexOf(value)) value = cycleReplacer.call(this, key, value)
    }
    else stack.push(value)

    return replacer == null ? value : replacer.call(this, key, value)
  }
}

var guid = uuid.v1()

var AttakProcessor = {
  utils: {
    topology: TopologyUtils
  },

  handler: function(processor, topology, source, handlerOpts) {
    return function(event, awsContext, finalCallback) {
      if (event.attakProcessorVerify) {
        return finalCallback(null, {ok: true})
      }

      var context = awsContext.aws ? awsContext : {aws: awsContext}
      context.topology = topology
      
      var didEnd = false
      var callbackErr = undefined
      var waitingEmits = 0
      var callbackData = undefined
      var threwException = false

      function callback() {
        if (callbackData && (callbackData.body || callbackData.headers)) {
          var requestBody = callbackData
        } else {
          if (handlerOpts.environment === 'development') {
            var body = callbackData || callbackErr || null
            callbackErr = undefined
          } else {
            var body = callbackData || null
          }

          var requestBody = {
            body: stringify(body),
            statusCode: callbackErr ? 500 : 200
          }
        }

        finalCallback(callbackErr, requestBody)
      }

      if(event && event.Records) {
        var payload = new Buffer(event.Records[0].kinesis.data, 'base64').toString('ascii');
        event = JSON.parse(payload);
      }

      function emitNotify(topic, data, opts) {
        waitingEmits += 1
      }

      function emitDoneNotify() {
        waitingEmits -= 1
        if (waitingEmits === 0 && didEnd) {
          callback()
        }
      }

      context.succeed = function(data) {
        callbackData = data
        callback()
      }

      context.fail = function(err) {
        callbackErr = err
        callback()
      }

      context.emit = AttakProcessor.getEmit(emitNotify, emitDoneNotify, processor, topology, source, handlerOpts, event, context)
      context.invoke = AttakProcessor.getInvoke(processor, topology, source, handlerOpts, event, context)
      context.invokeLocal = AttakProcessor.getInvokeLocal(processor, topology, source, handlerOpts, event, context)

      var d = domain.create()

      d.on('error', function(err) {
        console.log("CAUGHT ERROR", err, err.stack)
        callbackErr = err
        callback()
        threwException = true
      })

      d.run(function() {
        var handler = source.handler || source.impl || source
        handler.call(source, event, context, function(err, results) {
          callbackErr = err
          callbackData = results
          didEnd = true
          if (waitingEmits === 0) {
            callback()
          }
        })
      })
    }
  },

  getInvoke: function(processor, topology, source, handlerOpts, event, context) {
    return function(target, data, opts, cb) {
      if (cb === undefined && opts !== undefined && opts.constructor === Function) {
        cb = opts
        opts = undefined
      }
    }

    var lambda = new AWS.Lambda({
      region: handlerOpts.region || 'us-east-1'
    })

    var params = {
      FunctionName: target
    }

    if (data) {
      params.Payload = new Buffer(data).toString('base64')
    }

    lambda.invoke(params, function(err, results) {
      cb(err, results)
    })
  },

  getInvokeLocal: function(processor, topology, source, handlerOpts, event, context) {
    return function(target, data, opts, cb) {
      if (cb === undefined && opts !== undefined && opts.constructor === Function) {
        cb = opts
        opts = undefined
      }
    
      var impl = TopologyUtils.getProcessor({}, topology, target).impl
      var handler = AttakProcessor.handler(target, topology, impl, handlerOpts)
      var childContext = extend(true, {}, context)

      handler(data, childContext, function(err, results) {
        cb(err, results)
      })
    }
  },

  getEmit: function(emitNotify, emitDoneNotify, processor, topology, source, handlerOpts, event, context) {
    var emit = function(topic, data, opts, cb) {
      emitNotify(topic, data, opts)

      if (cb === undefined && opts !== undefined && opts.constructor === Function) {
        cb = opts
        opts = undefined
      }

      var nextProcs = getNext(context.topology, topic, processor);
      // console.log("GOT EMIT", processor, topic, data, nextProcs, context.topology, processor)
      async.each(nextProcs, function(nextProc, done) {
        var kinesis = new AWS.Kinesis({
          region: handlerOpts.region || 'us-east-1',
          endpoint: handlerOpts.services ? handlerOpts.services['AWS:Kinesis'].endpoint : undefined
        });

        var queueData = {
          data: data,
          opts: opts,
          topic: topic,
          emitTime: new Date().getTime(),
          topology: topology.name,
          processor: processor,
          sourceContext: context,
        }

        var params = {
          Data: new Buffer(stringify(queueData)),
          StreamName: context.topology.name + '-' + processor + '-' + nextProc,
          PartitionKey: guid,
        };
        
        console.log("EMIT PUT RECORD", data)
        kinesis.putRecord(params, function(err, results) {
            done(err);
          });
      }, function(err) {
        emitDoneNotify()
        if (cb) {
          cb(err)
        }
      })
    }

    if (handlerOpts.onEmit && handlerOpts.onEmit[processor]) {
      return function(topic, data, opts, cb) {
        return handlerOpts.onEmit[processor](topic, data, opts, function() {
          emit(topic, data, opts, function(err) {
            var done = cb || opts
            if (done && done.constructor === Function) {
              done(err)
            }
          })
        })
      }
    } else {
      return emit
    }
  }
}

module.exports = AttakProcessor
