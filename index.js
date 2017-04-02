"use strict"

var fs = require('fs')
var AWS = require('aws-sdk')
var uuid = require('uuid')
var async = require('async')
var domain = require('domain')
var extend = require('extend')
var TopologyUtils = require('./topology_utils')

var getNext = function(topology, topic, current) {
  var i, len, next, ref, stream;
  next = [];
  ref = topology.streams;
  for (i = 0, len = ref.length; i < len; i++) {
    stream = ref[i];
    if (stream.from === current && (stream.topic || topic) === topic) {
      next.push(stream.to);
    }
  }
  return next;
};

var guid = uuid.v1()

var AttakProcessor = {
  utils: {
    topology: TopologyUtils
  },

  handler: function(processor, topology, source, handlerOpts) {
    return function(event, awsContext, finalCallback) {
      if (event.attakProcessorVerify) {
        return finalCallback({ok: true})
      }

      var context = awsContext.aws ? awsContext : {aws: awsContext}

      context.topology = topology
      
      var didEnd = false
      var callbackErr = undefined
      var waitingEmits = 0
      var callbackData = undefined
      var threwException = false

      function callback() {
        if (callbackData && callbackData.body) {
          var requestBody = callbackData
        } else {
          if (handlerOpts.environment === 'development') {
            var body = callbackData || callbackErr || null
            callbackErr = undefined
          } else {
            var body = callbackData || null
          }

          var requestBody = {
            body: JSON.stringify(body),
            statusCode: callbackErr ? 500 : 200
          }
        }

        console.log("HANDLER FINAL CALLBACK", callbackErr, callbackData)
        finalCallback(callbackErr, requestBody)
      }

      if(event && event.Records) {
        var payload = new Buffer(event.Records[0].kinesis.data, 'base64').toString('ascii');
        event = JSON.parse(payload);
      }

      function emitNotify() {
        waitingEmits += 1
      }

      function emitDoneNotify() {
        waitingEmits -= 1
        if (waitingEmits === 0 && didEnd) {
          callback()
        }
      }

      context.emit = AttakProcessor.getEmit(emitNotify, emitDoneNotify, processor, topology, source, handlerOpts, event, context)
      context.invoke = AttakProcessor.getInvoke(processor, topology, source, handlerOpts, event, context)
      context.invokeLocal = AttakProcessor.getInvokeLocal(processor, topology, source, handlerOpts, event, context)

      var d = domain.create()

      d.on('error', function(err) {
        callbackErr = err
        callback()
        threwException = true
      })

      d.run(function() {
        var handler = source.handler ? source.handler : source

        handler.call(source, event, context, function(err, results) {
          callbackErr = err
          callbackData = results
          if (waitingEmits === 0) {
            callback()
          } else {
            didEnd = true
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
    
      var impl = TopologyUtils.getProcessor({}, topology, target)
      var handler = AttakProcessor.handler(target, topology, impl, handlerOpts)
      var childContext = extend(true, {}, context)

      handler(data, childContext, function(err, results) {
        cb(err, results)
      })
    }
  },

  getEmit: function(emitNotify, emitDoneNotify, processor, topology, source, handlerOpts, event, context) {
    return function(topic, data, opts, cb) {
      emitNotify(topic, data, opts)

      if (cb === undefined && opts !== undefined && opts.constructor === Function) {
        cb = opts
        opts = undefined
      }

      var nextProcs = getNext(context.topology, topic, processor);
      async.each(nextProcs, function(nextProc, done) {
        var kinesis = new AWS.Kinesis({
          region: handlerOpts.region || 'us-east-1',
          endpoint: handlerOpts.endpoints.kinesis
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
          Data: new Buffer(JSON.stringify(queueData)),
          StreamName: context.topology.name + '-' + processor + '-' + nextProc,
          PartitionKey: guid,
        };
        
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
  }
}

module.exports = AttakProcessor
