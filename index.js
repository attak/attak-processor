"use strict"

var AWS = require('aws-sdk')
var uuid = require('uuid')
var async = require('async')
var domain = require('domain')

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

  handler: function(processor, topology, source, handlerOpts) {
    return function(event, context, finalCallback) {
      context.topology = topology
      
      var didEnd = false
      var callbackErr = undefined
      var waitingEmits = 0
      var callbackData = undefined
      var threwException = false

      function callback() {
        if (threwException === false) {
          console.log("CALLING BACK WITH", callbackErr, callbackData)
          console.trace()
          finalCallback(callbackErr, callbackData)
        }
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

      var d = domain.create()

      d.on('error', function(err) {
        console.log("EXCEPTION", err)
        callback(err)
        threwException = true
      })

      d.run(function() {
        var handler = source.handler ? source.handler : source

        handler.call(source, event, context, function(err, results) {
          console.log("HANDLER FINISHED", err, results, waitingEmits)
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
          endpoint: handlerOpts.kinesisEndpoint
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
