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

guid = uuid.v1()

AttakProcessor = {

  handler: function(processor, topology, source, handlerOpts) {
    return function(event, context, finalCallback) {
      context.topology = topology
      
      var didEnd = false
      var waitingEmits = 0
      var threwException = false

      function callback() {
        if (threwException === false) {
          finalCallback.apply(this, arguments)
        }
      }

      if(event.Records) {
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

      d = domain.create()

      d.on('error', function(err) {
        callback(err)
        threwException = true
      })

      d.run(function() {
        source.handler(event, context, function() {
          if (waitingEmits === 0) {
            callback()
          } else {
            didEnd = true
          }
        });
      })
    }
  },

  getEmit: function(emitNotify, emitDoneNotify, processor, topology, source, handlerOpts, event, context) {
    return function(topic, data, opts, cb) {
      emitNotify()

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
