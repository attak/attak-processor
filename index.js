
var AWS = require('aws-sdk');
var uuid = require('uuid');
var async = require('async');

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

module.exports = {
  handler: function(processor, topology, source, handlerOpts) {
    return function(event, context, callback) {
      context.topology = topology;

      if(event.Records) {
        var payload = new Buffer(event.Records[0].kinesis.data, 'base64').toString('ascii');
        event = JSON.parse(payload);
      }

      context.emit = function(topic, data, opts) {
        var nextProcs = getNext(context.topology, topic, processor);

        async.each(nextProcs, function(nextProc, done) {
          var kinesis = new AWS.Kinesis({
            region: handlerOpts.region || 'us-east-1'
          });

          var params = {
            Data: new Buffer(JSON.stringify(data)),
            StreamName: context.topology.name + '-' + processor + '-' + nextProc,
            PartitionKey: guid
          };
          
          kinesis.putRecord(params, function(err, data) {
            done();
          });
        }, function() {

        })
      }
      
      source.handler(event, context, callback);
    }
  }
}