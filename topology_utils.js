// Compiled to JS from http://github.com/attak/attak/blob/master/lib/topology.coffee
var TopologyUtils, nodePath;

nodePath = require('path');

TopologyUtils = {
  loadTopology: function(opts) {
    var file, files, i, index, j, k, len, len1, len2, name, processorPath, processors, ref, ref1, ref2, ref3, stream, topology, workingDir;
    workingDir = opts.cwd || process.cwd();
    topology = opts.topology || require(workingDir);
    if (topology.streams.constructor === Function) {
      topology.streams = topology.streams();
    }
    ref = topology.streams;
    for (index = i = 0, len = ref.length; i < len; index = ++i) {
      stream = ref[index];
      if (stream.constructor === Array) {
        topology.streams[index] = {
          from: stream[0],
          to: stream[1],
          topic: stream[2]
        };
      }
    }
    if (((ref1 = topology.processors) != null ? ref1.constructor : void 0) === Function) {
      topology.processors = topology.processors();
    } else if (((ref2 = topology.processors) != null ? ref2.constructor : void 0) === String) {
      processorPath = nodePath.resolve(workingDir, topology.processors);
      files = fs.readdirSync(processorPath);
      processors = {};
      for (j = 0, len1 = files.length; j < len1; j++) {
        file = files[j];
        if (file === '.DS_Store') {
          continue;
        }
        name = nodePath.basename(file, nodePath.extname(file));
        processors[name] = topology.processors + "/" + file;
      }
      topology.processors = processors;
    } else if (topology.processors === void 0 && topology.processor.constructor === Function) {
      processors = {};
      ref3 = topology.streams;
      for (k = 0, len2 = ref3.length; k < len2; k++) {
        stream = ref3[k];
        if (processors[stream.to] === void 0) {
          processors[stream.to] = stream.to;
        }
        if (processors[stream.from] === void 0) {
          processors[stream.from] = stream.from;
        }
      }
      topology.processors = processors;
    }
    return topology;
  },
  getProcessor: function(program, topology, name) {
    var procData, processor, source, workingDir;
    workingDir = program.cwd || process.cwd();
    if (topology.processor) {
      procData = topology.processor(name);
    } else if (topology.processors.constructor === String) {
      procData = topology.processors + "/" + name;
    } else if (topology.processors.constructor === Function) {
      procData = topology.processors()[name];
    } else {
      procData = topology.processors[name];
    }

    if (procData === undefined) {
      console.log("MISSING DATA FOR PROCESSOR", name, topology)
    }

    if (procData.constructor === String) {
      source = procData;
    } else if ((procData != null ? procData.constructor : void 0) === Function || typeof (procData != null ? procData.constructor : void 0) === 'function') {
      source = procData;
    } else {
      source = procData.source;
    }
    if (source.handler) {
      return processor = source;
    } else if (source.constructor === Function) {
      return processor = {
        handler: source
      };
    } else {
      return processor = program.processor || require(nodePath.resolve(workingDir, source));
    }
  }
};

module.exports = TopologyUtils;