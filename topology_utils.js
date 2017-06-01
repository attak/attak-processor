var TopologyUtils, nodePath;

nodePath = require('path');

TopologyUtils = {
  loadTopology: function(opts) {
    var file, files, i, len, name, objDefs, procData, procName, processorPath, processors, ref, ref1, ref2, ref3, ref4, ref5, ref6, ref7, ref8, stream, streamName, streamsObj, topology, workingDir;
    workingDir = opts.cwd || process.cwd();
    if (opts.topology) {
      if (opts.topology.constructor === String) {
        topology = require(nodePath.resolve(workingDir, opts.topology));
      } else {
        topology = opts.topology;
      }
    } else {
      topology = require(workingDir);
    }
    if (((ref = topology.streams) != null ? ref.constructor : void 0) === Function) {
      topology.streams = topology.streams();
    }
    if (((ref1 = topology.streams) != null ? ref1.constructor : void 0) === Array) {
      streamsObj = {};
      ref2 = topology.streams || {};
      for (streamName in ref2) {
        stream = ref2[streamName];
        if (stream.constructor === Array) {
          stream = {
            from: stream[0],
            to: stream[1],
            topic: stream[2]
          };
        }
        streamName = topology.name + "-" + stream.from + "-" + stream.to;
        streamsObj[streamName] = stream;
      }
      topology.streams = streamsObj;
    }
    if (((ref3 = topology.processors) != null ? ref3.constructor : void 0) === Function) {
      topology.processors = topology.processors();
    } else if (((ref4 = topology.processors) != null ? ref4.constructor : void 0) === String) {
      processorPath = nodePath.resolve(workingDir, topology.processors);
      files = fs.readdirSync(processorPath);
      processors = {};
      for (i = 0, len = files.length; i < len; i++) {
        file = files[i];
        if (file === '.DS_Store') {
          continue;
        }
        name = nodePath.basename(file, nodePath.extname(file));
        processors[name] = topology.processors + "/" + file;
      }
      topology.processors = processors;
    } else if (topology.processors === void 0 && ((ref5 = topology.processor) != null ? ref5.constructor : void 0) === Function) {
      processors = {};
      ref6 = topology.streams;
      for (streamName in ref6) {
        stream = ref6[streamName];
        if (processors[stream.to] === void 0) {
          processors[stream.to] = stream.to;
        }
        if (processors[stream.from] === void 0) {
          processors[stream.from] = stream.from;
        }
      }
      topology.processors = processors;
    }
    ref7 = topology.processors || {};
    for (procName in ref7) {
      procData = ref7[procName];
      if (procData.constructor === String) {
        topology.processors[procName] = {
          path: procData
        };
      }
    }
    if (((ref8 = topology.api) != null ? ref8.constructor : void 0) === String) {
      objDefs = {
        handler: topology.api
      };
      topology.api = objDefs;
    }
    if (topology["static"]) {
      if (topology["static"].constructor === String) {
        topology["static"] = {
          "default": {
            dir: topology["static"]
          }
        };
      } else if (topology["static"].dir) {
        topology["static"] = {
          "default": topology["static"]
        };
      }
    }
    return topology;
  },
  getProcessor: function(opts, topology, name) {
    var loading, procData, workingDir;
    workingDir = opts.cwd || process.cwd();
    if (topology.processor) {
      procData = topology.processor(name);
    } else if (topology.processors.constructor === String) {
      procData = topology.processors + "/" + name;
    } else if (topology.processors.constructor === Function) {
      procData = topology.processors()[name];
    } else {
      procData = topology.processors[name];
    }
    if (procData === void 0) {
      throw new Error("Failed to find processor " + name);
    }
    loading = {};
    if (procData.constructor === String) {
      loading = {
        type: 'path',
        path: procData
      };
    } else if ((procData != null ? procData.constructor : void 0) === Function) {
      loading = {
        type: 'dynamic',
        impl: procData
      };
    } else if ((procData != null ? procData.handler : void 0) === Function) {
      loading = {
        type: 'dynamic',
        impl: procData.handler
      };
    } else if ((procData != null ? procData.source : void 0) || (procData != null ? procData.path : void 0)) {
      loading = {
        type: 'path',
        path: procData.path
      };
    } else {
      loading = {
        type: 'dynamic',
        impl: procData
      };
    }
    switch (loading.type) {
      case 'path':
        loading.impl = opts.processor || require(nodePath.resolve(workingDir, loading.path));
    }
    return loading;
  }
};

module.exports = TopologyUtils;