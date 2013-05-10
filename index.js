var es = require('event-stream');
var Stream = require('stream').Stream;
var uuid = require('node-uuid');
var util = require('util');


var MergeStream = function(key) {
  this.key = key;
  this.currentHead = null;
  
  this.writable = true;
  this.readable = true;
  
  this.buffers = {};
  
  Stream.call(this);
};

var Passthrough = function(guid) {
  
  this.guid = guid;

  this.writable = true;
  this.readable = true;
  
  Stream.call(this);
};

util.inherits(Passthrough, Stream);
util.inherits(MergeStream, Stream);

MergeStream.prototype.write = function(chunk) {
  // Assuming object for now. Will extend later.
  var guid = chunk.guid;
  var data = chunk.data;
  
  if (!this.buffers.hasOwnProperty(guid)) {
    this.buffers[guid] = {
      buffer: [],
      buffering: true
    };
  }
  
  this.buffers[guid].buffer.push(data);
  var buffers = this.shouldMerge();
  if (buffers) {
    this.flushBuffers(buffers);
    this.resetBuffers();
  }
};

MergeStream.prototype.resetBuffers = function() {
  for (var buffer in this.buffers) {
    this.buffers[buffer].buffering = false;
  }
};

MergeStream.prototype.flushBuffers = function(buffers) {
  var nextItem = null;
  var nextKey = null;
  var nextBuffer = 0;
  
  while (this.buffersHaveLength(buffers)) {
    nextKey = buffers[0][0][this.key];
    nextItem = buffers[0][0];
    nextBuffer = 0;
    
    for (var i = 1; i < buffers.length; i++) {
      if (buffers[i].length > 0) {
        if (buffers[i][0][this.key] < nextKey) {
          nextItem = buffers[i][0];
          nextKey = buffers[i][0][this.key];
          nextBuffer = i;
        }
      }
    }
  
    
    buffers[nextBuffer] = buffers[nextBuffer].slice(1);
    this.emit('data', nextItem);
  }
};

MergeStream.prototype.buffersHaveLength = function(buffers) {
  for (var i = 0; i < buffers.length; i++) {
    if (buffers[i].length > 0) {
      return true;
    }
  }
  return false;
};

MergeStream.prototype.shouldMerge = function() {
  if (Object.keys(this.buffers).length > 1) {
    var buffers = [];
    for (var item in this.buffers) {
      if (!this.buffers[item].buffering) {
        return false;
      } else {
        buffers.push(this.buffers[item].buffer);
      }
    }
    return buffers;
  } else {
    return false;
  }
};

MergeStream.prototype.end = function(chunk) {
  if (chunk) {
    this.writa(chunk);
  }
  this.readable = this.writable = false;
  this.emit('end');
};

MergeStream.prototype.destroy = function(chunk) {
  this.readable = this.writable = false;
  this.emit('end');
};

Passthrough.prototype.write = function(chunk) {
  // Just send uuid along with data
  this.emit('data', { guid: this.guid, data: chunk });
};

Passthrough.prototype.end = function(chunk) {
  if (chunk) {
    this.writa(chunk);
  }
  this.readable = this.writable = false;
  this.emit('end');
};

Passthrough.prototype.destroy = function(chunk) {
  this.readable = this.writable = false;
  this.emit('end');
};

exports.mergeSortStream = function(streams, key) {
  var mergeStream = new MergeStream(key);
  
  for (var i = 0; i < streams.length; i++) {
    var passthrough = new Passthrough(uuid.v4());
    streams[i].pipe(passthrough);
    passthrough.pipe(mergeStream);
  }
  
  return mergeStream;
};