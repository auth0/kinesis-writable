var stream = require('stream');
var util = require('util');
var AWS = require('aws-sdk');
var _ = require('lodash');

/**
 * [KinesisStream description]
 * @param {Object} params
 * @param {string} [params.accessKeyId] AWS credentials
 * @param {string} [params.secretAccessKey] AWS credential
 * @param {string} [params.region] AWS region
 * @param {string} params.streamName AWS Knesis stream name
 * @param {string|function} params.partitionKey Constant string to use as partitionKey
 *                                              or a function that return the partitionKey
 *                                              based on a msg passed by argument
 * @param {boolean|object} [params.buffer=true]
 * @param {number} [params.buffer.timeout] Max. number of seconds
 *                                         to wait before send msgs to stream
 * @param {number} [params.buffer.length] Max. number of msgs to queue
 *                                        before send them to stream.
 * @param {@function} [params.buffer.isPrioritaryMsg] Evaluates a message and returns true
 *                                                  when msg is prioritary
 */
function KinesisStream (params) {
  stream.Writable.call(this, { objectMode: params.objectMode });

  var defaultBuffer = {
    timeout: 5,
    length: 10
  };

  this._params = _.defaultsDeep(params || {}, {
    streamName: null,
    buffer: defaultBuffer,
    partitionKey: function() {        // by default the partition key will generate 
      return Date.now().toString();   // a "random" distribution between all shards
    }
  });

  // partitionKey must be a string or a function
  if (!this._params.partitionKey ||
      typeof this._params.partitionKey !== 'string' &&
      typeof this._params.partitionKey !== 'function') {
    throw new Error("'partitionKey' property should be a string or a function.");
  }

  // if partitionKey is a string, converts it to a function that allways returns the string
  if (typeof this._params.partitionKey === 'string') {
    this._params.partitionKey = _.constant(this._params.partitionKey);
  }

  if (this._params.buffer && typeof this._params.buffer === 'boolean') {
    this._params.buffer = defaultBuffer;
  }

  if (this._params.buffer) {
    this._queue = [];
    this._queueWait = this._queueSendEntries();
    this._params.buffer.isPrioritaryMsg = this._params.buffer.isPrioritaryMsg;
  }

  this._kinesis = new AWS.Kinesis(_.pick(params, [
    'accessKeyId',
    'secretAccessKey',
    'region',
    'httpOptions']));
}

util.inherits(KinesisStream, stream.Writable);

KinesisStream.prototype._queueSendEntries = function () {
  var self = this;
  return setTimeout(function(){
    self._sendEntries();
  }, this._params.buffer.timeout * 1000);
};

KinesisStream.prototype.setStreamName = function (streamName) {

  if (!streamName || typeof streamName !== 'string') {
    throw new Error('\'streamName\' must be a valid string.');
  }

  this._params.streamName = streamName;
};

KinesisStream.prototype.getStreamName = function () {

  return this._params.streamName;
};
/**
 * Map a msg to a record structure
 * @param  {@} msg -  entry
 * @return {Record} { Data, PartitionKey, StreamName }
 */
KinesisStream.prototype._mapEntry = function (msg, buffer) {
  if (buffer){
    return new BufferedRecord(msg, this._params.partitionKey(msg));
  } else {
    return new NonBufferedRecord(msg, this._params.partitionKey(msg), this._params.streamName);
  }
};

var BufferedRecord = function(data, pk){
  this.Data = data;
  this.PartitionKey = pk;
};

var NonBufferedRecord = function(data, pk, streamName){
  this.Data = data;
  this.PartitionKey = pk;
  this.StreamName = streamName;
};

KinesisStream.prototype._sendEntries = function () {
  const pending_records = this._queue;
  const self = this;
  this._queue = [];

  if (pending_records.length === 0) {
    this._queueWait = this._queueSendEntries();
    return;
  }

  if (!self._params.streamName) {
    this.emit('error', new Error('Stream\'s name was not set.'));
  }

  var requestContent = {
    StreamName: this._params.streamName,
    Records: pending_records
  };

  this._kinesis.putRecords(requestContent, function (err, result) {
    self._queueWait = self._queueSendEntries();
    if (err) {
      err.streamName = requestContent.StreamName;
      err.records = requestContent.Records;
      return self.emit('error', err);
    }

    if (result && result.FailedRecordCount) {
      result.Records
      .forEach(function (recordResult, index) {
        if (recordResult.ErrorCode) {
          recordResult.Record = requestContent.Records[index];
          self.emit('errorRecord', recordResult);
        }
      });
    }
  });
};

KinesisStream.prototype._write = function (chunk, encoding, done) {
  if (!this._params.streamName) {
    return setImmediate (done, new Error('Stream\'s name was not set.'));
  }

  var isPrioMessage = this._params.buffer.isPrioritaryMsg;

  try {
    var obj, msg;
    if (Buffer.isBuffer(chunk)) {
      msg = chunk;
      if (isPrioMessage){
        if (encoding === 'buffer'){
          obj = JSON.parse(chunk.toString());
        } else {
          obj = JSON.parse(chunk.toString(encoding));
        }
      }
    } else if (typeof chunk === 'string') {
      msg = chunk;
      if (isPrioMessage){
        obj = JSON.parse(chunk);
      }
    } else {
      obj = chunk;
      msg = JSON.stringify(obj);
    }

    var record = this._mapEntry(msg, !!this._params.buffer);

    if (this._params.buffer) {
      this._queue.push(record);

      // sends buffer when current chunk is for prioritary entry
      var shouldSendEntries = this._queue.length >= this._params.buffer.length ||  // queue reached max size
                              (isPrioMessage && isPrioMessage(obj));            // msg is prioritary

      if (shouldSendEntries) {
        clearTimeout(this._queueWait);
        this._sendEntries();
      }
      return setImmediate(done);
    }

    this._kinesis.putRecord(record, function (err) {
      if (err) {
        err.streamName = record.StreamName;
        err.records = [ _.omit(record, 'StreamName') ];
      }
      done(err);
    });
  } catch (e) {
    setImmediate(done, e);
  }
};

KinesisStream.prototype.stop = function () {
  clearTimeout(this._queueWait);
  this._queue = [];
};

module.exports = KinesisStream;