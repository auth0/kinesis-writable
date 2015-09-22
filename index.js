var stream = require('stream');
var util = require('util');
var AWS = require('aws-sdk');
var immediate = require('immediate-invocation');
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
  stream.Writable.call(this);

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
    this._queueWait = setTimeout(this._sendEntries.bind(this),  this._params.buffer.timeout * 1000);
    this._params.buffer.isPrioritaryMsg = this._params.buffer.isPrioritaryMsg || _.noop;
  }

  this._kinesis = new AWS.Kinesis(_.pick(params, ['accessKeyId', 'secretAccessKey', 'region']));
}

util.inherits(KinesisStream, stream.Writable);

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
 * @return {Object} { Data, PartitionKey }
 */
KinesisStream.prototype._mapEntry = function (msg) {
  return {
    Data: msg,
    PartitionKey: this._params.partitionKey(msg)
  };
};

KinesisStream.prototype._sendEntries = function () {
  const pending_records = _.clone(this._queue);
  const self = this;
  this._queue = [];

  if (pending_records.length === 0) {
    self._queueWait = setTimeout(self._sendEntries.bind(self), self._params.buffer.timeout * 1000);
    return;
  }

  if (!self._params.streamName) {
    self.emit('error', new Error('Stream\'s name was not set.'));
  }

  var requestContent = {
    StreamName: this._params.streamName,
    Records: pending_records
  };

  self._kinesis.putRecords(requestContent, function (err, result) {
    self._queueWait = setTimeout(self._sendEntries.bind(self), self._params.buffer.timeout * 1000);
    if (err) {
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
  var self = this;

  if (!this._params.streamName) {
    return immediate (done, new Error('Stream\'s name was not set.'));
  }

  try {
    var msg = chunk.toString();
    var record = self._mapEntry(msg);

    if (this._params.buffer) {
      this._queue.push(record);

      // sends buffer when msg is prioritary
      var shouldSendEntries = this._queue.length >= this._params.buffer.length ||  // queue reached max size
                              this._params.buffer.isPrioritaryMsg(msg);            // msg is prioritary
      
      if (shouldSendEntries) {
        clearTimeout(this._queueWait);
        this._sendEntries();
      }
      return immediate(done);
    }

    self._kinesis.putRecord(_.extend({
      StreamName: self._params.streamName
    }, record), function (err) {
      done(err);
    });
  } catch (e) {
    immediate(done, e);
  }
};

KinesisStream.prototype.stop = function () {
  clearTimeout(this._queueWait);
  this._queue = [];
};

module.exports = KinesisStream;