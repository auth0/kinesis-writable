var stream = require('stream');
var util = require('util');
var AWS = require('aws-sdk');
var retry = require('retry');
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
 * @param {object} [params.getCredentialsFromIAMRole=false] Explictly tells `aws-sdk` to get credentials using an IAM role
 * @param {object} [params.httpOptions={}] HTTP options that will be used on `aws-sdk` (e.g. timeout values)
 * @param {boolean|object} [params.buffer=true]
 * @param {number} [params.buffer.timeout] Max. number of seconds
 *                                         to wait before send msgs to stream
 * @param {number} [params.buffer.length] Max. number of msgs to queue
 *                                        before send them to stream.
 * @param {number} [params.buffer.maxBatchSize] Max. size in bytes of the batch sent to Kinesis. Default 5242880 (5MiB)
 * @param {@function} [params.buffer.isPrioritaryMsg] Evaluates a message and returns true
 *                                                  when msg is prioritary
 * @param {object} [params.retryConfiguration={}]
 * @param {number} [params.retryConfiguration.retries=0] Number of retries to perform after a failed attempt
 * @param {number} [params.retryConfiguration.factor=2] The exponential factor to use
 * @param {number} [params.retryConfiguration.minTimeout=1000] The number of milliseconds before starting the first retry
 * @param {boolean} [params.retryConfiguration.randomize=false] Randomizes the timeouts by multiplying with a factor between 1 to 2
 */

var MAX_BATCH_SIZE = 5*1024*1024 - 1024; // 4.99MiB

function KinesisStream (params) {
  stream.Writable.call(this, { objectMode: params.objectMode });

  var defaultBuffer = {
    timeout: 5,
    length: 10,
    maxBatchSize: MAX_BATCH_SIZE
  };

  this._retryConfiguration = params.retryConfiguration || {
    retries: 0,
    factor: 2,
    minTimeout: 1000,
    randomize: false
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

  if (params.getCredentialsFromIAMRole) {
    // increase the timeout to get credentials from the EC2 Metadata Service
    AWS.config.credentials = new AWS.EC2MetadataCredentials({
      httpOptions: params.httpOptions || { timeout: 5000 }
    });
  }

  this._logger = console;

  this._kinesis = new AWS.Kinesis(_.pick(params, [
    'accessKeyId',
    'secretAccessKey',
    'region',
    'httpOptions']));
}

util.inherits(KinesisStream, stream.Writable);

KinesisStream.prototype._emitError = function (records, err, attempts) {
  err.records = records;
  err.attempts = attempts;
  this.emit('error', err);
};

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
  this._batch_size = 0;

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

  this._putRecords(requestContent);
};

KinesisStream.prototype._putRecords = function(requestContent) {
  const self = this;

  var operation = this._getRetryOperation();
  operation.attempt(function(currentAttempt) {
    try {
      var req = self._kinesis.putRecords(requestContent, function (err, result) {
        try {
          self._queueWait = self._queueSendEntries();
          if (err) {
            if (!err.records) err.records = requestContent.Records;
            throw err;
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
        } catch(err) {
          if (operation.retry(err)) {
            return;
          } else {
            self._emitError(requestContent, err, currentAttempt);
          }
        }
      })
      .on('complete', function() {
        req.removeAllListeners();
        var response_stream = req.response.httpResponse.stream;
        if (response_stream) {
          response_stream.removeAllListeners();
        }
        var request_stream = req.httpRequest.stream;
        if (request_stream) {
          request_stream.removeAllListeners();
        }
      });
    } catch(err) {
      if (operation.retry(err)) {
        return;
      } else {
        self._emitError(requestContent, err, currentAttempt);
      }
    }
  });
};

KinesisStream.prototype._retryValidRecords = function(requestContent, err) {
  const self = this;

  // By default asumes that all records failed
  var failedRecords = requestContent.Records;

  // try to find within the error, which records have failed.
  var failedRecordIndexes = getRecordIndexesFromError(err);

  if (failedRecordIndexes.length > 0) {

    // failed records found, extract them from collection of records
    failedRecords = _.pullAt(requestContent.Records, failedRecordIndexes);

    // now, try one more time with records that didn't fail.
    self._putRecords({
      StreamName: requestContent.StreamName,
      Records: requestContent.Records
    });
  }

  // send error with failed records
  err.streamName = requestContent.StreamName;
  err.records = failedRecords;
  self.emit('error', err);
};

KinesisStream.prototype._write = function (chunk, encoding, done) {
  var self = this;
  if (!this._params.streamName) {
    return setImmediate (done, new Error('Stream\'s name was not set.'));
  }

  var isPrioMessage = this._params.buffer.isPrioritaryMsg;

  var operation = this._getRetryOperation();
  operation.attempt(function(currentAttempt) {
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

      var record = self._mapEntry(msg, !!self._params.buffer);

        if (self._params.buffer) {

        // sends buffer when current current record will exceed bmax batch size
        self._batch_size = (self._batch_size || 0) + msg.length;
        if (self._batch_size > self._params.buffer.maxBatchSize) {
          clearTimeout(self._queueWait);
          self._sendEntries();
        }

        self._queue.push(record);

        // sends buffer when current chunk is for prioritary entry
        var shouldSendEntries = self._queue.length >= self._params.buffer.length ||  // queue reached max size
                                (isPrioMessage && isPrioMessage(obj));            // msg is prioritary

        if (shouldSendEntries) {
          clearTimeout(self._queueWait);
          self._sendEntries();
        }
        return setImmediate(done);
      }

      var req = self._kinesis.putRecord(record, function (err) {
        if (err) {
          err.streamName = record.StreamName;
          err.records = [ _.omit(record, 'StreamName') ];
        }
        throw err;
      })
      .on('complete', function() {
        req.removeAllListeners();
        req.response.httpResponse.stream.removeAllListeners();
        req.httpRequest.stream.removeAllListeners();
      });
    } catch(err) {
      if (operation.retry(err)) {
        return;
      } else {
        self._emitError(err.records, err, currentAttempt);
        setImmediate(done, err);
      }
    }
  });
};

KinesisStream.prototype.stop = function () {
  clearTimeout(this._queueWait);
  this._queue = [];
};

KinesisStream.prototype._getRetryOperation = function () {
  if (this._retryConfiguration && this._retryConfiguration.retries > 0) {
    return retry.operation(this._retryConfiguration);
  } else {
    return {
      attempt: function(cb) {
        return cb(1);
      },
      retry: function () {
        return false;
      }
    };
  }
};

module.exports = KinesisStream;
module.exports.pool = require('./pool');


const RECORD_REGEXP = /records\.(\d+)\.member\.data/g;

function getRecordIndexesFromError (err) {
  var matches = [];
  if (!err) return matches;

  if (err && _.isString(err.message)) {
    var match = RECORD_REGEXP.exec(err.message);
    while (match !== null) {
      matches.push(parseInt(match[1], 10) - 1);
      match = RECORD_REGEXP.exec(err.message);
    }
  }

  return matches;
}
