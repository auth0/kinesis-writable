const util = require('util');
const assert = require('assert');
const Writable = require('stream').Writable;

const retry = require('retry');
const AWS = require('aws-sdk');
const merge = require('lodash.merge');
const cb = require('cb');

/**
 * [KinesisStream description]
 * @param {Object} params
 * @param {string} [params.accessKeyId] AWS access key
 * @param {string} [params.secretAccessKey] AWS secret
 * @param {string} [params.sessionToken] AWS session token
 * @param {string} [params.credentials] AWS credentials (in lieu of separate credentials)
 * @param {string} [params.region] AWS region
 * @param {string} [params.endpoint] AWS HTTP endpoint
 * @param {string} [params.objectMode] True if Javascript objects can be directly written to Kinesis
 *                                     (instead of strings)
 * @param {string} params.streamName AWS Knesis stream name
 * @param {function} params.partitionKey function that return the partitionKey based on a msg passed by argument
 * @param {object} [params.httpOptions={}] HTTP options that will be used on `aws-sdk` (e.g. timeout values)
 * @param {number} [params.buffer.timeout] Max. number of seconds
 *                                         to wait before send msgs to stream
 * @param {number} [params.buffer.length] Max. number of msgs to queue
 *                                        before send them to stream.
 * @param {@function} [params.buffer.isPrioritaryMsg] Evaluates a message and returns true if msg has priority (to be deprecated)
 * @param {@function} [params.buffer.hasPriority] Evaluates a message and returns true if msg has priority
 * @param {@function} [params.buffer.retry.retries] Attempts to be made to flush a batch
 * @param {@function} [params.buffer.retry.minTimeout] Min time to wait between attempts
 * @param {@function} [params.buffer.retry.maxTimeout] Max time to wait between attempts
 */

const defaultBuffer = {
  timeout: 5,
  length: 10,
  hasPriority: function() {
    return false;
  },
  retry: {
    retries: 2,
    minTimeout: 300,
    maxTimeout: 500
  }
};

function KinesisStream (params) {
  assert(params.streamName, 'streamName required');

  this.streamName = params.streamName;
  this.buffer = merge(defaultBuffer, params.buffer);
  this.partitionKey = params.partitionKey || function getPartitionKey() {
    return Date.now().toString();
  };

  this.hasPriority = this.buffer.isPrioritaryMsg || this.buffer.hasPriority;

  // increase the timeout to get credentials from the EC2 Metadata Service
  AWS.config.credentials = new AWS.EC2MetadataCredentials({
    httpOptions: { timeout: 5000 }
  });

  this.recordsQueue = [];

  this.kinesis = params.kinesis || new AWS.Kinesis({
    accessKeyId: params.accessKeyId,
    secretAccessKey: params.secretAccessKey,
    sessionToken: params.sessionToken,
    credentials: params.credentials,
    region: params.region,
    endpoint: params.endpoint,
    objectMode: params.objectMode,
    httpOptions: params.httpOptions
  });

  Writable.call(this, { objectMode: params.objectMode });
}

util.inherits(KinesisStream, Writable);

function parseChunk(chunk) {
  if (Buffer.isBuffer(chunk) ) {
    chunk = chunk.toString();
  }
  if (typeof chunk === 'string') {
    chunk = JSON.parse(chunk);
  }
  return chunk;
}

KinesisStream.prototype._write = function(chunk, enc, next) {
  chunk = parseChunk(chunk);

  const hasPriority = this.hasPriority(chunk);
  if (hasPriority) {
    this.recordsQueue.unshift(chunk);
  } else {
    this.recordsQueue.push(chunk);
  }

  if (this.timer) {
    clearTimeout(this.timer);
  }

  if (this.recordsQueue.length >= this.buffer.length || hasPriority) {
    this.flush();
  } else {
    this.timer = setTimeout(this.flush.bind(this), this.buffer.timeout * 1000);
  }

  return next();
};

KinesisStream.prototype.dispatch = function(records, cb) {
  if (records.length === 0) {
    return cb ? cb() : null;
  }

  const operation = retry.operation(this.buffer.retry);

  const formattedRecords = records.map((record) => {
    const partitionKey = typeof this.partitionKey === 'function'
	  ? this.partitionKey(record)
	  : this.partitionKey;
    return { Data: JSON.stringify(record), PartitionKey: partitionKey };
  });

  operation.attempt(() => {
    this.putRecords(formattedRecords, (err) => {
      if (operation.retry(err)) {
        return;
      }

      if (err) {
        this.emitRecordError(err, records);
      }

      if (cb) {
        return cb(err ? operation.mainError() : null);
      }
    });
  });
};

KinesisStream.prototype.putRecords = function(records, callback) {
  callback = cb(callback).once();

  const req = this.kinesis.putRecords({
    StreamName: this.streamName,
    Records: records
  }, callback);

  // remove all listeners which end up leaking
  req.on('complete', function() {
    req.removeAllListeners();
    req.response.httpResponse.stream && req.response.httpResponse.stream.removeAllListeners();
    if (req.httpRequest.stream) {
      req.httpRequest.stream.removeAllListeners();
      req.httpRequest.stream.once('error', () => {});
    }
  });
};

KinesisStream.prototype.flush = function() {
  this.dispatch(this.recordsQueue.splice(0, this.buffer.length));
};

KinesisStream.prototype.emitRecordError = function (err, records) {
  err.records = records;
  this.emit('error', err);
};

module.exports = KinesisStream;
