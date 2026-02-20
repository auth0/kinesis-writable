const util = require('util');
const assert = require('assert');
const Writable = require('stream').Writable;

const retry = require('retry');
const { KinesisClient, PutRecordsCommand } = require('@aws-sdk/client-kinesis');
const { fromNodeProviderChain } = require('@aws-sdk/credential-providers');
const { NodeHttpHandler } = require('@smithy/node-http-handler');
const merge = require('lodash.merge');
const safeStringify = require('fast-safe-stringify');

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

function isLambda() {
  return !!(
    (process.env.LAMBDA_TASK_ROOT && process.env.AWS_EXECUTION_ENV) ||
    false
  );
}

function KinesisStream (params) {
  assert(params.streamName, 'streamName required');

  if (params.kinesis && typeof params.kinesis.send !== 'function') {
    throw new Error('params.kinesis must be an AWS SDK v3 KinesisClient instance (missing .send() method)');
  }

  this.streamName = params.streamName;
  this.buffer = merge(defaultBuffer, params.buffer);
  this.partitionKey = params.partitionKey || function getPartitionKey() {
    return Date.now().toString();
  };

  this.hasPriority = this.buffer.isPrioritaryMsg || this.buffer.hasPriority;

  this.recordsQueue = [];

  // 1. Create unified HTTP Handler with proper timeout configuration
  const handlerOptions = {
    connectionTimeout: params.httpOptions?.connectionTimeout || params.httpOptions?.timeout || 5000,
    requestTimeout: params.httpOptions?.requestTimeout || 120000,
  };

  // Map v2's single agent to both httpAgent and httpsAgent for compatibility
  // (v3 requires separate agents; if user passes one, use it for both protocols)
  if (params.httpOptions?.agent) {
    handlerOptions.httpAgent = params.httpOptions.agent;
    handlerOptions.httpsAgent = params.httpOptions.agent;
  }

  const httpHandler = new NodeHttpHandler(handlerOptions);

  // 2. Build credentials based on provided parameters or default provider chain
  let credentials;
  if (params.credentials) {
    credentials = params.credentials;
  } else if (params.accessKeyId) {
    credentials = {
      accessKeyId: params.accessKeyId,
      secretAccessKey: params.secretAccessKey,
      sessionToken: params.sessionToken
    };
  } else {
    // Critical update: Pass httpHandler via clientConfig
    credentials = fromNodeProviderChain({
      clientConfig: { requestHandler: httpHandler }
    });
  }

  // 3. Create Kinesis client with proper configuration
  const clientConfig = { credentials, requestHandler: httpHandler };
  if (params.region) clientConfig.region = params.region;

  // v3 requires endpoint to include protocol (http:// or https://)
  // Auto-complete if missing to prevent TypeError [ERR_INVALID_URL]
  if (params.endpoint) {
    let safeEndpoint = params.endpoint;
    if (!/^https?:\/\//.test(safeEndpoint)) {
      safeEndpoint = 'https://' + safeEndpoint;
    }
    clientConfig.endpoint = safeEndpoint;
  }

  this.kinesis = params.kinesis || new KinesisClient(clientConfig);

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

  if (this.recordsQueue.length >= this.buffer.length || hasPriority) {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.flush();
  } else if (!this.timer) {
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
    return {
      Data: Buffer.from(safeStringify(record), 'utf8'),
      PartitionKey: String(partitionKey)
    };
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

KinesisStream.prototype.putRecords = function(records, cb) {
  this.kinesis.send(new PutRecordsCommand({
    StreamName: this.streamName,
    Records: records
  }))
    .then((result) => cb(null, result))
    .catch((err) => {
      if (!err.code && err.name) {
        err.code = err.name;
      }
      if (!err.statusCode && err.$metadata?.httpStatusCode) {
        err.statusCode = err.$metadata.httpStatusCode;
      }
      cb(err);
    });
};

KinesisStream.prototype.flush = function() {
  // reset timer so that next enqueue will start it again.
  this.timer = null;
  this.dispatch(this.recordsQueue.splice(0, this.buffer.length));
};

KinesisStream.prototype.emitRecordError = function (err, records) {
  err.records = records;
  this.emit('error', err);
};

module.exports = KinesisStream;
