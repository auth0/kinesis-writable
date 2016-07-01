const stream = require('stream');
const util = require('util');
const _ = require('lodash');

/**
 * KinesisStreamPool is a pool object that acts like a writable stream. It can
 * be configured with multiple KinesisWritable streams; one of them will be the
 * `currentStream` and that will be used to actually send data to Kinesis. In
 * case of an error, `currentStream` will be switched to another one and the
 * operations will be retried. After `params.retryPrimaryInterval`, the primary
 * stream will be used again, and will continue as the `currentStream` if there
 * are no errors.
 *
 * @param {Object} params
 * @param {string} [params.streams] Streams that should be part of the pool
 * @param {object} [params.retryPrimaryInterval] (optional) Time (in ms) to retry the primary stream again after a failure
 * @param {object} [params.logger] (optional) Object responding to `info` and `error`
 */

function KinesisStreamPool (params) {
  stream.Writable.call(this, { objectMode: params.objectMode });
  this.streams = params.streams;
  this.retryPrimaryInterval = params.retryPrimaryInterval || (5 * 60 * 1000);
  this.logger = params.logger || console;
  this.debug = params.debug || false;

  this.primaryStream = _.find(this.streams, (stream) => {
    return stream.primary;
  });
  // if no stream is defined as primary, use the first one
  // in the array
  if (!this.primaryStream) {
    this.streams[0].primary = true;
    this.primaryStream = this.streams[0];
  }
  this.currentStream = this.primaryStream;
  this.recordsToRetry = [];

  // bind failover logic to each stream's 'error' event
  var self = this;
  _.map(this.streams, (stream, index) => {
    stream.streamId = index;
    stream.on('error', (err) => {
      if (err.records) {
        self.recordsToRetry = _.union(self.recordsToRetry, Array.isArray(err.records) ? err.records : [err.records]);
      }

      // new stream failing
      stream.failing = true;
      self.latestFailingStream = stream;

      // try to get primary again, but if it's failing try another
      if (!self.primaryStream.failing) {
        self.currentStream = self.primaryStream;
      } else {
        var nextStream = _.find(self.streams, (stream) => {
          return !stream.failing;
        });
        if (nextStream) self.currentStream = nextStream;
      }

      // if everything fails, emit error; otherwise, retry what
      // was in the buffer with the current (non-failing) stream
      if (self.currentStream.failing) {
        self.emit('error', err);
      } else {
        self.retrySendingRecords();
      }
    });
  });

  // retry using the primary from time to time after a failure
  setInterval(() => {
    if (!self.primaryStream.failing) return;

    // reset primary stream
    self.primaryStream.failing = false;
    self.currentStream = self.primaryStream;

    // reset other streams
    _.map(self.streams, (stream) => {
      stream.failing = false;
    });

    self.retrySendingRecords();
  }, self.retryPrimaryInterval);

  self.on('error', function () {
    var everythingIsFailing = _.every(self.streams, (stream) => {
      return stream.failing === true;
    });
    if (everythingIsFailing) {
      self.emit('poolFailure', new Error('No kinesis stream available'));
    }
  });
}

util.inherits(KinesisStreamPool, stream.Writable);

KinesisStreamPool.prototype._write = function (chunk, encoding, done) {
  if (this.debug) this.logger.info(`Writing to ${this.currentStream.streamId}`);
  this.currentStream.write(chunk, encoding, done);
};

KinesisStreamPool.prototype.retrySendingRecords = function () {
  var self = this;
  if (self.debug) self.logger.info(`Retrying sending records to ${this.currentStream.streamId}`);
  while(self.recordsToRetry.length > 0 && !self.currentStream.failing) {
    var records = self.recordsToRetry.shift();
    self.currentStream.write(records.Data ? records.Data : records);
  }
};

KinesisStreamPool.prototype.setStreamName = function (streamName) {
  this.currentStream.setStreamName(streamName);
};

KinesisStreamPool.prototype.getStreamName = function () {
  return this.currentStream.getStreamName();
};

module.exports = KinesisStreamPool;
