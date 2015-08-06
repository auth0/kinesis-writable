var stream = require('stream');
var util = require('util');
var AWS = require('aws-sdk');
var _ = require('lodash');

/**
 * [KinesisStream description]
 * @param {Object} params
 * @param {string} [params.accessKeyId]
 * @param {string} [params.secretAccessKey]
 * @param {string} [params.region]
 * @param {string} params.streamName
 * @param {string|function} params.partitionKey
 * @param {boolean} [params.buffer=true]
 *
 */
function KinesisStream (params) {
  stream.Writable.call(this);

  this._params = _.extend({
    buffer: true
  }, params);

  if (this._params.buffer) {
    this._queue = [];
    this._queueWait = setTimeout(this._sendEntries.bind(this), 5 * 1000);
  }

  this._kinesis = new AWS.Kinesis(_.pick(params, ['accessKeyId', 'secretAccessKey', 'region']));
}

util.inherits(KinesisStream, stream.Writable);

/**
 * Map a bunyan log entry to a record structure
 * @param  {Object} entry - bunyan entry
 * @return {Object} { Data, PartitionKey }
 */
KinesisStream.prototype._mapEntry = function (entry) {
  var partitionKey = typeof this._params.partitionKey === 'function' ?
                          this._params.partitionKey(entry) :
                          this._params.partitionKey;

  return {
    Data: JSON.stringify(entry, null, 2),
    PartitionKey: partitionKey
  };
};

KinesisStream.prototype._sendEntries = function () {
  const pending_records = _.clone(this._queue);
  const self = this;
  this._queue = [];

  if (pending_records.length === 0) {
    self._queueWait = setTimeout(self._sendEntries.bind(self), 5000);
    return;
  }

  self._kinesis.putRecords({
    StreamName: this._params.streamName,
    Records: pending_records
  }, function (err) {
    if (err) {
      throw err;
    }
    self._queueWait = setTimeout(self._sendEntries.bind(self), 5000);
  });
};

KinesisStream.prototype._write = function (chunk, encoding, done) {
  var self = this;
  var entry = JSON.parse(chunk.toString());
  var record = self._mapEntry(entry);

  if (this._params.buffer) {
    this._queue.push(record);
    if (this._queue.length >= 10 || entry.level >= 40) {
      clearTimeout(this._queueWait);
      this._sendEntries();
    }
    return done();
  }

  self._kinesis.putRecord(_.extend({
    StreamName: self._params.streamName
  }, record), function (err) {
    if (err) {
      throw err;
    }
    done();
  });
};

KinesisStream.prototype.stop = function () {
  clearTimeout(this._queueWait);
  this._queue = [];
};

module.exports = KinesisStream;