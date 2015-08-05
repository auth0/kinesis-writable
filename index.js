var stream = require('stream');
var util = require('util');
var AWS = require('aws-sdk');
var _ = require('lodash');

var kinesis = new AWS.Kinesis({
  accessKeyId: 'AKIAIATMYMA4XVKJAGZQ',
  secretAccessKey: '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',
  region: 'us-west-1'
});

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
  this._params = params;
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

KinesisStream.prototype._write = function (chunk, encoding, done) {
  var self = this;
  var entry = JSON.parse(chunk.toString());

  kinesis.putRecord(_.extend({
    StreamName: self._params.streamName
  }, self._mapEntry(entry)), function (err, data) {
    if (err) {
      throw err;
    }
    done();
  });

};

module.exports = KinesisStream;