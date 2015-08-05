const KinesisStream = require('../');
const AWS = require('aws-sdk');
const assert = require('chai').assert;
const _ = require('lodash');

const kinesis = new AWS.Kinesis({
  region: 'us-west-1'
});

const STREAM_NAME   = 'jose_test';

function get_iterator (callback) {
  kinesis.describeStream({
    StreamName: STREAM_NAME
  }, function (err, stream) {
    if (err) return callback(err);
    var params = {
      ShardId: stream.StreamDescription.Shards[0].ShardId,
      ShardIteratorType: 'LATEST',
      StreamName: STREAM_NAME
    };
    kinesis.getShardIterator(params, callback);
  });
}

describe('with buffering', function () {
  var iterator;

  beforeEach(function (done) {
    get_iterator(function (err, data) {
      if (err) return done(err);
      iterator = data.ShardIterator;
      done();
    });
  });

  it('should not immediately send the message when buffering is on', function (done) {
    var bk = new KinesisStream({
      streamName: STREAM_NAME,
      partitionKey: 'test-123'
    });

    var log_entry = JSON.stringify({foo: 'bar'});
    bk._write(log_entry, null, function () {});

    kinesis.getRecords({
      ShardIterator: iterator,
      Limit: 1
    }, function (err, data) {
      bk.stop();
      if (err) return done(err);
      assert.equal(data.Records.length, 0);
      done();
    });

  });

  it('should send the events after 5 secs', function (done) {
    var bk = new KinesisStream({
      streamName: STREAM_NAME,
      partitionKey: 'test-123'
    });

    var log_entry = JSON.stringify({foo: 'bar'});
    bk._write(log_entry, null, _.noop);

    setTimeout(function () {
      kinesis.getRecords({
        ShardIterator: iterator,
        Limit: 1
      }, function (err, data) {
        bk.stop();
        if (err) return done(err);
        assert.equal(data.Records.length, 1);
        done();
      });
    }, 5100);

  });

  it('should send the events after 10 messages', function (done) {
    var bk = new KinesisStream({
      streamName: STREAM_NAME,
      partitionKey: 'test-123'
    });


    for (var i = 0; i < 10; i++) {
      var log_entry = JSON.stringify({foo: 'bar'});
      bk._write(log_entry, null, _.noop);
    }

    setTimeout(function () {
      kinesis.getRecords({
        ShardIterator: iterator,
        Limit: 11
      }, function (err, data) {
        bk.stop();
        if (err) return done(err);
        assert.equal(data.Records.length, 10);
        done();
      });
    }, 500);

  });

  it('should send the events after a error level entry', function (done) {
    var bk = new KinesisStream({
      streamName: STREAM_NAME,
      partitionKey: 'test-123'
    });


    bk._write(JSON.stringify({foo: 'bar'}), null, _.noop);
    bk._write(JSON.stringify({error: 'error', level: 50}), null, _.noop);


    setTimeout(function () {
      kinesis.getRecords({
        ShardIterator: iterator,
        Limit: 2
      }, function (err, data) {
        bk.stop();
        if (err) return done(err);
        assert.equal(data.Records.length, 2);
        done();
      });
    }, 500);

  });


});