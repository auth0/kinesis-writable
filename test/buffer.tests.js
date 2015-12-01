const KinesisStream = require('../');
const AWS = require('aws-sdk');
const assert = require('chai').assert;
const sinon = require('sinon');

const _ = require('lodash');

const kinesis = new AWS.Kinesis({
  region: 'us-west-1'
});

const STREAM_NAME   = 'vagrant_testing';

function isPrioritaryMsg(entry) {
  return entry.level >= 40;
}

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
  
  describe ('constructor', function() {
    var defaultBuffer = {
      timeout: 5,
      length: 10,
      isPrioritaryMsg: undefined
    };

    it('should be able to create an instance', function() {
      var instance = new KinesisStream({ streamName: STREAM_NAME });
      assert.ok(instance instanceof KinesisStream);
    });

    it ('should set default buffer by default', function () {
      var instance = new KinesisStream({ streamName: STREAM_NAME });
      assert.ok(instance._params.buffer);
      assert.deepEqual(instance._params.buffer, defaultBuffer);
      assert.equal(typeof instance._params.partitionKey, 'function');
    });

    it ('should set default buffer when it was set to true.', function () {
      var instance = new KinesisStream({ streamName: STREAM_NAME, buffer: true });
      assert.ok(instance._params.buffer);
      assert.deepEqual(instance._params.buffer, defaultBuffer);
    });

    it ('should be able to disable buffer', function () {
      var instance = new KinesisStream({ streamName: STREAM_NAME, buffer: false });
      assert.ok(!instance._params.buffer);
    });

    it ('should fail if partitionKey is invalid', function () {
      try {
        new KinesisStream( { streamName: STREAM_NAME, partitionKey: 10 });
        assert.fail("should not reach here!");
      } catch (e) {
        assert.ok(e instanceof Error);
        assert.equal(e.message, "'partitionKey' property should be a string or a function."); 
      }
    });

    it ('should fail if partitionKey is false', function () {
      try {
        new KinesisStream( { streamName: STREAM_NAME, partitionKey: '' });
        assert.fail("should not reach here!");
      } catch (e) {
        assert.ok(e instanceof Error);
        assert.equal(e.message, "'partitionKey' property should be a string or a function."); 
      }
    });
  });

  describe ('method setStreamName', function () {
    [
      null,
      undefined,
      {},
      true,
      10
    ].forEach(function (value) {

      it ('should throw an exception if the value is not a valid string (' + value +')', function () {
        var bk = new KinesisStream({
          region: 'us-west-1',
          partitionKey: 'test-123'
        });

        try {
          bk.setStreamName(value);
          assert.fail('should not reach here!');
        } catch (e) {
          assert.ok(e instanceof Error);
          assert.equal(e.message, '\'streamName\' must be a valid string.');
        }
      });
    });

    it ('should override previous value of stream-name', function () {
      var bk = new KinesisStream({
        region: 'us-west-1',
        partitionKey: 'test-123'
      });

      assert.ok(!bk.getStreamName());

      bk.setStreamName('foo');

      assert.ok(bk.getStreamName(), 'foo');
    });
  });

  describe('method getStreamName', function () {
    it ('should return null if not stream\'s name was configured.', function () {
      var bk = new KinesisStream({
        region: 'us-west-1',
        partitionKey: 'test-123'
      });

      assert.equal(bk.getStreamName(), null);
    });

    it ('should return the configured stream\'s name.', function () {
      var bk = new KinesisStream({
        streamName: 'foo',
        region: 'us-west-1',
        partitionKey: 'test-123'
      });

      assert.equal(bk.getStreamName(), 'foo');
    });

    it ('should return stream\'s name that was set.', function () {
      var bk = new KinesisStream({
        streamName: 'foo',
        region: 'us-west-1',
        partitionKey: 'test-123'
      });

      bk.setStreamName('bar');
      assert.equal(bk.getStreamName(), 'bar');
    });
  });

  describe ('method _write', function () {
    var iterator;

    this.timeout(10000);

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
        region: 'us-west-1',
        partitionKey: 'test-123'
      });
      var log_entry = JSON.stringify({foo: 'bar'});
      bk._write(log_entry, null, _.noop);
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

    it('should send the events after X secs', function (done) {
      var x = 1;
      var bk = new KinesisStream({
        region: 'us-west-1',
        streamName: STREAM_NAME,
        partitionKey: 'test-123',
        buffer: { timeout: x }
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
      }, x * 1000 + 100);
    });

    it('should support object events', function (done) {
      var x = 1;
      var bk = new KinesisStream({
        region: 'us-west-1',
        streamName: STREAM_NAME,
        partitionKey: 'test-123',
        buffer: { timeout: x }
      });

      var log_entry = {foo: 'bar'};
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
      }, x * 1000 + 100);
    });

    it('should send the events after X messages', function (done) {
      var x = 3;
      var bk = new KinesisStream({
        region: 'us-west-1',
        streamName: STREAM_NAME,
        buffer: { length: x },
        partitionKey: 'test-123'
      });

      for (var i = 0; i < x; i++) {
        var log_entry = JSON.stringify({foo: 'bar'});
        bk._write(log_entry, null, _.noop);
      }

      setTimeout(function () {
        kinesis.getRecords({
          ShardIterator: iterator,
          Limit: x + 1
        }, function (err, data) {
          bk.stop();
          if (err) return done(err);
          assert.equal(data.Records.length, x);
          done();
        });
      }, 500);
    });

    it('should send the events after an error level entry', function (done) {
      var bk = new KinesisStream({
        region: 'us-west-1',
        streamName: STREAM_NAME,
        buffer: { isPrioritaryMsg: isPrioritaryMsg },
        partitionKey: 'test-123'
      });


      bk._write(JSON.stringify({foo: 'bar'}), null, _.noop);
      bk._write(JSON.stringify({error: 'error', level: 50}), null, _.noop);


      setTimeout(function () {
        kinesis.getRecords({
          ShardIterator: iterator,
          Limit: 3
        }, function (err, data) {
          bk.stop();
          if (err) return done(err);
          assert.equal(data.Records.length, 2);
          done();
        });
      }, 500);
    });

    it ('should be able to use a function for compute the partitionKey', function (done) {

      var data = JSON.stringify({foo: 'bar'});

      var bk = new KinesisStream({
        region: 'us-west-1',
        streamName: STREAM_NAME,
        buffer: {
          length: 1
        },
        partitionKey: function (msg) {
          assert.equal(msg, data);
          return "prefix-" + msg;
        }
      });

      bk._write(data, null, _.noop);

      setTimeout(function () {
        kinesis.getRecords({
          ShardIterator: iterator,
          Limit: 1
        }, function (err, result) {
          bk.stop();
          if (err) return done(err);
          assert.equal(result.Records.length, 1);
          assert.equal(result.Records[0].Data, data);
          assert.equal(result.Records[0].PartitionKey, "prefix-" + data);
          done();
        });
      }, 500);
    });

    it('should be able to use the default partitionKey function', function (done) {

      var data = JSON.stringify({foo: 'bar'});

      var bk = new KinesisStream({
        region: 'us-west-1',
        streamName: STREAM_NAME,
        buffer: {
          length: 1
        }
      });

      bk._write(data, null, _.noop);

      setTimeout(function () {
        kinesis.getRecords({
          ShardIterator: iterator,
          Limit: 1
        }, function (err, result) {
          bk.stop();
          if (err) return done(err);
          assert.equal(result.Records.length, 1);
          assert.equal(result.Records[0].Data, data);
          assert.ok(result.Records[0].PartitionKey);
          assert.equal(typeof result.Records[0].PartitionKey, 'string');
          done();
        });
      }, 500);
    });

    it ('should return an error if partitionKey function throws an exception', function (done) {
      var bk = new KinesisStream({
        region: 'us-west-1',
        streamName: STREAM_NAME,
        partitionKey: function () {
          throw new Error("some error");
        }
      });

      bk._write("foo", null, function (err) {
        assert.ok(err instanceof Error);
        assert.equal(err.message, "some error");
        done();
      });
    });

    it ('should return an error if no stream\'s name was configured', function (done) {

      var data = JSON.stringify({foo: 'bar'});

      var bk = new KinesisStream({
        region: 'us-west-1',
        buffer: {
          length: 1
        }
      });

      bk._write(data, null, function (err) {
        assert.ok(err instanceof Error);
        assert.ok(err.message, 'Stream\'s name was not set.');
        done();
      });
    });

    it ('should emit error event when aws returns an error', function (done) {

      var bk = new KinesisStream({
        region: 'us-west-1',
        buffer: { length: 1 },
        streamName: STREAM_NAME,
        partitionKey: "foo"
      });  

      sinon.stub(bk._kinesis, 'putRecords')
        .onFirstCall()
        .yields(new Error("some error from AWS"));
      
      bk.on('error', function (err) {
        assert.ok(err instanceof Error);
        assert.equal(err.message, "some error from AWS");
        assert.ok(err.records);
        assert.equal(err.records.length, 1);
        assert.deepEqual(err.records[0], { Data: 'foo', PartitionKey: 'foo' });
        done();
      });

      bk._write("foo", null, function (err) {
        assert.ok(!err);
      });
    });

    it ("should emit recordError event when aws returns records' specifics an errors", function (done) {

      var bk = new KinesisStream({
        region: 'us-west-1',
        buffer: { length: 3 },
        streamName: STREAM_NAME,
        partitionKey: "foo"
      });  

      var response = {
        "FailedRecordCount": 2,
        "Records": [
          {
            "SequenceNumber": "49543463076548007577105092703039560359975228518395012686", 
            "ShardId": "shardId-000000000000"
          }, 
          {
            "ErrorCode": "ProvisionedThroughputExceededException",
            "ErrorMessage": "Rate exceeded for shard shardId-000000000001 in stream exampleStreamName under account 111111111111."
          },
          {
            "ErrorCode": "InternalFailure",
            "ErrorMessage": "Internal service failure."
          }
        ]
      };
      
      sinon.stub(bk._kinesis, 'putRecords')
        .onFirstCall()
        .yields(null, response);
      

      var errCount = 0;
      bk.on('errorRecord', function (err) {
        assert.ok(err);
        switch (++errCount) {
          case 1: 
            assert.deepEqual(err, {
              "ErrorCode": "ProvisionedThroughputExceededException",
              "ErrorMessage": "Rate exceeded for shard shardId-000000000001 in stream exampleStreamName under account 111111111111.",
              "Record": {
                "Data": "foo_1",
                "PartitionKey": "foo"
              }
            });
            break;
          case 2: 
            assert.deepEqual(err, {
              "ErrorCode": "InternalFailure",
              "ErrorMessage": "Internal service failure.",
              "Record": {
                "Data": "foo_2",
                "PartitionKey": "foo"
              }
            });
            done();
            break;
          default:
            assert.fail("Should not emit more than two events");
        }
      });

      for (var i=0; i<3 ;i++)
      bk._write("foo_" + i, null, function (err) {
        assert.ok(!err);
      });
    });
  });
});