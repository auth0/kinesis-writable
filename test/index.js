/* eslint-env node, mocha */

const sinon = require('sinon');
const assert = require('assert');
const KinesisStream = require('../');
const expect = require('chai').expect;

describe('KinesisStream', function() {
  describe('#constructor', function() {
    it('should throw if .streamName is not provided', function() {
      expect(function() {new KinesisStream({});}).to.throw(assert.AssertionError, /streamName/);
    });
    it('should build a stream with default configurations', function() {
      const ks = new KinesisStream({
        streamName: 'test',
        kinesis: {}
      });

      expect(ks.hasPriority).to.be.function;
      expect(ks.recordsQueue).to.exist;
      expect(ks.partitionKey).to.be.function;
    });
  });
  describe('#_write', function() {
    var ks;
    const message = {test: true};
    beforeEach(function() {
      ks = new KinesisStream({
        streamName: 'test',
        kinesis: {}
      });
      ks.dispatch = sinon.spy();
    });
    it('should call immediately dispatch with a single message if it has priority', function() {
      ks.hasPriority = function() {
        return true;
      };
      ks.write(new Buffer(JSON.stringify(message)));
      expect(ks.dispatch.calledOnce).to.be.true;
      expect(ks.dispatch.calledWith([message])).to.be.true;
    });
    it('should add message to queue if no priority was specified and add a timer', function() {
      ks.write(new Buffer(JSON.stringify(message)));
      expect(ks.dispatch.calledOnce).to.be.false;
      expect(ks.timer).to.exist;
      expect(ks.recordsQueue.length).to.equal(1);
    });
    it('should add message to queue and call flush is size has crossed the threshold', function() {
      for (var i = 0; i < 10; i++) {
        ks.write(new Buffer(JSON.stringify(message)));
      }
      expect(ks.dispatch.calledOnce).to.be.true;
      expect(ks.dispatch.calledWith([message, message, message, message, message, message, message, message, message, message])).to.be.true;
    });
    it('should call #dispatch once timer hits', function(done) {
      this.timeout(3000);
      ks.buffer.timeout = 2;
      for (var i = 0; i < 3; i++) {
        ks.write(new Buffer(JSON.stringify(message)));
      }
      setTimeout(function() {
        expect(ks.dispatch.calledOnce).to.be.true;
        expect(ks.dispatch.calledWith([message, message, message])).to.be.true;
        done();
      }, 2000);
    });
  });
  describe('#flush', function() {
    it('should call #dispatch with at most buffer size', function() {
      const ks = new KinesisStream({
        streamName: 'test',
        kinesis: {}
      });
      ks.dispatch = sinon.spy();
      ks.recordsQueue = [1,2,3];
      ks.flush();
      expect(ks.dispatch.calledOnce).to.be.true;
      expect(ks.dispatch.calledWith([1,2,3])).to.be.true;
    });
  });
  describe('#dispatch', function() {
    var ks, kinesis = {};
    beforeEach(function() {
      ks = new KinesisStream({
        streamName: 'test',
        kinesis: kinesis
      });
    });
    it('should return immediately if no messages are provided', function(done) {
      kinesis.putRecords = sinon.spy();
      ks.dispatch([], function() {
        expect(kinesis.putRecords.calledOnce).to.be.false;
        done();
      });
    });
    it('should emit and requeue the failed records', function(done) {
      const message = {test: true};
      ks.write = sinon.spy();
      kinesis.putRecords = function(params, cb) {
        expect(params.StreamName).to.equal('test');
        expect(params.Records[0].Data).to.equal(JSON.stringify(message));
        return cb(new Error(), { Records: [{}, {ErrorCode: 'ProvisionedThroughputExceededException'}]});
      };
      ks.on('error', function(err) {
        expect(err).to.exist;
        expect(err.records.length).to.equal(1);
        expect(ks.write.calledOnce).to.be.true;
        expect(ks.write.calledWith(message)).to.be.true;
        done();
      });
      ks.dispatch([message, message]);
    });
  });
  //describe('#emitError');
});
