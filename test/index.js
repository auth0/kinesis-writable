/* eslint-env node, mocha */

const sinon = require('sinon');
const assert = require('assert');
const EventEmitter = require('events').EventEmitter;
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

      expect(ks.hasPriority).to.be.a('function');
      expect(ks.recordsQueue).to.exist;
      expect(ks.partitionKey).to.be.string;
    });
    it('should build a stream with configured endpoint and objectMode', function() {
      const ks = new KinesisStream({
        streamName: 'test',
        objectMode: true,
        kinesis: {
          endpoint: "http://somehost:1234"
        }
      });

      expect(ks.hasPriority).to.be.a('function');
      expect(ks.recordsQueue).to.exist;
      expect(ks.partitionKey).to.be.a('function');
      expect(ks.kinesis.endpoint).to.equal("http://somehost:1234");
      expect(ks._writableState.objectMode).to.equal(true);
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
    it('should retry if #putRecords failed', function(done) {
      const message = {test: true};
      const stub = sinon.stub(ks, 'putRecords').callsFake(function(r, cb) {
        return cb(new Error());
      });

      ks.on('error', function(err) {
        expect(err).to.exist;
        expect(err.records.length).to.equal(2);
        expect(stub.calledThrice).to.be.true;
      });

      ks.dispatch([message, message], function(err) {
        expect(err).to.exist;
        done();
      });
    });
    it('should not get an uncaugh if stream emit an error after complete', function(done) {
      const message = {test: true};

      kinesis.putRecords = sinon.stub().callsFake(function(r, cb) {
        const awsReq = new EventEmitter();
        const clientReq = new EventEmitter();
        awsReq.httpRequest = { stream: clientReq};
        awsReq.response= { httpResponse : { stream: new EventEmitter() } };

        setTimeout(() => {
          cb();
          awsReq.emit('complete');
          clientReq.emit('error', new Error('etimedout'));
        }, 100);

        return awsReq;
      });

      ks.dispatch([message, message], function(err) {
        done();
      });
    });
  });
});
