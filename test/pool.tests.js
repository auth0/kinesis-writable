const KinesisStreamPool = require('../pool');
const stream = require('stream');

const assert = require('chai').assert;

console.info = console.error = () => {};

describe('KinesisStreamPool', () => {
  it ('should set up initial state', (done) => {
    var streams = [
      new stream.Writable({
        objectMode: true,
        write: () => {}
      }),
      new stream.Writable({
        objectMode: true,
        write: () => {}
      })
    ];
    streams[1].primary = true;

    var pool = new KinesisStreamPool({
      streams: streams
    });
    assert.ok(pool.primaryStream);
    assert.ok(pool.currentStream);
    assert.equal(pool.primaryStream, streams[1]);
    assert.equal(pool.primaryStream, pool.currentStream);
    assert.equal(pool.retryPrimaryInterval, 300000);
    assert.equal(pool.logger, console);
    done();
  });

  it ('should write to the primary stream', (done) => {
    var streams = [
      new stream.Writable({
        objectMode: true,
        write: (chunk) => {
          assert.deepEqual(chunk, { foo: 'bar' });
          done();
        }
      }),
      new stream.Writable({
        objectMode: true,
        write: () => {}
      })
    ];

    var pool = new KinesisStreamPool({
      objectMode: true,
      streams: streams
    });
    assert.equal(pool.primaryStream, streams[0]);
    assert.equal(pool.primaryStream, pool.currentStream);
    pool.write({ foo: 'bar' });
  });

  it ('should switch streams in case of an issue with the primary', (done) => {
    var pool;
    var streams = [
      new stream.Writable({
        objectMode: true,
        write: (chunk, encoding, cb) => {
          var err = new Error('houston, we have a problem');
          err.records = [chunk];
          cb(err);
        }
      }),
      new stream.Writable({
        objectMode: true,
        write: (chunk) => {
          assert.notEqual(pool.currentStream, pool.primaryStream);
          assert.deepEqual(chunk, { lol: 'lero' });
          done();
        }
      })
    ];

    pool = new KinesisStreamPool({
      objectMode: true,
      streams: streams
    });
    assert.equal(pool.primaryStream, streams[0]);
    assert.equal(pool.primaryStream, pool.currentStream);
    pool.write({ lol: 'lero' });
  });

  it ('should switch to the primary again after some time', (done) => {
    var streams = [
      new stream.Writable({
        objectMode: true,
        write: (chunk, encoding, cb) => {
          var err = new Error('houston, we have a temporary problem');
          err.records = [chunk];
          cb(err);
        }
      }),
      new stream.Writable({
        objectMode: true,
        write: (chunk, encoding, cb) => {
          assert.deepEqual(chunk, { hihihi: 'hahaha' });
          cb();
        }
      })
    ];

    var pool = new KinesisStreamPool({
      objectMode: true,
      streams: streams,
      retryPrimaryInterval: 1000
    });
    assert.equal(pool.primaryStream, streams[0]);
    assert.equal(pool.primaryStream, pool.currentStream);
    pool.write({ hihihi: 'hahaha' });
    setTimeout(() => {
      assert.notEqual(pool.currentStream, pool.primaryStream);
      assert.equal(pool.primaryStream.failing, true);
    }, 500);
    setTimeout(() => {
      assert.equal(pool.currentStream, pool.primaryStream);
      assert.equal(pool.primaryStream.failing, false);
      done();
    }, 1200);
  });

  it ('should emit an error event in case of total failure', (done) => {
    var pool;
    var streams = [
      new stream.Writable({
        objectMode: true,
        write: (chunk, encoding, cb) => {
          var err = new Error('houston, we have a different problem');
          err.records = {
            Data: JSON.stringify(chunk),
            length: JSON.stringify(chunk).length
          };
          cb(err);
        }
      }),
      new stream.Writable({
        objectMode: true,
        write: (chunk, encoding, cb) => {
          var err = new Error('houston, we have yet another problem');
          err.records = {
            Data: JSON.stringify(chunk),
            length: JSON.stringify(chunk).length
          };
          cb(err);
        }
      })
    ];

    pool = new KinesisStreamPool({
      objectMode: true,
      streams: streams
    });
    pool.on('poolFailure', (err) => {
      assert.equal(err.message, 'No kinesis stream available');
    });
    assert.equal(pool.primaryStream, streams[0]);
    assert.equal(pool.primaryStream, pool.currentStream);
    pool.write({ pum: 'pa' });
    setTimeout(done, 500);
  });
});
