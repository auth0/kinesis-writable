
var KinesisWritable = require('./index.js');
var moment = require('moment');

var kinesis = new KinesisWritable({
  // accessKeyId:     'KEY_ID',
  // secretAccessKey: 'SECRET_KEY',
  region:          'us-west-2',
  streamName:      'test',
  partitionKey:    'MyApp',
  buffer: {
    timeout: 1,                         // Messages will be send every second
    lenght: 100                         // or when 100 messages are in the queue
  }
});

kinesis.on('error', function (err) {
  console.log('ERROR', err.mesage, err.stack);
});

var log = {
  tenant: 'test',
  date: (new Date()).toISOString(),
  type: 'test'
};

var c = 1000;
var t = 0;
console.log("sending ...");

function sendLog(json) {
  setImmediate(function () { kinesis.write(json); });  
}

setInterval(function () {
  ++t;
  log.date = new Date();
  log.description = t.toString();
  //if (t===1) log.description = t;
  var json = JSON.stringify(log);
  if ((t % 100) === 1) {
    console.log(t, '=>', json);
  }
  kinesis.write(json); 
}, 330);

console.log("done", c);