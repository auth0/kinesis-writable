Kinesis writable stream for bunyan.

## Installation

```
npm install aws-kinesis-writable --save
```

## Usage

```javascript
var KinesisWritable = require('aws-kinesis-writable');

var kinesis = new KinesisWritable({
  accessKeyId:     'KEY_ID',
  secretAccessKey: 'SECRET_KEY',
  region:          'AWS_REGION',
  streamName:      'MyKinesisStream',
  partitionKey:    'MyApp'
});

process.stdin.resume();
process.stdin.pipe(kinesis);
```

### Configuration Parameters

`buffer` (defaults to true): It can be a boolean or an object describing its conditions.

This library uses by default an smart buffering approach. Messages are sent when one of the following conditions are meet:

-  X seconds after the last batch of messages sent. Default: 5 seconds.
-  X messages are queued waiting to be sent. Default: 10 messages.
-  a message is prioritary. Default: all messages are not prioritary

Example:
```javascript
new KinesisWritable({
  region:          'AWS_REGION',
  streamName:      'MyKinesisStream',
  partitionKey:     "foo",
  buffer: {
    timeout: 1,                         // Messages will be send every second
    length: 100,                        // or when 100 messages are in the queue
    isPrioritaryMsg: function (msg) {   // or the message has a type > 40
      var entry = JSON.parse(msg);
      return entry.type > 40;
    }
  }
});
```


`partitionKey` can be either an string or a function that accepts a message and returns a string. By default it is a function that returns the current EPOCH (Date.now()). Example:

```javascript
new KinesisWritable({
  region:          'AWS_REGION',
  streamName:      'MyKinesisStream',
  partitionKey:     function (msg) {
                      var entry = JSON.parse(msg);
                      return entry.level + '|' + entry.name;
                    }
});
```

Send an AWS Kinesis stream instance when creating the Writable. Useful for setting up custom
stuff like credentials, proxy etc and reuse the same AWS instance for multiple services.

```javascript

// Sample aws config
var AWS = require('aws-sdk')
var credentials = new AWS.SharedIniFileCredentials({profile: 'development'});
AWS.config.credentials = credentials;

new KinesisWritable({
  streamName :      'MyKinesisStream',
  partitionKey :    'foo',
  kinesis : new AWS.Kinesis()
});
```

`streamName` is the name of the Kinesis Stream.

### Methods
`getStreamName()`: returns Stream's name.

`setStreamName(name)`: set the name of the stream where messages will be send.

### Events
`errorRecord`: Emitted once for each failed record at the `aws.kinesis.putRecords`'s response.

**Note**: Amazon Credentials are not required. It will either use the environment variables, `~/.aws/credentials` or roles as every other aws sdk.

## License

MIT 2015 - AUTH0 INC.
