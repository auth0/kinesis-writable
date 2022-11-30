Kinesis writable stream for [bunyan](http://npmjs.com/package/bunyan).

## Installation

```sh
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
-  a message has priority. Default: all messages do no have priority

Example:
```javascript
new KinesisWritable({
  region: 'AWS_REGION',
  streamName: 'MyKinesisStream',
  partitionKey: 'foo',
  buffer: {
    timeout: 1, // Messages will be sent every second
    length: 100, // or when 100 messages are in the queue
    hasPriority: function (msg) { // or the message has a type > 40
      var entry = JSON.parse(msg);
      return entry.type > 40;
    }
  }
});
```

`partitionKey` can be either an string or a function that accepts a message and returns a string. By default it is a function that returns the current EPOCH (Date.now()). Example:

```javascript
new KinesisWritable({
  region: 'AWS_REGION',
  streamName: 'MyKinesisStream',
  partitionKey: function (msg) {
    var entry = JSON.parse(msg);
    return entry.level + '|' + entry.name;
  }
});
```

`streamName` is the name of the Kinesis Stream.

### Events

| Name | Description | Parameters |
|---|---|---|
| `error`| Emitted every time records are failed to be written after all retries fail | `err` will contain the latest retry error. Failed encoded records will be in `err.records` |
| `error`| Emitted every time records succeed to push to the kinesis stream. | `records` will contain `records.FailedRecordCount:0` and an array of objects on `records.Records` containing each `SequenceNumber:` and `ShardId` |

```javascript
new KinesisWritable({
  region: 'AWS_REGION',
  streamName: 'MyKinesisStream'
}).on('error', (err) => {
  console.log(`Failed to push ${err.records}`)
}).on('success', (records) => {
  console.log(`Succesfully pushed ${err.length} records`)
})
```
**Note**: Amazon Credentials are not required. It will either use the environment variables, `~/.aws/credentials` or roles as every other aws sdk.

## Issue Reporting

If you have found a bug or if you have a feature request, please report them at this repository issues section. Please do not report security vulnerabilities on the public GitHub issue tracker. The [Responsible Disclosure Program](https://auth0.com/whitehat) details the procedure for disclosing security issues.

## Author

[Auth0](auth0.com)

## License

This project is licensed under the MIT license. See the [LICENSE](LICENSE) file for more info.
