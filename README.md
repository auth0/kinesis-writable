Kinesis writable stream for [bunyan](http://npmjs.com/package/bunyan).

> **v5+**: Now powered by AWS SDK v3! Same public API, better security & performance. No code changes required.

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

* `error`: Emitted every time records are failed to be written.

### Credentials

Amazon Credentials are not required. The library automatically resolves credentials from (in order):

1. Constructor parameters (`accessKeyId`, `secretAccessKey`, `sessionToken`, or `credentials` object)
2. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`)
3. `~/.aws/credentials` file
4. `~/.aws/config` file
5. IAM role (EC2, ECS, Lambda, etc.)

**v5 Update**: Credentials are now resolved per-client (AWS SDK v3), providing better security and reliability. EC2 metadata timeouts are properly handled to prevent hangs on high-load instances.

Example with environment variables:
```bash
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
export AWS_REGION=us-east-1
node your-app.js
```

## Version History

- **v5.x**: AWS SDK v3 (current) - Requires Node.js >= 14
- **v4.x**: AWS SDK v2 (legacy) - [v4 branch](https://github.com/auth0/kinesis-writable/tree/v4.x)

See [CHANGELOG.md](CHANGELOG.md) for detailed release notes.

## Upgrading

If you're upgrading from v4 to v5:
- **No code changes required** - The public API is 100% compatible
- Requires Node.js >= 14 (v4 required >= 10)
- Now uses AWS SDK v3 internally for better security

## Issue Reporting

If you have found a bug or if you have a feature request, please report them at this repository issues section. Please do not report security vulnerabilities on the public GitHub issue tracker. The [Responsible Disclosure Program](https://auth0.com/whitehat) details the procedure for disclosing security issues.

## Author

[Auth0](auth0.com)

## License

This project is licensed under the MIT license. See the [LICENSE](LICENSE) file for more info.
