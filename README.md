Kinesis writteable stream for bunyan.

## Installation

```
npm i bunyan-kinesis --save
```

## Usage

```javascript
var BunyanKinesis = require('bunyan-kinesis');

var log = bunyan.createLogger({
  streams: [
    {
      stream: new BunyanKinesis({
        accessKeyId:     'KEY_ID',
        secretAccessKey: 'SECRET_KEY',
        region:          'AWS_REGION',
        streamName:      'MyKinesisStream',
        partitionKey:    'MyApp'
      })
    }
  ]
});
```

### Configuration Parameters

`buffer` (defaults to true): This library uses by default an smart buffering approach. Events are sent when one of the following conditions are meet:

-  5 seconds after the last batch of messages sent.
-  10 messages are queued waiting to be sent.
-  an entry with one of the following levels comes in **WARN**, **ERROR** or **FATAL**

`partitionKey` can be either an string or a function that accepts an log entry and returns a string. Example:

```javascript
new BunyanKinesis({
  region:          'AWS_REGION',
  streamName:      'MyKinesisStream',
  partitionKey:     function (entry) { return entry.level + '|' + entry.name; }
});
```

`streamName` is the name of the Kinesis Stream.

**Note**: Amazon Credentials are not required. It will either use the environment variables, `~/.aws/credentials` or roles as every other aws sdk.

## License

MIT 2015 - AUTH0 INC.