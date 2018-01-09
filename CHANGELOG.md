<a name="v4.1.4"></a>
# v4.1.4
- Update the AWS SDK to at least version 2.178.0 which has an updated version of crypto-browserify  which as vulnerable to Insecure Randomness due to using the cryptographically insecure Math.random().  See https://snyk.io/test/npm/aws-sdk/2.94.0.
