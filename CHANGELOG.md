# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [5.0.0] - 2026-02-18

### Major Update: AWS SDK v3

This release upgrades to AWS SDK v3 for improved security, performance, and reliability. **The public API remains 100% compatible** - no code changes required!

### Changed

- **BREAKING**: Upgraded from AWS SDK v2 to AWS SDK v3
  - Modular architecture: only Kinesis client is bundled (smaller footprint)
  - Better security: updated dependencies with latest patches
  - Improved performance: optimized HTTP handling

- **BREAKING**: Node.js >= 14 required (was >= 10)
  - AWS SDK v3 requires Node.js 14+

- Credential resolution now uses per-client configuration (v3 style)
  - More secure and isolated credential handling
  - Better timeouts on EC2 metadata service
  - Automatic retry with backoff

### Added

- Explicit HTTP timeout handling via NodeHttpHandler
  - Configurable connection and request timeouts
  - Prevents hangs on high-load instances
  - Better handling of EC2 metadata service delays

- Error code normalization for backward compatibility
  - v3 SDK uses `err.name`, we now also set `err.code` for v2 compatibility
  - Existing error handling code continues to work without changes

### Fixed

- EC2 metadata timeout issues on high-load systems
  - v3's credential provider chain now properly respects timeout settings
  - Prevents indefinite hangs when metadata service is slow

- HTTP handler configuration
  - Now uses @smithy/node-http-handler for proper timeout and connection management

### Dependencies

- Removed: `aws-sdk` v2 (entire SDK)
- Added: `@aws-sdk/client-kinesis` v3
- Added: `@aws-sdk/credential-providers` v3
- Added: `@smithy/node-http-handler` v3

### Performance Improvements

- Smaller bundle size: ~10x smaller (only kinesis client vs entire SDK v2)
- Faster module loading
- Better memory management with v3's architecture
- More efficient HTTP connection pooling

### Security Improvements

- All dependencies updated to latest secure versions
- Better isolation between credential providers
- Explicit timeout handling prevents resource exhaustion

---

## [4.1.4] and earlier

Update the AWS SDK to at least version 2.178.0 which has an updated version of crypto-browserify  which as vulnerable to Insecure Randomness due to using the cryptographically insecure Math.random().  See https://snyk.io/test/npm/aws-sdk/2.94.0.
