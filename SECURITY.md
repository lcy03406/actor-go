# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in actor-go, please report it to us
via GitHub Security Advisory:

1. Go to the [Security](https://github.com/lcy03406/actor-go/security) tab
2. Click "Report a vulnerability"
3. Provide a detailed description of the issue

We aim to respond within 48 hours and will keep you updated on the progress.

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Security Considerations

- actor-go uses WebSocket for RPC communication. Ensure TLS is used in production environments.
- The lease manager provides fencing tokens to prevent split-brain scenarios.
- Grain persistence drivers should be configured with appropriate access controls.