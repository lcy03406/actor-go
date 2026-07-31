# Contributing to actor-go

Thank you for your interest in contributing to actor-go!

## How to Contribute

### Reporting Issues

- Use the [GitHub Issues](https://github.com/lcy03406/actor-go/issues) page
- For bugs, include: Go version, OS, steps to reproduce, expected vs actual behavior
- For feature requests, describe the use case and proposed solution

### Development Setup

```bash
# Clone the repository
git clone https://github.com/lcy03406/actor-go.git
cd actor-go

# Ensure Go 1.23+ is installed
go version

# Run tests
go test ./...

# Run benchmarks
go test -bench=. ./actor/...
```

### Code Standards

- All code must be formatted with `gofmt` (or `go fmt ./...`)
- Follow effective Go conventions
- Add tests for new features
- Run `go vet ./...` before submitting

### Pull Request Process

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Make your changes
4. Ensure tests pass: `go test ./...`
5. Commit with clear messages
6. Push and create a Pull Request

### License

By contributing, you agree that your contributions will be licensed under the MIT License.