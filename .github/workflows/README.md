# GitHub Actions CI Configuration

This directory contains GitHub Actions workflows for continuous integration testing of pg_orca.

## Workflows

### ci.yml - PostgreSQL 17 Integration Test

This workflow runs on:
- Push to `main`, `master`, `develop`, or `claude/**` branches
- Pull requests to `main`, `master`, or `develop` branches
- Manual trigger via workflow_dispatch

#### Jobs

1. **build-and-test**:
   - Tests pg_orca against PostgreSQL 17
   - Builds the extension using CMake and Ninja
   - Runs the test suite via ctest (no external dependencies required)
   - Performs basic smoke tests

2. **build-coverage**:
   - Builds with code coverage enabled
   - Generates coverage reports using lcov
   - Uploads coverage artifacts

#### Dependencies

The CI installs the following:
- PostgreSQL 17 from the official PostgreSQL APT repository
- Build tools: cmake, ninja-build, clang, llvm

The test suite uses only PostgreSQL built-in functionality and requires no external extensions. This ensures tests can run reliably in any CI environment.

#### Artifacts

- Test results are uploaded as artifacts for each PostgreSQL version
- Coverage reports are uploaded when coverage build completes

## Running Tests Locally

To run the same tests locally:

```bash
# Install PostgreSQL 17 development files
sudo apt-get install postgresql-17 postgresql-server-dev-17

# Build the extension
export PATH=/usr/lib/postgresql/17/bin:$PATH
cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build

# Install the extension
sudo cmake --build build --target install

# Run tests (no external dependencies required)
cd build
ctest --output-on-failure --verbose
```

## Coverage Build

To build with coverage enabled:

```bash
cmake -B build -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DENABLE_COVERAGE=TRUE
cmake --build build

# Generate coverage report
cd build
lcov -d . -c -o coverage.info
lcov --summary coverage.info
```
