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
   - Attempts to install test dependencies (pg_tpch, pg_tpcds)
   - Runs the test suite via ctest
   - Performs basic smoke tests

2. **build-coverage**:
   - Builds with code coverage enabled
   - Generates coverage reports using lcov
   - Uploads coverage artifacts

#### Dependencies

The CI installs the following:
- PostgreSQL 17 from the official PostgreSQL APT repository
- Build tools: cmake, ninja-build, clang, llvm
- Test dependencies: pg_tpch and pg_tpcds (cloned from quantumiodb repos if available)

#### Test Dependencies Note

The tests require `pg_tpch` and `pg_tpcds` extensions. The CI attempts to clone and install these from:
- https://github.com/quantumiodb/pg_tpch
- https://github.com/quantumiodb/pg_tpcds

If these repositories are private or unavailable, those steps will be skipped (using `continue-on-error`), and the main build will still succeed.

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

# Run tests (requires pg_tpch and pg_tpcds to be installed)
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
