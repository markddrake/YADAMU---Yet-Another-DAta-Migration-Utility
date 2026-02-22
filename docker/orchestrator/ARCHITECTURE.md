# YADAMU Test Scripts Architecture

## Script Hierarchy

### Environment Setup
**setEnvironment.sh** - Sets all environment variables
- Used by ALL test scripts
- Fast - just exports variables
- No image rebuilds, no init container

### Initialization  
**initializeTests.sh** - Full test environment setup
- Calls setEnvironment.sh internally (via exports)
- Rebuilds all YADAMU images
- Runs initialization container
- Used by full test suite runners

### Test Scripts

**standardTests.sh** - Standard test suite
- Sources setEnvironment.sh
- Runs standard YADAMU tests

**extendedTests.sh** - Extended test suite  
- Sources setEnvironment.sh (if available)
- Defines all test functions
- Main execution wrapped in run_all_tests()
- Can be sourced (for individual tests) or executed directly (for full suite)

### Wrapper Scripts

**runStandardTests.sh** - Full standard test run
```bash
. initializeTests.sh      # Full init (rebuild images)
standardTests.sh          # Run standard tests
```

**runExtendedTests.sh** - Full extended test run
```bash
. initializeTests.sh      # Full init (rebuild images)
extendedTests.sh          # Run extended tests (calls run_all_tests)
```

**runIndividualTest.sh** - Individual test run
```bash
. setEnvironment.sh       # Just set env (no rebuild)
. extendedTests.sh        # Source functions only
run_test <test_name>      # Run one test
```

## Flow Diagrams

### Full Test Suite Flow
```
docker compose up extended
  → runExtendedTests.sh
    → initializeTests.sh (rebuild images, run init container)
    → extendedTests.sh (execute directly → run_all_tests())
      → Iterates EXTENDED_TESTS array
      → Calls run_test() for each
```

### Individual Test Flow
```
docker compose up oracle21c
  → runIndividualTest.sh oracle21c
    → setEnvironment.sh (just set vars)
    → extendedTests.sh (source only → functions defined)
    → run_test("oracle21c") (call function directly)
```

## Consistency Rules

1. **ALL scripts source setEnvironment.sh** (or initializeTests.sh which sets same vars)
2. **extendedTests.sh can be sourced OR executed**
   - Sourced: Only defines functions
   - Executed: Calls run_all_tests()
3. **All wrapper scripts use consistent pattern**
   - Init (full or light)
   - Execute test script
   - Log to $LOG_DIR

## Files Summary

| File | Purpose | Sources | Used By |
|------|---------|---------|---------|
| setEnvironment.sh | Set env vars | - | All test scripts |
| initializeTests.sh | Full init + env | setEnvironment (implicitly) | Full test runners |
| standardTests.sh | Run standard tests | setEnvironment.sh | runStandardTests.sh |
| extendedTests.sh | Run extended tests | setEnvironment.sh | runExtendedTests.sh, runIndividualTest.sh |
| runStandardTests.sh | Wrapper for standard | initializeTests.sh, standardTests.sh | docker-compose |
| runExtendedTests.sh | Wrapper for extended | initializeTests.sh, extendedTests.sh | docker-compose |
| runIndividualTest.sh | Wrapper for individual | setEnvironment.sh, extendedTests.sh | docker-compose |

## Installation

```bash
# Copy all scripts to /usr/local/bin/ in orchestrator
cp setEnvironment.sh /usr/local/bin/
cp initializeTests.sh /usr/local/bin/
cp standardTests.sh /usr/local/bin/
cp extendedTests.sh /usr/local/bin/
cp runStandardTests.sh /usr/local/bin/
cp runExtendedTests.sh /usr/local/bin/
cp runIndividualTest.sh /usr/local/bin/

# Make executable
chmod +x /usr/local/bin/*.sh

# Rebuild orchestrator
docker compose build
```

## Usage

```bash
# Full test suites (with image rebuild)
docker compose up standard
docker compose up extended
docker compose up comprehensive

# Individual tests (no rebuild, fast)
docker compose up oracle21c
docker compose up db2
docker compose up cmdline

# Recreate databases
docker compose up recreate-db
```

## Benefits of This Architecture

✅ **Single source of truth** - setEnvironment.sh defines all vars
✅ **DRY** - No duplicate environment setup code
✅ **Flexible** - extendedTests.sh works both ways (sourced or executed)
✅ **Fast individual tests** - Skip image rebuilds
✅ **Consistent** - All scripts follow same patterns
✅ **Maintainable** - Change env vars in one place
