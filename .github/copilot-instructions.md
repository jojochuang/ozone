# Apache Ozone Development Instructions

Apache Ozone is a scalable, redundant, and distributed object store for Hadoop and Cloud-native environments that supports S3 and Hadoop FileSystem APIs.

**ALWAYS reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.**

## Working Effectively

### Prerequisites and Environment
- Java 17+ is required (OpenJDK 17 available at `/usr/bin/java`)
- Maven 3.9.11+ is required (available at `/usr/bin/mvn`)
- Docker is available for cluster testing (`/usr/bin/docker`)
- This is a Maven multi-module project with complex dependencies

### Bootstrap and Build Process
- **Build from source:**
  ```bash
  mvn clean install -DskipTests -DskipDocs --no-transfer-progress -Djacoco.skip
  ```
  **NEVER CANCEL: Build takes 8-9 minutes. Set timeout to 15+ minutes.**

- **Build with distribution package:**
  ```bash
  mvn package -DskipTests -DskipDocs --no-transfer-progress -Djacoco.skip -pl hadoop-ozone/dist -am
  ```
  **NEVER CANCEL: Takes 8-9 minutes. Set timeout to 15+ minutes.**

### Testing Framework
- **Quick validation checks (each takes under 30 seconds):**
  ```bash
  ./hadoop-ozone/dev-support/checks/author.sh      # Check for @author tags
  ./hadoop-ozone/dev-support/checks/rat.sh         # License header check (23 seconds)
  ./hadoop-ozone/dev-support/checks/bats.sh        # Shell script tests (24 seconds)
  ```

- **Code style and quality checks:**
  ```bash
  ./hadoop-ozone/dev-support/checks/checkstyle.sh  # Java code style (54 seconds)
  ```

- **Unit tests:**
  ```bash
  ./hadoop-ozone/dev-support/checks/unit.sh        # Full unit test suite
  ```
  **NEVER CANCEL: Unit tests take 30-45+ minutes. Set timeout to 60+ minutes.**

- **Test a single module quickly:**
  ```bash
  mvn test -pl hadoop-hdds/config -B --no-transfer-progress
  ```
  Takes about 10 seconds for small modules.

### Running Ozone Clusters

#### Docker-based Cluster (Recommended for Testing)
- **Start basic cluster:**
  ```bash
  cd hadoop-ozone/dist/target/ozone-2.1.0-SNAPSHOT/compose/ozone
  ./run.sh -d
  ```
  Starts single datanode cluster in background.

- **Start with multiple datanodes:**
  ```bash
  OZONE_DATANODES=3 ./run.sh -d
  ```

- **Check cluster status:**
  ```bash
  docker ps
  ```

- **Access web UIs:**
  - Ozone Manager: http://localhost:9874
  - Storage Container Manager: http://localhost:9876
  - S3 Gateway: http://localhost:9878
  - Recon (monitoring): http://localhost:9888

#### CLI Tools
- **Main CLI tool:** `hadoop-ozone/dist/target/ozone-2.1.0-SNAPSHOT/bin/ozone`
- **Available commands:**
  ```bash
  ./bin/ozone --help                    # Show all commands
  ./bin/ozone sh                        # Object store operations
  ./bin/ozone s3                        # S3 operations
  ./bin/ozone admin                     # Admin operations
  ./bin/ozone freon                     # Load testing tool
  ./bin/ozone fs                        # Filesystem operations
  ```

### Load Testing and Validation
- **Generate test data with Freon:**
  ```bash
  docker exec ozone-scm-1 ozone freon ockg -n1000   # Generate 1000 objects
  ```

- **Available smoke test scenarios:**
  ```bash
  # In compose/ozone directory:
  ./test.sh                     # Basic functionality tests (3-5 minutes)
  ./test-s3a.sh                # S3A compatibility tests
  ./test-hadoop.sh             # Hadoop filesystem tests
  ./test-ec.sh                 # Erasure coding tests
  ```

- **Test with multiple scenarios:**
  ```bash
  OZONE_DATANODES=3 ./run.sh -d   # Start cluster with 3 datanodes
  ./test.sh                       # Run smoke tests
  ```

## Validation Requirements

### Before Committing Changes
**ALWAYS run these validation steps:**
1. **Quick checks:** `./hadoop-ozone/dev-support/checks/author.sh && ./hadoop-ozone/dev-support/checks/rat.sh`
2. **Code style:** `./hadoop-ozone/dev-support/checks/checkstyle.sh`
3. **Build validation:** `mvn clean install -DskipTests -DskipDocs --no-transfer-progress -Djacoco.skip`
4. **Basic functionality test:** Start Docker cluster and verify web UIs are accessible

### Manual Validation Scenarios
**ALWAYS test these scenarios after making changes:**
1. **Build and verify basic functionality:**
   - Build successfully completes
   - Docker cluster starts without errors
   - Web UIs are accessible at standard ports
   - S3 endpoint responds (http://localhost:9878)

2. **CLI functionality:**
   - Main ozone CLI shows help without errors
   - Can access container and run basic commands

3. **Load testing validation:**
   - Can run Freon load generator: `docker exec ozone-scm-1 ozone freon ockg -n10`
   - Basic smoke tests pass: `./test.sh` in compose directory

4. **Integration test sample:**
   - Individual module tests work: `mvn test -pl hadoop-hdds/config`
   - Build with distribution creates proper artifacts

### CI/CD Integration
- The project uses GitHub Actions for CI (`.github/workflows/`)
- Main workflow: `.github/workflows/ci.yml`
- Individual check scripts in `hadoop-ozone/dev-support/checks/`
- **Important CI checks that must pass:**
  - Build (compile)
  - Author tag validation
  - License header validation (RAT)
  - Checkstyle
  - Unit tests
  - Integration tests

## Key Project Structure

### Main Modules
- `hadoop-hdds/` - Hadoop Distributed Data Store (core storage layer)
- `hadoop-ozone/` - Ozone-specific components
  - `client/` - Client libraries
  - `om/` - Ozone Manager (metadata management)
  - `datanode/` - Data storage nodes  
  - `s3gateway/` - S3 compatibility layer
  - `recon/` - Monitoring and management service
  - `dist/` - Distribution packaging
  - `integration-test/` - Integration test suites

### Important Directories
- `hadoop-ozone/dev-support/checks/` - All validation scripts
- `hadoop-ozone/dist/src/main/compose/` - Docker compose configurations
- `.github/workflows/` - CI/CD pipeline definitions
- `tools/` - Additional tooling (fault injection, etc.)

## Common Issues and Solutions

### Build Issues
- **Out of memory:** Build uses `-Xmx4096m` by default, increase if needed
- **Missing dependencies:** Clean build with `mvn clean` first
- **Test failures:** Use `-DskipTests` for compilation-only builds

### Testing Issues  
- **Docker not starting:** Ensure Docker daemon is running
- **Port conflicts:** Default ports: 9874 (OM), 9876 (SCM), 9878 (S3), 9888 (Recon)
- **Slow tests:** Unit tests can take 30+ minutes, integration tests longer

### Development Tips
- Use `mvn compile -pl <module>` to build single modules faster
- Use `mvn test -pl <module>` to test single modules
- Check `pom.xml` files for module-specific configurations
- The project follows Apache coding standards (2 spaces, 120 char lines)
- Build outputs are in `hadoop-ozone/dist/target/ozone-2.1.0-SNAPSHOT/`
- Main distribution JAR: `hadoop-ozone/dist/target/ozone-dist-2.1.0-SNAPSHOT.jar`
- Use Docker compose configs in `hadoop-ozone/dist/target/ozone-2.1.0-SNAPSHOT/compose/`

### Available Tools and Utilities
- **Main CLI:** `bin/ozone` with subcommands (sh, s3, admin, freon, fs, etc.)
- **Dev support scripts:** All in `hadoop-ozone/dev-support/checks/`
- **Compose configurations:** Basic, HA, secure, monitoring variants available
- **Load testing:** Freon with various generators (ockg, rk, etc.)
- **Debugging tools:** `ozone debug`, `ozone insight`

## Timeout Guidelines
- **Build commands:** Minimum 15 minutes, recommended 20 minutes
- **Unit tests:** Minimum 60 minutes, recommended 75 minutes  
- **Integration tests:** Minimum 60 minutes, recommended 90 minutes
- **Acceptance tests:** Minimum 30 minutes, recommended 45 minutes  
- **Quick checks:** 30 seconds to 2 minutes each
- **Docker startup:** 1-2 minutes for basic cluster

**CRITICAL: NEVER CANCEL long-running builds or tests. They may appear stuck but are processing thousands of files and tests.**