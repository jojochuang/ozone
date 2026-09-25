<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Apache Ozone - Google Gemini CLI Instructions

This document provides instructions for Google Gemini CLI to work effectively with the Apache Ozone repository.

## Project Overview

Apache Ozone is a scalable, redundant, and distributed object store for Hadoop and Cloud-native environments. It is designed to scale to billions of objects of varying sizes and functions effectively in containerized environments such as Kubernetes and YARN.

**Key Features:**
- Multi-protocol support (S3, Hadoop File System APIs)
- Scalable to tens of billions of files and blocks
- Strongly consistent object store using RAFT protocol
- Cloud-native design for containerized environments
- Secure with Kerberos, ACLs, Ranger integration, TDE, and encryption
- Highly available with full replication

**Primary Language:** Java  
**Build Tool:** Apache Maven  
**Version:** 2.1.0-SNAPSHOT

## Repository Structure

```
ozone/
├── hadoop-hdds/          # Hadoop Distributed Data Storage
│   ├── client/           # HDDS client libraries
│   ├── common/           # Common utilities and classes
│   ├── container-service/# Container service implementation
│   ├── docs/             # Documentation source files
│   ├── framework/        # Core framework components
│   └── interface-*/      # API interfaces
├── hadoop-ozone/         # Ozone-specific implementations
│   ├── cli-shell/        # Ozone CLI shell implementation
│   ├── cli-admin/        # Admin CLI tools
│   ├── client/           # Ozone client libraries
│   ├── common/           # Common Ozone utilities
│   ├── dist/             # Distribution and packaging
│   ├── freon/            # Load generator and benchmark tools
│   ├── insight/          # Insight tools
│   ├── integration-test/ # Integration tests
│   ├── ozone-manager/    # Ozone Manager (metadata service)
│   ├── recon/            # Recon (monitoring and management UI)
│   ├── s3gateway/        # S3 Gateway implementation
│   └── tools/            # Various tools
├── dev-support/          # Development support scripts
├── tools/                # Additional tooling
└── pom.xml               # Maven project file
```

## Key Components

1. **Ozone Manager (OM)**: Metadata management service that handles namespace operations
2. **Storage Container Manager (SCM)**: Manages storage containers and datanodes
3. **Datanode**: Stores actual data in containers
4. **Recon**: Web UI for monitoring and management
5. **S3 Gateway**: Provides S3-compatible API
6. **Freon**: Load generator and benchmark tool

## Building the Project

### Requirements
- Unix System
- JDK 1.8 or higher
- Maven 3.6 or later
- Internet connection (for first build)

### Basic Build
```bash
mvn clean install -DskipTests
```

### Useful Build Options
- `-DskipShade` - Skip shaded Ozone FS jar creation (saves time)
- `-DskipRecon` - Skip building Recon Web UI (saves ~2 minutes)
- `-Pdist` - Build the binary tarball for distribution
- `-DskipTests` - Skip running tests

### Quick Build
```bash
mvn clean verify -DskipTests
```

## Testing

### Test Scripts Location
All test scripts are located in: `hadoop-ozone/dev-support/checks/`

### Available Tests
1. **Quick Checks** (< 2 minutes):
   - `author.sh` - Check for @author tags
   - `bats.sh` - Unit tests for shell scripts
   - `rat.sh` - Apache license header checks
   - `docs.sh` - Documentation sanity checks
   - `checkstyle.sh` - Checkstyle validation
   - `pmd.sh` - PMD static analysis

2. **Moderate Tests** (~10 minutes):
   - `findbugs.sh` - SpotBugs analysis
   - `kubernetes.sh` - Limited Kubernetes tests

3. **Comprehensive Tests** (1+ hours):
   - `unit.sh` - Pure unit tests
   - `integration.sh` - Java-based mini cluster tests
   - `acceptance.sh` - Docker Compose-based tests

### Running Tests
```bash
# From project root
cd hadoop-ozone/dev-support/checks/
./checkstyle.sh
./unit.sh
./acceptance.sh
```

## Code Conventions

### Style Guidelines
- **Indentation:** 2 spaces (no tabs)
- **Line Length:** 120 characters maximum
- **License Header:** Apache license header required in all files
- **Author Tags:** No `@author` tags (use Git history)
- **Code Style:** Defined in `.editorconfig`

### Checkstyle Configuration
Located at: `hadoop-hdds/dev-support/checkstyle/checkstyle.xml`

### Package Naming
- `org.apache.hadoop.ozone.*` - Ozone-specific code
- `org.apache.hadoop.hdds.*` - HDDS-specific code

## CLI Tools

### Main Ozone CLI
```bash
ozone [command] [options]
```

**Key Commands:**
- `ozone sh` - Ozone shell for object operations
- `ozone admin` - Administrative operations
- `ozone freon` - Load generation and benchmarking
- `ozone insight` - Diagnostic and debugging tools
- `ozone genconf` - Generate configuration templates
- `ozone completion` - Generate shell auto-completion scripts

### Freon (Load Generator) Subcommands
- `cgom` - Container generator for OM metadata
- `ombg` - OM bucket generator
- `ommg` - OM metadata generator
- Various other load generation tools

### Running Ozone Locally

**Docker Quick Start:**
```bash
docker run -p 9878:9878 apache/ozone
```

**From Source:**
```bash
cd hadoop-ozone/dist/target/ozone-*/compose/ozone
docker-compose up -d --scale datanode=3
```

## Development Workflow

### 1. Jira Issues
- All changes require a JIRA issue in the [HDDS project](https://issues.apache.org/jira/projects/HDDS/)
- Format: HDDS-XXXX
- Include clear description, problem statement, and value

### 2. Branch Naming
```bash
git checkout -b HDDS-1234
```

### 3. Code Changes
- Make minimal, focused changes
- Follow existing code patterns
- Update documentation if needed
- Add tests for new functionality

### 4. Pre-commit Checks
Run these before creating a PR:
```bash
./hadoop-ozone/dev-support/checks/checkstyle.sh
./hadoop-ozone/dev-support/checks/unit.sh
```

### 5. Pull Request
- Include JIRA link in PR description
- Provide testing instructions
- Set JIRA to "Patch Available"

## Important Files and Patterns

### Configuration Files
- `ozone-site.xml` - Main Ozone configuration
- `ozone-default.xml` - Default configuration values
- Located in: `hadoop-hdds/common/src/main/resources/`

### Protocol Buffers
- `.proto` files define RPC interfaces
- Generated code in `target/generated-sources/`
- Regenerate with: `mvn clean compile`

### CLI Implementation
- Uses Picocli framework for command-line parsing
- Commands extend `GenericCli` or `Callable<Void>`
- Subcommands use `@Command` annotation
- Example: `hadoop-ozone/cli-shell/src/main/java/org/apache/hadoop/ozone/shell/Shell.java`

## Common Patterns

### 1. Command-Line Tools
```java
@Command(name = "command-name",
    description = "Command description",
    mixinStandardHelpOptions = true)
public class MyCommand extends GenericCli implements Callable<Void> {
    @Override
    public Void call() throws Exception {
        // Implementation
    }
}
```

### 2. Configuration Access
```java
OzoneConfiguration conf = new OzoneConfiguration();
String value = conf.get("ozone.property.name");
```

### 3. Testing Patterns
```java
@Test
public void testFeature() {
    // Arrange
    // Act
    // Assert
}
```

## Documentation

### Location
- Developer docs: `hadoop-hdds/docs/content/`
- Format: Markdown
- Website: https://ozone.apache.org/docs/

### Building Docs
Documentation is built with Hugo and included in the main build.

## Debugging Tips

### 1. Enable Verbose Logging
```bash
ozone --verbose [command]
```

### 2. Check Logs
```bash
# Container logs
docker logs <container_id>

# Local deployment
tail -f hadoop-ozone/dist/target/ozone-*/logs/*.log
```

### 3. Common Issues
- **Build failures**: Check Maven version (3.6+) and Java version (1.8+)
- **Test failures**: Ensure Docker and docker-compose are installed
- **Port conflicts**: Default ports: 9878 (S3), 9862 (Datanode), 9874 (OM)

## Key Technologies and Frameworks

- **Apache Ratis**: RAFT consensus protocol implementation
- **RocksDB**: Embedded key-value store for metadata
- **Netty**: Async event-driven network framework
- **Protobuf**: Protocol buffers for RPC
- **Picocli**: Command-line interface framework
- **JUnit**: Testing framework
- **Mockito**: Mocking framework for tests
- **Docker**: Container runtime for testing and deployment
- **Kubernetes**: Orchestration support

## Best Practices for AI Assistance

### When Suggesting Code Changes:
1. **Understand the context**: Review related classes and interfaces
2. **Follow existing patterns**: Match the style of surrounding code
3. **Minimize changes**: Make surgical, focused modifications
4. **Consider backwards compatibility**: Ozone is a mature project
5. **Add tests**: Include unit tests for new functionality
6. **Update documentation**: If changing public APIs or behavior

### When Analyzing Code:
1. **Check JIRA**: Look for related issues and design documents
2. **Review commit history**: Understand why code was written this way
3. **Consider performance**: Ozone handles billions of objects
4. **Think about scale**: Changes should work at petabyte scale
5. **Security matters**: Always consider security implications

### When Answering Questions:
1. **Reference documentation**: Link to official docs when available
2. **Provide examples**: Include code snippets from the actual codebase
3. **Explain trade-offs**: Discuss pros and cons of different approaches
4. **Consider the architecture**: Understand distributed system implications

## Getting Help

- **Mailing List**: dev@ozone.apache.org
- **Slack**: #ozone channel on ASF Slack
- **GitHub Discussions**: https://github.com/apache/ozone/discussions
- **Weekly Calls**: Open community calls (see wiki)
- **Jira**: https://issues.apache.org/jira/projects/HDDS/

## Security Considerations

- Never commit credentials or secrets
- Follow secure coding practices
- Report security issues per SECURITY.md
- Test security features (Kerberos, ACLs, encryption)
- Consider multi-tenancy implications

## Additional Resources

- **Website**: https://ozone.apache.org
- **Documentation**: https://ozone.apache.org/docs/
- **Wiki**: https://cwiki.apache.org/confluence/display/OZONE/
- **GitHub**: https://github.com/apache/ozone
- **Design Docs**: Check JIRA for Ozone Enhancement Proposals (OEPs)

## Commands Reference Quick Sheet

```bash
# Build
mvn clean install -DskipTests

# Quick verification
mvn clean verify -DskipTests -DskipShade -DskipRecon

# Run checkstyle
./hadoop-ozone/dev-support/checks/checkstyle.sh

# Run unit tests
./hadoop-ozone/dev-support/checks/unit.sh

# Start local cluster
cd hadoop-ozone/dist/target/ozone-*/compose/ozone
docker-compose up -d --scale datanode=3

# Stop local cluster
docker-compose down

# View logs
docker-compose logs -f

# Run Freon benchmark
ozone freon randomkeys --num-of-keys 1000 --num-of-threads 10

# Check Ozone version
ozone version

# Generate shell completion
ozone completion bash > ozone-completion.sh
```

---

**Note**: This document is intended to help AI assistants like Google Gemini CLI understand and work effectively with the Apache Ozone codebase. When in doubt, consult the official documentation at https://ozone.apache.org/docs/ or ask the community at dev@ozone.apache.org.
