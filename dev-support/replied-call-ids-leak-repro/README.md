# RepliedCallIds `sent` map leak repro

Minimal Ratis-only repro for [CDPD-123869](https://cloudera.atlassian.net/browse/CDPD-123869) /
[RATIS-2729](https://issues.apache.org/jira/browse/RATIS-2729): read-only `watch` RPCs on a
long-lived `RaftClient` grow `RaftClientImpl.RepliedCallIds#sent` on unpatched Ratis clients.

## Run (patched Ratis / fix verified)

Use JDK 17+ and a local Ratis build that includes the RATIS-2729 fix (`ratis.version=3.3.0-SNAPSHOT` after
`mvn install -DskipTests` in the Ratis tree):

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-21.jdk/Contents/Home
mvn -f dev-support/replied-call-ids-leak-repro/pom.xml -q exec:java
```

Expected output ends with `Fix verified` and a bounded `sent map size` (typically 0 after bootstrap).

## Run (demonstrate leak on release bits)

```bash
mvn -f dev-support/replied-call-ids-leak-repro/pom.xml -q exec:java \
  -DexpectLeak=true -Dratis.version=3.3.1
```

Expected: `sent map size` grows by roughly one entry per write+watch iteration (~100).

## Ozone integration test

When the full Ozone tree builds, the same behavior is covered by
`hadoop-ozone/integration-test/.../TestRepliedCallIdsSentGrowth.java` against a mini Ozone cluster and
`XceiverClientRatis`.

```bash
mvn -pl :ozone-integration-test test -Dtest=TestRepliedCallIdsSentGrowth -DskipShade -DskipRecon -DskipDocs
```
