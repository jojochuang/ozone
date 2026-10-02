# RepliedCallIds `sent` map leak repro

Minimal Ratis-only repro for [CDPD-123869](https://cloudera.atlassian.net/browse/CDPD-123869):
read-only `watch` RPCs on a long-lived `RaftClient` grow `RaftClientImpl.RepliedCallIds#sent`.

## Run

Use JDK 17+:

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-21.jdk/Contents/Home
mvn -f dev-support/replied-call-ids-leak-repro/pom.xml -q exec:java
```

Expected output ends with `Repro succeeded` and a `sent map size` that increases by roughly one entry per
write+watch iteration (~100).

## Ozone integration test

When the full Ozone tree builds, the same behavior is covered by
`hadoop-ozone/integration-test/.../TestRepliedCallIdsSentGrowth.java` against a mini Ozone cluster and
`XceiverClientRatis`.

```bash
mvn -pl :ozone-integration-test test -Dtest=TestRepliedCallIdsSentGrowth -DskipShade -DskipRecon -DskipDocs
```
