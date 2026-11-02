---
title: "Spark Integration"
date: "2025-06-07"
weight: 3
menu:
  main:
    parent: Application Integrations
summary: How to use Apache Spark with Apache Ozone for data processing on YARN.
---
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

# Apache Spark Integration with Ozone

Apache Spark can run on YARN and access data stored in Apache Ozone. This allows Spark jobs to read from and write to Ozone buckets using the Ozone FileSystem (OFS) or Hadoop Compatible FileSystem (HCFS) interface.

## Accessing Ozone from Spark on YARN

If Ozone is not configured as the default file system, you may need to specify the Ozone file system URI using the `spark.yarn.access.hadoopFileSystems` property when submitting jobs. For example:

```bash
spark-shell \
  --conf "spark.yarn.access.hadoopFileSystems=ofs://ozone1707264383"
```

Similarly, for `spark-submit`:

```bash
spark-submit \
  --conf "spark.yarn.access.hadoopFileSystems=ofs://ozone1707264383" \
  --class org.example.YourApp your-app.jar
```

If `spark.yarn.access.hadoopFileSystems` is not specified, Spark may fail to request an Ozone delegation token, resulting in an error such as:

```bash
Caused by: org.apache.hadoop.security.AccessControlException: Client cannot authenticate via:[TOKEN, KERBEROS]
```

## Using Spark3

With Spark3, you can use the following property:

```bash
spark3-shell \
  --conf "spark.kerberos.access.hadoopFileSystems=ofs://ozone1707264383"
```

## Handling Token Renewal Errors

If the Spark shell fails due to token renewal errors like the following:

```bash
24/02/08 01:24:30 ERROR repl.Main: Failed to initialize Spark session.
org.apache.hadoop.yarn.exceptions.YarnException: Failed to submit application_1707350431298_0007 to YARN : Failed to renew token: Kind: HDFS_DELEGATION_TOKEN, Service: 10.140.99.144:8020, Ident: (token for systest:
 HDFS_DELEGATION_TOKEN owner=systest@SRC.COMOPS.SITE, renewer=yarn, realUser=, issueDate=1707355458945, maxDate=1707960258945, sequenceNumber=50, masterKeyId=14)
```

add the following property to exclude Ozone from token renewal:

```bash
--conf "spark.yarn.kerberos.renewal.excludeHadoopFileSystems=ofs://ozone1707264383"
```

For example:

```bash
spark3-shell \
  --conf "spark.kerberos.access.hadoopFileSystems=ofs://ozone1707264383" \
  --conf "spark.yarn.kerberos.renewal.excludeHadoopFileSystems=ofs://ozone1707264383"
```

## Spark accessing Ozone S3
