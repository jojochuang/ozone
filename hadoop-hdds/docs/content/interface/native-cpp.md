<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

---
title: "C Client for Ozone File System (libo3fs)"
summary: "Documentation for libo3fs, a JNI-based C API for Ozone File System."
weight: 40
keywords: ["ozone", "c client", "libo3fs", "libhdfs", "JNI", "native client", "interface"]
description: "Learn to use libo3fs, a JNI-based C API, to interact with Apache Ozone File System."
aliases: []
toc: true
draft: false
---

# C Client for Ozone File System (libo3fs)

## Overview

`libo3fs` is a JNI (Java Native Interface) C API for the Ozone File System, enabling read and write operations. It utilizes `libhdfs`, Hadoop's JNI C API for HDFS, which is typically pre-compiled in `$HADOOP_HDFS_HOME/lib/native/libhdfs.so`.

Use `libo3fs` to integrate Ozone into C applications, leveraging `libhdfs` patterns and JNI for communication with Ozone's Java components.

## The APIs

`libo3fs` offers a subset of Ozone FileSystem Java client features. API details are in the C header file `o3fs.h`, located at `${OZONE_HOME}/native-client/libo3fs/o3fs.h`.

## Requirements

1.  **Hadoop**: With compiled `libhdfs.so`.
2.  **Linux Kernel**: > 2.6.9.
3.  **Compiled Ozone**.

## Compilation

Compile `libo3fs` sources and examples, then link them. Replace `{OZONE_HOME}`, `{HADOOP_HOME}`, and JDK paths as needed.

### 1. Compile `o3fs.c`
In `${OZONE_HOME}/native-client/libo3fs`:
```bash
gcc -fPIC -pthread -I ${OZONE_HOME}/native-client/libo3fs -I ${HADOOP_HOME}/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs/include -g -c o3fs.c
```

### 2. Compile Example Files
In `${OZONE_HOME}/native-client/libo3fs-examples`:
```bash
# libo3fs_read.c
gcc -fPIC -pthread -I ${OZONE_HOME}/native-client/libo3fs -I ${HADOOP_HOME}/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs/include -g -c libo3fs_read.c

# libo3fs_write.c
gcc -fPIC -pthread -I ${OZONE_HOME}/native-client/libo3fs -I ${HADOOP_HOME}/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs/include -g -c libo3fs_write.c
```

### 3. Generate `libo3fs.so` Shared Library
Use `o3fs.o` and `hdfs.o` (path from your Hadoop build may vary):
```bash
# Path to hdfs.o is illustrative
gcc -shared o3fs.o ${HADOOP_HOME}/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs/mybuild/hdfs.o -o libo3fs.so
```
*(Note: The path to `hdfs.o` might differ. You might link against `libhdfs.so` directly.)*

### 4. Generate Example Binaries (`o3fs_read`, `o3fs_write`)
Link example object files against `libhdfs`, `libjvm`, and `libo3fs.so`. Adjust library (`-L`) and include (`-I`) paths for your Java, Hadoop, and Ozone setup.
```bash
# For o3fs_read (replace /path/to/your/jdk/jre/lib/amd64/server)
gcc -L${HADOOP_HOME}/hadoop-hdfs-project/hadoop-hdfs-native-client/target/native/target/usr/local/lib \\
    -o o3fs_read libo3fs_read.o \\
    -lhdfs -pthread \\
    -L/path/to/your/jdk/jre/lib/amd64/server -ljvm \\
    -L${OZONE_HOME}/native-client/libo3fs -lo3fs

# For o3fs_write
gcc -L${HADOOP_HOME}/hadoop-hdfs-project/hadoop-hdfs-native-client/target/native/target/usr/local/lib \\
    -o o3fs_write libo3fs_write.o \\
    -lhdfs -pthread \\
    -L/path/to/your/jdk/jre/lib/amd64/server -ljvm \\
    -L${OZONE_HOME}/native-client/libo3fs -lo3fs
```

## Deploying and Running Examples
Run compiled examples (`o3fs_write`, `o3fs_read`) after setting `LD_LIBRARY_PATH` and `CLASSPATH` (see Common Problems).

**`o3fs_write` Example:**
```bash
./o3fs_write <filename> <file_size> <buffer_size> <om_host> <om_port> <bucket_name> <volume_name>
```
Example:
```bash
./o3fs_write file1 102400 102400 127.0.0.1 9862 mybucket myvolume
```
This writes `file1` (100KB) to OM at `127.0.0.1:9862` in `myvolume/mybucket`. See example sources for `o3fs_read` usage.

## Common Problems

### 1. `CLASSPATH` Not Set
JNI needs Ozone/Hadoop Java classes. Set `CLASSPATH`:
```bash
# Adjust paths and versions for your Ozone installation
export OZONE_FS_CLASSPATH=$(${OZONE_HOME}/hadoop-ozone/dist/target/ozone-<version>/bin/ozone classpath ozone-filesystem --glob)
export CLASSPATH=$OZONE_FS_CLASSPATH:${OZONE_HOME}/hadoop-ozone/dist/target/ozone-<version>/share/ozone/lib/ozone-filesystem-<version>.jar:$CLASSPATH
```

### 2. `LD_LIBRARY_PATH` Not Set
Linker needs `libhdfs.so`, `libjvm.so` (JDK), and `libo3fs.so`. Set `LD_LIBRARY_PATH`:
```bash
# Adjust paths for your Hadoop, JDK, and Ozone setup
export LD_LIBRARY_PATH=${HADOOP_HOME}/hadoop-hdfs-project/hadoop-hdfs-native-client/target/native/target/usr/local/lib:/path/to/your/jdk/jre/lib/amd64/server:${OZONE_HOME}/native-client/libo3fs:$LD_LIBRARY_PATH
```

### 3. `ozone-site.xml` Not Configured/Accessible
`libo3fs` needs Ozone cluster configuration from `ozone-site.xml`. Ensure it's accessible and configured.
Minimal `ozone-site.xml` (match your cluster):
```xml
<configuration>
  <properties>
    <property>
      <name>ozone.om.address</name>
      <value>localhost:9862</value> <!-- Your OM address -->
    </property>
    <!-- Other necessary client configurations -->
  </properties>
</configuration>
```
Place in a Hadoop configuration directory (e.g., `${OZONE_HOME}/hadoop-ozone/dist/target/ozone-<version>/etc/hadoop/`) or a directory in your `CLASSPATH`.

---

For API details, see `o3fs.h` and example source code.
