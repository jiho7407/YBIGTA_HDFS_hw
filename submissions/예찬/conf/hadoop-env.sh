#!/usr/bin/env bash
# This file is sourced by Hadoop's own shell scripts.
# Do not enable strict shell modes here: Hadoop 3.3.x sources this file during
# its own bootstrap flow and references optional unset variables internally.

export JAVA_HOME="${JAVA_HOME:-/opt/java/openjdk}"
export HADOOP_HEAPSIZE_MAX="${HADOOP_HEAPSIZE_MAX:-384m}"
export HDFS_NAMENODE_OPTS="-Dhadoop.security.logger=INFO,RFAS ${HDFS_NAMENODE_OPTS:-}"
export HDFS_DATANODE_OPTS="-Dhadoop.security.logger=ERROR,RFAS ${HDFS_DATANODE_OPTS:-}"
export HDFS_JOURNALNODE_OPTS="-Dhadoop.security.logger=INFO,RFAS ${HDFS_JOURNALNODE_OPTS:-}"
export HDFS_ZKFC_OPTS="-Dhadoop.security.logger=INFO,RFAS ${HDFS_ZKFC_OPTS:-}"
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS:-} -Dhadoop.root.logger=ERROR,console"
