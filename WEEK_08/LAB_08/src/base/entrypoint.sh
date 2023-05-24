#!/bin/bash

# Format the NameNode (if it's the NameNode container)
if [[ $1 == "namenode" ]]; then
  $HADOOP_HOME/bin/hdfs namenode -format
fi

# Start the Hadoop services
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

# Keep the container running
tail -f /dev/null
