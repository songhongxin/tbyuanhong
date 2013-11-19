#!/bin/bash

#---------------------------------------------------
# Monitor zk. 
# Should ONLY be run on zk server!!!
#----------------------------------------------------


CUR_UESR=`whoami`

if [ $CUR_UESR != "root"  ];then
	echo \"$0\" "must be run with user - root"
	exit 1
fi

NUM_TAR=`ps -ef | grep org.apache.zookeeper.server.quorum.QuorumPeerMain| grep -v storm| grep -v grep | wc -l`

#echo $NUM_TAR 

if [ $NUM_TAR -eq 0 ]; then
	/opt/taobao/install/zookeeper-3.3.4/bin/zkServer.sh start
        sleep 5
fi

