#!/bin/bash

#---------------------------------------------------
# Monitor supervisor. 
# Should ONLY be run on supervisor host!!!
#----------------------------------------------------


CUR_UESR=`whoami`

if [ $CUR_UESR != "admin"  ];then
	echo \"$0\" "must be run with user - admin"
	exit 1
fi

NUM_SUPERVISOR=`ps -ef | grep -v "grep" | grep java| grep backtype.storm.daemon.supervisor | wc -l`

if [ $NUM_SUPERVISOR -eq 0 ]; then
	/home/admin/storm/bin/storm supervisor& > /dev/null 2>&1
fi

