#!/bin/bash

#---------------------------------------------------
# Monitor nimbus. Should ONLY be run on nimbus host
#----------------------------------------------------

CUR_UESR=`whoami`

if [ $CUR_UESR != "admin"  ];then
	echo \"$0\" "must be run with user - admin"
	exit 1
fi

CUR_HOST=`uname -n`
if [ $CUR_HOST != "storm3lzdp208124.cm4" ];then
	echo "ui should't run on this machine"
	exit 2
fi


NUM_TAR=`ps -ef | grep -v "grep" | grep java| grep backtype.storm.ui | wc -l`

if [ $NUM_TAR -eq 0 ]; then
	/home/admin/storm/bin/storm ui& > /dev/null 2>&1
	sleep 5
fi

