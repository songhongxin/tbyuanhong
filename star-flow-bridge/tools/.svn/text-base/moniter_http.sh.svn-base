#!/bin/bash

#---------------------------------------------------
# Monitor supervisor. 
# Should ONLY be run on supervisor host!!!
#----------------------------------------------------


NUM=`ps -ef | grep "java -jar monitor-http.jar" |grep -v grep | wc -l`

if [ $NUM -eq 0 ]; then
	(cd /home/guoxiang.lgx/star_storm/tools;java -jar monitor-http.jar &)
fi

NUM=`ps -ef | grep "java -jar subscribe-http.jar" |grep -v grep | wc -l`

if [ $NUM -eq 0 ]; then
	(cd /home/guoxiang.lgx/star_storm/tools;java -jar subscribe-http.jar &)
fi
