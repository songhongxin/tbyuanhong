#!/bin/bash

#---------------------------------------------------
# Monitor nginx. 
# Should ONLY be run on nginx server!!!
#----------------------------------------------------


NUM_TAR=`ps -ef | grep "nginx: master process"| grep -v grep | wc -l`

#echo $NUM_TAR 

if [ $NUM_TAR -eq 0 ]; then
	/home/guoxiang.lgx/nginx/sbin/nginx
fi

