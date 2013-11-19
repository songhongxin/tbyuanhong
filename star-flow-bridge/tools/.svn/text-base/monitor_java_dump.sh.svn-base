#!/bin/bash


if [ $# -ne 1 ]; then
	echo "Usage:" $0 "<monitered dir>" 
	exit 1
fi

USER1=`whoami`
if [ $USER1 != "admin" ];then
	echo $0 "must be run with user \"admin\""
	exit 2
fi

MNT_DIR=$1

if [ ! -d $MNT_DIR ];then
	#echo "dir not exist"
	exit 0
fi

cd $MNT_DIR

FILES=`ls -rt .`

#echo "FILES:" $FILES

NUM_FILES=`echo $FILES| wc -w `
#echo $NUM_FILES
FILE_LIMIT=10
#echo 2

n_file=$NUM_FILES

for  x  in   $FILES; do
	
	if [ $n_file -gt $FILE_LIMIT ];then
		rm -rf $x
		#echo "rm " $x 
		n_file=`expr $n_file - 1`
	else
		break
	fi
done


