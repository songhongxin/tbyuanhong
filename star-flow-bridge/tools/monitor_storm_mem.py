#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import sys
import re
import logging
import socket
import logparser
import time
import datetime

HOST = socket.gethostname()
MEM_LIMIT = {
	'web6lzdp208046.cm4'	:56,
	'storm3lzdp208124.cm4'	:56,
	'storm1lzdp208037.cm4'	:56,
	'async3lzdp.cm4'	:56,
	'mysql3lzdw.cm4'	:56,
	'mysql2lzdw.cm4'	:18,
	'st71lzdp144013.cm4'	:56,
	'dt35lzdp157093.cm4'	:56,
	'dt36lzdp157094.cm4'	:56,
	'dt37lzdp157095.cm4'	:56,
	'rcmdblzdp.cm4'		:56,
	'storm5lzdp143152.cm4'	:56,
	'storm4lzdp143102.cm4'	:56,
	'storm2lzdp143184.cm4'	:56,
	'storm6lzdp158044.cm4'	:56,
}

if not MEM_LIMIT.has_key(HOST): 
	raise Exception('Unknown host ' + HOST);

print 'MEMORY LIMIT %d GB' % MEM_LIMIT[HOST]

def alert(title, msg):
	msg = msg.replace("'", " ");
	msg = msg.replace("\n", " ");
	logging.warning(msg)
	os.system("sendww.sh -s '%s' -n '国相' -m '%s'" % (title, msg))
	os.system("sendww.sh -s '%s' -n '太奇' -m '%s'" % (title, msg))
	os.system("sendww.sh -s '%s' -n '九翎' -m '%s'" % (title, msg))
	os.system("sendww.sh -s '%s' -n '宋智' -m '%s'" % (title, msg))

def run_cmd(cmd):
	logging.debug(cmd)
	hResult = os.popen(cmd)
	result = hResult.readlines();
	hResult.close();
	return result


if not os.path.exists('/home/admin/star_storm'):
	os.mkdir('/home/admin/star_storm');
if not os.path.exists('/home/admin/star_storm/logs'):
	os.mkdir('/home/admin/star_storm/logs');
logging.basicConfig(filename='/home/admin/star_storm/logs/monitor.log', level=logging.DEBUG,
			format='[%(asctime)s] %(levelname)s %(message)s')

KEY = "backtype.storm.daemon";
MEM_PATTERN = re.compile('VmRSS:\s*(\d+)\s*kB')
#PS_CMD = 'ps aux | grep "%s"' % KEY
PS_CMD = 'ps aux'

def kill(pid, total, task, mem):
	kill_cmd = "kill %s" % pid
	logging.warning(kill_cmd)
	os.system(kill_cmd)
	msg = 'Total Memory %d KB, Task(PID = %s, NAME = %s, HOST=%s, USED_MEM=%d KB) is killed' % (
		total, pid, task, HOST, mem)
	alert('Memory Alert, Worker killed', msg)

start_time = datetime.datetime.now()
while True:
	mem_total = 0;
	mem_max = 0;
	task_max = '';
	pid_max = '';
	items = run_cmd("free")[2].split()
	mem_total = int(items[2])

	for line in run_cmd(PS_CMD)[1:]:
		items = line.split()
		pid = items[1]
		mem_used = int(items[5]) 
		task_name = "unknown";
		if len(items) > 10:
			task_name = items[10];

		if mem_used > 15 * 1024 * 1024:
			kill(pid, mem_total, task_name, mem_used);
			continue;

		if mem_used > mem_max:
			mem_max = mem_used;
			task_max = task_name
			pid_max = pid
		print task_name, mem_used

	if MEM_LIMIT[HOST] * 1024 * 1024 < mem_total:
		kill(pid_max, mem_total, task_max, mem_max);
	elif (datetime.datetime.now() -  start_time).seconds >= 55:
		break;
	else:
		time.sleep(5)
	

