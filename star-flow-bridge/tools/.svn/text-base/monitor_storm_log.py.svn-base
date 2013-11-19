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

if os.path.exists('/home/admin/star_storm/monitor.lock') :
	h_lock = open('/home/admin/star_storm/monitor.lock', 'r')
	pid = h_lock.read().strip();
	h_lock.close();
	if os.path.exists('/proc/%s' % pid):
		logging.warn('monitor process is still running, pid=%s, exit' % pid);
		alert('monitor_storm_worker.py abnormal', 
			'monitor_storm_worker.py on %s has running more than 5 minuts!' % HOST);
		sys.exit(1);
	else:
		logging.warn('monitor.lock with pid=%s exist, but process is not running, remove it.' % pid);
		os.remove('/home/admin/star_storm/monitor.lock');

h_lock = open('/home/admin/star_storm/monitor.lock', 'w')
h_lock.write(str(os.getpid()))
h_lock.close()

def parse_log_date(line):
	try:
		log_time = time.strptime(line,'%Y-%m-%d')
		return datetime.datetime(log_time[0], log_time[1], log_time[2]) 
	except Exception , e:
		return None

for log_file in os.listdir('/home/admin/storm/logs/'):
	pos =log_file.find('.log.')
	if pos > 0:
		log_date = parse_log_date('20' + log_file[pos + 5:])
		if log_date and logparser.NOW > log_date + datetime.timedelta(days=2):
			logging.info('remove ' + log_file)
			print 'remove ' + log_file
			os.remove('/home/admin/storm/logs/' + log_file)

	if log_file.startswith('worker') and log_file.endswith('.log'):
		parser = logparser.LogParser('/home/admin/storm/logs/%s' % log_file, HOST)
		count = parser.scan()
		print 'parsing %s,count = %d' % (log_file, count)
		logging.debug('parsing %s,count = %d' % (log_file, count))
		parser.alert(alert)


os.remove('/home/admin/star_storm/monitor.lock');
print "Done."
