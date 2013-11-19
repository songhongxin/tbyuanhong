#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
HOSTS=[
	'storm5lzdp143152.cm4',
	'storm4lzdp143102.cm4',
	'storm2lzdp143184.cm4',
	'st71lzdp144013.cm4',
	'mysql2lzdw.cm4',
	'mysql3lzdw.cm4',
	'dt35lzdp157093.cm4',
	'dt36lzdp157094.cm4',
	'dt37lzdp157095.cm4',
	'async3lzdp.cm4',
	'rcmdblzdp.cm4',
	'web6lzdp208046.cm4',
	'storm1lzdp208037.cm4',
	'storm3lzdp208124.cm4',
];

def alert(title, msg):
        msg = msg.replace("'", " ");
        msg = msg.replace("\n", " ");
        os.system("sendww.sh -s '%s' -n '国相' -m '%s'" % (title, msg))
        os.system("sendww.sh -s '%s' -n '太奇' -m '%s'" % (title, msg))

def run(cmd):
	hResult = os.popen(cmd);
	result = hResult.readlines();
	hResult.close()
	return result

for host in HOSTS:
	result = run('ping %s -W 3 -c 3' % host)
	print result
	if len(result) == 0:
		alert("!!!Storm Machine Is Down", "%s is unreachable!" % host);


html = run('curl http://storm3lzdp208124.cm4:8080');

if len(html) == 0:
	 alert("!!!Storm Machine Is Down!", "curl http://storm3lzdp208124.cm4:8080 failed");
html = " ".join(html);
i = html.find('Supervisor summary');
if(i < 0):
	 alert("Storm Machine Is Down!", "curl http://storm3lzdp208124.cm4:8080 failed");
html = html[i:]
for host in HOSTS:
	if html.find(host) < 0:
		alert("!!!Storm Machine Is Down", "%s is unreachable!" % host);
		
