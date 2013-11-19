import os
import time
import datetime

NOW = datetime.datetime.now();
SCAN_PERIOD = 600
READ_COUNT = 409600
MAX_LOG_COUNT = 10000
class LogParser:
	def __init__(self, log_file, host):
		self.last_lines = ['', '', '','','','','','']
		self.error_count = 0
		self.last_error = None
		self.log_file = log_file
		self.host = host
	
	def alert(self, alerter):
		if self.error_count == 0:
			return
		alerter('STORM ERROR LOG', 'Host: %s<br>LogFile: %s<br>ErrorCount:%d<br>LatestErrorLog:<br>%s' %(
                                        self.host,
                                        self.log_file,
					self.error_count,
					self.last_error))

	def process_line(self,line):

		if line.find('[ERROR]') < 0:
			return;

		if line.find('[Star-Storm]') < 0:
			return;

		self.error_count = self.error_count + 1;
		if self.last_error == None:
			self.last_error = '%s<br>%s<br>%s<br>%s<br>%s<br>%s<br>%s<br>%s<br>' %(
				self.last_lines[7],
				self.last_lines[6],
				self.last_lines[5],
				self.last_lines[4],
				self.last_lines[3],
				self.last_lines[2],
				self.last_lines[1],
				self.last_lines[0])

	def parse_log_time(self, line):
		try:
			log_time = time.strptime(line[0:19],'%Y-%m-%d %H:%M:%S')
			return datetime.datetime(
				log_time[0], log_time[1], log_time[2], 
				log_time[3], log_time[4], log_time[5])
		except:
			return None


	def reverse_reader(self, h_log):
		h_log.seek(0, 2);
		content = ''
		while True:
			read_pos = h_log.tell()
			if read_pos < READ_COUNT:
				break
			h_log.seek(-READ_COUNT, 1);
			content = h_log.read(READ_COUNT) + content;
			h_log.seek(-READ_COUNT, 1);
			pos = content.find('\n');
			if pos > 0:
				for line in reversed(content[pos + 1:].split('\n')):
					yield line
				content = content[0:pos];
		if read_pos > 0:
			h_log.seek(0,0);
			content = h_log.read(read_pos) + content;

		for line in reversed(content.split('\n')):
			yield line

	def scan(self):
		h_log = open(self.log_file, 'r');
		reader = self.reverse_reader(h_log)
		count = 0;
		for line in reader:
			self.last_lines.append(line)
			self.last_lines = self.last_lines[-8:]

			log_time = self.parse_log_time(line)
			if log_time == None:
				continue
			count = count + 1
			if count % 1000 == 0:
				print count, NOW - log_time
			if count > MAX_LOG_COUNT:
				break;
			if NOW > log_time + datetime.timedelta(seconds=SCAN_PERIOD):
				break;
			self.process_line(line)
		h_log.close()
		return count

