package com.etao.lz.star.output;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimerTask;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.Tools;
import com.etao.lz.star.output.hbase.LogWriter;
import com.google.protobuf.GeneratedMessage;

public class WriteToHBaseProcessor implements LogOutput {

	private static final long serialVersionUID = 1766703756308248243L;

	private StarConfig _config;

	private int taskId;
	

	private long refresh_queue_interval;

	private transient java.util.Timer timer;

	private static class StatItem
	{
		public long count;
		public long byteCount;
		public long ts;
	};
	private transient Map<String, StatItem> statmap;
	private Map<String,LogWriter> logProcessorMap;

	@Override
	public void config(StarConfig config) {
		_config = config;

		refresh_queue_interval = config.getInt("refresh_queue_interval");

		logProcessorMap = new HashMap<String, LogWriter>();
		statmap = new HashMap<String, StatItem>();
		for (String table : config.get("output_tables").split(",")) {
			table = table.trim();
			if (table.length() == 0)
				continue;

			LogWriter tableInfo = new LogWriter(config, table);

			logProcessorMap.put(tableInfo.getType(), tableInfo);

		}
	}

	@Override
	public final void open(int id) {


		if (_config.getBool("use_fixed_taskid", false)) {
			taskId = 1;
		} else {
			taskId = id;
		}


		timer = new java.util.Timer();

		for (LogWriter processor : logProcessorMap.values()) {
			processor.open(taskId);

		}

		timer.schedule(new TimerTask() {

			@Override
			public void run() {
				for (LogWriter processor : logProcessorMap.values()) {
					processor.flush();
				}
			}

		}, refresh_queue_interval, refresh_queue_interval);
		
		statmap = new HashMap<String, WriteToHBaseProcessor.StatItem>();
	}

	@Override
	public final void output(byte[] data, GeneratedMessage log) {
		String destType = _config.getDestType(log);
		LogWriter logWriter = logProcessorMap.get(destType);
		if(logWriter == null) return;
		logWriter.add(data, log, taskId);
		StatItem item = statmap.get(destType);
		if(item == null)
		{
			item = new StatItem();
			statmap.put(destType, item);
		}
		++item.count;
		item.byteCount += data.length;
		item.ts = _config.getLogTime(log);
	}

	@Override
	public void getStat(Map<String, String> map) {
		for(Entry<String,StatItem> entry:statmap.entrySet())
		{
			StringBuilder sb = new StringBuilder();
			sb.append("Count = ");
			sb.append(entry.getValue().count);
			sb.append("\nByteCount = ");
			sb.append(entry.getValue().byteCount);
			sb.append("\nLastTupleTime = ");
			sb.append(Tools.formatTime(entry.getValue().ts));
			map.put(entry.getKey(), sb.toString());
		}
	}
}
