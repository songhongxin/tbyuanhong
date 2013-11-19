package com.etao.lz.star.output;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.etao.lz.star.StarConfig;
import com.google.protobuf.GeneratedMessage;

public class MultiThreadLogOutput implements LogOutput {

	private static final long serialVersionUID = -3487560888293832161L;
	
	private StarConfig config;
	
	private OutputThread runner[];

	@Override
	public void config(StarConfig config) {
		this.config = config;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(int id) {
		int threads = config.getInt("output_threads");
		int capacity = config.getInt("queue_size");

		try {
			Class cls = Class.forName(config.get("multithread_target"));
			this.runner = new OutputThread[threads];
			for (int i = 0; i < threads; ++i) {
				runner[i] = new OutputThread((LogOutput) cls.newInstance(),
						capacity);
				runner[i].config(config);
				runner[i].open(i);
				new Thread(runner[i]).start();
			}
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
		
		
	}

	@Override
	public void output(byte[] data, GeneratedMessage log) {
	
		int index = 0;
		
		for (int i = 1; i < runner.length; ++i) {
			if (runner[i].getQueueSize() < runner[index].getQueueSize()) {
				index = i;
			}
		}
		runner[index].add(data, log);

	}

	@Override
	public void getStat(Map<String, String> map) {
		
		for (int i = 0; i < runner.length; ++i) {
			StringBuilder value = new StringBuilder();
			value.append("QueueSize = ");
			value.append(runner[i].getQueueSize());
			value.append("\nCount = ");
			value.append(runner[i].getCount());
			Map<String, String> lmap = new HashMap<String,String>();
			runner[i].getStat(lmap);
			for(Entry<String,String> entry:lmap.entrySet())
			{
				for(String s:entry.getValue().split("\n"))
				{
					value.append("\n");
					value.append(entry.getKey() + "_"+ s);
				}

			}
			map.put(String.valueOf(i), value.toString());
			
		}
	}
	
	public int getThreads() {
		return runner.length;
	}

	public int getQueueSize(int i) {
		return runner[i].getQueueSize();
	}

	public long getWriteCount(int i) {
		return runner[i].getCount();
	}

}
