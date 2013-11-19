package com.etao.lz.star.output;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.Tools;
import com.google.protobuf.GeneratedMessage;

public class OutputThread implements Runnable {
	private ArrayBlockingQueue<Item> queue;
	private LogOutput output;
	private long count;
	
	private static Log LOG = LogFactory.getLog(MultiThreadLogOutput.class);
	
	private static class Item {
		private byte[] data;
		private GeneratedMessage log;

		public Item(byte[] data, GeneratedMessage log) {
			this.data = data;
			this.log = log;
		}

		public byte[] getData() {
			return data;
		}

		public GeneratedMessage getLog() {
			return log;
		}
	}
	
	public OutputThread(LogOutput output, int capacity) {
		queue = new ArrayBlockingQueue<Item>(capacity);
		this.output = output;
	}

	public void add(byte[] data, GeneratedMessage log) {
		try {
			queue.put(new Item(data, log));
		} catch (InterruptedException e) {
			LOG.error(Tools.formatError("Queue Log Error", e));
		}
	}

	@Override
	public void run() {
		while (true) {

			try {
				Item take = queue.take();
				output.output(take.getData(), take.getLog());
				++count;
			} catch (Exception e) {
				LOG.error(Tools.formatError("Write Log Error", e));
			}
		}

	}
	
	public void config(StarConfig config)
	{
		output.config(config);
	}
	
	public void open(int id)
	{
		output.open(id);
	}
	public long getCount() {
		return count;
	}

	public int getQueueSize()
	{
		return queue.size();
	}

	public void getStat(Map<String, String> map) {
		output.getStat(map);
	}
};
