package com.etao.lz.star.spout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.etao.lz.star.StarConfig;
import com.google.protobuf.GeneratedMessage;

public class FileLogGenerator implements LogGenerator, Runnable {

	private static final long serialVersionUID = -5123664852556342061L;

	private Log LOG = LogFactory.getLog(FileLogGenerator.class);

	private BlockingQueue<byte[]> queue;
	private StarConfig starConfig;
	private String path;
	private transient TTLogBuilder logBuilder;
	private String tag = "";
	private transient ExecutorService service;

	@Override
	public void run() {
		InputStream ins = TTLogGenerator.class.getResourceAsStream(path);
		if (ins == null) {
			LOG.error(path + "not found!");
			throw new IllegalStateException(path + "not found!");
		}
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(ins,"utf-8"));
			String line = null;
			while ((line = reader.readLine()) != null) {
				for (GeneratedMessage log : logBuilder.build(tag,
						line.getBytes("utf-8"))) {
					try {
						byte[] data = log.toByteArray();
						queue.put(data);
					} catch (InterruptedException e) {
						LOG.error(e.getMessage());
					}
				}
			}
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}finally{
			if(reader != null)
			{
				try {
					reader.close();
				} catch (IOException e) {}
			}
		}
	}

	@Override
	public void config(StarConfig config) {
		this.starConfig = config;
		path = config.get("input_path");
		tag = config.get("tt_tag", "");
	}

	@Override
	public void open(BlockingQueue<byte[]> queue, String name, int thisTaskId) {
		this.queue = queue;
		String logBuilderClass = starConfig.get("log_builder");
		if (logBuilderClass == null) {
			throw new IllegalStateException("Missing log_builder config");
		}
		try {
			logBuilder = (TTLogBuilder) Class.forName(logBuilderClass)
					.newInstance();
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
		logBuilder.config(starConfig);
		service = Executors.newSingleThreadExecutor();
		service.execute(this);
	}

	public void shutdown() {
		service.shutdown();
		try {
			service.awaitTermination(1, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
		}
	}

	@Override
	public void getStat(HashMap<String, String> map) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long getLastTupleTime() {
		return 0;
	}
}
