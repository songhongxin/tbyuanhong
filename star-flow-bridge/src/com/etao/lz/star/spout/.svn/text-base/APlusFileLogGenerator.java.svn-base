package com.etao.lz.star.spout;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.etao.lz.star.StarConfig;
import com.google.common.io.ByteStreams;
import com.google.protobuf.GeneratedMessage;

public class APlusFileLogGenerator implements LogGenerator, Runnable {

	private static final long serialVersionUID = -5123664852556342061L;

	private Log LOG = LogFactory.getLog(APlusFileLogGenerator.class);

	private BlockingQueue<byte[]> queue;
	private StarConfig starConfig;
	private String path;
	private String tag;
	private transient TTLogBuilder logBuilder;

	private transient ExecutorService service;

	@Override
	public void run() {
		InputStream ins = TTLogGenerator.class.getResourceAsStream(path);
		if (ins == null) {
			throw new IllegalStateException(path + "not found!");
		}
		LOG.info("Reading " + path);
		try {
			int logCount = 0;
			while (true) {
				int a = ins.read();
				StringBuilder sb = new StringBuilder();
				while (a > 0 && a != '\n') {
					sb.append((char)a);
					a = ins.read();
				}
				if(a <=0 ) break;
				int count = Integer.parseInt(sb.toString());
				LOG.info("read " + count);
				byte[] data = new byte[count];
				ByteStreams.readFully(ins, data);
				logBuilder.build(tag, data);
				for (GeneratedMessage log : logBuilder.build(tag,data)) {
					try {
						byte[] b = log.toByteArray();
						queue.put(b);
						++logCount;
					} catch (InterruptedException e) {
						LOG.error(e.getMessage());
					}
				}
			}
			LOG.info("LogCount "+ logCount);
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}finally{
			try {
				ins.close();
			} catch (IOException e) {}
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

	}

	@Override
	public long getLastTupleTime() {
		return 0;
	}
}
