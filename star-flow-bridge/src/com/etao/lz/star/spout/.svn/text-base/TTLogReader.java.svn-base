package com.etao.lz.star.spout;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.Tools;
import com.etao.lz.star.monitor.StatInfo;
import com.google.protobuf.GeneratedMessage;
import com.taobao.timetunnel.client.Message;
import com.taobao.timetunnel.client.MessageConsumer;

public class TTLogReader implements Runnable {
	private Log LOG = LogFactory.getLog(TTLogGenerator.class);
	private transient MessageConsumer consumer;
	public static final byte TIMETUNNEL_DELIMITER = '\n';

	private Map<String, StatInfo> receiveCountMap;
	private BlockingQueue<byte[]> queue;
	private TTLogBuilder logBuilder;
	private StarConfig starConfig;
	private long lastTs;

	public TTLogReader(BlockingQueue<byte[]> queue, MessageConsumer consumer,
			final StarConfig starConfig) {
		this.queue = queue;
		this.consumer = consumer;
		this.receiveCountMap = new HashMap<String, StatInfo>();
		this.starConfig = starConfig;

		String logBuilderClass = starConfig.get("log_builder");
		if (logBuilderClass == null) {
			throw new IllegalStateException("Missing log_builder config");
		}
		try {
			logBuilder = (TTLogBuilder) Class.forName(logBuilderClass)
					.newInstance();
			LOG.info("TTLogBuilder " + logBuilderClass);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
		logBuilder.config(starConfig);

	}

	public Map<String, StatInfo> getReceiveCountMap() {
		return receiveCountMap;
	}

	public void run() {
		while (true) {
			Iterator<Message> iterator = null;
			try {
				iterator = consumer.iterator();
			} catch (Exception e) {
				LOG.error("TT consumer.iterator() failed:" + e.getMessage());
				try {
					Thread.sleep(100);
				} catch (InterruptedException e1) {
				}
				continue;
			}
			while (iterator != null && iterator.hasNext()) {
				Message msg = iterator.next();
				List<GeneratedMessage> messages = null;
				try {
					messages = logBuilder.build(msg.getTag(), msg.getData());
				} catch (Throwable e) {
					LOG.error(Tools.formatError("Parse TT Log:", e));
					continue;
				}
				for (GeneratedMessage log : messages) {
					String destType = starConfig.getDestType(log);
					StatInfo l = receiveCountMap.get(destType);
					if (l == null) {
						l = new StatInfo();
						receiveCountMap.put(destType, l);
					}
					l.update();
					lastTs = starConfig.getLogTime(log);
					l.setLastReceivedLogTime(lastTs);

					try {
						byte[] result = log.toByteArray();
						queue.put(result);
					} catch (InterruptedException e) {
						LOG.error(Tools.formatError("TTLogReader", e));
					}
				}
			}
		}
	}

	public String getStatus() {

		StringBuilder sb = new StringBuilder();
		sb.append("error_count = ");
		sb.append(logBuilder.getErrorCount());
		sb.append("\n");
		sb.append("error_msg = ");
		sb.append(logBuilder.getLastErrorMsg());
		sb.append("\n");

		for (Entry<String, StatInfo> entry : receiveCountMap.entrySet()) {
			StatInfo statInfo = entry.getValue();
			sb.append(entry.getKey());
			sb.append("_count");
			sb.append(" = ");
			sb.append(statInfo.getCount());
			sb.append("\n");
			sb.append(entry.getKey());
			sb.append("_last_time");
			sb.append(" = ");
			sb.append(Tools.formatTime(statInfo.getLastReceivedLogTime()));
			sb.append("\n");
		}
		return sb.toString();
	}

	public long getLastTupleTime() {
		return lastTs;
	}

}
