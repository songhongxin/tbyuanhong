package com.etao.lz.star.spout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.etao.lz.star.StarConfig;
import com.taobao.timetunnel.client.MessageConsumer;
import com.taobao.timetunnel.client.TimeTunnelClientException;
import com.taobao.timetunnel.client.TimeTunnelSessionFactory;
import com.taobao.timetunnel.client.conf.ConsumerConfig;
import com.taobao.timetunnel.client.conf.TimeTunnelConfig;

/**
 * 
 * Receive TimeTunel log, The TT API is like this:
 * 
 * <pre>
 * 1) TimeTunnelSessionFactory sessionFactory = TimeTunnelSessionFactory.getInstance();
 * 	  MessageConsumer consumer = sessionFactory.createConsumer(consumerConfig);
 * 2) Iterator<Message> iterator = consumer.iterator();
 *    while (iterator != null && iterator.hasNext()) {
 *      Message next = iterator.next();
 *      //process next.getData()
 *    }
 * </pr>
 * TTLogGenerator will do step 1, then it will initiate multi-thread to do step 2(TTLogReader)
 */
public class TTLogGenerator implements LogGenerator {

	private static final long serialVersionUID = -5672552953849732007L;

	private transient TimeTunnelConfig ttConfig;
	private transient TimeTunnelSessionFactory sessionFactory;
	private transient ConsumerConfig consumerConfig;
	private transient MessageConsumer consumer;
	private transient TTLogReader logReaders[];
	private Log LOG = LogFactory.getLog(TTLogGenerator.class);

	private StarConfig starConfig;
	private String topic;
	private String subscriber;
	private String router;
	private String username;
	private String password;
	private int poolsize;
	private int poll_thread_num;

	private transient BlockingQueue<byte[]> queue;
	private transient ExecutorService service;

	@Override
	public void config(StarConfig config) {
		starConfig = config;
		topic = (String) config.get("topic");
		subscriber = (String) config.get("subscriber");
		router = (String) config.get("router");
		username = (String) config.get("username");
		password = (String) config.get("password");
		poolsize = Integer.parseInt(config.get("poolsize"));

		LOG.info(String
				.format("TT topic = %s, subscriber = %s, username = %s, password = %s, poolsize = %d",
						topic, subscriber, username, password, poolsize));
		LOG.info(String.format("TT router = %s", router));
	}

	public void open(BlockingQueue<byte[]> queue, String name, int taskId) {

		this.queue = queue;

		ttConfig = new TimeTunnelConfig(topic);
		ttConfig.setRouterURL(router);
		ttConfig.setUser(username);
		ttConfig.setPassword(password);

		sessionFactory = TimeTunnelSessionFactory.getInstance();
		consumerConfig = new ConsumerConfig(ttConfig);
		consumerConfig.setSubscriberId(subscriber);
		consumerConfig.setConnectionPoolSize(poolsize);

		List<String> tags = new ArrayList<String>(starConfig.getLogMappings()
				.getMapNames());
		if (tags.isEmpty()) {
			throw new IllegalStateException("Missing topic mapping!");
		} else if (tags.size() > 1) {
			StringBuilder sb = new StringBuilder();
			sb.append(String.format("__subtopic__='%s'", tags.get(0)));
			tags.remove(0);
			for (String tag : tags) {
				sb.append(String.format(" or __subtopic__='%s' ", tag));
			}
			LOG.info("Filter:" + sb);
			consumerConfig.setFetchFilter(sb.toString());
		}

		consumerConfig.setFetchTimeout(0);

		try {
			consumer = sessionFactory.createConsumer(consumerConfig);
		} catch (TimeTunnelClientException e) {
			LOG.error(e.toString());
			throw new IllegalStateException(e);
		}
		if (tags.size() == 1) {
			consumer.subscribe(tags.get(0));
		}
		poll_thread_num = starConfig.getInt("poll_thread_num");
		service = Executors.newFixedThreadPool(poll_thread_num);
		logReaders = new TTLogReader[poll_thread_num];
		for (int i = 0; i < poll_thread_num; ++i) {
			logReaders[i] = new TTLogReader(queue, consumer, starConfig);
			service.execute(logReaders[i]);
		}

	}

	public void notifyEmit(byte[] msg) {

	}

	public BlockingQueue<byte[]> getQueue() {
		return queue;
	}

	public long getLastTupleTime() {
		long ts = 0;
		for (int i = 0; i < poll_thread_num; ++i) {
			if (ts < logReaders[i].getLastTupleTime()) {
				ts = logReaders[i].getLastTupleTime();
			}
		}
		return ts;
	}

	@Override
	public void getStat(HashMap<String, String> map) {
		for (int i = 0; i < poll_thread_num; ++i) {
			map.put("Thread-"+(i + 1), logReaders[i].getStatus());
		}
	}
}
