package com.etao.lz.star.output;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.Tools;
import com.etao.lz.star.ZKTools;
import com.etao.lz.star.output.broadcast.SendDataThread;
import com.etao.lz.star.output.broadcast.SubscriberInfo;
import com.google.common.collect.Sets;
import com.google.protobuf.GeneratedMessage;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.api.CuratorListener;

public class BroadcastLogOutput implements LogOutput {

	private static final long serialVersionUID = 397859909933671702L;
	private static Log LOG = LogFactory.getLog(BroadcastLogOutput.class);
	private String cluster;
	private String topo;

	private transient CuratorFramework client;
	private transient ExecutorService service;
	private int _max_queue_size;
	private int refresh_interval;
	private StarConfig config;
	private transient Map<String, SendDataThread> threadMap;

	@Override
	public void config(StarConfig config) {
		this.config = config;
		cluster = config.get(StarConfig.StormZookeeperCluster);
		topo = config.getTopoName();
		refresh_interval = config.getInt("refresh_interval", 5000);
		_max_queue_size = config.getInt("broadcast_queue_length", 0);
		LOG.info("subscribe parent:" + SubscriberInfo.getZKParent(topo));
	}

	public synchronized void termiante() {
		for (SendDataThread info : threadMap.values()) {
			info.close();
		}
	}

	SendDataThread getBroadcastThread(String name) {
		return threadMap.get(name);
	}

	private synchronized void refreshConnection() {
		try {
			String parent = SubscriberInfo.getZKParent(topo);
			Set<String> children = new HashSet<String>(client.getChildren()
					.forPath(parent));

			Set<String> currentThreads = new HashSet<String>(threadMap.keySet());
			for (String dead : Sets.difference(currentThreads, children)) {
				SendDataThread info = threadMap.remove(dead);
				if (info == null)
					continue;
				LOG.info(String.format("%s Detect %s dead, close connection.",
						Tools.getIdAndHost(), dead));
				info.close();
			}

			for (String id : children) {
				String path = SubscriberInfo.getZKPath(topo, id);
				String attributes = new String(client.getData().forPath(path),
						"UTF-8");
				SubscriberInfo item = new SubscriberInfo(topo, id);
				item.setMaxQueueSize(_max_queue_size);

				item.parse(attributes);
				SendDataThread thread = threadMap.get(id);
				if (thread == null) {
					if (item.getCluster() == null || !item.isEnable())
						continue;
					thread = new SendDataThread(item, config);
					threadMap.put(id, thread);
					service.execute(thread);
				} else {
					if(item.isEnable()) continue;
					threadMap.remove(id);
					thread.close();
					LOG.info(String.format("%s Detect %s disabled, close connection.",
						Tools.getIdAndHost(), id));
				}

			}
		} catch (Exception e) {
			LOG.error(Tools.formatError("BroadcastLogOutput.refresh", e));
			client.close();
			client = ZKTools.startCuratorClient(cluster);
		}
	}

	@Override
	public void open(int id) {

		service = Executors.newFixedThreadPool(100);

		threadMap = Collections
				.synchronizedMap(new HashMap<String, SendDataThread>());

		final String parent = SubscriberInfo.getZKParent(topo);
		client = ZKTools.startCuratorClient(cluster);
		try {
			if (client.checkExists().forPath(parent) == null) {
				throw new IllegalStateException("No directory in Zookeeper:"
						+ parent);
			}
			client.getChildren().watched().forPath(parent);
		} catch (Exception e) {
			LOG.error(Tools.formatError("BroadcastLogOutput.open", e));
		}

		client.getCuratorListenable().addListener(new CuratorListener() {
			@Override
			public void eventReceived(CuratorFramework arg0, CuratorEvent evt)
					throws Exception {
				if (evt.getType() == CuratorEventType.WATCHED) {
					WatchedEvent watchedEvent = evt.getWatchedEvent();
					EventType type = watchedEvent.getType();
					if (type == EventType.NodeChildrenChanged) {
						refreshConnection();
					}
					synchronized (BroadcastLogOutput.this) {
						client.getChildren().watched().forPath(parent);
					}
				}
			}
		});
		LOG.info("Start zookeeper watch!");
		Timer timer = new Timer();
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				refreshConnection();
			}
		}, 0, refresh_interval);
	}

	@Override
	public void output(byte[] data, GeneratedMessage log) {
		List<SendDataThread> threads = new ArrayList<SendDataThread>(
				threadMap.values());
		for (SendDataThread info : threads) {
			info.addData(data, log);
		}

	}

	@Override
	public void getStat(Map<String, String> map) {
		if(threadMap == null) return;
		for (SendDataThread thread : threadMap.values()) {
			StringBuilder sb = new StringBuilder();

			sb.append(String.format("%s=%d\n", SubscriberInfo.QUEUE_LEN, thread.getQueue().size()));
			sb.append(String.format("%s=%d\n", SubscriberInfo.SEND_COUNT, thread.getSendCount()));
			sb.append(String.format("%s=%d\n", SubscriberInfo.IGNORE_COUNT, thread.getIgnoreCount()));
			map.put(thread.getId(), sb.toString());
		}
	}

}
