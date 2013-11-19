package com.etao.lz.star.output.broadcast;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.etao.lz.star.Tools;

public class SubscriberInfo {

	public static final String CLUSTER = "Cluster";
	public static final String FILTER = "Filter";
	public static final String QUEUE_MAX = "QueueMax";
	public static final String QUEUE_LEN = "QueueLen";
	public static final String SEND_COUNT = "SendCount";
	public static final String IGNORE_COUNT = "IgnoreCount";
	public static final String WWNOTIFY = "WWNotify";
	public static final String ENABLE = "Enable";
	public static final String PROJECTION = "Projection";

	public static final String ZK_DIR = "subscribe";

	private String topo;
	private String id;
	private String cluster;
	private String filter;
	private int max_queue_size;

	private HashSet<String> projection;

	private int queue_len;
	private long send_count;
	private long ignore_count;

	private String worker;
	private String wwnotify;

	private boolean enable;

	public void setEnable(boolean enable) {
		this.enable = enable;
	}

	private Log LOG = LogFactory.getLog(SubscriberInfo.class);

	public SubscriberInfo(String topo, String id) {
		this.topo = topo;
		this.id = id;
		this.cluster = "";
		this.filter = "";
		this.max_queue_size = 0;
		enable = false;
	}

	public static String getZKPath(String topo, String id) {
		return String.format("%s/%s/%s", topo, ZK_DIR, id);
	}

	public static String getZKParent(String topo) {
		return String.format("%s/%s", topo, ZK_DIR);
	}

	public void parse(String data) {
		for (String line : data.split("\n")) {
			int pos = line.indexOf('=');
			if (pos < 0)
				continue;
			String key = line.substring(0, pos).trim();
			String value = line.substring(pos + 1).trim();
			
			try {
				if (key.equals(SubscriberInfo.FILTER)) {
					filter = value;
				} else if (key.equals(SubscriberInfo.CLUSTER)) {
					cluster = value;
				} else if (key.equals(SubscriberInfo.IGNORE_COUNT)) {
					ignore_count = Long.parseLong(value);
				} else if (key.equals(SubscriberInfo.SEND_COUNT)) {
					send_count = Long.parseLong(value);
				} else if (key.equals(SubscriberInfo.QUEUE_LEN)) {
					queue_len = Integer.parseInt(value);
				} else if (key.equals(SubscriberInfo.QUEUE_MAX)) {
					max_queue_size = Integer.parseInt(value);
				} else if (key.equals(SubscriberInfo.WWNOTIFY)) {
					wwnotify = value;
				} else if (key.equals(SubscriberInfo.ENABLE)) {
					enable = value.toLowerCase().equals("true");
				} else if (key.equals(SubscriberInfo.PROJECTION)) {
					projection = new HashSet<String>();
					String[] ss = value.split(",");
					for (int i = 0; i < ss.length; ++i) {
						ss[i] = ss[i].trim();
						if (ss[i].length() > 0) {
							projection.add(ss[i]);
						}
					}
				}
			} catch (Exception e) {
				LOG.error(Tools.formatError(
						"BroadcastLogOutput Cluster config error", e));
			}
		}
	}

	public void setQueueLen(int queue_len) {
		this.queue_len = queue_len;
	}

	public void setSendCount(long send_count) {
		this.send_count = send_count;
	}

	public void setIgnoreCount(long ignore_count) {
		this.ignore_count = ignore_count;
	}

	public int getQueueLen() {
		return queue_len;
	}

	public long getSendCount() {
		return send_count;
	}

	public long getIgnoreCount() {
		return ignore_count;
	}

	public String getWorker() {
		return worker;
	}

	public void setWorker(String worker) {
		this.worker = worker;
	}

	public String getNotify() {
		return wwnotify;
	}

	public String build() {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("%s=%s\n", CLUSTER, cluster));
		if (filter != null && filter.length() > 0) {
			sb.append(String.format("%s=%s\n", FILTER, filter));
		}
		sb.append(String.format("%s=%d\n", QUEUE_MAX, max_queue_size));
		sb.append(String.format("%s=%b\n", ENABLE, enable));
		if (projection != null) {
			sb.append(String.format("%s=%s\n", PROJECTION,
					Tools.join(projection)));
		}
		if (wwnotify != null && wwnotify.length() > 0) {
			sb.append(String.format("%s=%s\n", WWNOTIFY, wwnotify));
		}
		return sb.toString();
	}

	public void setWwnotify(String wwnotify) {
		this.wwnotify = wwnotify;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}

	public void setCluster(String cluster) {
		this.cluster = cluster.trim();
	}

	public void setMaxQueueSize(int max_queue_size) {
		this.max_queue_size = max_queue_size;
	}

	public String getCluster() {
		return cluster;
	}

	public String getFilter() {
		return filter;
	}

	public int getMaxQueueSize() {
		return max_queue_size;
	}

	public String getTopo() {
		return topo;
	}

	public String getId() {
		return id;
	}

	public boolean isEnable() {
		return enable;
	}

	public Set<String> getProjection() {
		return projection;
	}

	public void setProjection(String[] split) {
		if(split == null)
		{
			projection = null;
			return;
		}
		projection = new HashSet<String>();
		for (int i = 0; i < split.length; ++i) {
			split[i] = split[i].trim();
			if (split[i].length() > 0) {
				projection.add(split[i]);
			}
		}
	}

}
