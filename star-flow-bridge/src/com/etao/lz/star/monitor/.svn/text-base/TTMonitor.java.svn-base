package com.etao.lz.star.monitor;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;

import com.etao.lz.star.Tools;
import com.netflix.curator.framework.CuratorFramework;

public abstract class TTMonitor implements java.io.Serializable {

	public static final String ZK_NAMESPACE = "/star_storm";

	private static final long serialVersionUID = -8414021557949009288L;

	private transient CuratorFramework client;

	protected Log LOG = LogFactory.getLog(getClass());
	private long lastTime = 0;
	private long lastDay = -1;
	private long lastEmitCount = 0;
	private int taskid;
	private String name;
	private String topoName;

	private void create(String path) throws Exception {
		if (client.checkExists().forPath(path) == null) {
			LOG.info("create zookeeper dir:" + path);
			client.create().forPath(path);
		}
	}

	public void update(String value) throws Exception {
		if (client == null)
			return;
		create(String.format("/%s", topoName));
		create(String.format("/%s/%s", topoName, name));

		String path = String.format("/%s/%s/%d", topoName, name, taskid);

		if (client.checkExists().forPath(path) == null) {
			LOG.info("create zookeeper file:" + path);
			client.create().withMode(CreateMode.EPHEMERAL)
					.forPath(path, value.getBytes("UTF-8"));
		} else {
			client.setData().forPath(path, value.getBytes("UTF-8"));
		}
	}

	public void updateChild(String child, String value)
			throws Exception {
		if (client == null)
			return;

		create(String.format("/%s", topoName));
		create(String.format("/%s/%s", topoName, name));

		String path = String.format("/%s/%s/%d_%s", topoName, name, taskid,
				child);
		if (client.checkExists().forPath(path) == null) {
			LOG.info("create zookeeper file:" + path);
			client.create().withMode(CreateMode.EPHEMERAL)
					.forPath(path, value.getBytes("UTF-8"));
		} else {
			client.setData().forPath(path, value.getBytes("UTF-8"));
		}
	}

	protected abstract void updateStat();

	private class StatTimerTask extends TimerTask {

		@Override
		public void run() {
			try {
				Calendar instance = Calendar.getInstance();
				if(lastDay != instance.get(Calendar.DAY_OF_WEEK))
				{
					reset();
				}
				
				StringBuilder sb = new StringBuilder();
				sb.append("Name = ");
				sb.append(name);
				sb.append("\n");
				sb.append("PID = ");
				sb.append(Tools.getIdAndHost());
				sb.append("\n");
					try {
					HashMap<String, String> processInfo = new HashMap<String, String>();
					Tools.getProcessInfo(processInfo);
					
					long vmRss = Long.parseLong(processInfo.get(Tools.PROCESS_VM_RSS));
					sb.append("VmRSS = " + vmRss + "\n");
					sb.append("CPU = " + processInfo.get(Tools.PROCESS_CPU)+ "\n");
					sb.append("StartFrom = " + processInfo.get(Tools.PROCESS_START)+ "\n");
				} catch (Exception e) {
					LOG.error(Tools.formatError("parseLong:", e));
				}
				
				sb.append("Port = ");
				sb.append(getListenPort());
				sb.append("\n");

				int queueSize = getQueueSize();
				if (queueSize >= 0) {
					sb.append("QueueSize");
					sb.append(" = ");
					sb.append(queueSize);
					sb.append("\n");
				}
				sb.append("LastTupleTime = ");
				sb.append(Tools.formatTime(getLastTupleTime()));
				sb.append("\n");
				long emitCount = getEmitCount();
				sb.append("EmitCount = ");
				sb.append(emitCount);
				sb.append("\n");

				long currentTimeMillis = instance.getTimeInMillis();
				if (lastEmitCount != 0 && currentTimeMillis > lastTime) {
					sb.append("EmitPerSecond = ");

					sb.append((emitCount - lastEmitCount) * 1000
							/ (currentTimeMillis - lastTime));
					sb.append("\n");
				}
				lastEmitCount = emitCount;
				lastTime = currentTimeMillis;
				lastDay = instance.get(Calendar.DAY_OF_WEEK);
				try {
					update(sb.toString());
				} catch (Exception e) {
					LOG.error(Tools.formatError("TTMonitor update stat error",
							e));
				}

				updateStat();
			} catch (Exception e) {
				LOG.error(Tools.formatError(String.format(
						"Failed to update status for %s-%d", name, taskid), e));
			}
		}

	};

	protected abstract long getEmitCount();

	protected abstract int getQueueSize();

	protected abstract long getLastTupleTime();

	protected abstract void reset();
	
	private String getListenPort() {
		try {
			Process exec = Runtime.getRuntime().exec("netstat -tlnp");
			String pid = Tools.getPId();
			return Tools.getListenPort(exec.getInputStream(), pid);
		} catch (IOException e) {
			return "Unknown";
		}
	}

	public void open(int statInterval, int taskid,
			String topoName, String name, CuratorFramework client) {
		this.topoName = topoName;
		this.taskid = taskid;
		this.name = name;

		LOG.info(String.format("%s-%d stat-interval %d", name,
				taskid, statInterval));

		this.client = client;

		Timer timer = new Timer();
		timer.schedule(new StatTimerTask(), statInterval, statInterval);
	}

}
