package com.etao.lz.star.output.broadcast;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.Tools;
import com.google.protobuf.GeneratedMessage;

public class SendDataThread implements Runnable {
	private LinkedBlockingQueue<byte[]> queue;

	private Socket socket = null;
	private volatile boolean alive;

	private long sendCount;

	private SubscriberInfo info;

	private static Log LOG = LogFactory.getLog(SendDataThread.class);

	private final SendingState SendingState;
	private final IgnoreState IgnoreState;
	private BroadcastState currentState;

	public SendDataThread(SubscriberInfo item, StarConfig config) {
		this.info = item;

		LOG.info(String.format(
				"%s Open new connection for %s, cluster:%s, filter:%s, max queue size:%s",
						Tools.getIdAndHost(), item.getId(), item.getCluster(),
						item.getFilter(), item.getMaxQueueSize()));

		queue = new LinkedBlockingQueue<byte[]>();

		SendingState = new SendingState(queue, item, config);
		IgnoreState = new IgnoreState(info.getId(), info.getNotify());
		currentState = SendingState;

		alive = true;

		sendCount = 0;

	}

	public void addData(byte[] data, GeneratedMessage log) {
		int max_queue_size = info.getMaxQueueSize();
		BroadcastState curState = null;
		synchronized (this) {
			if (currentState == SendingState && max_queue_size > 0
					&& queue.size() >= max_queue_size) {
				currentState = IgnoreState;
			} else if (currentState == IgnoreState
					&& queue.isEmpty()) {
				currentState = SendingState;
			}
			curState = currentState;
		}
		curState.addData(data, log);

	}

	public void run() {
		ZMQ.Context context = ZMQ.context(1);
		ZMQ.Socket socket = context.socket(ZMQ.PUSH);
		socket.setHWM(1);
		socket.setReconnectIVL(3000);
		for (String hostAndPort : info.getCluster().split(",")) {
			hostAndPort = hostAndPort.trim();
			int portStart = hostAndPort.lastIndexOf(':');
			if (portStart <= 0) {
				LOG.error(String.format("[Star-Storm] Missing port:%s",
						hostAndPort));
				continue;
			}
			LOG.info(String.format("%s tcp connect to %s",
					Tools.getIdAndHost(), hostAndPort));
			socket.connect("tcp://" + hostAndPort);
			LOG.info(String.format("%s connected", hostAndPort));
		}
		while (alive) {
			try {
				byte[] data = queue.take();
				if (data == null || data.length == 0) {
					continue;
				}
				socket.send(data, 0);
				++sendCount;
			} catch (InterruptedException e) {
			}

		}

		LOG.info(info.getId() + " terminated");

	}

	public void close() {
		alive = false;
		queue.add(new byte[0]);
		if (socket != null) {
			socket.close();
		}
	}

	public LinkedBlockingQueue<byte[]> getQueue() {
		return queue;
	}

	public long getSendCount() {
		return sendCount;
	}

	public long getIgnoreCount() {
		return IgnoreState.getIgnoreCount();
	}

	public String getId() {
		return info.getId();
	}
	
	public SubscriberInfo getSubscriberInfo()
	{
		return info;
	}

}
