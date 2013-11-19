package com.etao.lz.star;

import java.io.File;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.zeromq.ZMQ;

import com.etao.lz.star.StarLogProtos.BusinessStarLog;
import com.etao.lz.star.StarLogProtos.FlowStarLog;
import com.etao.lz.star.output.PriorityNode;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * 
 * @author guoxiang.lgx<br>
 *         <p>This class is used for subscribing star log in storm task(Spout or
 *         Bolt).</p>
 * 
 *         <p>The problem of using StarLogSubscriber in storm task is that
 *         generally we only assign one ZeroMQ port to each host machine, but
 *         there may be two tasks being scheduled by Storm into same machine,
 *         therefore they must share same ZeroMQ port to receive star log. This
 *         will cause the second task throwing 'address already in use'
 *         exception while attempting to bind at the port. So we do following
 *         trick to make it work. </p>
 *         for each task in tasks who share same port in same machine:
 *         <ol>
 *         <li>start a thread T and bind at the assigned port, if failed(address
 *         already in use), sleep 3 seconds and try again until bind success.</li>
 *         <li>periodically scan /tmp/subscribe_forward/[port] directory, if it
 *         found a IPC file under it, it connect to this IPC.</li>
 *         <li>forwards all received log from binded port at step 1 to connected
 *         IPC file in the thread T.</li>
 *         <li>create a unique IPC file under /tmp/subscribe_forward/[port],
 *         receive log from this IPC file and return the log to client.</li>
 *         </ol>
 *         Note that:
 *         <ol>
 *         <li>In each moment, at most only one task is binding at the shared
 *         port and forwarding logs, others keep retry.</li>
 *         <li>If the binded task failed, another task's bind retry will success
 *         and starts forwarding logs.</li>
 *         <li>all alive task can receive log from IPC</li>
 *         </ol>
 */
public class StarLogStormSubscriber implements Runnable {
	private static Log LOG = LogFactory.getLog(StarLogStormSubscriber.class);

	private int port;
	private ZMQ.Socket socket;
	private String myid;
	private Set<String> connected;
	private File forwardIPCDir;

	// A good choice of id is Storm task id
	public StarLogStormSubscriber(String hostAndPort, String id) {
		connected = new HashSet<String>();

		hostAndPort = hostAndPort.trim();
		this.myid = id.trim();
		int portStart = hostAndPort.indexOf(':');
		if (portStart <= 0) {
			throw new IllegalStateException("Missing port for" + hostAndPort);
		}
		port = Integer.parseInt(hostAndPort.substring(portStart + 1).trim());

		forwardIPCDir = new File("/tmp/subscribe_forward/" + port);
		forwardIPCDir.mkdirs();
		if (!forwardIPCDir.exists()) {
			throw new IllegalStateException(forwardIPCDir + "，文件不存在");
		}
	}

	public void run() {
		ZMQ.Context context = ZMQ.context(1);
		
		ZMQ.Socket recvSocket = null;
		while (true) {
			String s = String.format("tcp://*:%d", port);
			try {
				LOG.info(String.format("(id=%s)before bind at %s", myid, s));
				recvSocket = context.socket(ZMQ.PULL);
				recvSocket.bind(s);
				LOG.info(String.format("(id=%s) bind at %s", myid, s));
				break;
			} catch (Exception e) {
				LOG.info(String.format("(id=%s)failed binding at %s, retry", myid, s));
				if(recvSocket != null)
				{
					recvSocket.close();
					recvSocket = null;
				}
				try {
					Thread.sleep(3000);
				} catch (InterruptedException ee) {
				}
			}
		}
		ZMQ.Socket forwardSocket = context.socket(ZMQ.PUSH);
		forwardSocket.setHWM(1);
		long lastTime = 0;
		while (true) {
			byte[] recv = recvSocket.recv(0);
			if (System.currentTimeMillis() - lastTime > 3000) {
				lastTime = System.currentTimeMillis();
				while (true) {

					for (String id : forwardIPCDir.list()) {
						if (connected.contains(id))
							continue;
						String s = String.format("ipc://%s/%s",
								forwardIPCDir.getPath(), id);
						forwardSocket.connect(s);
						LOG.info(String.format("connected(id = %s forward):",
								this.myid, s));
						connected.add(id);
					}
					if (!connected.isEmpty())
						break;
					// sleep until we found a listener
					try {
						Thread.sleep(3000);
					} catch (InterruptedException e) {
					}
				}
			}
			forwardSocket.send(recv, 0);
		}
	}

	public void start() {
		Thread t = new Thread(this);
		t.start();

		ZMQ.Context context = ZMQ.context(1);
		socket = context.socket(ZMQ.PULL);
		String s = String.format("ipc://%s/%s", forwardIPCDir.getPath(), myid);
		try {
			socket.bind(s);
		} catch (Exception e) {
			throw new IllegalStateException("[StarLogStormSubscriber]:" + e);
		}
		LOG.info(String.format("bind at %s", s));
	}

	public void stop() {
		if (socket != null) {
			socket.close();
		}
	}

	public byte[] receive(boolean noblock)
			throws InvalidProtocolBufferException {
		return socket.recv(noblock ? ZMQ.NOBLOCK : 0);
	}

	public FlowStarLog receiveFlowLog(boolean noblock)
			throws InvalidProtocolBufferException {
		byte[] recv = socket.recv(noblock ? ZMQ.NOBLOCK : 0);
		if (recv == null)
			return null;
		return StarLogProtos.FlowStarLog.parseFrom(recv);
	}

	public BusinessStarLog receiveBusinessLog(boolean noblock)
			throws InvalidProtocolBufferException {
		byte[] recv = socket.recv(noblock ? ZMQ.NOBLOCK : 0);
		if (recv == null)
			return null;
		return StarLogProtos.BusinessStarLog.parseFrom(recv);
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.out
					.println("Usage BroadcastLogOutput host:port id [flow|business] queue-size");
			return;
		}

		final StarLogStormSubscriber subscriber = new StarLogStormSubscriber(
				args[0], args[1]);

		boolean isFlow = args[2].equals("flow");
		try {
			subscriber.start();
		} catch (Exception e) {
			System.err
					.println("[SUGGESTION] Could not bind!, please delete /tmp/subscribe_forward/"
							+ args[1] + " and try again using 'admin' account.");
			System.exit(1);
		}

		System.out.println("Running StarLogSubscriber on " + args[0]);
		long count = 0;
		PriorityQueue<PriorityNode> queue = new PriorityQueue<PriorityNode>();
		int QueueSize = Integer.parseInt(args[3]);
		while (true) {
			PriorityNode node;
			if (isFlow) {
				node = new PriorityNode(subscriber.receiveFlowLog(false));
			} else {
				node = new PriorityNode(subscriber.receiveBusinessLog(false));
			}
			if (queue.size() > QueueSize) {
				System.out.println(queue.poll().getLog());
			}
			queue.add(node);
			++count;
			if (count % 10000 == 0) {
				System.out.println(count + " received");
			}
		}
	}

}
