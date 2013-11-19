package com.etao.lz.star;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.zeromq.ZMQ;

import com.etao.lz.star.StarLogProtos.BusinessStarLog;
import com.etao.lz.star.StarLogProtos.FlowStarLog;
import com.google.protobuf.InvalidProtocolBufferException;
/**
 * 
 * @author guoxiang.lgx
 * If you want to subscribe star log in a storm task (Spout or Bolt), consider using StarLogStormSubscriber.
 * @see StarLogStormSubscriber
 */
public class StarLogSubscriber {
	private static Log LOG = LogFactory.getLog(StarLogSubscriber.class);

	private int port;
	private ZMQ.Socket socket;
	private boolean binded = false;

	public StarLogSubscriber(String hostAndPort) {
		hostAndPort = hostAndPort.trim();
		int portStart = hostAndPort.indexOf(':');
		if (portStart <= 0) {
			throw new IllegalStateException("Missing port for" + hostAndPort);
		}
		port = Integer.parseInt(hostAndPort.substring(portStart + 1).trim());
	}

	public byte[] doReceive(boolean noblock) {

		while (!binded) {
			synchronized (this) {
				//avoid thread contention
				if(binded) break;
				ZMQ.Context context = ZMQ.context(1);
				socket = context.socket(ZMQ.PULL);

				String s = String.format("tcp://*:%d", port);
				try {
					socket.bind(s);
					LOG.info(String.format("bind at %s", s));
					binded = true;
					break;
				} catch (Exception e) {
					if (noblock) {
						return null;
					}
				}
			}
			//failed to bind, and noblock = false;
			try {
				Thread.sleep(3000);
			} catch (InterruptedException ee) {
			}
		}
		return socket.recv(noblock ? ZMQ.NOBLOCK : 0);
	}

	public void start()
	{
		
	}
	
	public void stop() {
		if (socket != null) {
			socket.close();
		}
	}

	public FlowStarLog receiveFlowLog(boolean noblock)
			throws InvalidProtocolBufferException {
		byte[] recv = doReceive(noblock);
		if (recv == null)
			return null;
		return StarLogProtos.FlowStarLog.parseFrom(recv);
	}

	public BusinessStarLog receiveBusinessLog(boolean noblock)
			throws InvalidProtocolBufferException {
		byte[] recv = doReceive(noblock);
		if (recv == null)
			return null;
		return StarLogProtos.BusinessStarLog.parseFrom(recv);
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out
					.println("Usage BroadcastLogOutput host:port [flow|business]");
			return;
		}
		final StarLogSubscriber subscriber = new StarLogSubscriber(args[0]);
		subscriber.start();
		
		boolean isFlow = args[1].equals("flow");
		System.out.println("Running StarLogSubscriber on " + args[0]);
		while (true) {
			if (isFlow) {
				System.out.println(subscriber.receiveFlowLog(false));
			} else {
				System.out.println(subscriber.receiveBusinessLog(false));
			}

		}
	}

}
