package com.etao.lz.star;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.zeromq.ZMQ;

import com.etao.lz.star.StarLogProtos.FlowStarLog;

public class SendLog {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("SendLog [host:port] [prefix]");
			return;
		}
		ZMQ.Context context = ZMQ.context(1);
		final String[] hosts = args[0].split(",");
		final ZMQ.Socket[] sockets = new ZMQ.Socket[hosts.length];
		for (int i = 0; i < sockets.length; ++i) {
			sockets[i] = context.socket(ZMQ.PUSH);
			sockets[i].setHWM(1);
			sockets[i].setReconnectIVL(3000);
			sockets[i].connect("tcp://" + hosts[i]);
			final int index = i;
			final String path = args[1];
			Thread thread = new Thread(new Runnable(){
				
				@Override
				public void run() {
					try {
						System.out.println("tcp://" + hosts[index] + " started!");
						sendLog(path, sockets[index], index, sockets.length);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			});
			thread.start();
		}
	}
	private static void sendLog(String path, ZMQ.Socket socket, int index, int host_num) throws IOException
	{
		byte[] len_bytes = new byte[4];

		for (int fileno = 0; fileno < 100; ++fileno) {
			File f = new File(path + fileno);
			if (!f.exists()) {
				continue;
			}
			System.out.println("sending logs in " + f.getPath() + "...");
			FileInputStream reader = new FileInputStream(f);
			long count = 0;
			while (reader.read(len_bytes) == 4) {
				int len = Bytes.toInt(len_bytes);
				byte[] bytes = new byte[len];
				int start = 0;
				while (start < len) {
					int readed = reader.read(bytes, start, len - start);
					if (readed <= 0) {
						System.out.println("Wrong log!!!");
						reader.close();
						return;
					}
					start += readed;
				}
				if (start != len) {
					System.out.println("Wrong log!!!");
					reader.close();
					return;
				}
				if (reader.read(len_bytes) != 4
						|| Bytes.toInt(len_bytes) != 0xdeadbeaf) {
					System.out.println("Wrong log!!!");
					reader.close();
					return;
				}
				++count;
				try {

					FlowStarLog log = StarLogProtos.FlowStarLog.parseFrom(bytes);
					if ((count % 100000) == 0) {
						String time = Tools.formatTime(log.getTs());
						System.out.printf("index %d:%s", index, time);
					}
					int hash = log.getAuctionid().hashCode() % host_num;
					if (hash < 0)
						hash += host_num;
					if(hash == index)
					{
						socket.send(log.toByteArray(), 0);
					}
				} catch (Exception e) {
					System.out.println("Wrong log!!!");
					reader.close();
					return;
				}

			}
			reader.close();
		}
		socket.close();
	}
}
