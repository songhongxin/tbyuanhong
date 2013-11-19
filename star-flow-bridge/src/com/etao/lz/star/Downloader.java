package com.etao.lz.star;

import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;

import com.etao.lz.star.StarLogProtos.FlowStarLog;

public class Downloader {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.out
					.println("Usage BroadcastLogOutput [host:port] [save path] [start date(yyyyMMddHHmmss)] [end date(yyyyMMddHHmmss)]");
			return;
		}
		final StarLogSubscriber subscriber = new StarLogSubscriber(args[0]);
		subscriber.start();

		System.out.println("Running StarLogSubscriber on " + args[0]);
		long start = Tools.parseSimpleTime(args[2]);
		long end = Tools.parseSimpleTime(args[3]);
		long lastts = 0;
		long count = 0;
		long file = 0;
		final long FILE_COUNT = 10000000;
		FileOutputStream out = new FileOutputStream(String.format("%s-%d", args[1], file));
		while (true) {
			byte[] data = subscriber.doReceive(false);
			FlowStarLog log = StarLogProtos.FlowStarLog.parseFrom(data);
			if (log.getTs() >= end || log.getTs() < start) {
				continue;
			}
			if(log.getTs() / 1000 / 60 > lastts / 1000 / 60)
			{
				System.out.println(Tools.formatTime(log.getTs()));
				lastts = log.getTs();
				out.flush();
			}
			++count;
			if(count / FILE_COUNT > file)
			{
				file = count / FILE_COUNT ;
				out.close();
				out = new FileOutputStream(String.format("%s-%d", args[1], file));
			}
			int len = data.length;
			byte[] bytes = Bytes.toBytes(len);
			if (bytes.length != 4) {
				throw new IllegalStateException();
			}
			out.write(bytes);
			out.write(data);
			out.write(Bytes.toBytes(0xdeadbeaf));
		}

	}

}
