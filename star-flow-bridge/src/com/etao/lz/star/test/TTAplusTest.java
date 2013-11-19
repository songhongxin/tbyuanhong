package com.etao.lz.star.test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.etao.lz.star.spout.Aplus;
import com.etao.lz.star.spout.Aplus.AplusLog;
import com.taobao.timetunnel.client.Message;
import com.taobao.timetunnel.client.MessageConsumer;
import com.taobao.timetunnel.client.SessionFactory;
import com.taobao.timetunnel.client.TimeTunnelClientException;
import com.taobao.timetunnel.client.TimeTunnelSessionFactory;
import com.taobao.timetunnel.client.conf.ConsumerConfig;
import com.taobao.timetunnel.client.conf.TimeTunnelConfig;
import com.taobao.timetunnel.client.parser.MessageParser;

public class TTAplusTest {
	/**
	 * @param args
	 * @throws TimeTunnelClientException
	 * @throws IOException
	 */

	public static void main(String[] args) throws TimeTunnelClientException,
			IOException {
		if (args.length < 2) {
			System.out.println("TTTest bin-filename txt-filename");
			return;
		}
		TimeTunnelConfig ttConfig = new TimeTunnelConfig("LHW_P1_1HOUR_004");
		ttConfig.setRouterURL("ttrouter3.cm3.tbsite.net:9090,ttrouter2.cm3.tbsite.net:9090,ttrouter1.cm4.tbsite.net:9090,ttrouter2.cm4.tbsite.net:9090,ttrouter1.cm6.tbsite.net:9090,ttrouter2.cm6.tbsite.net:9090");
		ttConfig.setUser("xxdf");
		ttConfig.setPassword("xxdf");

		SessionFactory sessionFactory = TimeTunnelSessionFactory.getInstance();

		// 创建一个消费者
		final ConsumerConfig consumerConfig = new ConsumerConfig(ttConfig);
		consumerConfig.setSubscriberId("1114163517A8GFCJ5U");
		MessageConsumer consumer = sessionFactory
				.createConsumer(consumerConfig);
		consumer.subscribe("aplus");
		FileOutputStream writer1 = new FileOutputStream(args[0]);
		FileOutputStream writer2 = new FileOutputStream(args[1]);
		boolean done = false;
		while (!done) {
			Iterator<Message> iterator = consumer.iterator();
			while (iterator != null && iterator.hasNext()) {
				Message next = iterator.next();
				byte[] data = next.getData();
				if(data.length == 0)
				{
					continue;
				}
				done = true;
				String head = String.valueOf(data.length)+"\n";
				writer1.write(head.getBytes("utf-8"));
				writer1.write(data);
				List<byte[]> split = MessageParser.parseProtoBufsFromBytes(data);
				for(byte[] log: split)
				{
					AplusLog obj = Aplus.AplusLog.parseFrom(log);
					writer2.write(obj.toString().getBytes("utf-8"));
					writer2.write("##########################\n".getBytes("utf-8"));
				}
				break;
			}
		}
		writer1.close();
		writer2.close();
		System.exit(0);
	}

}
