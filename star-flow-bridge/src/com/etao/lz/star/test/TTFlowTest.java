package com.etao.lz.star.test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.etao.lz.star.Tools;
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

public class TTFlowTest {
	/**
	 * @param args
	 * @throws TimeTunnelClientException
	 * @throws IOException
	 */
	public static void main(String[] args) throws TimeTunnelClientException,
			IOException {
		if (args.length < 1) {
			System.out.println("TTTest filename");
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
		FileOutputStream writer = new FileOutputStream(args[0]);
		while (true) {
			Iterator<Message> iterator = consumer.iterator();
			while (iterator != null && iterator.hasNext()) {
				Message next = iterator.next();
				List<byte[]> split = MessageParser.parseProtoBufsFromBytes(next.getData());
				for(byte[] log: split)
				{
					AplusLog obj = Aplus.AplusLog.parseFrom(log);
					if(!obj.getLogkey().toStringUtf8().equals("/a.gif")) continue;
					String s = obj.toString();
					writer.write(s.getBytes("utf-8"));
				}
			}
		}

	}

}
