package com.etao.lz.star.test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

import com.taobao.timetunnel.client.Message;
import com.taobao.timetunnel.client.MessageConsumer;
import com.taobao.timetunnel.client.SessionFactory;
import com.taobao.timetunnel.client.TimeTunnelClientException;
import com.taobao.timetunnel.client.TimeTunnelSessionFactory;
import com.taobao.timetunnel.client.conf.ConsumerConfig;
import com.taobao.timetunnel.client.conf.TimeTunnelConfig;

public class TTB2BTest {
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
		TimeTunnelConfig ttConfig = new TimeTunnelConfig("LHW_P1_30MIN_011");
		ttConfig.setRouterURL("ttrouter2.cm3.tbsite.net:9090,ttrouter3.cm3.tbsite.net:9090,ttrouter1.cm4.tbsite.net:9090,ttrouter2.cm4.tbsite.net:9090,ttrouter1.cm6.tbsite.net:9090,ttrouter2.cm6.tbsite.net:9090");
		ttConfig.setUser("xxdf");
		ttConfig.setPassword("xxdf");

		SessionFactory sessionFactory = TimeTunnelSessionFactory.getInstance();

		// 创建一个消费者
		final ConsumerConfig consumerConfig = new ConsumerConfig(ttConfig);
		consumerConfig.setSubscriberId("0312131252BYYH1C7G");
		MessageConsumer consumer = sessionFactory
				.createConsumer(consumerConfig);
		consumer.subscribe("b2b_acookie");
		FileOutputStream writer = new FileOutputStream(args[0]);
		while (true) {
			Iterator<Message> iterator = consumer.iterator();
			while (iterator != null && iterator.hasNext()) {
				Message next = iterator.next();
				writer.write(next.getData());
			}
		}

	}

}
