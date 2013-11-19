package com.etao.lz.star.test;

import java.io.FileOutputStream;
import java.util.Iterator;

import com.taobao.timetunnel.client.Message;
import com.taobao.timetunnel.client.MessageConsumer;
import com.taobao.timetunnel.client.SessionFactory;
import com.taobao.timetunnel.client.TimeTunnelClientException;
import com.taobao.timetunnel.client.TimeTunnelSessionFactory;
import com.taobao.timetunnel.client.conf.ConsumerConfig;
import com.taobao.timetunnel.client.conf.TimeTunnelConfig;

public class TTP4PTest {

	private static FileOutputStream writer;

	/**
	 * @param args
	 * @throws TimeTunnelClientException 
	 */
	public static void main(String[] args) throws Exception {
		TimeTunnelConfig ttConfig = new TimeTunnelConfig("LHW_P1_30MIN_002");
		ttConfig.setRouterURL("ttrouter3.cm3.tbsite.net:9090,ttrouter2.cm3.tbsite.net:9090,ttrouter1.cm4.tbsite.net:9090,ttrouter2.cm4.tbsite.net:9090,ttrouter1.cm6.tbsite.net:9090,ttrouter2.cm6.tbsite.net:9090");
		ttConfig.setUser("xxdf");
		ttConfig.setPassword("xxdf");

		SessionFactory sessionFactory = TimeTunnelSessionFactory.getInstance();

		// 创建一个消费者
		final ConsumerConfig consumerConfig = new ConsumerConfig(ttConfig);
		consumerConfig.setSubscriberId("1225100719PN2O157J");
		MessageConsumer consumer = sessionFactory
				.createConsumer(consumerConfig);
		consumer.subscribe("p4p_pv2");
		writer = new FileOutputStream(args[0]);
		while (true) {
			Iterator<Message> iterator = consumer.iterator();
			while (iterator != null && iterator.hasNext()) {
				Message next = iterator.next();
				writer.write(next.getData());
				writer.flush();
			}
		}

	}

}
