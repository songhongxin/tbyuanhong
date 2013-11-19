package com.etao.lz.star.test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

import com.taobao.timetunnel.client.Message;
import com.taobao.timetunnel.client.MessageConsumer;
import com.taobao.timetunnel.client.TimeTunnelClientException;
import com.taobao.timetunnel.client.TimeTunnelSessionFactory;
import com.taobao.timetunnel.client.conf.ConsumerConfig;
import com.taobao.timetunnel.client.conf.TimeTunnelConfig;

public class TTMemberCartTest {

	/**
	 * @param args
	 * @throws TimeTunnelClientException 
	 * @throws IOException 
	 */
	@SuppressWarnings("resource")
	public static void main(String[] args) throws TimeTunnelClientException, IOException {
		if(args.length   == 0)
		{
			System.out.println("TTTest filename");
			return;
		}
		TimeTunnelConfig ttConfig = new TimeTunnelConfig("LHW_P1_1HOUR_020");
		ttConfig.setRouterURL("ttrouter2.cm3.tbsite.net:9090,ttrouter3.cm3.tbsite.net:9090,ttrouter1.cm4.tbsite.net:9090,ttrouter2.cm4.tbsite.net:9090,ttrouter1.cm6.tbsite.net:9090,ttrouter2.cm6.tbsite.net:9090"); // 设置TT的路由集羄1�7
		ttConfig.setUser("xxdf");
		ttConfig.setPassword("xxdf");

		TimeTunnelSessionFactory sessionFactory = TimeTunnelSessionFactory.getInstance();
		ConsumerConfig consumerConfig = new ConsumerConfig(ttConfig);
		consumerConfig.setSubscriberId("03071518020ASTDVZQ");
		consumerConfig.setConnectionPoolSize(5);
		//consumerConfig.setFetchFilter("__subtopic__='tc_biz_order' or __subtopic__='tc_pay_order'");
		//consumerConfig.setFetchFilter("__subtopic__='tc_pay_order'");
		consumerConfig.setFetchTimeout(0);
		MessageConsumer consumer = sessionFactory.createConsumer(consumerConfig);
		//consumer.subscribe(args[1]);
		FileOutputStream writer = new FileOutputStream(args[0]);
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
