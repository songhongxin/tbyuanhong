package com.etao.lz.star.bolt;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

import org.junit.Assert;
import org.junit.Test;
import org.w3c.dom.Node;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.StarLogProtos;
import com.etao.lz.star.XMLParser;

public class BusinessFillProcessorTest {

	@Test
	public void testisNewGenerated() throws Exception {
		BusinessFillProcessor processor = new BusinessFillProcessor();
		StarConfig config = new StarConfig("test");

		XMLParser xml = new XMLParser("/business/filling.xml");

		Node node = xml
				.getNode(
						null,
						"/BoltConfig/LogProcessor[@class='com.etao.lz.star.bolt.BusinessFillProcessor']");
		config.parse(xml, node);

		config.addAttribute("processor_output", "is_new_generated");
		processor.config(config);
		processor.open(0);
		StarLogProtos.BusinessStarLog.Builder builder = StarLogProtos.BusinessStarLog
				.newBuilder();
		builder.setDbAction("insert");
		processor.process(builder);
		Assert.assertEquals(1, builder.getIsNewGenerated());

		for (String status : new String[] {"TRADE_FINISHED","TRADE_CLOSED", "TRADE_REFUSE","TRADE_REFUSE_DEALING","TRADE_CANCEL"}) {
			builder.clear();
			builder.setDbAction("insert");
			builder.setTradeStatus(status);
			processor.process(builder);
			Assert.assertEquals(0, builder.getIsNewGenerated());
		}

		builder.clear();
		builder.setDbAction("update");
		processor.process(builder);
		Assert.assertEquals(0, builder.getIsNewGenerated());

		ObjectOutputStream out = new ObjectOutputStream(
				new ByteArrayOutputStream());
		out.writeObject(processor);
	}

	@Test
	public void testisPay() throws Exception {
		BusinessFillProcessor processor = new BusinessFillProcessor();
		StarConfig config = new StarConfig("test");

		XMLParser xml = new XMLParser("/business/filling.xml");

		Node node = xml
				.getNode(
						null,
						"/BoltConfig/LogProcessor[@class='com.etao.lz.star.bolt.BusinessFillProcessor']");
		config.parse(xml, node);

		config.addAttribute("processor_output", "is_pay");
		processor.config(config);
		processor.open(0);
		StarLogProtos.BusinessStarLog.Builder builder = StarLogProtos.BusinessStarLog
				.newBuilder();
		builder.setDbAction("update:10,13");
		builder.setPayStatus(2);
		builder.setLogSrc("tc_biz_order");
		processor.process(builder);
		Assert.assertEquals(1, builder.getIsPay());

		builder.setDbAction("update:10,13");
		builder.setPayStatus(3);
		builder.setLogSrc("tc_biz_order");
		processor.process(builder);
		Assert.assertEquals(0, builder.getIsPay());
		
		builder.clear();
		builder.setPayStatus(2);
		builder.setDbAction("update:6,3");
		builder.setLogSrc("tc_pay_order");
		processor.process(builder);
		Assert.assertEquals(1, builder.getIsPay());

		builder.clear();
		builder.setPayStatus(3);
		builder.setDbAction("update:6,3");
		builder.setLogSrc("tc_pay_order");
		processor.process(builder);
		Assert.assertEquals(0, builder.getIsPay());

		builder.clear();
		builder.setPayStatus(3);
		builder.setDbAction("update:6,3");
		builder.setLogSrc("tc_no_order");
		processor.process(builder);
		Assert.assertEquals(0, builder.getIsPay());
		
		builder.clear();
		builder.setPayStatus(2);
		builder.setDbAction("update:16,13");
		builder.setLogSrc("tc_pay_order");
		processor.process(builder);
		Assert.assertEquals(0, builder.getIsPay());

		builder.clear();
		builder.setPayStatus(2);
		builder.setDbAction("insert:6,3");
		builder.setLogSrc("tc_pay_order");
		processor.process(builder);
		Assert.assertEquals(0, builder.getIsPay());

		builder.clear();
		builder.setPayStatus(2);
		builder.setDbAction("update:10");
		builder.setLogSrc("order_0000");
		processor.process(builder);
		Assert.assertEquals(1, builder.getIsPay());
		
		builder.clear();
		builder.setPayStatus(2);
		builder.setDbAction("update:11");
		builder.setLogSrc("order_0000");
		processor.process(builder);
		Assert.assertEquals(0, builder.getIsPay());
		
		builder.clear();
		builder.setDbAction("insert");
		builder.setLogSrc("order_0000");
		builder.setTradeStatus("TRADE_SUCCESS");
		processor.process(builder);
		Assert.assertEquals(1, builder.getIsPay());
		
		builder.clear();
		builder.setDbAction("insert");
		builder.setLogSrc("order_0000");
		builder.setTradeStatus("WAIT_SELLER_SEND_GOODS");
		processor.process(builder);
		Assert.assertEquals(1, builder.getIsPay());
		
		builder.clear();
		builder.setDbAction("insert");
		builder.setLogSrc("order_0000");
		builder.setTradeStatus("TRADE_CANCEL");
		processor.process(builder);
		Assert.assertEquals(0, builder.getIsPay());
		
		ObjectOutputStream out = new ObjectOutputStream(
				new ByteArrayOutputStream());
		out.writeObject(processor);
	}
}
