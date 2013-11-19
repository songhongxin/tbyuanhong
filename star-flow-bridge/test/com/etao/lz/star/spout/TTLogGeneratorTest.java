package com.etao.lz.star.spout;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.junit.Assert;
import org.junit.Test;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.StarLogProtos.BusinessStarLog;
import com.etao.lz.star.XMLParser;

public class TTLogGeneratorTest {
	
	@Test
	public void testBizOrder() throws Exception {

		XMLParser commonXml = new XMLParser("/business/common.xml");
		XMLParser xml = new XMLParser("/business/spout.xml");
		NodeList nodes = xml.getNodeList(null, "/SpoutConfig/LogGenerator");

		Node node = nodes.item(0);		
			
		StarConfig config = new StarConfig("test");
		config.parseCommon(commonXml);
		config.parse(xml, node);
		
		TTLogBuilder logBuilder = new MySQLLogBuilder();
		logBuilder.config(config);
		InputStream ins = TTLogGeneratorTest.class
				.getResourceAsStream("/testbizorder.data");
		BufferedReader reader = new BufferedReader(new InputStreamReader(ins));
		String line = reader.readLine();
		Assert.assertNotNull(line);
		BusinessStarLog log = (BusinessStarLog) logBuilder.build("tc_biz_order", line.getBytes("utf-8")).get(0);
		Assert.assertEquals(log.getLogSrc(), "tc_biz_order");
		Assert.assertEquals(log.getDestType(), "biz_order");
		Assert.assertEquals(log.getOrderId(), "182492200123858");
		Assert.assertEquals(log.getParentId(), "182492200093858");
		Assert.assertEquals(log.getSellerId(), "405455103");
		Assert.assertEquals(log.getBuyerId(), "253855838");
		Assert.assertEquals(log.getAuctionId(), "10037171091");
		Assert.assertEquals(log.getAuctionPrice(), 840);
		Assert.assertEquals(log.getDiscountFee(), 320);
		Assert.assertEquals(log.getBuyAmount(), 2);
		Assert.assertEquals(log.getGmtCreate(), "2012-06-07 17:40:30");
		Assert.assertEquals(log.getGmtModified(), "2012-06-07 17:40:31");
		Assert.assertEquals(log.getIsDetail(), 1);
		Assert.assertEquals(log.getIsMain(), 0);
		Assert.assertEquals(log.getPayStatus(), 7);
		Assert.assertEquals(log.getDbAction(), "update:16,17");
		Assert.assertEquals(1339062031104L, log.getOrderModifiedT());
		line = reader.readLine();
		Assert.assertNotNull(line);
		line = reader.readLine();
		Assert.assertNull(line);
	}
	@Test
	public void testPayOrder() throws Exception {
		XMLParser commonXml = new XMLParser("/business/common.xml");
		XMLParser xml = new XMLParser("/business/spout.xml");
		NodeList nodes = xml.getNodeList(null, "/SpoutConfig/LogGenerator");

		Node node = nodes.item(0);		
			
		StarConfig config = new StarConfig("test");
		config.parseCommon(commonXml);
		config.parse(xml, node);
		
		TTLogBuilder logBuilder = new MySQLLogBuilder();
		logBuilder.config(config);

		InputStream ins = TTLogGeneratorTest.class
				.getResourceAsStream("/testpayorder.data");
		BufferedReader reader = new BufferedReader(new InputStreamReader(ins));
		String line = reader.readLine();
		Assert.assertNotNull(line);
		BusinessStarLog log = (BusinessStarLog) logBuilder.build("tc_pay_order", line.getBytes("utf-8")).get(0);
		Assert.assertEquals(log.getLogSrc(), "tc_pay_order");
		Assert.assertEquals(log.getDestType(), "pay_order");
		Assert.assertEquals(log.getOrderId(), "139899507667605");
		Assert.assertEquals(log.getParentId(), "");
		Assert.assertEquals(log.getSellerId(), "2088002052941478");
		Assert.assertEquals(log.getBuyerId(), "2088002116880804");
		Assert.assertEquals(log.getAuctionId(), "");
		Assert.assertEquals(log.getAuctionPrice(), 0);
		Assert.assertEquals(log.getDiscountFee(), 0);
		Assert.assertEquals(log.getBuyAmount(), 0);
		Assert.assertEquals(log.getGmtCreate(), "2012-06-07 11:37:56");
		Assert.assertEquals(log.getGmtModified(), "2012-06-07 17:40:32");
		Assert.assertEquals(log.getIsDetail(), 0);
		Assert.assertEquals(log.getIsMain(), 0);
		Assert.assertEquals(log.getPayStatus(), 4);
		Assert.assertEquals(log.getDbAction(), "update:20");
		Assert.assertEquals(1339062032042L, log.getOrderModifiedT());
		line = reader.readLine();
		Assert.assertNotNull(line);
		line = reader.readLine();
		Assert.assertNotNull(line);
		line = reader.readLine();		
		Assert.assertNull(line);
	}	
}

