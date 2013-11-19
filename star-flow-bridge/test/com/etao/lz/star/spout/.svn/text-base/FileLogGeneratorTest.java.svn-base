package com.etao.lz.star.spout;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Assert;
import org.junit.Test;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.XMLParser;

public class FileLogGeneratorTest {

	@Test
	public void testFlow() throws Exception {
		XMLParser xml = new XMLParser("/flow/spout.xml");
		XMLParser commonXml = new XMLParser("/flow/common.xml");
		
		NodeList nodes = xml.getNodeList(null, "/SpoutConfig/LogGenerator");
	
		Node node = nodes.item(0);		
	
		StarConfig config = new StarConfig("test");
		config.parseCommon(commonXml);
		config.parse(xml, node);
		
		config.addAttribute("input_path", "/local_flow/aplus.data");
		config.addAttribute("log_builder", "com.etao.lz.star.spout.APlusLogBuilder");
		config.addAttribute("tt_tag", "aplus");
		config.addAttribute("log_class", "flow");
		
		APlusFileLogGenerator generator = new APlusFileLogGenerator();
		generator.config(config);
		BlockingQueue<byte[]> queue = new LinkedBlockingQueue<byte[]>();
		generator.open(queue, "test", 0);
		generator.shutdown();
		Assert.assertEquals(queue.size(), 48985);
		queue.clear();
	}
	
	
	
	
	@Test
	public void testBizOrder() throws Exception {
		XMLParser xml = new XMLParser("/business/spout.xml");
		XMLParser commonXml = new XMLParser("/business/common.xml");
		
		NodeList nodes = xml.getNodeList(null, "/SpoutConfig/LogGenerator");
	
		Node node = nodes.item(0);		
	
		StarConfig config = new StarConfig("test");
		config.parseCommon(commonXml);
		config.parse(xml, node);
		config.addAttribute("input_path", "/local_bizorder/biz_order.data");
		config.addAttribute("tt_tag", "tc_biz_order");		
		config.addAttribute("log_builder", "com.etao.lz.star.spout.MySQLLogBuilder");
		config.addAttribute("log_class", "business");
		
		FileLogGenerator gen = new FileLogGenerator();
		gen.config(config);
		BlockingQueue<byte[]> queue = new LinkedBlockingQueue<byte[]>();
		gen.open(queue, "test", 0);
		gen.shutdown();
		Assert.assertEquals(queue.size(), 24533);
	}
	
	@Test
	public void testPayOrder() throws Exception {
		XMLParser xml = new XMLParser("/business/spout.xml");
		XMLParser commonXml = new XMLParser("/business/common.xml");
		
		NodeList nodes = xml.getNodeList(null, "/SpoutConfig/LogGenerator");
	
		Node node = nodes.item(0);
	
		StarConfig config = new StarConfig("test");
		config.parseCommon(commonXml);
		config.parse(xml, node);
		config.addAttribute("input_path", "/local_payorder/pay_order.data");
		config.addAttribute("tt_tag", "tc_pay_order");
		config.addAttribute("log_builder", "com.etao.lz.star.spout.MySQLLogBuilder");
		config.addAttribute("log_class", "business");
		
		FileLogGenerator gen = new FileLogGenerator();
		gen.config(config);
		BlockingQueue<byte[]> queue = new LinkedBlockingQueue<byte[]>();
		gen.open(queue, "test", 0);
		gen.shutdown();
		Assert.assertEquals(queue.size(), 3793);
	}	
	@Test
	public void testCollect() throws Exception {
		XMLParser xml = new XMLParser("/business/spout.xml");
		XMLParser commonXml = new XMLParser("/business/common.xml");
		
		NodeList nodes = xml.getNodeList(null, "/SpoutConfig/LogGenerator");
	
		Node node = nodes.item(2);		
	
		StarConfig config = new StarConfig("test");
		config.parseCommon(commonXml);
		config.parse(xml, node);
		config.addAttribute("input_path", "/local_collect_info/collect_info.data");
		config.addAttribute("tt_tag", "collect_info");
		config.addAttribute("log_builder", "com.etao.lz.star.spout.OceanbaseLogBuilder");
		config.addAttribute("log_class", "business");
		
		FileLogGenerator gen = new FileLogGenerator();
		gen.config(config);
		BlockingQueue<byte[]> queue = new LinkedBlockingQueue<byte[]>();
		gen.open(queue, "test", 0);
		gen.shutdown();
		Assert.assertEquals(queue.size(), 34432);
	}	

}
