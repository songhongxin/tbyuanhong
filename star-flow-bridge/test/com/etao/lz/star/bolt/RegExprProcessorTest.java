package com.etao.lz.star.bolt;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

import org.junit.Assert;
import org.junit.Test;
import org.w3c.dom.NodeList;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.StarLogProtos;
import com.etao.lz.star.StarLogProtos.FlowStarLog.Builder;
import com.etao.lz.star.XMLParser;

public class RegExprProcessorTest {

	@Test
	public void testAuctionId() throws Exception {
		RegExprProcessor processor = new RegExprProcessor();
		StarConfig config = new StarConfig("test");
		config.addAttribute("LogType", "flow");
		XMLParser xml = new XMLParser("/flow/filling.xml");
		NodeList nodeList = xml.getNodeList(null, "/BoltConfig/LogProcessor[@class='com.etao.lz.star.bolt.RegExprProcessor']");
		config.parse(xml, nodeList.item(0));
		config.addAttribute("output_field", "auctionid");
		processor.config(config);
		processor.open(0);
		Builder builder = StarLogProtos.FlowStarLog.newBuilder();
		builder.setUrl("http://detail.tmall.com/item.htm?id=13650957560");
		processor.process(builder);
		Assert.assertEquals(builder.getAuctionid(), "13650957560");
		
		builder.clear();
		builder.setUrl("http://item.taobao.com/item.htm?id=13928586222&ali_trackid=2:mm_28757764_0_0,124252994fce186950c84:1338906734_4z7_846353361");
		processor.process(builder);
		Assert.assertEquals("13928586222",builder.getAuctionid());
		
		builder.clear();
		builder.setUrl("http://item.taobao.com/item.htm?scm=1007.77.0.0&id=17679500357&ad_id=&am_id=&cm_id=&pm_id=");
		processor.process(builder);
		Assert.assertEquals("17679500357",builder.getAuctionid());
		
		builder.clear();
		builder.setUrl("http://wt.taobao.com/detail.htm?spm=3.305943.277688.44&id=14166778696");
		processor.process(builder);
		Assert.assertEquals("14166778696",builder.getAuctionid());
		
		ObjectOutputStream out = new ObjectOutputStream(new ByteArrayOutputStream());
		out.writeObject(processor);
	}
}
