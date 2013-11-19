package com.etao.lz.star.bolt;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

import junit.framework.Assert;

import org.junit.Test;
import org.w3c.dom.Node;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.StarLogProtos;
import com.etao.lz.star.StarLogProtos.FlowStarLog.Builder;
import com.etao.lz.star.XMLParser;

public class AtPanelFillProcessorTest {
	@Test
	public void testSid() throws Exception {
		AtPanelFillProcessor processor = new AtPanelFillProcessor();
		StarConfig config = new StarConfig("test");
		XMLParser xml = new XMLParser("/flow/filling.xml");
		Node node = xml.getNode(null, "/BoltConfig/LogProcessor[@class='com.etao.lz.star.bolt.AtPanelFillProcessor']");
		config.parse(xml, node);
		config.addAttribute("processor_output", "sid");
		processor.config(config);
		processor.open(0);
		Builder builder = StarLogProtos.FlowStarLog.newBuilder();
		builder.setSid("aaaa");
		processor.process(builder);
		Assert.assertEquals("aaaa",builder.getSid());
		
		builder.clear();
		builder.setSid("cccc_bbbb");
		processor.process(builder);
		Assert.assertEquals("cccc",builder.getSid());
		
		ObjectOutputStream out = new ObjectOutputStream(new ByteArrayOutputStream());
		out.writeObject(processor);
	}
	
	@Test
	public void testUidMid() throws Exception {
		AtPanelFillProcessor processor = new AtPanelFillProcessor();
		StarConfig config = new StarConfig("test");
		XMLParser xml = new XMLParser("/flow/filling.xml");
		Node node = xml.getNode(null, "/BoltConfig/LogProcessor[@class='com.etao.lz.star.bolt.AtPanelFillProcessor']");
		config.parse(xml, node);
		config.addAttribute("processor_output", "uid_mid");
		processor.config(config);
		processor.open(0);
		Builder builder = StarLogProtos.FlowStarLog.newBuilder();
		builder.setUid("aaa");
		builder.setMid("bbb");
		processor.process(builder);
		Assert.assertEquals("aaa",builder.getUidMid());
		
		builder.clear();
		builder.setMid("ccc");
		processor.process(builder);
		Assert.assertEquals("ccc",builder.getUidMid());
		
		builder.clear();
		builder.setUid("-");
		builder.setMid("ccc");
		processor.process(builder);
		Assert.assertEquals("ccc",builder.getUidMid());
	}
	
	@Test
	public void testPuid() throws Exception {
		AtPanelFillProcessor processor = new AtPanelFillProcessor();
		StarConfig config = new StarConfig("test");
		
		XMLParser xml = new XMLParser("/flow/filling.xml");
		
		Node node = xml.getNode(null, "/BoltConfig/LogProcessor[@class='com.etao.lz.star.bolt.AtPanelFillProcessor']");
		config.parse(xml, node);
		
		config.addAttribute("processor_output", "puid");
		processor.config(config);
		processor.open(0);
		Builder builder = StarLogProtos.FlowStarLog.newBuilder();
		builder.setUid("123");
		processor.process(builder);
		Assert.assertEquals("123:",builder.getPuid());
		
		builder.clear();
		builder.setSid("123");
		processor.process(builder);
		Assert.assertEquals(":123",builder.getPuid());
		
		ObjectOutputStream out = new ObjectOutputStream(new ByteArrayOutputStream());
		out.writeObject(processor);
	}
}
