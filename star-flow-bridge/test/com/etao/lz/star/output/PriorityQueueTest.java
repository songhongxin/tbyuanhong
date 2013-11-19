package com.etao.lz.star.output;

import java.util.PriorityQueue;

import junit.framework.Assert;

import org.junit.Test;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.StarLogProtos.BusinessStarLog;
import com.etao.lz.star.StarLogProtos.FlowStarLog;
import com.etao.lz.star.StarLogProtos.FlowStarLog.Builder;

public class PriorityQueueTest {

	@Test
	public void testFlow() {
		PriorityQueue<PriorityNode> q = new PriorityQueue<PriorityNode>();
		
		Builder builder = FlowStarLog.newBuilder();
		builder.setTs(3);
		FlowStarLog log = builder.build();
		StarConfig config= new StarConfig("test");
		config.addAttribute("LogType", "flow");
		q.add(new PriorityNode(log, log.toByteArray(), config));
		
		builder.setTs(1);
		log = builder.build();
		q.add(new PriorityNode(log, log.toByteArray(), config));
		
		builder.setTs(5);
		log = builder.build();
		q.add(new PriorityNode(log, log.toByteArray(), config));
		
		PriorityNode node = q.poll();
		Assert.assertEquals(config.getLogTime(node.getLog()), 1);
		
		node = q.poll();
		Assert.assertEquals(config.getLogTime(node.getLog()), 3);
		
		node = q.poll();
		Assert.assertEquals(config.getLogTime(node.getLog()), 5);
	}
	@Test
	public void testBusiness() {
		PriorityQueue<PriorityNode> q = new PriorityQueue<PriorityNode>();
		
		BusinessStarLog.Builder builder = BusinessStarLog.newBuilder();
		builder.setOrderModifiedT(3);
		BusinessStarLog log = builder.build();
		StarConfig config= new StarConfig("test");
		config.addAttribute("LogType", "business");
		
		q.add(new PriorityNode(log, log.toByteArray(),config));
		
		builder.setOrderModifiedT(1);
		log = builder.build();
		q.add(new PriorityNode(log, log.toByteArray(),config));
		
		builder.setOrderModifiedT(5);
		log = builder.build();
		q.add(new PriorityNode(log, log.toByteArray(),config));
		
		PriorityNode node = q.poll();
		Assert.assertEquals(config.getLogTime(node.getLog()), 1);
		
		node = q.poll();
		Assert.assertEquals(config.getLogTime(node.getLog()), 3);
		
		node = q.poll();
		Assert.assertEquals(config.getLogTime(node.getLog()), 5);
	}
}
