package com.etao.lz.star.output;

import org.easymock.EasyMock;
import org.junit.Test;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.StarLogProtos;
import com.etao.lz.star.StarLogProtos.FlowStarLog;
import com.etao.lz.star.StarLogProtos.FlowStarLog.Builder;
import com.google.protobuf.GeneratedMessage;

public class OutputBoltTest {
	private final String zkCluster = "127.0.0.1:2888";
	@Test
	public void test() {
		StarConfig config = new StarConfig("test");
		config.addAttribute("LogType", "flow");
		config.addAttribute("stat_interval", "1000");
		config.addAttribute(StarConfig.StormZookeeperCluster, zkCluster);
		OutputBolt bolt = new OutputBolt(config);
		LogOutput processor = EasyMock.createMock(LogOutput.class);
		bolt.addOutputProcessor(processor);
		
		OutputCollector collector = EasyMock.createMock(OutputCollector.class);
		TopologyContext context = EasyMock.createMock(TopologyContext.class);
		Tuple  tuple = EasyMock.createMock(Tuple.class);
		
		EasyMock.expect(context.getThisTaskId()).andReturn(123);
			
		collector.ack(tuple);
		
		Builder builder = StarLogProtos.FlowStarLog.newBuilder();
		FlowStarLog log = builder.build();
		EasyMock.expect(tuple.getValueByField("log")).andReturn(log.toByteArray());
		EasyMock.expect(context.getThisTaskId()).andReturn(1);
		processor.open(123);
		processor.output(EasyMock.eq(log.toByteArray()), (GeneratedMessage)EasyMock.anyObject());
		
		EasyMock.replay(context);
		EasyMock.replay(tuple);
		EasyMock.replay(collector);
		
		bolt.prepare(null, context, collector);
		bolt.execute(tuple);
		
		EasyMock.verify(context);
		EasyMock.verify(tuple);
		EasyMock.verify(collector);
	}

}
