package com.etao.lz.star.output;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.StarLogProtos;
import com.etao.lz.star.StarLogProtos.FlowStarLog;
import com.etao.lz.star.StarLogProtos.FlowStarLog.Builder;
import com.etao.lz.star.StarLogSubscriber;
import com.etao.lz.star.Tools;
import com.etao.lz.star.ZKTools;
import com.etao.lz.star.output.broadcast.SubscriberInfo;
import com.google.protobuf.GeneratedMessage;
import com.netflix.curator.framework.CuratorFramework;

public class BroadcastLogOutputTest {

	private static ZooKeeperServer zooKeeperCluster;
	private CuratorFramework client;
	private final String zkCluster = "127.0.0.1:2888";



	@BeforeClass
	public static void startZK() throws Exception {
		File file = new File("test_zk");
		if (file.exists()) {
			Tools.deleteDir(file);
		}
		zooKeeperCluster = new ZooKeeperServer(file, file, 2000);
		NIOServerCnxn.Factory factory = new NIOServerCnxn.Factory(
				new InetSocketAddress(2888));
		factory.startup(zooKeeperCluster);
		CuratorFramework client = ZKTools.startCuratorClient("127.0.0.1:2888");
		client.create().forPath("/test-topo");
		client.create().forPath(SubscriberInfo.getZKParent("test-topo"));
		client.close();
	}

	@AfterClass
	public static void stopZK() {
		zooKeeperCluster.shutdown();
	}

	@Before
	public void setUp() throws Exception {

		client = ZKTools.startCuratorClient(zkCluster);

	}

	@After
	public void tearDown() {
		try {
			client.delete().forPath(
					SubscriberInfo.getZKPath("test-topo", "test"));
		} catch (Exception e) {
		}
	}

	@Test
	public void test() throws Exception {

		SubscriberInfo item = new SubscriberInfo("test-topo", "test");
		item.setCluster("127.0.0.1:5678");
		item.setMaxQueueSize(0);
		item.setEnable(true);
		client.create().forPath(SubscriberInfo.getZKPath("test-topo", "test"),
				item.build().getBytes("UTF-8"));

		StarLogSubscriber subscriber = new StarLogSubscriber("127.0.0.1:5678");

		BroadcastLogOutput output = new BroadcastLogOutput();
		StarConfig conf = new StarConfig("test-topo");
		conf.addAttribute("refresh_interval", "1000");
		conf.addAttribute("LogType", "flow");
		
		conf.addAttribute(StarConfig.StormZookeeperCluster, zkCluster);
		output.config(conf);
		output.open(1);
		Builder builder = StarLogProtos.FlowStarLog.newBuilder();
		builder.setAdid("123");
		builder.setAgent("aaa");
		builder.setTs(1234);
		GeneratedMessage log = builder.build();
		Map<String, String> map = new HashMap<String,String>();
		
		while (!map.containsKey("test")) {
			Thread.sleep(1000);
			output.getStat(map);
		}
		output.output(log.toByteArray(), log);
		FlowStarLog log2 = subscriber.receiveFlowLog(false);
		Assert.assertEquals(log2.getAdid(), "123");
		Assert.assertEquals(log2.getAgent(), "aaa");
		Assert.assertEquals(log2.getTs(), 1234);
		Thread.sleep(2000);
		map.clear();
		output.getStat(map);
		
		SubscriberInfo stat = new SubscriberInfo("test-topo", "test");
		stat.parse(map.get("test"));
		Assert.assertEquals(stat.getSendCount(), 1);
		Assert.assertEquals(stat.getQueueLen(), 0);
		Assert.assertEquals(stat.getIgnoreCount(), 0);
		Assert.assertNotNull(stat);
		subscriber.stop();
		output.termiante();
		client.delete().forPath(SubscriberInfo.getZKPath("test-topo", "test"));
		while (map.containsKey("test")) {
			Thread.sleep(1000);
			map.clear();
			output.getStat(map);
		}

	}

	@Test
	public void testFilter() throws Exception {

		SubscriberInfo item = new SubscriberInfo("test-topo", "test");
		item.setFilter("$log.adid == 456");
		item.setCluster("127.0.0.1:5678");
		item.setMaxQueueSize(0);
		item.setEnable(true);
		client.create().forPath(SubscriberInfo.getZKPath("test-topo", "test"),
				item.build().getBytes("UTF-8"));

		StarLogSubscriber subscriber = new StarLogSubscriber("127.0.0.1:5678");

		BroadcastLogOutput output = new BroadcastLogOutput();
		StarConfig conf = new StarConfig("test-topo");
		conf.addAttribute(StarConfig.StormZookeeperCluster, zkCluster);
		conf.addAttribute("LogType", "flow");
		output.config(conf);
		output.open(1);

		Map<String, String> map = new HashMap<String,String>();
		
		while (!map.containsKey("test")) {
			Thread.sleep(1000);
			output.getStat(map);
		}

		Builder builder = StarLogProtos.FlowStarLog.newBuilder();
		builder.setAdid("123");
		builder.setAgent("aaa");
		builder.setTs(1234);
		GeneratedMessage log = builder.build();

		output.output(log.toByteArray(), log);

		builder = StarLogProtos.FlowStarLog.newBuilder();
		builder.setAdid("456");
		builder.setAgent("bbb");
		builder.setTs(5678);
		log = builder.build();
		output.output(log.toByteArray(), log);

		FlowStarLog log2 = subscriber.receiveFlowLog(false);
		Assert.assertEquals(log2.getAdid(), "456");
		Assert.assertEquals(log2.getAgent(), "bbb");
		Assert.assertEquals(log2.getTs(), 5678);
		subscriber.stop();
		output.termiante();
	}

	@Test
	public void testMoreConnection() throws Exception {
		SubscriberInfo item = new SubscriberInfo("test-topo", "test");
		item.setCluster("127.0.0.1:5600,127.0.0.1:5601");
		item.setMaxQueueSize(0);
		item.setEnable(true);
		client.create().forPath(SubscriberInfo.getZKPath("test-topo", "test"),
				item.build().getBytes("UTF-8"));

		StarLogSubscriber subscriber1 = new StarLogSubscriber("127.0.0.1:5600");

		StarLogSubscriber subscriber2 = new StarLogSubscriber("127.0.0.1:5601");

		BroadcastLogOutput output = new BroadcastLogOutput();
		StarConfig conf = new StarConfig("test-topo");
		conf.addAttribute(StarConfig.StormZookeeperCluster, zkCluster);
		conf.addAttribute("LogType", "flow");
		output.config(conf);
		output.open(1);
		Map<String, String> map = new HashMap<String,String>();
		while (!map.containsKey("test")) {
			Thread.sleep(1000);
			map.clear();
			output.getStat(map);
		}

		Builder builder = StarLogProtos.FlowStarLog.newBuilder();
		builder.setAdid("123");
		builder.setAgent("aaa");
		builder.setTs(1234);
		GeneratedMessage log = builder.build();

		output.output(log.toByteArray(), log);

		builder.setAdid("456");
		builder.setAgent("bbb");
		builder.setTs(5678);
		log = builder.build();

		output.output(log.toByteArray(), log);

		FlowStarLog log2 = subscriber1.receiveFlowLog(false);
		Assert.assertEquals(log2.getAdid(), "123");
		Assert.assertEquals(log2.getAgent(), "aaa");
		Assert.assertEquals(log2.getTs(), 1234);

		log2 = subscriber2.receiveFlowLog(false);
		Assert.assertEquals(log2.getAdid(), "456");
		Assert.assertEquals(log2.getAgent(), "bbb");
		Assert.assertEquals(log2.getTs(), 5678);
		subscriber1.stop();
		output.termiante();
	}
}
