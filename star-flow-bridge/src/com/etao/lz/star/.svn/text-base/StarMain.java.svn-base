package com.etao.lz.star;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

import com.etao.lz.star.bolt.LogProcessor;
import com.etao.lz.star.bolt.StarBolt;
import com.etao.lz.star.output.LogOutput;
import com.etao.lz.star.output.OutputBolt;
import com.etao.lz.star.output.broadcast.SubscriberInfo;
import com.etao.lz.star.spout.LogGenerator;
import com.etao.lz.star.spout.StarSpout;
import com.netflix.curator.framework.CuratorFramework;

public class StarMain {

	private XMLParser spoutInput = null;
	private XMLParser fillingInput = null;
	private XMLParser writerInput = null;
	private String storm_name;

	private Map<String, BaseRichBolt> boltMap;
	private Map<String, BaseRichSpout> spoutMap;
	private String log_type;
	private StarConfig commonConf;
	private boolean local = false;

	public StarMain(String base, String storm_name)
			throws Exception {
		this.storm_name = storm_name;
		commonConf = new StarConfig(storm_name);
		boltMap = new HashMap<String, BaseRichBolt>();
		spoutMap = new HashMap<String, BaseRichSpout>();
		
		XMLParser commonInput = new XMLParser(String.format("/%s/common.xml", base));
		commonConf.parseCommon(commonInput);
		log_type = commonConf.get("LogType");
		
		local = commonConf.get("RunMode", "normal").equals("local");

		spoutInput = new XMLParser(String.format("/%s/spout.xml", base));

		fillingInput = new XMLParser(String.format("/%s/filling.xml", base));
		writerInput = new XMLParser(String.format("/%s/writer.xml", base));

		if (spoutInput == null) {
			throw new IllegalStateException("missing spout config file!");
		}
		
	}

	public BaseRichBolt getBolt(String name) {
		return boltMap.get(name);
	}

	public BaseRichSpout getSpout(String name) {
		return spoutMap.get(name);
	}
	
	public void createZKDir()
	{
		CuratorFramework client = ZKTools.startCuratorClient(commonConf.get(StarConfig.StormZookeeperCluster));
		try {
			if (client.checkExists().forPath(storm_name) == null) {
				LOG.info("create " + storm_name);
				client.create().forPath(storm_name);
			}
		} catch (Exception e) {
			LOG.error(Tools.formatError("BroadcastLogOutput.open", e));
		}
		String parent = SubscriberInfo.getZKParent(storm_name);
		try {
			if (client.checkExists().forPath(parent) == null) {
				LOG.info("create " + parent);
				client.create().forPath(parent);
			}
		} catch (Exception e) {
			LOG.error(Tools.formatError("BroadcastLogOutput.open", e));
		}
		client.close();
	}
	
	private BaseRichBolt createOutputBolt(XMLParser xml, String name)
			throws Exception {
		NodeList nodes = xml.getNodeList(null, "/BoltConfig/LogOutput");
		OutputBolt bolt = new OutputBolt(commonConf);
		for (int i = 0; i < nodes.getLength(); ++i) {
			Node node = nodes.item(i);
			LogOutput processor = (LogOutput) xml.getClassInstance(node,
					"@class");
			LOG.info(String.format("Bolt %s %s", name, processor.getClass()
					.getName()));

			StarConfig config = new StarConfig(storm_name, commonConf);
			config.addAttribute("component_name", name);
			config.parse(xml, node);
			
			processor.config(config);
			bolt.addOutputProcessor(processor);
		}
		boltMap.put(name, bolt);
		return bolt;
	}

	private BaseRichBolt createBolt(XMLParser xml, String name) throws Exception {
		NodeList nodes = xml.getNodeList(null, "/BoltConfig/LogProcessor");

		StarBolt bolt = new StarBolt(log_type, commonConf);
		for (int i = 0; i < nodes.getLength(); ++i) {
			Node node = nodes.item(i);
			LogProcessor processor = (LogProcessor) xml.getClassInstance(node,
					"@class");
			LOG.info(String.format("Bolt %s %s", name, processor.getClass()
					.getName()));

			StarConfig config = new StarConfig(storm_name, commonConf);
			config.addAttribute("component_name", name);
			config.parse(xml, node);
			processor.config(config);
			bolt.addProcessor(processor);
		}
		boltMap.put(name, bolt);
		return bolt;
	}

	public void config(TopologyBuilder stormBuilder) throws Exception {

		BoltDeclarer fillingBolt = stormBuilder.setBolt(
				"filling",
				createBolt(fillingInput, "filling"),
				commonConf.getInt("filling_number"));
		BoltDeclarer finalBolt = stormBuilder.setBolt("writer",
				createOutputBolt(writerInput, "writer"),
				commonConf.getInt("writer_number"));

		finalBolt.shuffleGrouping("filling");

		NodeList nodes = spoutInput.getNodeList(null,
				"/SpoutConfig/LogGenerator");

		for (int i = 0; i < nodes.getLength(); i++) {
			Node node = nodes.item(i);
			StarSpout spout = null;
			LogGenerator generator = (LogGenerator) spoutInput
					.getClassInstance(node, "@class");
			LOG.info("Spout generator " + generator.getClass().getName());
			String name = spoutInput.getString(node, "@name");

			StarConfig config = new StarConfig(storm_name, commonConf);
			config.addAttribute("component_name", name);
			config.parse(spoutInput, node);

			generator.config(config);
			
			spout = new StarSpout(generator, name, config);
			spoutMap.put(name, spout);
			stormBuilder.setSpout(name, spout,config.getInt("spout_number"));
	//		fillingBolt.shuffleGrouping(name);//需要修改成按shopid 分组的-add by yuanhong.shx
			fillingBolt.fieldsGrouping(name, new Fields("shopid"));
		}
	}

	public void run(int waitTime) throws Exception {

		TopologyBuilder stormBuilder = new TopologyBuilder();
		config(stormBuilder);
		Config conf = new Config();
		// conf.setDebug(true);
		conf.setNumAckers(commonConf.getInt("number_ack", 0));
		conf.setNumWorkers(commonConf.getInt("worker_slots"));
		conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, " -Xmx5000m");
		if (!local) {
			StormSubmitter.submitTopology(storm_name, conf,
					stormBuilder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(storm_name, conf,
					stormBuilder.createTopology());
			Thread.sleep(waitTime * 1000);
			cluster.shutdown();
			System.exit(0);
		}
	}
	
	private static Log LOG = LogFactory.getLog(StarBolt.class);

	public static void main(String[] args) throws Exception {
		if(args.length < 2)
		{
			System.out.println("StarMain dir-name topology-name");
			return;
		}
		StarMain main = new StarMain(args[1], args[0]);
		main.createZKDir();
		LOG.info("Star Storm Started");
		main.run(args.length == 2 ? 10 : Integer.parseInt(args[2]));
	}

}
