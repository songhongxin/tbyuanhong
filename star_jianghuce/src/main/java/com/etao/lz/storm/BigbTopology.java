package com.etao.lz.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * 江湖策 实时成交计算 Storm 任务总入口
 * 
 * 执行方式：
 * <code>star-flow-bigb jar <packaged-code.jar> com.etao.lz.storm.BigbTopology star_jianghuce</code>
 * 
 * 停止方式： <code>storm kill star_jianghuce</code>
 * 
 * @author yuanhong.shx
 * 
 */
public class BigbTopology {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		PropConfig pc = new PropConfig("conf.properties");
		
		int topoWorkers = Integer.valueOf(pc.getProperty("topo_workers"));

		int businessSpoutTasks = Integer.valueOf(pc
				.getProperty("bspout_num"));
		builder.setSpout(MbConstants.BUSINESS_SPOUT_ID, new BusinessSpout(),
				businessSpoutTasks);
		
		int trafficSpoutTasks = Integer.valueOf(pc
				.getProperty("tspout_num"));
		builder.setSpout(MbConstants.TRAFFIC_SPOUT_ID, new TrafficSpout(),
				trafficSpoutTasks);

		int detailBoltTasks = Integer.valueOf(pc
				.getProperty("detail_num"));
		builder.setBolt(MbConstants.DETAIL_BOLT_ID, new DetailBolt(),
				detailBoltTasks)
				.fieldsGrouping(MbConstants.BUSINESS_SPOUT_ID,
						MbConstants.BIZ_STREAMID, new Fields("seller_id"))
				.fieldsGrouping(MbConstants.TRAFFIC_SPOUT_ID,
						MbConstants.TRAFFIC_STREAMID, new Fields("seller_id"));
/*
		int reduceBoltTasks = Integer.valueOf(pc
				.getProperty("reduce_num"));
		builder.setBolt(Constants.REDUCE_BOLT_ID, new ReduceBolt(),
				reduceBoltTasks).fieldsGrouping(Constants.DETAIL_BOLT_ID,
				new Fields("seller_id"));
*/
		Config conf = new Config();

		conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS,
		// "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:/home/admin/storm/logs/%ID%-gc.log "
		// +
				"-Xms3000M -Xmx3000M");
		
        //吸星大法端口
		String  port =  pc.getProperty("subscriber_port");
		conf.put("port", port);
		conf.put("zkServers", pc.getProperty("zk_servers"));
		conf.put("topologyName",pc.getProperty("topology_name"));
		conf.put("monitorInterval", Integer.valueOf(pc.getProperty("monitor_interval")));
		
		
		
		
		if (args != null && args.length > 0) {
			// 实时计算不需要可靠消息，故关闭 acker 节省通信资源
			conf.setNumAckers(0);
			// 设置独立 java 进程数，一般设为同 spout 和 bolt 的总 tasks 数量相等或更多，使每个 task
			// 都运行在独立的 java 进程中，以避免多 task 集中在一个 jvm 里运行产生 GC 瓶颈
			conf.setNumWorkers(topoWorkers);
			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} else {
			// 本地测试
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(10000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}

}
